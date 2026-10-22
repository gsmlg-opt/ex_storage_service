defmodule ExStorageServiceS3.StorageListenerHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    bucket = "gaoxy-storage-#{suffix}"
    {:ok, user} = User.create_user("gaoxy-storage-#{suffix}")
    {:ok, key} = AccessKey.create_access_key(user.id)

    {:ok, policy} =
      Policy.create_policy("gaoxy-storage-#{suffix}", Policy.full_access_statements())

    :ok = Policy.attach_policy(user.id, policy.id)
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      for name <- ["progressing", "finite", "idle", "large", "encoded", "h2"] do
        ObjectService.delete(bucket, name, nil)
      end

      assert :ok = Metadata.delete_bucket(bucket)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
      Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
    end)

    %{bucket: bucket, key: key}
  end

  test "progressing signed uploads outlive a finite total budget while idle bodies expire", ctx do
    body = :binary.copy("raw-object-bytes", 20)
    port = listener(request_timeout: :infinity, read_timeout: 200)
    {wire, elapsed} = paced_upload(ctx, port, "progressing", body)
    assert elapsed > 300
    assert wire =~ "HTTP/1.1 200"
    assert {:ok, %{metadata: %{size: size}}} = ObjectService.head(ctx.bucket, "progressing")
    assert size == byte_size(body)

    finite = listener(request_timeout: 150, read_timeout: 200)
    {wire, _elapsed} = paced_upload(ctx, finite, "finite", body)
    refute wire =~ "HTTP/1.1 200"
    assert {:error, :object_not_found} = ObjectService.head(ctx.bucket, "finite")

    socket = connect(port)
    send_request(socket, ctx, port, "idle", body)
    assert :ok = :gen_tcp.send(socket, binary_part(body, 0, 16))
    wire = receive_closed(socket, [])
    refute wire =~ "HTTP/1.1 200"
    assert {:error, :object_not_found} = ObjectService.head(ctx.bucket, "idle")
  end

  test "the storage listener streams bodies above Gaoxy's default 8 MiB ceiling", ctx do
    port = listener([])
    url = "http://localhost:#{port}/#{ctx.bucket}/large"
    body = :binary.copy(<<0, 255, 31, 139>>, div(9 * 1_048_576, 4))
    options = [retry: false, decode_body: false, aws_sigv4: credentials(ctx.key)]

    assert %{status: 200} = Req.put!(url, [body: body] ++ options)
    assert %{status: 200, body: ^body} = Req.get!(url, options)
    head = Req.head!(url, options)
    assert head.status == 200
    assert head.body == ""
    assert head.headers["content-length"] == [Integer.to_string(byte_size(body))]

    {size, digest, elapsed} = paced_file(ctx, port, byte_size(body))
    assert size == byte_size(body)
    assert digest == :crypto.hash(:sha256, body)
    assert elapsed > 500

    finite = listener(request_timeout: 500)
    {size, _digest, _elapsed} = paced_file(ctx, finite, byte_size(body))
    assert size < byte_size(body)
  end

  test "signed cleartext HTTP/2 preserves object and range response semantics", ctx do
    port = listener([])
    body = :binary.copy(<<0, 255, 31, 139>>, 16_384)
    size = byte_size(body)

    assert %Req.Response{status: 200} = put = signed_h2_request(ctx, port, :put, body)
    assert put.body == ""

    assert %Req.Response{status: 200} = head = signed_h2_request(ctx, port, :head)
    assert head.headers["content-length"] == [Integer.to_string(size)]
    assert head.body == ""

    assert %Req.Response{status: 200} = get = signed_h2_request(ctx, port, :get)
    assert get.headers["content-length"] == [Integer.to_string(size)]
    assert get.body == body

    assert %Req.Response{status: 206} =
             range = signed_h2_request(ctx, port, :get, "", [{"range", "bytes=7-15"}])

    assert range.headers["content-range"] == ["bytes 7-15/#{size}"]
    assert range.headers["content-length"] == ["9"]
    assert range.body == binary_part(body, 7, 9)
  end

  test "AWS wire overhead does not consume the decoded object-size allowance", ctx do
    previous = Application.fetch_env!(:ex_storage_service, :max_object_size)
    Application.put_env(:ex_storage_service, :max_object_size, 9)
    on_exit(fn -> Application.put_env(:ex_storage_service, :max_object_size, previous) end)
    port = listener([])
    url = "http://localhost:#{port}/#{ctx.bucket}/encoded"

    for {bytes, status} <- [{"123456789", 200}, {"1234567890", 413}] do
      checksum = Base.encode64(<<:erlang.crc32(bytes)::32>>)

      encoded =
        Integer.to_string(byte_size(bytes), 16) <>
          "\r\n" <>
          bytes <>
          "\r\n0\r\nx-amz-checksum-crc32: #{checksum}\r\n\r\n"

      assert byte_size(encoded) > 9

      headers =
        Req.Utils.aws_sigv4_headers(
          credentials(ctx.key) ++
            [
              datetime: DateTime.utc_now(),
              method: :put,
              url: url,
              body: encoded,
              body_digest: "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
              headers: [
                {"content-encoding", "aws-chunked"},
                {"x-amz-decoded-content-length", Integer.to_string(byte_size(bytes))},
                {"x-amz-trailer", "x-amz-checksum-crc32"}
              ]
            ]
        )

      assert %{status: ^status} = Req.put!(url, body: encoded, headers: headers, retry: false)
    end

    assert %{status: 200, body: "123456789"} =
             Req.get!(url, retry: false, decode_body: false, aws_sigv4: credentials(ctx.key))
  end

  defp signed_h2_request(ctx, port, method, body \\ "", headers \\ []) do
    url = "http://localhost:#{port}/#{ctx.bucket}/h2"

    headers =
      Req.Utils.aws_sigv4_headers(
        credentials(ctx.key) ++
          [datetime: DateTime.utc_now(), method: method, url: url, body: body, headers: headers]
      )

    # Force prior-knowledge H2 with no HTTP/1 fallback.
    Req.request!(
      url: url,
      method: method,
      headers: headers,
      body: body,
      connect_options: [protocols: [:http2]],
      retry: false,
      redirect: false,
      decode_body: false
    )
  end

  defp listener(overrides) do
    [{Gaoxy.HTTP, options}] = ExStorageServiceS3.Application.children(enabled: true, port: 0)
    limits = Map.merge(options[:http_limits], Map.new(overrides))
    options = options |> Keyword.put(:ip, {127, 0, 0, 1}) |> Keyword.put(:http_limits, limits)
    server = start_supervised!({Gaoxy.HTTP, options}, id: {Gaoxy.HTTP, make_ref()})
    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)
    port
  end

  defp credentials(key) do
    [
      access_key_id: key.access_key_id,
      secret_access_key: key.secret_access_key,
      region: "us-east-1",
      service: :s3
    ]
  end

  defp connect(port) do
    assert {:ok, socket} =
             :gen_tcp.connect(~c"localhost", port, [:binary, active: false], 2_000)

    on_exit(fn -> :gen_tcp.close(socket) end)
    socket
  end

  defp paced_upload(ctx, port, name, body) do
    socket = connect(port)
    send_request(socket, ctx, port, name, body)
    started = System.monotonic_time(:millisecond)

    for <<piece::binary-size(16) <- body>>, reduce: :ok do
      :ok ->
        Process.sleep(25)
        :gen_tcp.send(socket, piece)

      error ->
        error
    end

    wire = receive_closed(socket, [])
    {wire, System.monotonic_time(:millisecond) - started}
  end

  defp send_request(socket, ctx, port, name, body) do
    path = "/#{ctx.bucket}/#{name}"

    headers =
      Req.Utils.aws_sigv4_headers(
        credentials(ctx.key) ++
          [
            datetime: DateTime.utc_now(),
            method: :put,
            url: "http://localhost:#{port}" <> path,
            headers: [{"content-length", Integer.to_string(byte_size(body))}],
            body: body
          ]
      )

    assert :ok =
             :gen_tcp.send(socket, [
               "PUT ",
               path,
               " HTTP/1.1\r\nConnection: close\r\n",
               Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
               "\r\n"
             ])
  end

  defp paced_file(ctx, port, expected_size) do
    assert {:ok, socket} =
             :gen_tcp.connect(
               ~c"localhost",
               port,
               [:binary, active: false, packet: :http_bin, recbuf: 65_536, buffer: 65_536],
               2_000
             )

    on_exit(fn -> :gen_tcp.close(socket) end)
    path = "/#{ctx.bucket}/large"

    headers =
      Req.Utils.aws_sigv4_headers(
        credentials(ctx.key) ++
          [
            datetime: DateTime.utc_now(),
            method: :get,
            url: "http://localhost:#{port}" <> path,
            headers: [],
            body: ""
          ]
      )

    started = System.monotonic_time(:millisecond)

    assert :ok =
             :gen_tcp.send(socket, [
               "GET ",
               path,
               " HTTP/1.1\r\nConnection: close\r\n",
               Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
               "\r\n"
             ])

    assert {:ok, {:http_response, {1, 1}, 200, _}} = :gen_tcp.recv(socket, 0, 2_000)
    headers = response_headers(socket, [])
    assert {"content-length", Integer.to_string(expected_size)} in headers
    assert :ok = :inet.setopts(socket, packet: :raw)
    {size, hash} = drain_file(socket, 0, :crypto.hash_init(:sha256))
    {size, :crypto.hash_final(hash), System.monotonic_time(:millisecond) - started}
  end

  defp response_headers(socket, headers) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, {:http_header, _, name, _, value}} ->
        response_headers(socket, [{String.downcase(to_string(name)), value} | headers])

      {:ok, :http_eoh} ->
        headers

      other ->
        flunk("unexpected file response headers: #{inspect(other)}")
    end
  end

  defp drain_file(socket, size, hash) do
    Process.sleep(5)

    case :gen_tcp.recv(socket, 16_384, 2_000) do
      {:ok, bytes} ->
        drain_file(socket, size + byte_size(bytes), :crypto.hash_update(hash, bytes))

      {:error, :closed} ->
        {size, hash}

      {:error, reason} ->
        flunk("file response did not terminate: #{inspect(reason)}")
    end
  end

  defp receive_closed(socket, chunks) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, bytes} -> receive_closed(socket, [bytes | chunks])
      {:error, :closed} -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      {:error, reason} -> flunk("storage socket did not close: #{inspect(reason)}")
    end
  end
end
