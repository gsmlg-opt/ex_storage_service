defmodule ExStorageService.HTTPClientTest do
  use ExUnit.Case, async: true

  alias ExStorageService.CloudCache.{Client, Config}
  alias ExStorageService.{HTTPClient, Notifications}

  test "preserves request bytes and JSON response bytes without automatic decoding" do
    url = serve(200, [{"Content-Type", "application/json"}], "{\"ok\":true}")
    payload = <<0, 255, 128, 1>>

    assert {:ok, %{status: 200, body: "{\"ok\":true}", headers: headers}} =
             HTTPClient.request(url <> "/object", method: :put, body: payload)

    assert Enum.any?(headers, fn {name, value} ->
             String.downcase(name) == "content-type" and value == "application/json"
           end)

    assert_receive {:request, "PUT", "/object", request_headers, ^payload}
    assert request_headers["content-length"] == "4"
  end

  test "fixed-length uploads lazily split large binary and iodata producer items" do
    url =
      serve_raw("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", true)

    owner = self()
    item = :binary.copy(<<0, 255, 128, 1>>, 65_536)
    size = byte_size(item) * 3

    source =
      Stream.map(1..3, fn index ->
        send(owner, {:producer_item, index, self()})

        receive do
          {:release_item, ^index} ->
            if index == 2,
              do: [binary_part(item, 0, 131_072), [binary_part(item, 131_072, 131_072)]],
              else: item
        after
          5_000 -> raise "producer item was not released"
        end
      end)

    refute_receive {:producer_item, _index, _producer}

    task =
      Task.async(fn ->
        HTTPClient.request(url <> "/stream",
          method: :put,
          headers: [{"content-length", Integer.to_string(size)}],
          body: source
        )
      end)

    assert_receive {:producer_item, 1, producer}, 1_000
    assert_receive {:request_head, "PUT", "/stream", headers}, 1_000
    assert headers["content-length"] == Integer.to_string(size)
    refute Map.has_key?(headers, "transfer-encoding")
    refute_receive {:producer_item, 2, _producer}
    send(producer, {:release_item, 1})

    assert_receive {:producer_item, 2, ^producer}, 1_000
    refute_receive {:producer_item, 3, _producer}
    send(producer, {:release_item, 2})
    assert_receive {:producer_item, 3, ^producer}, 1_000
    send(producer, {:release_item, 3})

    assert {:ok, %{status: 200}} = Task.await(task, 5_000)
    assert_receive {:request, "PUT", "/stream", _headers, body}
    assert body == :binary.copy(item, 3)
  end

  test "consumes chunked responses and reports interrupted streams as errors" do
    url =
      serve_raw(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" <>
          "2\r\n" <> <<0, 255>> <> "\r\n2\r\n" <> <<128, 1>> <> "\r\n0\r\n\r\n"
      )

    assert {:ok, %{body: <<0, 255, 128, 1>>}} = HTTPClient.request(url)

    interrupted =
      serve_raw(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" <>
          "8\r\npartial"
      )

    assert {:error, _reason} = HTTPClient.request(interrupted)
  end

  test "buffered requests preserve gzip and deflate entity bytes and headers" do
    for {encoding, compress} <- [{"gzip", &:zlib.gzip/1}, {"deflate", &:zlib.compress/1}] do
      stored = compress.(<<0, 255, 128, 1>>)
      url = serve(200, [{"Content-Encoding", encoding}], stored)

      assert {:ok, %{body: ^stored, headers: headers}} = HTTPClient.request(url)
      headers = Map.new(headers, fn {name, value} -> {String.downcase(name), value} end)
      assert headers["content-encoding"] == encoding
      assert headers["content-length"] == Integer.to_string(byte_size(stored))
    end
  end

  test "streaming fetch preserves compressed chunks and acknowledgement backpressure" do
    for {encoding, compress} <- [{"gzip", &:zlib.gzip/1}, {"deflate", &:zlib.compress/1}] do
      stored = compress.(<<0, 255, 128, 1>>)
      <<first::binary-size(5), rest::binary>> = stored

      url =
        serve_raw(
          "HTTP/1.1 200 OK\r\nContent-Encoding: #{encoding}\r\n" <>
            "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n" <>
            "5\r\n" <>
            first <>
            "\r\n" <>
            Integer.to_string(byte_size(rest), 16) <> "\r\n" <> rest <> "\r\n0\r\n\r\n"
        )

      assert {:ok, %HTTP.Response{body: stream} = response} = HTTPClient.fetch(url)
      assert is_pid(stream)
      assert HTTP.Response.get_header(response, "content-encoding") == encoding

      send(stream, {:read_chunk, self(), :ack})
      assert_receive {:stream_chunk, ^stream, ^first, ack_ref}, 1_000
      refute_receive {:stream_chunk, ^stream, _chunk, _ack_ref}, 25
      send(stream, {:stream_chunk_ack, ack_ref})

      assert collect_stream(stream, [first]) == stored
    end
  end

  test "HEAD returns headers without reading the advertised response body" do
    url = serve(200, [{"Content-Length", "8192"}, {"ETag", "\"head\""}], "")

    assert {:ok, %{status: 200, body: "", headers: headers}} =
             HTTPClient.request(url, method: :head)

    headers = Map.new(headers, fn {name, value} -> {String.downcase(name), value} end)
    assert headers["content-length"] == "8192"
    assert headers["etag"] == "\"head\""
  end

  test "returns redirects and failed HTTP responses to the caller" do
    redirect = serve(307, [{"Location", "http://127.0.0.1:1/redirected"}], "")
    assert {:ok, %{status: 307}} = HTTPClient.request(redirect)

    unavailable = serve(503, [], "unavailable")
    assert {:ok, %{status: 503, body: "unavailable"}} = HTTPClient.request(unavailable)
  end

  test "connection failures return error tuples" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    :ok = :gen_tcp.close(listener)

    assert {:error, _reason} = HTTPClient.request("http://127.0.0.1:#{port}")
  end

  test "per-request timeout closes a connection that never returns a response" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    owner = self()

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {_head, ""} = read_head(socket, "")
        send(owner, {:timeout_connection, :gen_tcp.recv(socket, 0, 5_000)})
        :gen_tcp.close(socket)
      end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :gen_tcp.close(listener)
    end)

    assert {:error, :request_timeout} =
             HTTPClient.request("http://127.0.0.1:#{port}", timeout: 50)

    assert_receive {:timeout_connection, {:error, :closed}}, 1_000
  end

  test "cloud uploads preserve SigV4 headers, encoded keys, and binary bodies" do
    config = cloud_config(serve(200, [], ""))
    payload = <<0, 255, 128, 1>>

    assert :ok = Client.put_object(config, "folder/a b.bin", payload, "application/octet-stream")

    assert_receive {:request, "PUT", "/remote/folder/a%20b.bin", headers, ^payload}
    assert headers["host"] == URI.parse(config.endpoint).authority
    assert headers["content-type"] == "application/octet-stream"

    assert headers["x-amz-content-sha256"] ==
             Base.encode16(:crypto.hash(:sha256, payload), case: :lower)

    assert headers["authorization"] =~ "AWS4-HMAC-SHA256 Credential=test-access/"
    assert headers["authorization"] =~ "/us-east-1/s3/aws4_request"

    assert headers["authorization"] =~
             "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date"
  end

  test "cloud downloads preserve binary data and not-found errors" do
    payload = <<0, 255, 128, 1>>
    config = cloud_config(serve(200, [{"Content-Type", "application/json"}], payload))

    assert {:ok, ^payload} = Client.get_object(config, "object.bin")
    assert_receive {:request, "GET", "/remote/object.bin", headers, ""}
    assert headers["authorization"] =~ "AWS4-HMAC-SHA256"

    missing = cloud_config(serve(404, [], "missing"))
    assert {:error, :not_found} = Client.get_object(missing, "missing")
  end

  test "cloud downloads preserve stored gzip and deflate object bytes" do
    for {encoding, compress} <- [{"gzip", &:zlib.gzip/1}, {"deflate", &:zlib.compress/1}] do
      stored = compress.(<<0, 255, 128, 1>>)
      config = cloud_config(serve(200, [{"Content-Encoding", encoding}], stored))

      assert {:ok, ^stored} = Client.get_object(config, "compressed.bin")
    end
  end

  test "cloud HEAD parses tuple response headers" do
    config =
      cloud_config(
        serve(
          200,
          [
            {"Content-Length", "4096"},
            {"ETag", "\"metadata\""},
            {"Content-Type", "application/octet-stream"},
            {"Last-Modified", "Wed, 07 Oct 2026 00:00:00 GMT"}
          ],
          ""
        )
      )

    assert {:ok,
            %{
              content_length: 4096,
              etag: "metadata",
              content_type: "application/octet-stream",
              last_modified: "Wed, 07 Oct 2026 00:00:00 GMT"
            }} = Client.head_object(config, "object.bin")
  end

  test "cloud listing preserves pagination, XML, and request query parameters" do
    xml = """
    <ListBucketResult><IsTruncated>true</IsTruncated>
    <NextContinuationToken>next-page</NextContinuationToken>
    <Contents><Key>folder/a.bin</Key><Size>4</Size><ETag>"etag"</ETag>
    <LastModified>2026-10-07T00:00:00Z</LastModified></Contents>
    <CommonPrefixes><Prefix>folder/nested/</Prefix></CommonPrefixes></ListBucketResult>
    """

    config = cloud_config(serve(200, [{"Content-Type", "application/xml"}], xml))

    assert {:ok,
            %{
              keys: [{"folder/a.bin", %{size: 4, etag: "etag"}}],
              common_prefixes: ["folder/nested/"],
              truncated: true,
              next_continuation_token: "next-page"
            }} =
             Client.list_objects(config,
               prefix: "folder/",
               delimiter: "/",
               max_keys: 10,
               continuation_token: "a+b"
             )

    assert_receive {:request, "GET", target, _headers, ""}
    assert URI.parse(target).path == "/remote"

    assert URI.decode_query(URI.parse(target).query) == %{
             "list-type" => "2",
             "prefix" => "folder/",
             "delimiter" => "/",
             "max-keys" => "10",
             "continuation-token" => "a+b"
           }
  end

  test "cloud deletion remains idempotent and connectivity reports forbidden credentials" do
    assert :ok = Client.delete_object(cloud_config(serve(404, [], "")), "missing")
    assert {:error, :forbidden} = Client.test_connection(cloud_config(serve(403, [], "")))
  end

  test "notification API delivers the object event as JSON to the webhook" do
    endpoint = serve(204, [], "") <> "/webhook"
    bucket = "http-webhook-#{System.unique_integer([:positive])}"

    assert :ok =
             Notifications.put_config(bucket, [
               %{events: ["s3:ObjectCreated:*"], endpoint: endpoint, enabled: true}
             ])

    on_exit(fn -> Notifications.delete_config(bucket) end)

    assert :ok =
             Notifications.notify(bucket, "object.bin", "s3:ObjectCreated:Put", %{"size" => 4})

    assert_receive {:request, "POST", "/webhook", headers, body}, 5_000
    assert headers["content-type"] == "application/json"

    assert %{
             "Records" => [
               %{
                 "eventName" => "s3:ObjectCreated:Put",
                 "s3" => %{
                   "bucket" => %{"name" => ^bucket},
                   "object" => %{"key" => "object.bin", "size" => 4}
                 }
               }
             ]
           } = JSON.decode!(body)
  end

  defp cloud_config(endpoint) do
    %Config{
      provider: :s3_compat,
      endpoint: endpoint,
      bucket: "remote",
      access_key_id: "test-access",
      encrypted_secret: Config.encrypt_secret("test-secret")
    }
  end

  defp collect_stream(stream, chunks) do
    receive do
      {:stream_chunk, ^stream, chunk, ack_ref} ->
        send(stream, {:stream_chunk_ack, ack_ref})
        collect_stream(stream, [chunk | chunks])

      {:stream_end, ^stream} ->
        chunks |> Enum.reverse() |> IO.iodata_to_binary()

      {:stream_error, ^stream, reason} ->
        flunk("unexpected stream error: #{inspect(reason)}")
    after
      1_000 -> flunk("stream did not finish")
    end
  end

  defp serve(status, headers, body) do
    headers =
      if Enum.any?(headers, fn {name, _} -> String.downcase(name) == "content-length" end),
        do: headers,
        else: [{"Content-Length", to_string(byte_size(body))} | headers]

    head = Enum.map_join(headers, "", fn {name, value} -> "#{name}: #{value}\r\n" end)
    serve_raw("HTTP/1.1 #{status} Response\r\n#{head}Connection: close\r\n\r\n#{body}")
  end

  defp serve_raw(response, notify_head \\ false) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    owner = self()

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {head, body} = read_head(socket, "")
        [request_line | header_lines] = String.split(head, "\r\n")
        [method, target, _version] = String.split(request_line, " ")

        headers =
          Map.new(header_lines, fn line ->
            [name, value] = String.split(line, ":", parts: 2)
            {String.downcase(name), String.trim(value)}
          end)

        if notify_head, do: send(owner, {:request_head, method, target, headers})
        length = String.to_integer(headers["content-length"] || "0")
        body = read_body(socket, body, length)
        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        send(owner, {:request, method, target, headers, body})
      end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :gen_tcp.close(listener)
    end)

    "http://127.0.0.1:#{port}"
  end

  defp read_head(socket, data) do
    case :binary.split(data, "\r\n\r\n") do
      [head, body] ->
        {head, body}

      [_partial] ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
        read_head(socket, data <> chunk)
    end
  end

  defp read_body(_socket, body, length) when byte_size(body) >= length, do: body

  defp read_body(socket, body, length) do
    {:ok, chunk} = :gen_tcp.recv(socket, length - byte_size(body), 5_000)
    body <> chunk
  end
end
