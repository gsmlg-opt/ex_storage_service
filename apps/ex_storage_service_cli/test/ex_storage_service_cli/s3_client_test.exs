defmodule ExStorageServiceCli.S3ClientTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.S3Client
  alias ExStorageServiceCli.SigV4

  test "signed uploads preserve binary payloads, encoded keys, and ETags" do
    body = <<0, 255, 1, 128>>
    client = serve(200, [{"ETag", "\"uploaded\""}], "")

    assert {:ok, %{etag: "uploaded"}} =
             S3Client.put_object(client, "bucket", "folder/a b.bin", body)

    assert_receive {:request, "PUT", "/bucket/folder/a%20b.bin", headers, ^body}
    assert headers["content-type"] == "application/octet-stream"
    assert headers["host"] == URI.parse(client.endpoint).authority

    assert headers["x-amz-content-sha256"] ==
             Base.encode16(:crypto.hash(:sha256, body), case: :lower)

    amz_date = headers["x-amz-date"]

    <<year::binary-size(4), month::binary-size(2), day::binary-size(2), "T", hour::binary-size(2),
      minute::binary-size(2), second::binary-size(2), "Z">> = amz_date

    assert {:ok, now, 0} =
             DateTime.from_iso8601("#{year}-#{month}-#{day}T#{hour}:#{minute}:#{second}Z")

    expected =
      SigV4.sign_headers(
        "PUT",
        client.endpoint <> "/bucket/folder/a%20b.bin",
        [{"content-type", "application/octet-stream"}],
        body,
        access_key_id: client.access_key_id,
        secret_access_key: client.secret_access_key,
        region: client.region,
        now: now
      )

    assert headers["authorization"] == Map.new(expected)["authorization"]
  end

  test "downloads consume chunked responses without decoding binary content" do
    client =
      serve_raw(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n" <>
          "Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n" <>
          "2\r\n" <> <<0, 255>> <> "\r\n2\r\n" <> <<128, 1>> <> "\r\n0\r\n\r\n"
      )

    assert {:ok, %{body: <<0, 255, 128, 1>>, content_type: "application/octet-stream"}} =
             S3Client.get_object(client, "bucket", "object.bin")
  end

  test "downloads preserve stored gzip and deflate bytes" do
    for {encoding, compress} <- [{"gzip", &:zlib.gzip/1}, {"deflate", &:zlib.compress/1}] do
      stored = compress.(<<0, 255, 128, 1>>)

      client =
        serve(
          200,
          [
            {"Content-Type", "application/octet-stream"},
            {"Content-Encoding", encoding}
          ],
          stored
        )

      assert {:ok, %{body: ^stored, content_type: "application/octet-stream"}} =
               S3Client.get_object(client, "bucket", "compressed.bin")
    end
  end

  test "HEAD preserves metadata without reading the advertised body length" do
    client =
      serve(
        200,
        [
          {"Content-Type", "text/plain"},
          {"Content-Length", "4096"},
          {"ETag", "\"metadata\""},
          {"Last-Modified", "Wed, 07 Oct 2026 00:00:00 GMT"}
        ],
        ""
      )

    assert {:ok,
            %{
              content_type: "text/plain",
              content_length: "4096",
              etag: "metadata",
              last_modified: "Wed, 07 Oct 2026 00:00:00 GMT"
            }} = S3Client.head_object(client, "bucket", "object.txt")

    assert_receive {:request, "HEAD", "/bucket/object.txt", _headers, ""}
  end

  test "listing objects preserves query parameters and XML parsing" do
    xml = """
    <ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated>
    <CommonPrefixes><Prefix>folder/</Prefix></CommonPrefixes></ListBucketResult>
    """

    client = serve(200, [{"Content-Type", "application/xml"}], xml)

    assert {:ok, %{key_count: 0, is_truncated: false, common_prefixes: ["folder/"]}} =
             S3Client.list_objects(client, "bucket",
               prefix: "folder/",
               delimiter: "/",
               max_keys: 10,
               continuation_token: "a+b"
             )

    assert_receive {:request, "GET", target, _headers, ""}
    assert URI.parse(target).path == "/bucket"

    assert URI.decode_query(URI.parse(target).query) == %{
             "list-type" => "2",
             "prefix" => "folder/",
             "delimiter" => "/",
             "max-keys" => "10",
             "continuation-token" => "a+b"
           }
  end

  test "health uses an unsigned request and returns decoded JSON" do
    client = serve(200, [{"Content-Type", "application/json"}], "{\"status\":\"ok\"}")

    assert {:ok, %{"status" => "ok"}} = S3Client.health(client)
    assert_receive {:request, "GET", "/health", headers, ""}
    refute Map.has_key?(headers, "authorization")
  end

  test "health continues to decode compressed JSON responses" do
    client =
      serve(
        200,
        [{"Content-Type", "application/json"}, {"Content-Encoding", "gzip"}],
        :zlib.gzip("{\"status\":\"ok\"}")
      )

    assert {:ok, %{"status" => "ok"}} = S3Client.health(client)
  end

  test "S3 errors retain their code, message, and status" do
    client =
      serve(403, [], "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>")

    assert {:error, "AccessDenied: Denied (HTTP 403)"} = S3Client.create_bucket(client, "bucket")
  end

  test "missing objects retain the not-found error" do
    client = serve(404, [], "")
    assert {:error, :not_found} = S3Client.get_object(client, "bucket", "missing")
  end

  test "signed requests return redirects instead of following them" do
    client = serve(307, [{"Location", "http://127.0.0.1:1/redirected"}], "")
    assert {:error, "HTTP 307"} = S3Client.create_bucket(client, "bucket")
  end

  test "transport failures return an error tuple" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    :ok = :gen_tcp.close(listener)

    client = S3Client.new(%{endpoint: "http://127.0.0.1:#{port}"})
    assert {:error, _reason} = S3Client.health(client)
  end

  test "interrupted chunked downloads return an error tuple" do
    client =
      serve_raw(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n" <>
          "Connection: close\r\n\r\n8\r\npartial"
      )

    assert {:error, _reason} = S3Client.get_object(client, "bucket", "interrupted")
  end

  defp serve(status, headers, body) do
    headers =
      if Enum.any?(headers, fn {name, _} -> String.downcase(name) == "content-length" end),
        do: headers,
        else: [{"Content-Length", to_string(byte_size(body))} | headers]

    header_lines = Enum.map_join(headers, "", fn {name, value} -> "#{name}: #{value}\r\n" end)
    serve_raw("HTTP/1.1 #{status} Response\r\n#{header_lines}Connection: close\r\n\r\n#{body}")
  end

  defp serve_raw(response) do
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

        length = String.to_integer(headers["content-length"] || "0")
        body = read_body(socket, body, length)
        send(owner, {:request, method, target, headers, body})
        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
      end)

    on_exit(fn ->
      Process.exit(server, :kill)
      :gen_tcp.close(listener)
    end)

    S3Client.new(%{
      endpoint: "http://127.0.0.1:#{port}",
      access_key_id: "test-access-key",
      secret_access_key: "test-secret-key",
      region: "us-east-1"
    })
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
