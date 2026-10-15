Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.StreamingBodyTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, S3.Client, S3.Encoder, S3.StreamingBody}
  alias ExStorageServiceCli.Test.WireServer

  test "unsigned trailer framing signs its marker and retains exact encoded length" do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"ok\""}], ""}])
    context = %Context{endpoint: endpoint, access_key_id: "access", secret_access_key: "secret"}

    assert {:ok, _} =
             Client.call(context, "PutObject", %{"Bucket" => "bucket", "Key" => "key"},
               body_source: ["1234", "56789"],
               content_length: 9,
               streaming: :unsigned,
               now: ~U[2025-01-01 00:00:00Z]
             )

    assert_receive {:wire_request, %{body: body, headers: headers}}
    assert body == "9\r\n123456789\r\n0\r\nx-amz-checksum-crc64nvme:rosUhgp5mIg=\r\n\r\n"
    assert headers["content-length"] == to_string(byte_size(body))
    assert headers["x-amz-decoded-content-length"] == "9"
    assert headers["x-amz-content-sha256"] == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    assert headers["x-amz-trailer"] == "x-amz-checksum-crc64nvme"
    assert headers["authorization"] =~ "x-amz-trailer"
    refute Map.has_key?(headers, "x-amz-checksum-crc64nvme")
    refute Map.has_key?(headers, "transfer-encoding")
  end

  test "signed framing derives its seed from the fresh request and signs trailers" do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"ok\""}], ""}])

    context = %Context{
      endpoint: endpoint,
      access_key_id: "access",
      secret_access_key: "secret",
      session_token: "temporary"
    }

    assert {:ok, _} =
             Client.call(
               context,
               "PutObject",
               %{
                 "Bucket" => "bucket",
                 "Key" => "key",
                 "ChecksumAlgorithm" => "SHA256",
                 "ContentEncoding" => "gzip"
               },
               body_source: ["123456789"],
               content_length: 9,
               streaming: :signed,
               now: ~U[2025-01-01 00:00:00Z]
             )

    assert_receive {:wire_request, %{body: body, headers: headers}}

    assert Regex.match?(
             ~r/^9;chunk-signature=[0-9a-f]{64}\r\n123456789\r\n0;chunk-signature=[0-9a-f]{64}\r\n/,
             body
           )

    assert body =~ "x-amz-checksum-sha256:FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU="
    assert body =~ "x-amz-trailer-signature:"
    assert headers["content-length"] == to_string(byte_size(body))
    assert headers["content-encoding"] == "gzip,aws-chunked"
    assert headers["x-amz-security-token"] == "temporary"
    assert headers["x-amz-date"] == "20250101T000000Z"
  end

  test "AWS HTTPS selects unsigned checksum trailers; custom HTTP retains raw bytes" do
    input = %{"Bucket" => "bucket", "Key" => "key", "Body" => "123456789"}
    {:ok, request} = Encoder.encode("PutObject", input, allow_streaming: true)
    context = %Context{frontend: :aws}

    assert {:ok, prepared, opts} =
             StreamingBody.prepare(request, input, "https://example.com/bucket/key", context, [])

    assert prepared.payload_hash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    assert opts[:streaming_spec][:mode] == :unsigned

    assert {:ok, ^request, []} =
             StreamingBody.prepare(request, input, "http://example.com/bucket/key", context, [])

    explicit = Map.put(input, "ChecksumCRC64NVME", "rosUhgp5mIg=")
    {:ok, request} = Encoder.encode("PutObject", explicit, allow_streaming: true)

    assert {:ok, ^request, []} =
             StreamingBody.prepare(
               request,
               explicit,
               "https://example.com/bucket/key",
               context,
               []
             )
  end
end
