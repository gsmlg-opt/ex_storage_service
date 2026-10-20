defmodule ExStorageServiceS3.UploadIntegrityTest do
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn
  alias ExStorageServiceS3.Handlers.Shared

  test "altered declared SHA256 is rejected by the staging reader" do
    conn =
      conn(:put, "/bucket/key", "altered")
      |> put_req_header(
        "x-amz-content-sha256",
        Base.encode16(:crypto.hash(:sha256, "original"), case: :lower)
      )

    {reader, state} = Shared.decoded_body_reader(conn)
    assert {:error, :payload_hash_mismatch, _state} = reader.(state)
  end

  test "invalid Content-MD5 is rejected independently of payload SHA256" do
    conn =
      conn(:put, "/bucket/key", "body")
      |> put_req_header("content-md5", Base.encode64(:crypto.hash(:md5, "other")))

    {reader, state} = Shared.decoded_body_reader(conn)
    assert {:error, :bad_digest, _state} = reader.(state)
  end

  test "aws-chunked rejects bytes after the terminal chunk" do
    assert {:error, :malformed_chunked} = Shared.decode_aws_chunked("1\r\na\r\n0\r\n\r\ntrailing")
  end

  test "all mandatory wire algorithms verify independent 123456789 vectors" do
    for {algorithm, hex} <- [
          {"crc32", "CBF43926"},
          {"crc32c", "E3069283"},
          {"crc64nvme", "AE8B14860A799888"},
          {"sha1", "F7C3BC1D808E04732ADF679965CCC34CA7AE3441"},
          {"sha256", "15E2B0D3C33891EBB0F1EF609EC419420C20E320CE94C65FBC8C3312448EB225"}
        ] do
      expected = hex |> Base.decode16!() |> Base.encode64()

      conn =
        conn(:put, "/bucket/key", "123456789")
        |> put_req_header("x-amz-checksum-" <> algorithm, expected)

      {reader, state} = Shared.decoded_body_reader(conn)
      assert {:ok, "123456789", final_state} = reader.(state)
      assert final_state.checksums[algorithm] == expected

      damaged =
        put_req_header(
          conn,
          "x-amz-checksum-" <> algorithm,
          Base.encode64(:binary.copy(<<0>>, div(byte_size(hex), 2)))
        )

      {reader, state} = Shared.decoded_body_reader(damaged)
      assert {:error, :bad_digest, _} = reader.(state)
    end
  end

  test "unknown algorithms and invalid checksum encodings fail before body delivery" do
    for {header, value, reason} <- [
          {"x-amz-checksum-unknown", "AA==", :unsupported_checksum},
          {"content-md5", "invalid", :invalid_digest},
          {"x-amz-checksum-type", "COMPOSITE", :unsupported_checksum_type}
        ] do
      conn = conn(:put, "/bucket/key", "body") |> put_req_header(header, value)
      {reader, state} = Shared.decoded_body_reader(conn)
      assert {:error, ^reason, _} = reader.(state)
    end
  end

  test "unsigned checksum trailers validate framing, lengths, data and trailer checksum" do
    valid = "9\r\n123456789\r\n0\r\nx-amz-checksum-crc32: y/Q5Jg==\r\n\r\n"

    for {body, expected} <- [
          {valid, :ok},
          {String.replace(valid, "123456789", "923456789"), :bad_digest},
          {valid <> "trailing", :malformed_chunked},
          {String.trim_trailing(valid), :malformed_chunked},
          {String.replace(valid, "9\r\n", "8\r\n", global: false), :malformed_chunked}
        ] do
      conn =
        conn(:put, "/bucket/key", body)
        |> put_req_header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        |> put_req_header("content-encoding", "aws-chunked,gzip")
        |> put_req_header("x-amz-decoded-content-length", "9")
        |> put_req_header("x-amz-trailer", "x-amz-checksum-crc32")

      {reader, state} = Shared.decoded_body_reader(conn)

      case expected do
        :ok -> assert {:ok, "123456789", _} = reader.(state)
        reason -> assert {:error, ^reason, _} = reader.(state)
      end
    end
  end

  test "plain reader enforces size and declared decoded length" do
    {reader, state} = Shared.decoded_body_reader(conn(:put, "/bucket/key", "body"), 3)
    assert {:error, :entity_too_large, _} = reader.(state)

    conn =
      conn(:put, "/bucket/key", "body") |> put_req_header("x-amz-decoded-content-length", "5")

    {reader, state} = Shared.decoded_body_reader(conn)
    assert {:error, :invalid_content_length, _} = reader.(state)
  end

  test "signed chunks validate an independently generated HMAC chain at every boundary" do
    # Python stdlib hmac/hashlib, AWS4(test-secret), date 20261018, us-east-1/s3.
    context = %{
      key: Base.decode16!("6F74E186BF32606EB5379C13CC2192F0D95639E6FB3D8FCBA8D084119DA9DA77"),
      datetime: "20261018T000000Z",
      scope: "20261018/us-east-1/s3/aws4_request",
      signature: String.duplicate("0", 64)
    }

    encoded =
      "5;chunk-signature=6b7cb23b119393a1dba3a9d62c7d7c09d1b56fca756c32ede3ba0d6baf5c5f64\r\nhello\r\n" <>
        "6;chunk-signature=533df6facbb879a22296bfda6f27ebb655ca7c7d190e95034342b0b7ff5b76de\r\n world\r\n" <>
        "0;chunk-signature=07782b3ec9c946028a42a45203046bb1877834a5821b75851d0b9c0b64d25622\r\n\r\n"

    conn =
      conn(:put, "/bucket/key", encoded)
      |> put_req_header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
      |> put_req_header("x-amz-decoded-content-length", "11")
      |> assign(:sigv4_streaming, context)

    {reader, state} = Shared.decoded_body_reader(conn)
    assert {:ok, "hello world", _} = reader.(state)

    {:ok, decoder} = ExStorageServiceS3.AwsChunked.new(conn)

    {bytes, decoder} =
      Enum.reduce(for(<<byte <- encoded>>, do: <<byte>>), {[], decoder}, fn byte,
                                                                            {acc, decoder} ->
        assert {:ok, decoded, decoder} = ExStorageServiceS3.AwsChunked.feed(byte, decoder)
        {[decoded | acc], decoder}
      end)

    assert IO.iodata_to_binary(Enum.reverse(bytes)) == "hello world"
    assert ExStorageServiceS3.AwsChunked.finished?(decoder)

    for damaged <- [
          String.replace(encoded, "hello", "jello"),
          String.replace(encoded, "07782", "17782")
        ] do
      conn = %{conn | adapter: Map.fetch!(conn(:put, "/bucket/key", damaged), :adapter)}
      {reader, state} = Shared.decoded_body_reader(conn)
      assert {:error, :chunk_signature_mismatch, _} = reader.(state)
    end
  end
end
