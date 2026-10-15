defmodule ExStorageServiceCli.S3.StreamingSignatureTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.StreamingSignature}

  @oracle __DIR__
          |> Path.join("../../fixtures/special_s3/streaming-signatures.json")
          |> File.read!()
          |> JSON.decode!()
  defp options do
    [
      secret_access_key: @oracle["secret_access_key"],
      seed_signature: @oracle["signatures"]["request"],
      region: @oracle["region"],
      service: @oracle["service"],
      now: ~U[2013-05-24 00:00:00Z]
    ]
  end

  test "AWS published S3 chunk signatures, terminal signature and trailer signature" do
    assert {:ok, state} = StreamingSignature.new(options())
    assert {:ok, first, state} = StreamingSignature.chunk(state, String.duplicate("a", 65536))

    assert String.starts_with?(
             first,
             "10000;chunk-signature=" <> @oracle["signatures"]["first_chunk"] <> "\r\n"
           )

    assert {:ok, second, state} = StreamingSignature.chunk(state, String.duplicate("a", 1024))

    assert String.starts_with?(
             second,
             "400;chunk-signature=" <> @oracle["signatures"]["second_chunk"] <> "\r\n"
           )

    assert {:ok, final, state} = StreamingSignature.finish(state)
    assert final == "0;chunk-signature=" <> @oracle["signatures"]["final_chunk"] <> "\r\n\r\n"
    headers = Enum.map(@oracle["trailer_headers"], &List.to_tuple/1)
    assert {:ok, signature} = StreamingSignature.sign_trailers(state, Enum.reverse(headers))
    assert signature == @oracle["signatures"]["trailing_headers"]
    assert byte_size(first <> second <> final) == 66824
    assert {:ok, 66824} = StreamingSignature.encoded_length(66560)
    assert {:error, %Error{code: :streaming_finished}} = StreamingSignature.finish(state)
  end

  test "unsigned checksum trailers match the pinned botocore raw-byte envelope" do
    opts = [mode: :unsigned, checksum_algorithm: "CRC32C", chunk_size: 4, decoded_length: 9]
    assert {:ok, stream} = StreamingSignature.encode(["12", "34", "56", "78", "9"], opts)

    assert Enum.to_list(stream) == [
             "4\r\n1234\r\n",
             "4\r\n5678\r\n",
             "1\r\n9\r\n",
             "0\r\nx-amz-checksum-crc32c:4waSgw==\r\n\r\n"
           ]

    assert {:ok, length} = StreamingSignature.encoded_length(9, opts)
    assert length == byte_size(Enum.join(stream))

    assert {:ok, headers} =
             StreamingSignature.headers(9, Keyword.put(opts, :content_encoding, "gzip"))

    assert {"content-encoding", "gzip,aws-chunked"} in headers
    assert {"x-amz-decoded-content-length", "9"} in headers
    assert {"x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"} in headers
    assert {"content-length", Integer.to_string(length)} in headers
  end

  test "signed checksum trailer uses canonical LF hashing and exact framed length" do
    opts =
      Keyword.merge(options(), checksum_algorithm: "CRC32C", chunk_size: 4, decoded_length: 9)

    assert {:ok, stream} = StreamingSignature.encode(["1234", "5678", "9"], opts)
    frames = Enum.to_list(stream)
    last = List.last(frames)
    assert last =~ "\r\nx-amz-checksum-crc32c:4waSgw==\r\nx-amz-trailer-signature:"
    assert String.ends_with?(last, "\r\n\r\n")

    [signature] =
      Regex.run(~r/x-amz-trailer-signature:([a-f0-9]{64})/, last, capture: :all_but_first)

    {:ok, state} = StreamingSignature.new(opts)
    {:ok, _, state} = StreamingSignature.chunk(state, "1234")
    {:ok, _, state} = StreamingSignature.chunk(state, "5678")
    {:ok, _, state} = StreamingSignature.chunk(state, "9")
    {:ok, _, state} = StreamingSignature.finish(state)

    assert {:ok, ^signature} =
             StreamingSignature.sign_trailers(state, [{"X-Amz-Checksum-Crc32c", " 4waSgw==\t"}])

    assert {:ok, length} = StreamingSignature.encoded_length(9, opts)
    assert length == IO.iodata_length(frames)
    assert StreamingSignature.payload_hash(opts) == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
  end

  test "deterministic bounded rechunking is lazy and independent of incoming splits" do
    opts = Keyword.merge(options(), chunk_size: 3, decoded_length: 6)
    owner = self()

    source =
      Stream.map(["a", "bc", "de", "f"], fn bytes ->
        send(owner, {:read, bytes})
        bytes
      end)

    assert {:ok, stream} = StreamingSignature.encode(source, opts)
    refute_received {:read, _}
    [first] = Enum.take(stream, 1)
    assert first =~ "\r\nabc\r\n"
    assert_receive {:read, "a"}
    assert_receive {:read, "bc"}
    refute_receive {:read, "de"}
    assert {:ok, expected} = StreamingSignature.encode(["abc", "def"], opts)
    assert Enum.to_list(stream) == Enum.to_list(expected)
  end

  test "all checksum algorithms have exact terminal lengths including empty objects" do
    for mode <- [:signed, :unsigned],
        algorithm <- ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256),
        bytes <- ["", "abc", "abcdef"] do
      opts =
        Keyword.merge(options(),
          mode: mode,
          checksum_algorithm: algorithm,
          chunk_size: 3,
          decoded_length: byte_size(bytes)
        )

      chunks = for <<chunk::binary-size(3) <- bytes>>, do: chunk
      assert {:ok, stream} = StreamingSignature.encode(chunks, opts)
      assert {:ok, length} = StreamingSignature.encoded_length(byte_size(bytes), opts)
      assert length == IO.iodata_length(Enum.to_list(stream))
    end
  end

  test "length violations, oversized upstream chunks and invalid trailers fail explicitly" do
    opts = Keyword.merge(options(), chunk_size: 3, decoded_length: 4)
    assert {:ok, stream} = StreamingSignature.encode(["abc"], opts)
    assert_raise StreamingSignature.StreamError, fn -> Enum.to_list(stream) end
    assert {:ok, stream} = StreamingSignature.encode(["abc", "de"], opts)
    assert_raise StreamingSignature.StreamError, fn -> Enum.to_list(stream) end
    assert {:ok, stream} = StreamingSignature.encode(["abcde"], opts)
    assert_raise StreamingSignature.StreamError, fn -> Enum.to_list(stream) end
    {:ok, state} = StreamingSignature.new(options())

    assert {:error, %Error{code: :streaming_trailers}} =
             StreamingSignature.sign_trailers(state, [{"x", "value\nInjected"}])

    assert {:error, %Error{code: :streaming_trailers}} =
             StreamingSignature.sign_trailers(state, [{"X", "one"}, {"x", "two"}])

    assert {:error, %Error{code: :streaming_seed}} =
             StreamingSignature.new(Keyword.put(options(), :auth, :sigv4a))
  end

  test "seed extraction and inspection never expose signing key material" do
    seed = @oracle["signatures"]["request"]

    assert {:ok, ^seed} =
             StreamingSignature.seed("AWS4-HMAC-SHA256 Credential=redacted, Signature=" <> seed)

    assert {:error, %Error{code: :streaming_seed}} = StreamingSignature.seed("invalid")
    {:ok, state} = StreamingSignature.new(options())
    refute inspect(state) =~ @oracle["secret_access_key"]
    refute inspect(state) =~ seed
    assert {:ok, headers} = StreamingSignature.headers(1, chunk_size: 1)
    assert {"x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"} in headers
  end
end
