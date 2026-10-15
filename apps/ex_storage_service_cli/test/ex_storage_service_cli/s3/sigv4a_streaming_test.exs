defmodule ExStorageServiceCli.S3.SigV4aStreamingTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.SigV4a, S3.StreamingSignature}

  @fixture __DIR__
           |> Path.join("../../fixtures/special_s3/sigv4a-streaming.json")
           |> File.read!()
           |> JSON.decode!()
  @values @fixture["values"]
  @public Base.decode16!(
            "04" <> @values["s_chunked_test_ecc_pub_x"] <> @values["s_chunked_test_ecc_pub_y"],
            case: :mixed
          )

  test "AWS C public key and independent strings-to-sign verify each ECDSA chunk and trailer" do
    opts = options()
    assert SigV4a.public_key(opts) == @public
    assert {:ok, state} = StreamingSignature.new(opts)

    {state, frames, previous} =
      Enum.reduce(
        [{65536, "s_chunk1_sts_post_signature"}, {1024, "s_chunk2_sts_post_signature"}],
        {state, [], opts[:seed_signature]},
        fn {size, suffix}, {state, frames, previous} ->
          assert {:ok, frame, state} =
                   StreamingSignature.chunk(state, String.duplicate("a", size))

          signature = frame_signature(frame)
          assert byte_size(signature) == 144
          text = @values["s_chunk_sts_pre_signature"] <> previous <> @values[suffix]
          assert valid?(text, signature)
          refute valid?(text <> "changed", signature)
          {state, frames ++ [frame], unpad(signature)}
        end
      )

    assert {:ok, final, state} = StreamingSignature.finish(state)
    signature = frame_signature(final)

    text =
      @values["s_chunk_sts_pre_signature"] <> previous <> @values["s_chunk3_sts_post_signature"]

    assert valid?(text, signature)

    assert {:ok, trailer} =
             StreamingSignature.sign_trailers(state, [
               {"third", "3rd"},
               {"first", "1st"},
               {"second", "2nd"}
             ])

    assert byte_size(trailer) == 144

    assert valid?(
             @values["s_trailing_headers_expected_sts_pre_signature"] <>
               unpad(signature) <>
               @values["s_trailing_headers_expected_sts_post_signature"],
             trailer
           )

    assert {:ok, 67064} = StreamingSignature.encoded_length(66560, auth: :sigv4a)
    assert IO.iodata_length(frames ++ [final]) == 67064
  end

  test "ECDSA checksum trailer signs the final chunk and raw canonical trailer hash" do
    opts =
      Keyword.merge(options(), checksum_algorithm: "CRC32C", decoded_length: 9, chunk_size: 4)

    assert {:ok, stream} = StreamingSignature.encode(["1234", "5678", "9"], opts)
    frames = Enum.to_list(stream)
    [first, second, third, final] = frames
    signature = frame_signature(final)

    [trailer_signature] =
      Regex.run(~r/x-amz-trailer-signature:([a-f0-9*]{144})/, final, capture: :all_but_first)

    canonical = "x-amz-checksum-crc32c:4waSgw==\n"

    assert valid?(
             "AWS4-ECDSA-P256-SHA256-TRAILER\n20130524T000000Z\n20130524/s3/aws4_request\n" <>
               unpad(signature) <> "\n" <> hash(canonical),
             trailer_signature
           )

    assert {:ok, length} = StreamingSignature.encoded_length(9, opts)
    assert length == IO.iodata_length(frames)
    assert Enum.all?([first, second, third], &(byte_size(frame_signature(&1)) == 144))

    assert {:ok, headers} =
             StreamingSignature.headers(9,
               auth: :sigv4a,
               checksum_algorithm: "CRC32C",
               chunk_size: 4
             )

    assert {"content-length", Integer.to_string(length)} in headers
    assert {"x-amz-content-sha256", "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"} in headers
  end

  test "signed and pinned unsigned SigV4a trailer modes retain exact encoded lengths" do
    for mode <- [:signed, :unsigned],
        algorithm <- ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256),
        bytes <- ["", "abc", "abcdef"] do
      opts =
        Keyword.merge(options(),
          mode: mode,
          checksum_algorithm: algorithm,
          decoded_length: byte_size(bytes),
          chunk_size: 3
        )

      chunks = for <<chunk::binary-size(3) <- bytes>>, do: chunk
      assert {:ok, stream} = StreamingSignature.encode(chunks, opts)
      assert {:ok, length} = StreamingSignature.encoded_length(byte_size(bytes), opts)
      assert length == IO.iodata_length(Enum.to_list(stream))
    end

    assert {:ok, stream} =
             StreamingSignature.encode(["123456789"],
               auth: :sigv4a,
               mode: :unsigned,
               checksum_algorithm: "CRC32C"
             )

    assert Enum.join(stream) == "9\r\n123456789\r\n0\r\nx-amz-checksum-crc32c:4waSgw==\r\n\r\n"
  end

  test "ECDSA seed extraction accepts valid padded chains and does not expose credentials" do
    opts = options()
    expected = opts[:seed_signature]

    assert {:ok, ^expected} =
             StreamingSignature.seed(
               "AWS4-ECDSA-P256-SHA256 Credential=example, Signature=" <> expected
             )

    padded = String.pad_trailing(expected, 144, "*")
    assert {:ok, state} = StreamingSignature.new(Keyword.put(opts, :seed_signature, padded))
    assert state.previous == expected
    refute inspect(state) =~ opts[:secret_access_key]
    refute inspect(state) =~ expected

    assert {:error, %Error{code: :streaming_seed}} =
             StreamingSignature.new(Keyword.delete(opts, :access_key_id))

    assert {:error, %Error{code: :streaming_seed}} =
             StreamingSignature.new(Keyword.put(opts, :seed_signature, "invalid"))

    assert StreamingSignature.payload_hash(auth: :sigv4a) ==
             "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"

    assert StreamingSignature.payload_hash(auth: :sigv4a, mode: :unsigned) ==
             "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
  end

  defp options do
    credentials = [
      access_key_id: @fixture["access_key_id"],
      secret_access_key: @fixture["secret_access_key"]
    ]

    credentials ++
      [
        auth: :sigv4a,
        seed_signature: SigV4a.sign_message("request seed", credentials),
        now: ~U[2013-05-24 00:00:00Z]
      ]
  end

  defp frame_signature(frame),
    do:
      Regex.run(~r/^\w+;chunk-signature=([a-f0-9*]{144})\r\n/, frame, capture: :all_but_first)
      |> hd()

  defp unpad(signature), do: String.trim_trailing(signature, "*")
  defp hash(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp valid?(text, signature),
    do:
      :crypto.verify(:ecdsa, :sha256, text, Base.decode16!(unpad(signature), case: :mixed), [
        @public,
        :secp256r1
      ])
end
