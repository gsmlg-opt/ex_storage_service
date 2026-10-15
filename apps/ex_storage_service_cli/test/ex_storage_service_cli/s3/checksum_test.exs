defmodule ExStorageServiceCli.S3.ChecksumTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.Checksum}

  @fixtures __DIR__
            |> Path.join("../../fixtures/special_s3/checksum-vectors.json")
            |> File.read!()
            |> JSON.decode!()

  for vector <- @fixtures["vectors"] do
    @vector vector
    test "independent checksum #{vector["algorithm"]}/#{vector["name"]}" do
      vector = @vector
      bytes = Base.decode64!(vector["data_base64"])
      {:ok, state} = Checksum.new(vector["algorithm"])

      state =
        for <<byte <- bytes>>, reduce: state do
          state -> Checksum.update(state, <<byte>>)
        end

      assert Checksum.encoded(state) == vector["digest_base64"]
      assert {:ok, :verified} = Checksum.verify(state, vector["digest_base64"])
      {:ok, raw} = Checksum.digest(vector["algorithm"], bytes)
      assert Base.encode64(raw) == vector["digest_base64"]

      if vector["algorithm"] in ["CRC32", "CRC32C", "CRC64NVME"] do
        [first, second] = vector["parts"]
        left_size = div(byte_size(bytes), 2)

        assert {:ok, combined} =
                 Checksum.combine(vector["algorithm"], [
                   {first, left_size},
                   {second, byte_size(bytes) - left_size}
                 ])

        assert combined == vector["digest_base64"]
      end

      if expected = vector["composite"] do
        assert {:ok, ^expected} = Checksum.composite(vector["algorithm"], vector["parts"])

        assert {:ok, :verified} =
                 Checksum.verify(state, expected,
                   checksum_type: "COMPOSITE",
                   parts: vector["parts"]
                 )

        assert {:error, %Error{code: :composite_checksum_unverified}} =
                 Checksum.verify(state, expected)
      else
        assert {:error, %Error{code: :invalid_composite_algorithm}} =
                 Checksum.composite(vector["algorithm"], vector["parts"])
      end
    end
  end

  test "header/trailer verification detects corruption and conflicting values" do
    {:ok, state} = Checksum.new("CRC32C")
    state = Checksum.update(state, "123456789")
    checksum = Checksum.encoded(state)
    header = Checksum.header("CRC32C")
    assert {:ok, :not_provided} = Checksum.verify_response(state, [])
    assert {:ok, :verified} = Checksum.verify_response(state, [], [{header, checksum}])

    assert {:ok, :verified} =
             Checksum.verify_response(state, [{String.upcase(header), checksum}], [
               {header, checksum}
             ])

    assert {:error, %Error{code: :conflicting_checksums}} =
             Checksum.verify_response(state, [{header, checksum}], [{header, "AAAAAA=="}])

    assert {:error, %Error{code: :checksum_mismatch}} = Checksum.verify(state, "AAAAAA==")
    assert {:error, %Error{code: :checksum_mismatch}} = Checksum.verify(state, "AAAA")
    assert {:error, %Error{code: :invalid_composite_parts}} = Checksum.composite("SHA256", [])
    assert {:error, %Error{code: :invalid_checksum}} = Checksum.composite("SHA256", ["AA=="])
    assert {:error, %Error{code: :unsupported_checksum}} = Checksum.new("FAKE")
  end

  test "lazy unsigned checksum envelope matches the pinned botocore framing contract" do
    parent = self()

    source =
      Stream.map(["1234", "", "56789"], fn bytes ->
        send(parent, {:read, bytes})
        bytes
      end)

    {:ok, encoded} = Checksum.encode_trailers(source, "CRC32C")
    refute_received {:read, _}

    assert Enum.to_list(encoded) == [
             "4\r\n1234\r\n",
             "5\r\n56789\r\n",
             "0\r\nx-amz-checksum-crc32c:4waSgw==\r\n\r\n"
           ]

    {:ok, oversized} = Checksum.encode_trailers(["12345"], "SHA256", max_chunk_bytes: 4)
    assert_raise ArgumentError, fn -> Enum.to_list(oversized) end
    {:ok, empty} = Checksum.encode_trailers([], "CRC64NVME")
    assert Enum.to_list(empty) == ["0\r\nx-amz-checksum-crc64nvme:AAAAAAAAAAA=\r\n\r\n"]
  end
end
