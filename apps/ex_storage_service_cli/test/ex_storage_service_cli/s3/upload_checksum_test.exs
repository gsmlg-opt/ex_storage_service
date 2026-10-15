defmodule ExStorageServiceCli.S3.UploadChecksumTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Transfer.Coordinator, Transfer.Source}
  alias ExStorageServiceCli.S3.UploadChecksum
  @moduletag :tmp_dir
  @vectors __DIR__
           |> Path.join("../../fixtures/special_s3/checksum-vectors.json")
           |> File.read!()
           |> JSON.decode!()
           |> Map.fetch!("vectors")

  test "selection is serializable and preserves explicit and default types" do
    assert {:ok, contract} = UploadChecksum.select(%Context{}, %{})
    assert contract == %{"algorithm" => "CRC64NVME", "type" => "FULL_OBJECT", "expected" => nil}
    assert JSON.decode!(JSON.encode!(contract)) == contract

    assert UploadChecksum.initiation(contract, %{"Bucket" => "b"}) == %{
             "Bucket" => "b",
             "ChecksumAlgorithm" => "CRC64NVME",
             "ChecksumType" => "FULL_OBJECT"
           }

    assert {:ok, %{"type" => "COMPOSITE"}} =
             UploadChecksum.select(%Context{}, %{"ChecksumAlgorithm" => "SHA256"})

    assert {:ok, %{"type" => "FULL_OBJECT"}} =
             UploadChecksum.select(%Context{}, %{"ChecksumAlgorithm" => "SHA256"}, :object)

    assert {:ok, nil} =
             UploadChecksum.select(
               %Context{options: %{request_checksum_calculation: "when_required"}},
               %{}
             )

    assert {:error, %Error{code: :upload_checksum_type}} =
             UploadChecksum.select(%Context{}, %{
               "ChecksumAlgorithm" => "CRC64NVME",
               "ChecksumType" => "COMPOSITE"
             })

    assert {:error, %Error{code: :upload_checksum_type}} =
             UploadChecksum.select(%Context{}, %{
               "ChecksumAlgorithm" => "SHA256",
               "ChecksumType" => "FULL_OBJECT"
             })

    assert {:error, %Error{code: :upload_checksum_algorithm}} =
             UploadChecksum.select(%Context{}, %{
               "ChecksumAlgorithm" => "SHA256",
               "ChecksumCRC32" => "AAAAAA=="
             })
  end

  test "server copy selects only explicit checksums and retains only the selected provider digest" do
    assert {:ok, nil} = UploadChecksum.select(%Context{}, %{}, :copy)

    assert {:ok, contract} =
             UploadChecksum.select(%Context{}, %{"ChecksumAlgorithm" => "SHA256"}, :copy)

    assert contract["type"] == "COMPOSITE"
    digest = Base.encode64(:crypto.hash(:sha256, "part bytes"))
    response = %{"ChecksumSHA256" => digest, "ChecksumCRC32" => "AAAAAA=="}
    assert {:ok, %{}} = UploadChecksum.copied_part(nil, response)
    assert {:ok, %{"SHA256" => ^digest}} = UploadChecksum.copied_part(contract, response)

    assert {:error, %Error{code: :missing_copy_checksum}} =
             UploadChecksum.copied_part(contract, %{})

    assert {:error, %Error{code: :invalid_upload_checksum}} =
             UploadChecksum.copied_part(contract, %{"ChecksumSHA256" => "short"})
  end

  for vector <- @vectors do
    @vector vector
    test "local part retention and completion oracle #{vector["algorithm"]}/#{vector["name"]}", %{
      tmp_dir: root
    } do
      vector = @vector
      bytes = Base.decode64!(vector["data_base64"])
      path = Path.join(root, "source")
      File.write!(path, bytes)
      assert {:ok, source} = Source.file(path, chunk_size: 17)

      assert {:ok, contract} =
               UploadChecksum.select(%Context{}, %{"ChecksumAlgorithm" => vector["algorithm"]})

      left = div(byte_size(bytes), 2)
      assert {:ok, first} = Source.range(source, 0, left)
      assert {:ok, second} = Source.range(source, left, byte_size(bytes) - left)
      assert {:ok, a} = UploadChecksum.part(contract, first)
      assert {:ok, b} = UploadChecksum.part(contract, second)
      assert a[vector["algorithm"]] == Enum.at(vector["parts"], 0)
      assert b[vector["algorithm"]] == Enum.at(vector["parts"], 1)

      parts = [
        %{"number" => 1, "length" => first.length, "etag" => "a", "checksums" => a},
        %{"number" => 2, "length" => second.length, "etag" => "b", "checksums" => b}
      ]

      assert {:ok, fields, expected} = UploadChecksum.completion(contract, Enum.reverse(parts))
      assert fields["ChecksumType"] == contract["type"]
      assert fields["MpuObjectSize"] == byte_size(bytes)
      assert length(fields["MultipartUpload"]["Parts"]) == 2
      member = "Checksum" <> vector["algorithm"]

      if vector["algorithm"] == "CRC64NVME" do
        assert fields[member] == vector["digest_base64"]
        assert expected[member] == vector["digest_base64"]
      else
        refute Map.has_key?(fields, member)
        assert expected[member] == vector["composite"]

        assert {:ok, full_contract} =
                 UploadChecksum.select(
                   %Context{},
                   %{"ChecksumAlgorithm" => vector["algorithm"]},
                   :object
                 )

        assert {:ok, %{}} =
                 UploadChecksum.object(full_contract, source, %{member => vector["digest_base64"]})

        if vector["algorithm"] in ["CRC32", "CRC32C"] do
          full_contract = %{contract | "type" => "FULL_OBJECT"}
          assert {:ok, full, full_expected} = UploadChecksum.completion(full_contract, parts)
          assert full[member] == vector["digest_base64"]
          assert full_expected[member] == vector["digest_base64"]
        end
      end

      assert {:ok, :verified} = UploadChecksum.verify(expected, expected)

      assert {:ok, :digest_verified} =
               UploadChecksum.verify(expected, Map.delete(expected, "ChecksumType"))

      assert {:ok, :not_provided} = UploadChecksum.verify(expected, %{})
    end
  end

  test "all provider-returned digests and explicit full-object checksums are validated", %{
    tmp_dir: root
  } do
    path = Path.join(root, "source")
    File.write!(path, "123456789")
    {:ok, source} = Source.file(path)
    {:ok, contract} = UploadChecksum.select(%Context{}, %{})

    assert {:error, %Error{code: :upload_checksum_mismatch}} =
             UploadChecksum.part(contract, source, %{
               "ChecksumSHA256" => Base.encode64(<<0::256>>)
             })

    assert {:ok, requested} =
             UploadChecksum.select(%Context{}, %{"ChecksumCRC32C" => "4waSgw=="}, :object)

    assert {:ok, checksums} = UploadChecksum.object(requested, source, %{})
    assert checksums["CRC32C"] == "4waSgw=="
    bad = %{requested | "expected" => "AAAAAA=="}

    assert {:error, %Error{code: :upload_checksum_mismatch}} =
             UploadChecksum.object(bad, source, %{})

    assert {:error, %Error{code: :upload_checksum_type}} =
             UploadChecksum.acknowledge(contract, %{"ChecksumType" => "COMPOSITE"})

    assert {:error, %Error{code: :upload_checksum_type}} =
             UploadChecksum.verify(%{"ChecksumType" => "FULL_OBJECT"}, %{
               "ChecksumType" => "COMPOSITE"
             })
  end

  test "completion rejects lost local digests, nonconsecutive parts and explicit mismatch" do
    {:ok, contract} = UploadChecksum.select(%Context{}, %{})
    part = %{"number" => 1, "length" => 1, "etag" => "a", "checksums" => %{}}

    assert {:error, %Error{code: :upload_checksum_parts}} =
             UploadChecksum.completion(contract, [part])

    part = %{part | "checksums" => %{"CRC64NVME" => "AAAAAAAAAAA="}, "number" => 2}

    assert {:error, %Error{code: :upload_checksum_parts}} =
             UploadChecksum.completion(contract, [part])

    assert {:error, %Error{code: :upload_checksum_parts}} =
             UploadChecksum.completion(contract, [])
  end

  test "short long empty and malformed provider digests return typed checksum mismatches" do
    for vector <- @vectors do
      member = "Checksum" <> vector["algorithm"]
      digest = vector["digest_base64"]

      for malformed <- [
            "",
            "YQ==",
            digest <> "extra",
            String.duplicate("!", byte_size(digest)),
            nil
          ] do
        assert {:error, %Error{code: :upload_checksum_mismatch}} =
                 UploadChecksum.verify(%{member => digest}, %{member => malformed})
      end
    end
  end

  test "source mutation and cancellation never produce retained checksums", %{tmp_dir: root} do
    path = Path.join(root, "source")
    File.write!(path, "old")
    {:ok, source} = Source.file(path)
    {:ok, contract} = UploadChecksum.select(%Context{}, %{})
    File.write!(path, "new")
    assert {:error, %Error{}} = UploadChecksum.part(contract, source)
    coordinator = start_supervised!({Coordinator, []})
    {:ok, source} = Source.file(path, coordinator: coordinator)
    :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = UploadChecksum.part(contract, source)
  end
end
