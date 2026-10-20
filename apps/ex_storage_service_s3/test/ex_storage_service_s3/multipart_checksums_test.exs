defmodule ExStorageServiceS3.MultipartChecksumsTest do
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn
  alias ExStorageServiceS3.MultipartChecksums
  alias ExStorageServiceS3.Handlers.Object.CloudCopy
  alias ExStorageService.BlobStore.Source

  test "all applicable multipart checksum types negotiate and invalid combinations fail" do
    for algorithm <- ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256), type <- ~w(COMPOSITE FULL_OBJECT) do
      request =
        conn(:post, "/")
        |> put_req_header("x-amz-checksum-algorithm", algorithm)
        |> put_req_header("x-amz-checksum-type", type)

      if (type == "FULL_OBJECT" and algorithm in ~w(CRC32 CRC32C CRC64NVME)) or
           (type == "COMPOSITE" and algorithm != "CRC64NVME") do
        assert {:ok, %{checksum_algorithm: ^algorithm, checksum_type: ^type}} =
                 MultipartChecksums.initiation(request)
      else
        assert {:error, :unsupported_checksum_type} = MultipartChecksums.initiation(request)
      end
    end

    assert {:error, :unsupported_checksum} =
             MultipartChecksums.initiation(
               conn(:post, "/")
               |> put_req_header("x-amz-checksum-algorithm", "MD5")
             )

    assert {:error, :unsupported_checksum_type} =
             MultipartChecksums.initiation(
               conn(:post, "/")
               |> put_req_header("x-amz-checksum-type", "FULL_OBJECT")
             )
  end

  test "full-object vectors cover every wire algorithm without buffering a source" do
    source = Source.stream(["123", "456", "789"], 9)

    assert {:ok, checksums} =
             MultipartChecksums.checksum_source(source, ~w(crc32 crc32c crc64nvme sha1 sha256))

    assert checksums["crc32"] == "y/Q5Jg=="
    assert checksums["crc32c"] == "4waSgw=="
    assert checksums["crc64nvme"] == "rosUhgp5mIg="
    assert checksums["sha1"] == Base.encode64(:crypto.hash(:sha, "123456789"))
    assert checksums["sha256"] == Base.encode64(:crypto.hash(:sha256, "123456789"))
  end

  test "composite checksums hash raw part digests and retain the part count" do
    for algorithm <- ~w(crc32 crc32c sha1 sha256) do
      {:ok, first} = MultipartChecksums.checksum_source(Source.stream(["first"], 5), [algorithm])
      {:ok, second} = MultipartChecksums.checksum_source(Source.stream(["last"], 4), [algorithm])
      digest_bytes = Base.decode64!(first[algorithm]) <> Base.decode64!(second[algorithm])

      {:ok, expected} =
        MultipartChecksums.checksum_source(
          Source.stream([digest_bytes], byte_size(digest_bytes)),
          [algorithm]
        )

      prepared = %{
        upload: %{
          attributes: %{checksum_algorithm: String.upcase(algorithm), checksum_type: "COMPOSITE"},
          completion: %{
            part_checksums: [
              %{part_number: 1, checksums: first},
              %{part_number: 2, checksums: second}
            ]
          }
        }
      }

      assert {:ok, %{^algorithm => value}} =
               MultipartChecksums.complete(conn(:post, "/"), prepared)

      assert value == expected[algorithm] <> "-2"
      assert :ok = MultipartChecksums.validate_parts(prepared, [{1, first}, {2, second}])

      assert {:error, :bad_digest} =
               MultipartChecksums.validate_parts(prepared, [{1, second}, {2, first}])
    end
  end

  test "copy recomputes multipart source checksums for the destination representation" do
    source = Source.stream(["raw ", "bytes"], 9)
    old = Base.encode64(:crypto.hash(:sha256, "a part digest")) <> "-2"
    metadata = %{checksums: %{"sha256" => old}}

    attrs = %{
      content_type: "application/octet-stream",
      metadata: %{},
      checksums: metadata.checksums,
      system_headers: [
        {"cache-control", "private"},
        {"x-amz-checksum-sha256", old},
        {"x-amz-checksum-type", "COMPOSITE"}
      ]
    }

    assert {:ok, result} = CloudCopy.copy_attributes_from_source(source, attrs, metadata)
    expected = Base.encode64(:crypto.hash(:sha256, "raw bytes"))
    assert result.checksums == %{"sha256" => expected}
    assert result.checksum_type == "FULL_OBJECT"
    assert {"cache-control", "private"} in result.headers
    assert {"x-amz-checksum-sha256", expected} in result.headers
    refute {"x-amz-checksum-sha256", old} in result.headers
    refute {"x-amz-checksum-type", "COMPOSITE"} in result.headers

    assert {:error, :unsupported_checksum} =
             CloudCopy.copy_attributes_from_source(source, attrs, metadata,
               checksum_algorithm: "MD5"
             )
  end
end
