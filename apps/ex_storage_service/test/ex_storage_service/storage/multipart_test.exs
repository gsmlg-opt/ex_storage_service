defmodule ExStorageService.Storage.MultipartTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine, Multipart, Pack}

  setup do
    bucket = "multipart-core-#{:erlang.unique_integer([:positive])}"
    {:ok, upload_id} = Multipart.init_upload(bucket, "streamed.bin")

    on_exit(fn ->
      Multipart.abort_upload(bucket, upload_id)

      data_root =
        Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

      File.rm_rf!(Path.join(data_root, bucket))
    end)

    %{bucket: bucket, upload_id: upload_id}
  end

  test "store_part writes enumerable data without requiring a buffered binary", %{
    bucket: bucket,
    upload_id: upload_id
  } do
    stream = Stream.map(["streamed", "-", "part"], & &1)
    expected_data = "streamed-part"
    expected_etag = :md5 |> :crypto.hash(expected_data) |> Base.encode16(case: :lower)

    assert {:ok, ^expected_etag} = Multipart.store_part(bucket, upload_id, 1, stream)

    assert {:ok, [%{part_number: 1, size: 13, etag: ^expected_etag}]} =
             Multipart.list_parts(bucket, upload_id)
  end

  test "completed multipart content lands in the global CAS" do
    bucket = "mpu-cas-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)

    {:ok, upload_id} = ExStorageService.Storage.Multipart.init_upload(bucket, "big-object")

    part = String.duplicate("a", 5 * 1024 * 1024)
    {:ok, _etag1} = ExStorageService.Storage.Multipart.store_part(bucket, upload_id, 1, part)
    {:ok, etag2} = ExStorageService.Storage.Multipart.store_part(bucket, upload_id, 2, "tail")

    parts = [{1, ""}, {2, etag2}]

    assert {:ok, {content_hash, _etag, _size, _manifest_hash}} =
             ExStorageService.Storage.Multipart.complete_upload(bucket, upload_id, parts)

    assert File.exists?(ExStorageService.Storage.CAS.blob_path(content_hash))
    assert {:ok, _} = ExStorageService.Metadata.get_blob_meta(content_hash)
  end

  test "store_part commits the part to the global CAS and records its hash" do
    bucket = "mpu-part-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")

    data = "part-data-#{System.unique_integer()}"
    expected_hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    {:ok, _etag} = Multipart.store_part(bucket, upload_id, 1, data)

    assert File.exists?(ExStorageService.Storage.CAS.blob_path(expected_hash))
    assert {:ok, [part]} = Multipart.list_parts(bucket, upload_id)
    assert part.hash == expected_hash
    # no bucket-local part files
    refute File.dir?(
             Path.join([ExStorageService.Storage.CAS.data_root(), bucket, "multipart", upload_id])
           )
  end

  test "complete_upload creates a manifest describing the parts" do
    bucket = "mpu-man-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")

    p1 = String.duplicate("x", 5 * 1024 * 1024)
    p2 = "tail-#{System.unique_integer()}"
    {:ok, etag1} = Multipart.store_part(bucket, upload_id, 1, p1)
    {:ok, etag2} = Multipart.store_part(bucket, upload_id, 2, p2)

    assert {:ok, {content_hash, _etag, size, manifest_hash}} =
             Multipart.complete_upload(bucket, upload_id, [{1, etag1}, {2, etag2}])

    assert size == byte_size(p1) + byte_size(p2)
    # whole-object blob equals the concatenation
    assert File.read!(ExStorageService.Storage.CAS.blob_path(content_hash)) == p1 <> p2

    assert {:ok, manifest} = ExStorageService.Storage.Manifest.get_manifest(manifest_hash)
    assert [%{number: 1, etag: ^etag1}, %{number: 2, etag: ^etag2}] = manifest.parts
    assert manifest.total_size == size

    # part records cleaned up after completion
    assert {:ok, []} = Multipart.list_parts(bucket, upload_id)
  end

  test "complete_upload streams a part that only exists in a pack" do
    bucket = "mpu-packed-#{:erlang.unique_integer([:positive])}"
    Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "packed-part")

    data = "packed-multipart-part-#{System.unique_integer()}"
    {:ok, etag} = Multipart.store_part(bucket, upload_id, 1, data)
    assert {:ok, [%{hash: part_hash}]} = Multipart.list_parts(bucket, upload_id)

    {:ok, {filler_hash, _etag, _size}} =
      Engine.put_object(bucket, "pack-filler", "pack-filler-#{System.unique_integer()}")

    {:ok, {trailing_hash, _etag, _size}} =
      Engine.put_object(bucket, "pack-trailing", "pack-trailing-#{System.unique_integer()}")

    assert {:ok, %{packed: 3}} = Pack.pack_blobs([filler_hash, part_hash, trailing_hash])
    assert :ok = File.rm(CAS.blob_path(part_hash))

    assert {:ok, {:pack, pack_path, pack_offset, pack_size}} =
             Engine.get_object_location(bucket, part_hash)

    assert pack_offset > 0
    assert pack_offset + pack_size < File.stat!(pack_path).size

    assert {:ok, {content_hash, _etag, _size, _manifest_hash}} =
             Multipart.complete_upload(bucket, upload_id, [{1, etag}])

    assert {:ok, ^data} = Engine.read_object(bucket, content_hash)
  end

  test "complete_upload with a never-uploaded part returns missing_part" do
    bucket = "mpu-miss-#{:erlang.unique_integer([:positive])}"
    ExStorageService.Metadata.create_bucket(bucket)
    {:ok, upload_id} = Multipart.init_upload(bucket, "obj")
    {:ok, etag1} = Multipart.store_part(bucket, upload_id, 1, "only-part")

    assert {:error, {:missing_part, 2, _reason}} =
             Multipart.complete_upload(bucket, upload_id, [{1, etag1}, {2, "bogus"}])
  end
end
