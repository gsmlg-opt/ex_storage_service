defmodule ExStorageService.Storage.EngineTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.Engine

  defp unique_bucket, do: "engine-test-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  test "put_object stores content in the global CAS and returns hash/etag/size" do
    bucket = unique_bucket()
    data = "engine-global-#{System.unique_integer()}"
    expected_hash = sha256_hex(data)

    assert {:ok, {^expected_hash, etag, size}} = Engine.put_object(bucket, "k1", data)
    assert etag == Base.encode16(:crypto.hash(:md5, data), case: :lower)
    assert size == byte_size(data)

    assert File.exists?(CAS.blob_path(expected_hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), bucket, expected_hash))
  end

  test "identical content in two buckets stores exactly one physical blob" do
    data = "dedup-me-#{System.unique_integer()}"
    hash = sha256_hex(data)

    assert {:ok, {^hash, _, _}} = Engine.put_object(unique_bucket(), "a", data)
    assert {:ok, {^hash, _, _}} = Engine.put_object(unique_bucket(), "b", data)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, meta} = ExStorageService.Metadata.get_blob_meta(hash)
    assert meta.size == byte_size(data)
  end

  test "put_object accepts a stream of chunks" do
    bucket = unique_bucket()
    chunks = ["chunk-one-", "chunk-two-", "#{System.unique_integer()}"]
    data = IO.iodata_to_binary(chunks)
    hash = sha256_hex(data)

    assert {:ok, {^hash, _etag, size}} = Engine.put_object_stream(bucket, "k", chunks)
    assert size == byte_size(data)
    assert File.read!(CAS.blob_path(hash)) == data
  end

  test "get_object resolves CAS content" do
    bucket = unique_bucket()
    data = "read-me-#{System.unique_integer()}"
    {:ok, {hash, _, _}} = Engine.put_object(bucket, "k", data)

    assert {:ok, path} = Engine.get_object(bucket, hash)
    assert path == CAS.blob_path(hash)
    assert File.read!(path) == data
  end

  test "get_object falls back to the legacy bucket-local layout" do
    bucket = unique_bucket()
    data = "legacy-#{System.unique_integer()}"
    hash = sha256_hex(data)

    legacy_path = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy_path))
    File.write!(legacy_path, data)

    assert {:ok, ^legacy_path} = Engine.get_object(bucket, hash)
  end

  test "promote_to_global moves a legacy blob into the CAS" do
    bucket = unique_bucket()
    data = "promote-#{System.unique_integer()}"
    hash = sha256_hex(data)

    legacy_path = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy_path))
    File.write!(legacy_path, data)

    assert :ok = Engine.promote_to_global(bucket, hash)
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(legacy_path)
    # idempotent, and blob metadata was created
    assert :ok = Engine.promote_to_global(bucket, hash)
    assert {:ok, _} = ExStorageService.Metadata.get_blob_meta(hash)
    # missing content is reported
    assert {:error, :not_found} =
             Engine.promote_to_global(bucket, sha256_hex("nope-#{System.unique_integer()}"))
  end
end
