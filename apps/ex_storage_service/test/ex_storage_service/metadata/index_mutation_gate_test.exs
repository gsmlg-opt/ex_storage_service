defmodule ExStorageService.Metadata.IndexMutationGateTest do
  use ExUnit.Case, async: false
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Metadata.{BucketLifecycle, ListIndex}

  test "a building bucket rejects a hosted reader before consuming or staging its body" do
    bucket = "index-gate-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, bucket_meta} = Metadata.get_bucket(bucket)
    root_key = ListIndex.root_key(bucket, BucketLifecycle.generation(bucket_meta))
    {:ok, root} = Concord.get(root_key)

    :ok =
      Concord.put(root_key, Map.merge(root, %{state: :building, build_token: "operator-only"}))

    parent = self()

    reader = fn state ->
      send(parent, :body_consumed)
      {:done, state}
    end

    assert {:error, :listing_index_unavailable, :initial} =
             ObjectService.put_from_reader(bucket, "key", reader, :initial, "text/plain", %{})

    refute_received :body_consumed
    assert {:error, :not_found} = Concord.get("obj:#{bucket}:key")
    :ok = Concord.put(root_key, root)
    :ok = Metadata.delete_bucket(bucket)
  end

  test "retained legacy bytes cannot reappear in a newly authoritative generation" do
    bucket = "index-generation-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Concord.put("obj:#{bucket}:old", %{etag: "retained", size: 0})
    :ok = Concord.put("obj_ver:#{bucket}:old:retained", %{etag: "retained", size: 0})
    :ok = Metadata.delete_bucket(bucket)
    :ok = Metadata.create_bucket(bucket)
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "old")

    assert {:error, :not_found} =
             ExStorageService.Storage.Versioning.get_version(bucket, "old", "retained")

    assert {:ok, %{keys: [], common_prefixes: []}} = Metadata.list_objects(bucket)
    assert {:ok, %{entries: []}} = ExStorageService.Storage.VersionListing.list(bucket)
    assert {:ok, %{etag: "retained"}} = Concord.get("obj:#{bucket}:old")
    :ok = Metadata.delete_bucket(bucket)
    :ok = Concord.delete("obj:#{bucket}:old")
    :ok = Concord.delete("obj_ver:#{bucket}:old:retained")
  end
end
