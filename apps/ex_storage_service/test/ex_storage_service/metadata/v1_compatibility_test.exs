defmodule ExStorageService.Metadata.V1CompatibilityTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.Versioning

  test "version reads and listing fall back to unmigrated v1 records" do
    suffix = System.unique_integer([:positive])
    bucket = "v1-fallback-#{suffix}"
    key = "folder/object-#{suffix}"
    version_id = "legacy-version"
    version_key = "obj_ver:#{bucket}:#{key}:#{version_id}"
    list_key = "obj_ver_list:#{bucket}:#{key}"

    on_exit(fn ->
      Concord.delete(version_key)
      Concord.delete(list_key)
    end)

    metadata = %{
      content_hash: "legacy-hash",
      size: 42,
      created_at: "2026-07-18T00:00:00Z"
    }

    assert :ok = Concord.put(version_key, metadata)
    assert :ok = Concord.put(list_key, [version_id])

    assert {:ok, %{version_id: ^version_id, content_hash: "legacy-hash"}} =
             Versioning.get_version(bucket, key, nil)

    assert {:ok, [%{version_id: ^version_id, content_hash: "legacy-hash"}]} =
             Versioning.list_versions(bucket, key)
  end
end
