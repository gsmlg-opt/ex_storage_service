defmodule ExStorageService.Metadata.KeysTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.Keys

  @components [
    "",
    ":",
    "bucket:with:colons",
    "folder/child/object.txt",
    "資料/😀/ключ",
    String.duplicate("long-key/:資料", 1_024)
  ]

  test "encoded components round trip without delimiter ambiguity" do
    for component <- @components do
      encoded = Keys.encode_component(component)

      refute String.contains?(encoded, ":")
      assert {:ok, ^component} = Keys.decode_component(encoded)
    end
  end

  test "object keys keep bucket and object components unambiguous" do
    bucket = "bucket:one/two"
    key = "folder:one/two/資料"
    version_id = "version-1"

    bucket64 = Keys.encode_component(bucket)
    key64 = Keys.encode_component(key)

    assert Keys.object_head(bucket, key) ==
             "ess:v2:object_head:#{bucket64}:#{key64}"

    assert Keys.object_version(bucket, key, version_id) ==
             "ess:v2:object_version:#{bucket64}:#{key64}:#{version_id}"

    assert Keys.object_version_prefix(bucket, key) ==
             "ess:v2:object_version:#{bucket64}:#{key64}:"
  end

  test "blob and outbox keys use the v2 schema" do
    assert Keys.blob("abc123") == "ess:v2:blob:abc123"
    assert Keys.outbox("operation-1") == "ess:v2:outbox:operation-1"
  end

  test "cluster, location, and multipart keys encode variable components" do
    node_id = "data:東京/1"
    upload_id = "upload:one/two"
    node64 = Keys.encode_component(node_id)
    upload64 = Keys.encode_component(upload_id)

    assert Keys.cluster_node(node_id) == "ess:v2:cluster_node:#{node64}"
    assert Keys.cluster_node_prefix() == "ess:v2:cluster_node:"
    assert Keys.cluster_status_owner(node_id) == "ess:v2:cluster_status_owner:#{node64}"
    assert Keys.cluster_status_owner_prefix() == "ess:v2:cluster_status_owner:"

    assert Keys.blob_location("abc123", node_id) ==
             "ess:v2:blob_location:abc123:#{node64}"

    assert Keys.blob_location_prefix("abc123") == "ess:v2:blob_location:abc123:"
    assert Keys.multipart_upload(upload_id) == "ess:v2:multipart_upload:#{upload64}"
    assert Keys.multipart_upload_prefix() == "ess:v2:multipart_upload:"
    assert Keys.multipart_part(upload_id, 42) == "ess:v2:multipart_part:#{upload64}:42"
    assert Keys.multipart_part_prefix(upload_id) == "ess:v2:multipart_part:#{upload64}:"
  end
end
