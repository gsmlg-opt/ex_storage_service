defmodule ExStorageService.Storage.VersioningTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning

  defp unique_bucket, do: "ver-#{:erlang.unique_integer([:positive])}"

  defp meta_for(hash) do
    %{
      content_hash: hash,
      size: 10,
      etag: "etag-#{hash}",
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      updated_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  test "put_version stamps object_type and parent_version_id" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, v2} = Versioning.put_version(bucket, "k", meta_for("h2"))

    assert {:ok, ver1} = Versioning.get_version(bucket, "k", v1)
    assert ver1.object_type == :blob
    assert ver1.parent_version_id == nil

    assert {:ok, ver2} = Versioning.get_version(bucket, "k", v2)
    assert ver2.parent_version_id == v1

    # obj: ref points at the latest version
    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v2
    assert obj.content_hash == "h2"
  end

  test "delete marker removes the obj: latest view but keeps versions readable" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, marker_id, :delete_marker} = Versioning.delete_version(bucket, "k")

    # latest view is gone — GET/HEAD/list see no object
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")

    # marker is the latest version; the old version is still readable
    assert {:ok, %{is_delete_marker: true}} = Versioning.get_version(bucket, "k", nil)
    assert {:ok, %{content_hash: "h1"}} = Versioning.get_version(bucket, "k", v1)
    assert marker_id != v1
  end

  test "PUT after a delete marker restores the latest view" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, _v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, _marker, :delete_marker} = Versioning.delete_version(bucket, "k")
    {:ok, v3} = Versioning.put_version(bucket, "k", meta_for("h3"))

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v3
    assert obj.content_hash == "h3"
  end

  test "deleting the latest version by id repoints obj: to the previous version" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, v2} = Versioning.put_version(bucket, "k", meta_for("h2"))

    {:ok, ^v2, :deleted} = Versioning.delete_version(bucket, "k", v2)

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v1
    assert obj.content_hash == "h1"
    assert {:error, :not_found} = Versioning.get_version(bucket, "k", v2)
  end

  test "deleting a delete-marker version by id undeletes the object" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, marker_id, :delete_marker} = Versioning.delete_version(bucket, "k")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")

    {:ok, ^marker_id, :deleted} = Versioning.delete_version(bucket, "k", marker_id)

    assert {:ok, obj} = Metadata.get_object_meta(bucket, "k")
    assert obj.version_id == v1
  end

  test "deleting the only version removes obj: entirely" do
    bucket = unique_bucket()
    Versioning.set_versioning(bucket, :enabled)

    {:ok, v1} = Versioning.put_version(bucket, "k", meta_for("h1"))
    {:ok, ^v1, :deleted} = Versioning.delete_version(bucket, "k", v1)

    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")
    assert {:ok, []} = Versioning.list_versions(bucket, "k")
  end

  test "disabled buckets keep plain semantics" do
    bucket = unique_bucket()

    {:ok, "null"} = Versioning.put_version(bucket, "k", meta_for("h1"))
    assert {:ok, %{content_hash: "h1"}} = Metadata.get_object_meta(bucket, "k")

    {:ok, "null", :deleted} = Versioning.delete_version(bucket, "k")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "k")
  end
end
