defmodule ExStorageServiceS3.VersionedObjectsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "vobj-#{:erlang.unique_integer([:positive])}"

  defp create_versioned_bucket do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")

    versioning_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
    """

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}?versioning", body: versioning_xml)
    bucket
  end

  defp version_id(resp) do
    [vid] = Req.Response.get_header(resp, "x-amz-version-id")
    vid
  end

  test "each PUT creates a distinct version; old versions readable by versionId" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "version-one")
    {:ok, r2} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "version-two")
    v1 = version_id(r1)
    v2 = version_id(r2)
    assert v1 != v2

    {:ok, %{status: 200, body: latest}} = Req.get("#{@base_url}/#{bucket}/doc.txt")
    assert latest == "version-two"

    {:ok, %{status: 200, body: old}} = Req.get("#{@base_url}/#{bucket}/doc.txt?versionId=#{v1}")
    assert old == "version-one"
  end

  test "PUT on unversioned bucket returns no x-amz-version-id header" do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")

    {:ok, resp} = Req.put("#{@base_url}/#{bucket}/k.txt", body: "plain")
    assert resp.status == 200
    assert Req.Response.get_header(resp, "x-amz-version-id") == []
  end

  test "versions of identical content share one CAS blob" do
    bucket = create_versioned_bucket()
    data = "same-content-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/dup.txt", body: data)
    {:ok, r2} = Req.put("#{@base_url}/#{bucket}/dup.txt", body: data)
    assert version_id(r1) != version_id(r2)

    assert File.exists?(CAS.blob_path(hash))

    # both versions resolve to the same blob
    {:ok, %{status: 200, body: b1}} =
      Req.get("#{@base_url}/#{bucket}/dup.txt?versionId=#{version_id(r1)}")

    assert b1 == data
  end

  test "CopyObject destination gets a version in a versioned bucket" do
    src = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{src}", body: "")
    dst = create_versioned_bucket()

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/a.txt", body: "copy-src")

    {:ok, copy_resp} =
      Req.put("#{@base_url}/#{dst}/b.txt",
        headers: [{"x-amz-copy-source", "/#{src}/a.txt"}],
        body: ""
      )

    assert copy_resp.status == 200
    vid = version_id(copy_resp)

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/b.txt?versionId=#{vid}")
    assert body == "copy-src"
  end

  test "CopyObject from a versioned bucket to an unversioned bucket drops source version IDs" do
    src = create_versioned_bucket()
    dst = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{dst}", body: "")

    {:ok, first_put} = Req.put("#{@base_url}/#{src}/a.txt", body: "version-one")
    first_version_id = version_id(first_put)

    {:ok, second_put} = Req.put("#{@base_url}/#{src}/a.txt", body: "version-two")
    second_version_id = version_id(second_put)

    assert {:ok, %{version_id: ^second_version_id, parent_version_id: ^first_version_id}} =
             Metadata.get_object_meta(src, "a.txt")

    {:ok, copy_resp} =
      Req.put("#{@base_url}/#{dst}/b.txt",
        headers: [{"x-amz-copy-source", "/#{src}/a.txt"}],
        body: ""
      )

    assert copy_resp.status == 200
    assert Req.Response.get_header(copy_resp, "x-amz-version-id") == []

    assert {:ok, %{parent_version_id: nil} = dest_meta} =
             Metadata.get_object_meta(dst, "b.txt")

    refute Map.has_key?(dest_meta, :version_id)

    assert {:ok, %{status: 200, body: "version-two"}} =
             Req.get("#{@base_url}/#{dst}/b.txt")
  end

  test "DELETE creates a marker; old version readable; PUT restores; versionId deletes repoint" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-one")
    v1 = version_id(r1)
    {:ok, _r2} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-two")

    # DELETE → delete marker
    {:ok, del} = Req.delete("#{@base_url}/#{bucket}/life.txt")
    assert del.status == 204
    assert Req.Response.get_header(del, "x-amz-delete-marker") == ["true"]
    [marker_id] = Req.Response.get_header(del, "x-amz-version-id")

    # latest view reports the delete marker for normal GET and HEAD
    {:ok, latest_get} = Req.get("#{@base_url}/#{bucket}/life.txt")
    assert latest_get.status == 404
    assert Req.Response.get_header(latest_get, "x-amz-delete-marker") == ["true"]
    assert Req.Response.get_header(latest_get, "x-amz-version-id") == [marker_id]

    {:ok, latest_head} = Req.head("#{@base_url}/#{bucket}/life.txt")
    assert latest_head.status == 404
    assert Req.Response.get_header(latest_head, "x-amz-delete-marker") == ["true"]
    assert Req.Response.get_header(latest_head, "x-amz-version-id") == [marker_id]

    {:ok, %{status: 200, body: "v-one"}} =
      Req.get("#{@base_url}/#{bucket}/life.txt?versionId=#{v1}")

    # GET of the marker version 404s and flags the marker
    {:ok, marker_get} = Req.get("#{@base_url}/#{bucket}/life.txt?versionId=#{marker_id}")
    assert marker_get.status == 404
    assert Req.Response.get_header(marker_get, "x-amz-delete-marker") == ["true"]

    # PUT after marker restores visibility
    {:ok, r3} = Req.put("#{@base_url}/#{bucket}/life.txt", body: "v-three")
    v3 = version_id(r3)
    {:ok, %{status: 200, body: "v-three"}} = Req.get("#{@base_url}/#{bucket}/life.txt")

    # permanently delete the current version → marker becomes latest → 404
    {:ok, del_v3} = Req.delete("#{@base_url}/#{bucket}/life.txt?versionId=#{v3}")
    assert del_v3.status == 204
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/life.txt")

    # permanently delete the marker → undelete: v-two visible again
    {:ok, del_marker} = Req.delete("#{@base_url}/#{bucket}/life.txt?versionId=#{marker_id}")
    assert del_marker.status == 204
    {:ok, %{status: 200, body: "v-two"}} = Req.get("#{@base_url}/#{bucket}/life.txt")
  end

  test "a deleted marker-only bucket takes precedence over its stale marker metadata" do
    bucket = create_versioned_bucket()
    key = "stale-marker.txt"

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/#{key}", body: "value")
    {:ok, delete_resp} = Req.delete("#{@base_url}/#{bucket}/#{key}")
    assert delete_resp.status == 204
    assert Req.Response.get_header(delete_resp, "x-amz-delete-marker") == ["true"]
    assert [_marker_id] = Req.Response.get_header(delete_resp, "x-amz-version-id")

    {:ok, %{status: 204}} = Req.delete("#{@base_url}/#{bucket}")

    {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
    assert get_resp.status == 404
    assert String.contains?(get_resp.body, "NoSuchBucket")
    assert Req.Response.get_header(get_resp, "x-amz-delete-marker") == []
    assert Req.Response.get_header(get_resp, "x-amz-version-id") == []

    {:ok, head_resp} = Req.head("#{@base_url}/#{bucket}/#{key}")
    assert head_resp.status == 404
    assert Req.Response.get_header(head_resp, "x-amz-delete-marker") == []
    assert Req.Response.get_header(head_resp, "x-amz-version-id") == []
  end

  test "batch DeleteObjects creates markers on versioned buckets" do
    bucket = create_versioned_bucket()

    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/batch.txt", body: "keep-me")
    v1 = version_id(r1)

    delete_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <Delete><Object><Key>batch.txt</Key></Object></Delete>
    """

    {:ok, %{status: 200}} = Req.post("#{@base_url}/#{bucket}?delete", body: delete_xml)

    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/batch.txt")

    {:ok, %{status: 200, body: "keep-me"}} =
      Req.get("#{@base_url}/#{bucket}/batch.txt?versionId=#{v1}")
  end

  test "DELETE on unversioned bucket has no marker headers and stays idempotent" do
    bucket = unique_bucket()
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/p.txt", body: "x")

    {:ok, del} = Req.delete("#{@base_url}/#{bucket}/p.txt")
    assert del.status == 204
    assert Req.Response.get_header(del, "x-amz-delete-marker") == []

    {:ok, del_again} = Req.delete("#{@base_url}/#{bucket}/p.txt")
    assert del_again.status == 204
  end
end
