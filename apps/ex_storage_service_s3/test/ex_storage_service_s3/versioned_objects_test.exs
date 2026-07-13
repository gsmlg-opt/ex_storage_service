defmodule ExStorageServiceS3.VersionedObjectsTest do
  use ExUnit.Case, async: false

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

    # latest view gone, old versions remain readable
    {:ok, %{status: 404}} = Req.get("#{@base_url}/#{bucket}/life.txt")

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
