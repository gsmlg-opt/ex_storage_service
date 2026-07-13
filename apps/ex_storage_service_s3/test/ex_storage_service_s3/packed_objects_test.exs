defmodule ExStorageServiceS3.PackedObjectsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.{CAS, Packer}

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "packed-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  # PUT an object, then force its blob into a pack.
  defp put_packed_object(bucket, key, data) do
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/#{key}", body: data)
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert packed >= 1
    refute File.exists?(CAS.blob_path(hash))
    hash
  end

  test "GET serves a packed object with correct body, etag, and content-length" do
    bucket = create_bucket(unique_bucket())
    data = "packed-serving-#{System.unique_integer()}-#{String.duplicate("x", 1000)}"
    put_packed_object(bucket, "cold.bin", data)

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/cold.bin")
    assert resp.status == 200
    assert resp.body == data
    assert Req.Response.get_header(resp, "content-length") == [to_string(byte_size(data))]
  end

  test "Range GET on a packed object returns the right slice" do
    bucket = create_bucket(unique_bucket())
    data = "0123456789abcdefghij-#{System.unique_integer()}"
    put_packed_object(bucket, "ranged.bin", data)

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/ranged.bin", headers: [{"range", "bytes=5-9"}])
    assert resp.status == 206
    assert resp.body == binary_part(data, 5, 5)
    assert Req.Response.get_header(resp, "content-range") == ["bytes 5-9/#{byte_size(data)}"]
  end

  test "two packed objects in one pack are served independently" do
    bucket = create_bucket(unique_bucket())
    d1 = "first-in-pack-#{System.unique_integer()}"
    d2 = "second-in-pack-#{System.unique_integer()}"
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/one.bin", body: d1)
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/two.bin", body: d2)

    for d <- [d1, d2] do
      hash = Base.encode16(:crypto.hash(:sha256, d), case: :lower)
      File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    end

    {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 2)
    assert packed >= 2

    {:ok, %{status: 200, body: b1}} = Req.get("#{@base_url}/#{bucket}/one.bin")
    {:ok, %{status: 200, body: b2}} = Req.get("#{@base_url}/#{bucket}/two.bin")
    assert b1 == d1 and b2 == d2
  end

  test "CopyObject from a packed source is metadata-only and readable" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "copy-packed-#{System.unique_integer()}"
    put_packed_object(src, "orig.bin", data)

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/copy.bin",
        headers: [{"x-amz-copy-source", "/#{src}/orig.bin"}],
        body: ""
      )

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/copy.bin")
    assert body == data
  end

  test "versioned GET of a packed old version works" do
    bucket = create_bucket(unique_bucket())

    versioning_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
    """

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}?versioning", body: versioning_xml)

    data_v1 = "packed-v1-#{System.unique_integer()}"
    {:ok, r1} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: data_v1)
    [v1] = Req.Response.get_header(r1, "x-amz-version-id")
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/doc.txt", body: "v2-current")

    hash_v1 = Base.encode16(:crypto.hash(:sha256, data_v1), case: :lower)
    File.touch!(CAS.blob_path(hash_v1), System.os_time(:second) - 90 * 86_400)
    {:ok, %{packed: p}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert p >= 1

    {:ok, resp} = Req.get("#{@base_url}/#{bucket}/doc.txt?versionId=#{v1}")
    assert resp.status == 200
    assert resp.body == data_v1
  end
end
