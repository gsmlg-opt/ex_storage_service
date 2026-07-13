defmodule ExStorageServiceS3.GlobalCasTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.Engine

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "gcas-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp create_bucket(bucket) do
    {:ok, %{status: 201}} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  test "PUT of identical content to two buckets stores one physical blob" do
    b1 = create_bucket(unique_bucket())
    b2 = create_bucket(unique_bucket())
    data = "same-bytes-#{System.unique_integer()}"
    hash = sha256_hex(data)

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{b1}/one.txt", body: data)
    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{b2}/two.txt", body: data)

    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), b1, hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), b2, hash))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{b2}/two.txt")
    assert body == data
  end

  test "cross-bucket CopyObject is metadata-only and destination is readable" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "copy-me-#{System.unique_integer()}"
    hash = sha256_hex(data)

    {:ok, %{status: 200}} = Req.put("#{@base_url}/#{src}/orig.txt", body: data)

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/copied.txt",
        headers: [{"x-amz-copy-source", "/#{src}/orig.txt"}],
        body: ""
      )

    # exactly one physical file: the CAS blob; no legacy dest copy
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), dst, hash))

    {:ok, %{status: 200, body: body}} = Req.get("#{@base_url}/#{dst}/copied.txt")
    assert body == data

    # source unaffected
    {:ok, %{status: 200, body: src_body}} = Req.get("#{@base_url}/#{src}/orig.txt")
    assert src_body == data
  end

  test "CopyObject promotes pre-migration legacy content into the CAS" do
    src = create_bucket(unique_bucket())
    dst = create_bucket(unique_bucket())
    data = "legacy-copy-#{System.unique_integer()}"
    hash = sha256_hex(data)

    # simulate a pre-migration object: legacy file + obj: metadata, no CAS blob
    legacy = Engine.legacy_content_path(Engine.data_root(), src, hash)
    File.mkdir_p!(Path.dirname(legacy))
    File.write!(legacy, data)

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    ExStorageService.Metadata.put_object_meta(src, "old.txt", %{
      content_hash: hash,
      size: byte_size(data),
      etag: Base.encode16(:crypto.hash(:md5, data), case: :lower),
      content_type: "text/plain",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    {:ok, %{status: 200}} =
      Req.put("#{@base_url}/#{dst}/new.txt",
        headers: [{"x-amz-copy-source", "/#{src}/old.txt"}],
        body: ""
      )

    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(legacy)

    # both source and destination readable after promotion
    {:ok, %{status: 200, body: b1}} = Req.get("#{@base_url}/#{src}/old.txt")
    {:ok, %{status: 200, body: b2}} = Req.get("#{@base_url}/#{dst}/new.txt")
    assert b1 == data and b2 == data
  end

  test "bucket named cas is rejected" do
    {:ok, resp} = Req.put("#{@base_url}/cas", body: "")
    assert resp.status == 400
  end
end
