defmodule ExStorageServiceS3.ApiTest do
  use ExUnit.Case, async: false

  alias ExStorageService.CloudCache.Config, as: CloudCacheConfig
  alias ExStorageService.Metadata

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "test-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, _} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  defp cleanup_bucket(bucket) do
    # Delete all objects first
    case Req.get("#{@base_url}/#{bucket}?list-type=2") do
      {:ok, %{status: 200, body: body}} ->
        # Extract keys from XML
        Regex.scan(~r/<Key>([^<]+)<\/Key>/, body)
        |> Enum.each(fn [_, key] ->
          Req.delete("#{@base_url}/#{bucket}/#{key}")
        end)

      _ ->
        :ok
    end

    Req.delete("#{@base_url}/#{bucket}")
  end

  defp enable_cloud_cache(bucket, remote_bucket) do
    :ok =
      CloudCacheConfig.set_config(bucket, %{
        enabled: true,
        provider: :s3_compat,
        endpoint: @base_url,
        region: "us-east-1",
        bucket: remote_bucket,
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key",
        cache_enabled: false
      })

    on_exit(fn -> CloudCacheConfig.delete_config(bucket) end)
  end

  describe "health check" do
    test "GET /health returns 200" do
      {:ok, resp} = Req.get("#{@base_url}/health")
      assert resp.status == 200
      assert resp.body == %{"status" => "ok"}
    end
  end

  describe "bucket operations" do
    test "create, head, list, and delete bucket" do
      bucket = unique_bucket()

      # Create bucket (S3 returns 201 Created)
      {:ok, resp} = Req.put("#{@base_url}/#{bucket}", body: "")
      assert resp.status == 201

      # Head bucket
      {:ok, resp} = Req.head("#{@base_url}/#{bucket}")
      assert resp.status == 200

      # List buckets
      {:ok, resp} = Req.get("#{@base_url}/")
      assert resp.status == 200
      assert String.contains?(resp.body, bucket)

      # Delete bucket
      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}")
      assert resp.status == 204

      # Head should now 404
      {:ok, resp} = Req.head("#{@base_url}/#{bucket}")
      assert resp.status == 404
    end

    test "create duplicate bucket returns 409" do
      bucket = create_bucket(unique_bucket())

      {:ok, resp} = Req.put("#{@base_url}/#{bucket}", body: "")
      assert resp.status == 409

      cleanup_bucket(bucket)
    end

    test "delete non-existent bucket returns 404" do
      {:ok, resp} = Req.delete("#{@base_url}/nonexistent-bucket-#{:rand.uniform(99999)}")
      assert resp.status == 404
    end
  end

  describe "object operations" do
    test "put and get object" do
      bucket = create_bucket(unique_bucket())
      body = "Hello, S3!"

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}/hello.txt",
          body: body,
          headers: [{"content-type", "text/plain"}]
        )

      assert resp.status == 200
      assert Map.has_key?(resp.headers, "etag")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/hello.txt")
      assert resp.status == 200
      assert resp.body == body

      cleanup_bucket(bucket)
    end

    test "head object returns metadata" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/meta.txt",
        body: "test content",
        headers: [{"content-type", "text/plain"}]
      )

      {:ok, resp} = Req.head("#{@base_url}/#{bucket}/meta.txt")
      assert resp.status == 200
      assert resp.headers["content-type"] == ["text/plain"]
      assert resp.headers["content-length"] == ["12"]

      cleanup_bucket(bucket)
    end

    test "get non-existent object returns 404" do
      bucket = create_bucket(unique_bucket())

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/nonexistent.txt")
      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchKey")

      cleanup_bucket(bucket)
    end

    test "put object to non-existent bucket returns 404" do
      {:ok, resp} =
        Req.put("#{@base_url}/no-such-bucket-#{:rand.uniform(99999)}/file.txt", body: "data")

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchBucket")
    end

    test "delete object" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/to-delete.txt", body: "delete me")

      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}/to-delete.txt")
      assert resp.status == 204

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/to-delete.txt")
      assert resp.status == 404

      cleanup_bucket(bucket)
    end

    test "delete non-existent object returns 204" do
      bucket = create_bucket(unique_bucket())

      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}/no-such-key.txt")
      assert resp.status == 204

      cleanup_bucket(bucket)
    end

    test "list objects" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/a.txt", body: "a")
      Req.put("#{@base_url}/#{bucket}/b.txt", body: "b")
      Req.put("#{@base_url}/#{bucket}/dir/c.txt", body: "c")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?list-type=2")
      assert resp.status == 200
      assert String.contains?(resp.body, "a.txt")
      assert String.contains?(resp.body, "b.txt")
      assert String.contains?(resp.body, "dir/c.txt")

      cleanup_bucket(bucket)
    end

    test "list objects with prefix" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/photos/1.jpg", body: "img1")
      Req.put("#{@base_url}/#{bucket}/photos/2.jpg", body: "img2")
      Req.put("#{@base_url}/#{bucket}/docs/readme.md", body: "readme")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?list-type=2&prefix=photos/")
      assert resp.status == 200
      assert String.contains?(resp.body, "photos/1.jpg")
      assert String.contains?(resp.body, "photos/2.jpg")
      refute String.contains?(resp.body, "docs/readme.md")

      cleanup_bucket(bucket)
    end

    test "list objects with delimiter" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/a.txt", body: "a")
      Req.put("#{@base_url}/#{bucket}/folder/b.txt", body: "b")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?list-type=2&delimiter=/")
      assert resp.status == 200
      assert String.contains?(resp.body, "a.txt")

      assert String.contains?(
               resp.body,
               "<CommonPrefixes><Prefix>folder/</Prefix></CommonPrefixes>"
             )

      cleanup_bucket(bucket)
    end

    test "copy object" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/source.txt",
        body: "copy me",
        headers: [{"content-type", "text/plain"}]
      )

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}/dest.txt",
          body: "",
          headers: [{"x-amz-copy-source", "/#{bucket}/source.txt"}]
        )

      assert resp.status == 200
      assert String.contains?(resp.body, "CopyObjectResult")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/dest.txt")
      assert resp.status == 200
      assert resp.body == "copy me"

      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}/dest.txt")
      assert resp.status == 204

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/source.txt")
      assert resp.status == 200
      assert resp.body == "copy me"

      cleanup_bucket(bucket)
    end

    test "cloud-backed GET and HEAD honor a local delete marker before upstream content" do
      bucket = create_bucket(unique_bucket())
      remote_bucket = create_bucket(unique_bucket())
      key = "marker-shadow.txt"
      upstream_body = "upstream content must stay hidden"

      enable_cloud_cache(bucket, remote_bucket)

      versioning_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
      """

      {:ok, %{status: 200}} =
        Req.put("#{@base_url}/#{bucket}?versioning", body: versioning_xml)

      {:ok, %{status: 200}} = Req.put("#{@base_url}/#{bucket}/#{key}", body: "local version")
      {:ok, delete_resp} = Req.delete("#{@base_url}/#{bucket}/#{key}")
      assert delete_resp.status == 204
      assert Req.Response.get_header(delete_resp, "x-amz-delete-marker") == ["true"]
      assert [marker_id] = Req.Response.get_header(delete_resp, "x-amz-version-id")

      {:ok, %{status: 200}} =
        Req.put("#{@base_url}/#{remote_bucket}/#{key}", body: upstream_body)

      assert {:ok, %{status: 200, body: ^upstream_body}} =
               Req.get("#{@base_url}/#{remote_bucket}/#{key}")

      {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
      assert get_resp.status == 404
      assert Req.Response.get_header(get_resp, "x-amz-delete-marker") == ["true"]
      assert Req.Response.get_header(get_resp, "x-amz-version-id") == [marker_id]

      {:ok, head_resp} = Req.head("#{@base_url}/#{bucket}/#{key}")
      assert head_resp.status == 404
      assert Req.Response.get_header(head_resp, "x-amz-delete-marker") == ["true"]
      assert Req.Response.get_header(head_resp, "x-amz-version-id") == [marker_id]

      CloudCacheConfig.delete_config(bucket)
      cleanup_bucket(bucket)
      cleanup_bucket(remote_bucket)
    end

    test "copy object to cloud-backed bucket fails when upstream write fails" do
      source_bucket = create_bucket(unique_bucket())
      dest_bucket = create_bucket(unique_bucket())
      missing_remote_bucket = "missing-upstream-#{:erlang.unique_integer([:positive])}"

      enable_cloud_cache(dest_bucket, missing_remote_bucket)

      Req.put("#{@base_url}/#{source_bucket}/source.txt",
        body: "copy me",
        headers: [{"content-type", "text/plain"}]
      )

      {:ok, resp} =
        Req.put("#{@base_url}/#{dest_bucket}/dest.txt",
          body: "",
          headers: [{"x-amz-copy-source", "/#{source_bucket}/source.txt"}]
        )

      assert resp.status == 500
      assert String.contains?(resp.body, "InternalError")

      {:ok, resp} = Req.get("#{@base_url}/#{dest_bucket}/dest.txt")
      assert resp.status == 404

      CloudCacheConfig.delete_config(dest_bucket)
      cleanup_bucket(source_bucket)
      cleanup_bucket(dest_bucket)
    end

    test "copy object to cloud-backed bucket fails when source content is missing" do
      source_bucket = create_bucket(unique_bucket())
      dest_bucket = create_bucket(unique_bucket())
      remote_bucket = create_bucket(unique_bucket())

      enable_cloud_cache(dest_bucket, remote_bucket)

      now = DateTime.utc_now() |> DateTime.to_iso8601()

      :ok =
        Metadata.put_object_meta(source_bucket, "ghost.txt", %{
          content_hash: String.duplicate("0", 64),
          size: 5,
          etag: "missing",
          content_type: "text/plain",
          metadata: %{},
          created_at: now,
          updated_at: now
        })

      {:ok, resp} =
        Req.put("#{@base_url}/#{dest_bucket}/ghost-copy.txt",
          body: "",
          headers: [{"x-amz-copy-source", "/#{source_bucket}/ghost.txt"}]
        )

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchKey")

      {:ok, resp} = Req.get("#{@base_url}/#{dest_bucket}/ghost-copy.txt")
      assert resp.status == 404

      CloudCacheConfig.delete_config(dest_bucket)
      cleanup_bucket(source_bucket)
      cleanup_bucket(dest_bucket)
      cleanup_bucket(remote_bucket)
    end

    test "cloud-backed put rejects malformed aws-chunked body" do
      bucket = create_bucket(unique_bucket())
      remote_bucket = create_bucket(unique_bucket())

      enable_cloud_cache(bucket, remote_bucket)

      malformed_body = "10;chunk-signature=abc\r\nshort\r\n"

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}/bad-chunked.txt",
          body: malformed_body,
          headers: [
            {"content-encoding", "aws-chunked"},
            {"x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}
          ]
        )

      assert resp.status == 400
      assert String.contains?(resp.body, "InvalidRequest")

      CloudCacheConfig.delete_config(bucket)
      cleanup_bucket(bucket)
      cleanup_bucket(remote_bucket)
    end

    test "local put streams aws-chunked framing into decoded object bytes" do
      bucket = create_bucket(unique_bucket())

      encoded_body =
        "5;chunk-signature=abc\r\nhello\r\n" <>
          "6;chunk-signature=def\r\n world\r\n" <>
          "0;chunk-signature=ghi\r\n\r\n"

      {:ok, put_response} =
        Req.put("#{@base_url}/#{bucket}/streamed-chunked.txt",
          body: encoded_body,
          headers: [
            {"content-encoding", "aws-chunked"},
            {"x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}
          ]
        )

      assert put_response.status == 200

      assert {:ok, %{status: 200, body: "hello world"}} =
               Req.get("#{@base_url}/#{bucket}/streamed-chunked.txt")

      cleanup_bucket(bucket)
    end

    test "delete multiple objects" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/del1.txt", body: "1")
      Req.put("#{@base_url}/#{bucket}/del2.txt", body: "2")

      delete_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <Delete>
        <Object><Key>del1.txt</Key></Object>
        <Object><Key>del2.txt</Key></Object>
      </Delete>
      """

      {:ok, resp} =
        Req.post("#{@base_url}/#{bucket}?delete",
          body: delete_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 200
      assert String.contains?(resp.body, "Deleted")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/del1.txt")
      assert resp.status == 404

      cleanup_bucket(bucket)
    end

    test "delete non-empty bucket returns 409" do
      bucket = create_bucket(unique_bucket())

      Req.put("#{@base_url}/#{bucket}/file.txt", body: "data")

      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}")
      assert resp.status == 409
      assert String.contains?(resp.body, "BucketNotEmpty")

      cleanup_bucket(bucket)
    end
  end

  describe "custom metadata" do
    test "put and get object with custom metadata" do
      bucket = create_bucket(unique_bucket())

      {:ok, _} =
        Req.put("#{@base_url}/#{bucket}/meta-test.txt",
          body: "content",
          headers: [
            {"content-type", "text/plain"},
            {"x-amz-meta-author", "test-user"},
            {"x-amz-meta-project", "example"}
          ]
        )

      {:ok, resp} = Req.head("#{@base_url}/#{bucket}/meta-test.txt")
      assert resp.status == 200
      assert resp.headers["x-amz-meta-author"] == ["test-user"]
      assert resp.headers["x-amz-meta-project"] == ["example"]

      cleanup_bucket(bucket)
    end
  end
end
