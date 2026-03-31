defmodule ExStorageServiceS3.ApiTest do
  use ExUnit.Case, async: false

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
