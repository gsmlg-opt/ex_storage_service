defmodule ExStorageServiceS3.MultipartTest do
  use ExUnit.Case, async: false

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "mpu-test-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, _} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  defp cleanup_bucket(bucket) do
    case Req.get("#{@base_url}/#{bucket}?list-type=2") do
      {:ok, %{status: 200, body: body}} ->
        Regex.scan(~r/<Key>([^<]+)<\/Key>/, body)
        |> Enum.each(fn [_, key] ->
          Req.delete("#{@base_url}/#{bucket}/#{key}")
        end)

      _ ->
        :ok
    end

    Req.delete("#{@base_url}/#{bucket}")
  end

  describe "multipart upload: full lifecycle" do
    test "create -> upload parts -> complete -> verify object exists" do
      bucket = create_bucket(unique_bucket())
      key = "multipart-file.bin"

      # 1. Initiate multipart upload
      {:ok, resp} = Req.post("#{@base_url}/#{bucket}/#{key}?uploads", body: "")
      assert resp.status == 200
      assert String.contains?(resp.body, "InitiateMultipartUploadResult")
      assert String.contains?(resp.body, "<Key>#{key}</Key>")

      # Extract upload ID
      [_, upload_id] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, resp.body)
      assert is_binary(upload_id) and upload_id != ""

      # 2. Upload parts
      part1_data = String.duplicate("A", 1024)
      part2_data = String.duplicate("B", 2048)
      part3_data = String.duplicate("C", 512)

      {:ok, resp1} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
          body: part1_data
        )

      assert resp1.status == 200
      [etag1] = resp1.headers["etag"]
      assert etag1 != nil

      {:ok, resp2} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=2&uploadId=#{upload_id}",
          body: part2_data
        )

      assert resp2.status == 200
      [etag2] = resp2.headers["etag"]

      {:ok, resp3} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=3&uploadId=#{upload_id}",
          body: part3_data
        )

      assert resp3.status == 200
      [etag3] = resp3.headers["etag"]

      # 3. Complete multipart upload
      complete_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>#{etag1}</ETag></Part>
        <Part><PartNumber>2</PartNumber><ETag>#{etag2}</ETag></Part>
        <Part><PartNumber>3</PartNumber><ETag>#{etag3}</ETag></Part>
      </CompleteMultipartUpload>
      """

      {:ok, resp} =
        Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
          body: complete_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 200
      assert String.contains?(resp.body, "CompleteMultipartUploadResult")
      assert String.contains?(resp.body, "<Key>#{key}</Key>")

      # 4. Verify the object exists and has correct content
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
      assert resp.status == 200
      assert resp.body == part1_data <> part2_data <> part3_data

      # 5. Verify HEAD returns correct size
      {:ok, resp} = Req.head("#{@base_url}/#{bucket}/#{key}")
      assert resp.status == 200
      expected_size = byte_size(part1_data) + byte_size(part2_data) + byte_size(part3_data)
      assert resp.headers["content-length"] == [to_string(expected_size)]

      cleanup_bucket(bucket)
    end
  end

  describe "multipart upload: abort" do
    test "abort upload cleans up" do
      bucket = create_bucket(unique_bucket())
      key = "abort-test.bin"

      # Initiate
      {:ok, resp} = Req.post("#{@base_url}/#{bucket}/#{key}?uploads", body: "")
      assert resp.status == 200
      [_, upload_id] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, resp.body)

      # Upload a part
      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
          body: "some data"
        )

      assert resp.status == 200

      # Abort
      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      assert resp.status == 204

      # Verify upload no longer exists (list parts should fail)
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchUpload")

      # Object should not exist
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
      assert resp.status == 404

      cleanup_bucket(bucket)
    end
  end

  describe "multipart upload: list parts" do
    test "list parts returns uploaded parts" do
      bucket = create_bucket(unique_bucket())
      key = "list-parts-test.bin"

      # Initiate
      {:ok, resp} = Req.post("#{@base_url}/#{bucket}/#{key}?uploads", body: "")
      [_, upload_id] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, resp.body)

      # Upload two parts
      {:ok, _} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
          body: "part one data"
        )

      {:ok, _} =
        Req.put("#{@base_url}/#{bucket}/#{key}?partNumber=2&uploadId=#{upload_id}",
          body: "part two data!!"
        )

      # List parts
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      assert resp.status == 200
      assert String.contains?(resp.body, "ListPartsResult")
      assert String.contains?(resp.body, "<PartNumber>1</PartNumber>")
      assert String.contains?(resp.body, "<PartNumber>2</PartNumber>")
      assert String.contains?(resp.body, "<UploadId>#{upload_id}</UploadId>")

      # Verify sizes are present
      assert String.contains?(resp.body, "<Size>#{byte_size("part one data")}</Size>")
      assert String.contains?(resp.body, "<Size>#{byte_size("part two data!!")}</Size>")

      # Cleanup
      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end
  end

  describe "multipart upload: error cases" do
    test "create multipart upload on non-existent bucket returns 404" do
      {:ok, resp} =
        Req.post("#{@base_url}/no-such-bucket-#{:rand.uniform(99999)}/file.bin?uploads", body: "")

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchBucket")
    end

    test "upload part with invalid upload ID returns 404" do
      bucket = create_bucket(unique_bucket())

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}/file.bin?partNumber=1&uploadId=nonexistent",
          body: "data"
        )

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchUpload")

      cleanup_bucket(bucket)
    end

    test "complete with invalid upload ID returns 404" do
      bucket = create_bucket(unique_bucket())

      complete_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
      </CompleteMultipartUpload>
      """

      {:ok, resp} =
        Req.post("#{@base_url}/#{bucket}/file.bin?uploadId=nonexistent",
          body: complete_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchUpload")

      cleanup_bucket(bucket)
    end
  end
end
