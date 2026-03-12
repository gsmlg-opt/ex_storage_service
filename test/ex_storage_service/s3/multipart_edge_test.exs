defmodule ExStorageService.S3.MultipartEdgeTest do
  use ExUnit.Case, async: false

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "mpu-edge-#{:erlang.unique_integer([:positive])}"

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

  defp initiate_upload(bucket, key) do
    {:ok, resp} = Req.post("#{@base_url}/#{bucket}/#{key}?uploads", body: "")
    assert resp.status == 200
    [_, upload_id] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, resp.body)
    upload_id
  end

  defp upload_part(bucket, key, upload_id, part_number, data) do
    {:ok, resp} =
      Req.put(
        "#{@base_url}/#{bucket}/#{key}?partNumber=#{part_number}&uploadId=#{upload_id}",
        body: data
      )

    assert resp.status == 200
    [etag] = resp.headers["etag"]
    etag
  end

  defp complete_upload(bucket, key, upload_id, parts) do
    parts_xml =
      Enum.map(parts, fn {part_number, etag} ->
        "<Part><PartNumber>#{part_number}</PartNumber><ETag>#{etag}</ETag></Part>"
      end)
      |> Enum.join("\n    ")

    complete_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUpload>
      #{parts_xml}
    </CompleteMultipartUpload>
    """

    Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
      body: complete_xml,
      headers: [{"content-type", "application/xml"}]
    )
  end

  describe "invalid part numbers" do
    test "upload part with part number 0 returns 400 InvalidArgument" do
      bucket = create_bucket(unique_bucket())
      key = "edge-pn0.bin"
      upload_id = initiate_upload(bucket, key)

      {:ok, resp} =
        Req.put(
          "#{@base_url}/#{bucket}/#{key}?partNumber=0&uploadId=#{upload_id}",
          body: "data"
        )

      assert resp.status == 400
      assert String.contains?(resp.body, "InvalidArgument")

      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end

    test "upload part with part number > 10000 returns 400 InvalidArgument" do
      bucket = create_bucket(unique_bucket())
      key = "edge-pn-big.bin"
      upload_id = initiate_upload(bucket, key)

      {:ok, resp} =
        Req.put(
          "#{@base_url}/#{bucket}/#{key}?partNumber=10001&uploadId=#{upload_id}",
          body: "data"
        )

      assert resp.status == 400
      assert String.contains?(resp.body, "InvalidArgument")

      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end

    test "upload part with non-numeric part number returns 400 InvalidArgument" do
      bucket = create_bucket(unique_bucket())
      key = "edge-pn-nan.bin"
      upload_id = initiate_upload(bucket, key)

      {:ok, resp} =
        Req.put(
          "#{@base_url}/#{bucket}/#{key}?partNumber=abc&uploadId=#{upload_id}",
          body: "data"
        )

      assert resp.status == 400
      assert String.contains?(resp.body, "InvalidArgument")

      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end
  end

  describe "complete multipart with empty parts" do
    test "complete multipart upload with empty parts XML handles gracefully" do
      bucket = create_bucket(unique_bucket())
      key = "edge-empty-parts.bin"
      upload_id = initiate_upload(bucket, key)

      empty_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <CompleteMultipartUpload>
      </CompleteMultipartUpload>
      """

      {:ok, resp} =
        Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
          body: empty_xml,
          headers: [{"content-type", "application/xml"}]
        )

      # Should return an error (400 MalformedXML) or handle gracefully
      assert resp.status in [400, 200]

      # Cleanup: abort in case it wasn't completed
      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end
  end

  describe "abort then complete" do
    test "abort upload then attempt to complete returns 404 NoSuchUpload" do
      bucket = create_bucket(unique_bucket())
      key = "edge-abort-complete.bin"
      upload_id = initiate_upload(bucket, key)

      # Upload a part so the upload has content
      upload_part(bucket, key, upload_id, 1, "some data")

      # Abort the upload
      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      assert resp.status == 204

      # Try to complete the aborted upload
      {:ok, resp} =
        complete_upload(bucket, key, upload_id, [{1, "\"fake-etag\""}])

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchUpload")

      cleanup_bucket(bucket)
    end
  end

  describe "list parts on non-existent upload" do
    test "list parts with non-existent upload ID returns 404 NoSuchUpload" do
      bucket = create_bucket(unique_bucket())
      key = "edge-no-upload.bin"

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/#{key}?uploadId=non-existent-upload-id")

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchUpload")

      cleanup_bucket(bucket)
    end
  end

  describe "out-of-order parts" do
    test "upload parts out of order (3, 1, 2) then complete produces valid object" do
      bucket = create_bucket(unique_bucket())
      key = "edge-ooo.bin"
      upload_id = initiate_upload(bucket, key)

      part1_data = "AAAA-part-one"
      part2_data = "BBBB-part-two"
      part3_data = "CCCC-part-three"

      # Upload in order 3, 1, 2
      etag3 = upload_part(bucket, key, upload_id, 3, part3_data)
      etag1 = upload_part(bucket, key, upload_id, 1, part1_data)
      etag2 = upload_part(bucket, key, upload_id, 2, part2_data)

      # Complete with parts in ascending order
      {:ok, resp} =
        complete_upload(bucket, key, upload_id, [
          {1, etag1},
          {2, etag2},
          {3, etag3}
        ])

      assert resp.status == 200
      assert String.contains?(resp.body, "CompleteMultipartUploadResult")

      # Verify the object data is concatenated in part-number order
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
      assert resp.status == 200
      assert resp.body == part1_data <> part2_data <> part3_data

      cleanup_bucket(bucket)
    end
  end

  describe "data integrity after complete" do
    test "complete multipart upload then verify object data integrity" do
      bucket = create_bucket(unique_bucket())
      key = "edge-integrity.bin"
      upload_id = initiate_upload(bucket, key)

      # Use distinct, recognizable content per part
      part1_data = String.duplicate("X", 256)
      part2_data = String.duplicate("Y", 512)
      part3_data = String.duplicate("Z", 128)

      etag1 = upload_part(bucket, key, upload_id, 1, part1_data)
      etag2 = upload_part(bucket, key, upload_id, 2, part2_data)
      etag3 = upload_part(bucket, key, upload_id, 3, part3_data)

      {:ok, resp} =
        complete_upload(bucket, key, upload_id, [
          {1, etag1},
          {2, etag2},
          {3, etag3}
        ])

      assert resp.status == 200

      # GET the object and verify exact content
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
      assert resp.status == 200

      expected = part1_data <> part2_data <> part3_data
      assert resp.body == expected
      assert byte_size(resp.body) == 256 + 512 + 128

      # HEAD should report correct content-length
      {:ok, head_resp} = Req.head("#{@base_url}/#{bucket}/#{key}")
      assert head_resp.status == 200
      assert head_resp.headers["content-length"] == [to_string(256 + 512 + 128)]

      cleanup_bucket(bucket)
    end
  end

  describe "negative part number" do
    test "upload part with negative part number returns 400 InvalidArgument" do
      bucket = create_bucket(unique_bucket())
      key = "edge-neg-pn.bin"
      upload_id = initiate_upload(bucket, key)

      {:ok, resp} =
        Req.put(
          "#{@base_url}/#{bucket}/#{key}?partNumber=-1&uploadId=#{upload_id}",
          body: "data"
        )

      assert resp.status == 400
      assert String.contains?(resp.body, "InvalidArgument")

      Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
      cleanup_bucket(bucket)
    end
  end
end
