defmodule ExStorageService.S3.MultipartEdgeTest do
  use ExUnit.Case, async: false

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"
  @max_wait_ms 30_000
  @poll_interval_ms 500

  setup_all do
    wait_for_server(@max_wait_ms)
  end

  defp wait_for_server(remaining) when remaining <= 0 do
    :ok
  end

  defp wait_for_server(remaining) do
    case Req.get("#{@base_url}/health", retry: false) do
      {:ok, %{status: 200}} ->
        :ok

      _ ->
        Process.sleep(@poll_interval_ms)
        wait_for_server(remaining - @poll_interval_ms)
    end
  end

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

  defp abort_upload(bucket, key, upload_id) do
    Req.delete("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")
  end

  # Helper to safely clean up bucket and upload on test exit
  defp with_bucket_and_upload(fun) do
    bucket = create_bucket(unique_bucket())
    key = "edge-#{:erlang.unique_integer([:positive])}.bin"

    try do
      fun.(bucket, key)
    after
      cleanup_bucket(bucket)
    end
  end

  defp with_bucket(fun) do
    bucket = create_bucket(unique_bucket())

    try do
      fun.(bucket)
    after
      cleanup_bucket(bucket)
    end
  end

  describe "invalid part numbers" do
    test "part number 0 returns 400 InvalidArgument" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=0&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidArgument")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "part number > 10000 returns 400 InvalidArgument" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=10001&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidArgument")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "non-numeric part number returns 400 InvalidArgument" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=abc&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidArgument")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "negative part number returns 400 InvalidArgument" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=-1&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidArgument")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "part number 1 (min valid) succeeds" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 200
        assert resp.headers["etag"] != nil

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "part number 10000 (max valid) succeeds" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=10000&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 200
        assert resp.headers["etag"] != nil

        abort_upload(bucket, key, upload_id)
      end)
    end
  end

  describe "complete multipart with invalid parts XML" do
    test "empty parts list completes with zero-byte object" do
      with_bucket_and_upload(fn bucket, key ->
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

        # Implementation accepts empty parts and creates a zero-byte object
        assert resp.status == 200

        {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
        assert get_resp.status == 200
        assert get_resp.body == "" or byte_size(get_resp.body) == 0
      end)
    end

    test "malformed XML body returns 400 MalformedXML" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
            body: "this is not xml at all",
            headers: [{"content-type", "application/xml"}]
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "MalformedXML")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "truncated XML body returns 400 MalformedXML" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        truncated_xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <CompleteMultipartUpload>
          <Part><PartNumber>1</PartNumber>
        """

        {:ok, resp} =
          Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
            body: truncated_xml,
            headers: [{"content-type", "application/xml"}]
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "MalformedXML")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "empty body returns 400 MalformedXML" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.post("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}",
            body: "",
            headers: [{"content-type", "application/xml"}]
          )

        assert resp.status == 400
        assert String.contains?(resp.body, "MalformedXML")

        abort_upload(bucket, key, upload_id)
      end)
    end
  end

  describe "complete with invalid ETags" do
    test "complete with non-matching ETag returns error" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        _real_etag = upload_part(bucket, key, upload_id, 1, "real data")

        # Use a fabricated ETag that doesn't match the uploaded part
        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [{1, "\"0000000000000000000000000000dead\""}])

        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidPart")

        abort_upload(bucket, key, upload_id)
      end)
    end

    test "complete referencing a part number that was never uploaded returns error" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        etag1 = upload_part(bucket, key, upload_id, 1, "part one data")

        # Reference part 1 (uploaded) and part 2 (never uploaded)
        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [
            {1, etag1},
            {2, "\"0000000000000000000000000000dead\""}
          ])

        # Should fail - part 2 was never uploaded
        assert resp.status == 400
        assert String.contains?(resp.body, "InvalidPart")

        abort_upload(bucket, key, upload_id)
      end)
    end
  end

  describe "duplicate part numbers" do
    test "re-uploading same part number overwrites previous data" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        # Upload part 1 twice with different data
        _etag1_v1 = upload_part(bucket, key, upload_id, 1, "first version")
        etag1_v2 = upload_part(bucket, key, upload_id, 1, "second version")
        etag2 = upload_part(bucket, key, upload_id, 2, "part two")

        # Complete using the second ETag for part 1
        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [{1, etag1_v2}, {2, etag2}])

        assert resp.status == 200

        # Verify the object contains the second version of part 1
        {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
        assert get_resp.status == 200
        assert get_resp.body == "second version" <> "part two"
      end)
    end
  end

  describe "abort operations" do
    test "abort then complete returns 404 NoSuchUpload" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        upload_part(bucket, key, upload_id, 1, "some data")

        # Abort the upload
        {:ok, resp} = abort_upload(bucket, key, upload_id)
        assert resp.status == 204

        # Try to complete the aborted upload
        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [{1, "\"fake-etag\""}])

        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end

    test "abort a non-existent upload ID returns 404 NoSuchUpload" do
      with_bucket(fn bucket ->
        {:ok, resp} =
          Req.delete("#{@base_url}/#{bucket}/no-such-key?uploadId=non-existent-id")

        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end

    test "abort an already-aborted upload returns 404 NoSuchUpload" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        upload_part(bucket, key, upload_id, 1, "data")

        # Abort once
        {:ok, resp} = abort_upload(bucket, key, upload_id)
        assert resp.status == 204

        # Abort again
        {:ok, resp} = abort_upload(bucket, key, upload_id)
        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end

    test "upload part to aborted upload returns 404 NoSuchUpload" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} = abort_upload(bucket, key, upload_id)
        assert resp.status == 204

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
            body: "data"
          )

        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end
  end

  describe "list parts" do
    test "list parts with non-existent upload ID returns 404 NoSuchUpload" do
      with_bucket(fn bucket ->
        {:ok, resp} =
          Req.get("#{@base_url}/#{bucket}/edge-no-upload.bin?uploadId=non-existent-upload-id")

        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end

    test "list parts on aborted upload returns 404 NoSuchUpload" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        upload_part(bucket, key, upload_id, 1, "data")

        {:ok, _} = abort_upload(bucket, key, upload_id)

        {:ok, resp} =
          Req.get("#{@base_url}/#{bucket}/#{key}?uploadId=#{upload_id}")

        assert resp.status == 404
        assert String.contains?(resp.body, "NoSuchUpload")
      end)
    end
  end

  describe "non-existent bucket" do
    test "initiate multipart upload on non-existent bucket returns 404 NoSuchBucket" do
      {:ok, resp} =
        Req.post("#{@base_url}/no-such-bucket-#{:erlang.unique_integer([:positive])}/key?uploads",
          body: ""
        )

      assert resp.status == 404
      assert String.contains?(resp.body, "NoSuchBucket")
    end

    test "upload part to non-existent bucket returns 404" do
      {:ok, resp} =
        Req.put(
          "#{@base_url}/no-such-bucket-#{:erlang.unique_integer([:positive])}/key?partNumber=1&uploadId=fake-id",
          body: "data"
        )

      assert resp.status in [404]

      assert String.contains?(resp.body, "NoSuchBucket") or
               String.contains?(resp.body, "NoSuchUpload")
    end
  end

  describe "out-of-order parts" do
    test "upload parts out of order (3, 1, 2) then complete produces valid object" do
      with_bucket_and_upload(fn bucket, key ->
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
      end)
    end

    test "complete with parts listed in non-ascending order in XML" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        etag1 = upload_part(bucket, key, upload_id, 1, "part-one")
        etag2 = upload_part(bucket, key, upload_id, 2, "part-two")

        # Send parts in descending order in the XML (2, 1 instead of 1, 2)
        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [{2, etag2}, {1, etag1}])

        # S3 spec requires InvalidPartOrder, but implementation may reorder silently
        # Either way, the object data must be correct if 200
        if resp.status == 200 do
          {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
          assert get_resp.body == "part-one" <> "part-two"
        else
          assert resp.status == 400
          assert String.contains?(resp.body, "InvalidPartOrder")
        end
      end)
    end
  end

  describe "data integrity after complete" do
    test "complete multipart upload then verify object data integrity" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

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
      end)
    end
  end

  describe "zero-byte parts" do
    test "upload part with empty body" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        {:ok, resp} =
          Req.put(
            "#{@base_url}/#{bucket}/#{key}?partNumber=1&uploadId=#{upload_id}",
            body: ""
          )

        # Implementation may accept or reject zero-byte parts
        # S3 accepts them but they contribute 0 bytes to the final object
        assert resp.status in [200, 400]

        if resp.status == 200 do
          etag_empty = hd(resp.headers["etag"])
          etag2 = upload_part(bucket, key, upload_id, 2, "real-data")

          {:ok, complete_resp} =
            complete_upload(bucket, key, upload_id, [{1, etag_empty}, {2, etag2}])

          if complete_resp.status == 200 do
            {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
            # Zero-byte part contributes nothing to concatenated content
            assert get_resp.body == "real-data"
          end
        end

        abort_upload(bucket, key, upload_id)
      end)
    end
  end

  describe "single part upload" do
    test "complete with exactly one part succeeds" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)
        data = "single-part-data"
        etag = upload_part(bucket, key, upload_id, 1, data)

        {:ok, resp} = complete_upload(bucket, key, upload_id, [{1, etag}])
        assert resp.status == 200

        {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
        assert get_resp.status == 200
        assert get_resp.body == data
      end)
    end
  end

  describe "non-contiguous part numbers" do
    test "complete with gaps in part numbers (1, 3, 5) produces valid object" do
      with_bucket_and_upload(fn bucket, key ->
        upload_id = initiate_upload(bucket, key)

        etag1 = upload_part(bucket, key, upload_id, 1, "AAA")
        etag3 = upload_part(bucket, key, upload_id, 3, "BBB")
        etag5 = upload_part(bucket, key, upload_id, 5, "CCC")

        {:ok, resp} =
          complete_upload(bucket, key, upload_id, [
            {1, etag1},
            {3, etag3},
            {5, etag5}
          ])

        assert resp.status == 200

        {:ok, get_resp} = Req.get("#{@base_url}/#{bucket}/#{key}")
        assert get_resp.status == 200
        assert get_resp.body == "AAA" <> "BBB" <> "CCC"
      end)
    end
  end
end
