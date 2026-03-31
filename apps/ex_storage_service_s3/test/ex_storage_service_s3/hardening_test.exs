defmodule ExStorageServiceS3.HardeningTest do
  use ExUnit.Case, async: false

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "hardening-test-#{:erlang.unique_integer([:positive])}"

  defp create_bucket(bucket) do
    {:ok, _} = Req.put("#{@base_url}/#{bucket}", body: "")
    bucket
  end

  defp put_object(bucket, key, body, opts \\ []) do
    headers = Keyword.get(opts, :headers, [{"content-type", "text/plain"}])
    {:ok, resp} = Req.put("#{@base_url}/#{bucket}/#{key}", body: body, headers: headers)
    resp
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

  # ── Range Header Tests ──

  describe "Range header support" do
    test "GET with Range returns 206 Partial Content" do
      bucket = create_bucket(unique_bucket())
      content = "Hello, Range requests!"
      put_object(bucket, "range-test.txt", content)

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/range-test.txt",
          headers: [{"range", "bytes=0-4"}],
          raw: true
        )

      assert resp.status == 206
      assert resp.body == "Hello"
      assert resp.headers["content-range"] == ["bytes 0-4/#{byte_size(content)}"]
      assert resp.headers["content-length"] == ["5"]

      cleanup_bucket(bucket)
    end

    test "GET with Range suffix returns last N bytes" do
      bucket = create_bucket(unique_bucket())
      content = "Hello, Range requests!"
      put_object(bucket, "range-suffix.txt", content)

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/range-suffix.txt",
          headers: [{"range", "bytes=-9"}],
          raw: true
        )

      assert resp.status == 206
      assert resp.body == "requests!"

      cleanup_bucket(bucket)
    end

    test "GET with Range open end returns from offset to end" do
      bucket = create_bucket(unique_bucket())
      content = "Hello, Range requests!"
      put_object(bucket, "range-open.txt", content)

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/range-open.txt",
          headers: [{"range", "bytes=7-"}],
          raw: true
        )

      assert resp.status == 206
      assert resp.body == "Range requests!"

      cleanup_bucket(bucket)
    end

    test "GET with invalid Range returns 416" do
      bucket = create_bucket(unique_bucket())
      content = "short"
      put_object(bucket, "range-invalid.txt", content)

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/range-invalid.txt",
          headers: [{"range", "bytes=100-200"}],
          raw: true
        )

      assert resp.status == 416
      assert resp.headers["content-range"] == ["bytes */#{byte_size(content)}"]

      cleanup_bucket(bucket)
    end

    test "GET without Range returns 200 with accept-ranges header" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "no-range.txt", "full content")

      {:ok, resp} = Req.get("#{@base_url}/#{bucket}/no-range.txt")

      assert resp.status == 200
      assert resp.headers["accept-ranges"] == ["bytes"]

      cleanup_bucket(bucket)
    end
  end

  # ── Range Parser Unit Tests ──

  describe "parse_range/2" do
    alias ExStorageServiceS3.Handlers

    test "parses bytes=0-9 range" do
      assert Handlers.parse_range("bytes=0-9", 100) == {:ok, 0, 10}
    end

    test "parses bytes=50- open end range" do
      assert Handlers.parse_range("bytes=50-", 100) == {:ok, 50, 50}
    end

    test "parses bytes=-10 suffix range" do
      assert Handlers.parse_range("bytes=-10", 100) == {:ok, 90, 10}
    end

    test "clamps range end to file size" do
      assert Handlers.parse_range("bytes=0-999", 50) == {:ok, 0, 50}
    end

    test "returns error for start beyond file size" do
      assert Handlers.parse_range("bytes=100-200", 50) == {:error, :invalid_range}
    end

    test "returns error for reversed range" do
      assert Handlers.parse_range("bytes=50-10", 100) == {:error, :invalid_range}
    end

    test "returns error for malformed range" do
      assert Handlers.parse_range("invalid", 100) == {:error, :invalid_range}
    end

    test "returns error for bytes=- (empty range)" do
      assert Handlers.parse_range("bytes=-", 100) == {:error, :invalid_range}
    end
  end

  # ── Conditional Request Tests ──

  describe "If-None-Match (ETag conditional)" do
    test "GET returns 304 when ETag matches" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "etag-test.txt", "etag content")

      # First, get the ETag
      {:ok, head_resp} = Req.head("#{@base_url}/#{bucket}/etag-test.txt")
      [etag] = head_resp.headers["etag"]

      # Request with matching If-None-Match
      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/etag-test.txt",
          headers: [{"if-none-match", etag}],
          raw: true
        )

      assert resp.status == 304

      cleanup_bucket(bucket)
    end

    test "GET returns 200 when ETag does not match" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "etag-mismatch.txt", "some content")

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/etag-mismatch.txt",
          headers: [{"if-none-match", "\"wrong-etag\""}],
          raw: true
        )

      assert resp.status == 200

      cleanup_bucket(bucket)
    end

    test "HEAD returns 304 when ETag matches" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "head-etag.txt", "head etag test")

      {:ok, head_resp} = Req.head("#{@base_url}/#{bucket}/head-etag.txt")
      [etag] = head_resp.headers["etag"]

      {:ok, resp} =
        Req.head("#{@base_url}/#{bucket}/head-etag.txt",
          headers: [{"if-none-match", etag}]
        )

      assert resp.status == 304

      cleanup_bucket(bucket)
    end
  end

  describe "If-Modified-Since conditional" do
    test "GET returns 304 when object is not modified since" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "ims-test.txt", "ims content")

      # Use a future date so the object will not have been modified since
      future_date = "Thu, 01 Jan 2099 00:00:00 GMT"

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/ims-test.txt",
          headers: [{"if-modified-since", future_date}],
          raw: true
        )

      assert resp.status == 304

      cleanup_bucket(bucket)
    end

    test "GET returns 200 when object is modified since" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "ims-old.txt", "old content")

      # Use a past date
      past_date = "Thu, 01 Jan 2000 00:00:00 GMT"

      {:ok, resp} =
        Req.get("#{@base_url}/#{bucket}/ims-old.txt",
          headers: [{"if-modified-since", past_date}],
          raw: true
        )

      assert resp.status == 200

      cleanup_bucket(bucket)
    end

    test "HEAD returns 304 when object is not modified since" do
      bucket = create_bucket(unique_bucket())
      put_object(bucket, "head-ims.txt", "head ims")

      future_date = "Thu, 01 Jan 2099 00:00:00 GMT"

      {:ok, resp} =
        Req.head("#{@base_url}/#{bucket}/head-ims.txt",
          headers: [{"if-modified-since", future_date}]
        )

      assert resp.status == 304

      cleanup_bucket(bucket)
    end
  end

  # ── Rate Limiter Unit Tests ──

  describe "rate limiter" do
    test "allows requests under the limit" do
      # Ensure the ETS table exists
      ExStorageServiceS3.Plugs.RateLimiter.ensure_table()

      conn =
        Plug.Test.conn(:get, "/test-bucket/test-key")
        |> Map.put(:remote_ip, {192, 168, 1, 100})

      # With rate limiting disabled by default in test, just verify plug doesn't crash
      result = ExStorageServiceS3.Plugs.RateLimiter.call(conn, [])
      # Should pass through (not halted) since rate limit config defaults to enabled: true
      # but with 100 tokens, first request should always pass
      refute result.halted
    end
  end

  # ── Telemetry Tests ──

  describe "telemetry events" do
    test "get_object emits start and stop events" do
      ref = make_ref()
      parent = self()

      handler_id = "test-telemetry-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ex_storage_service, :s3, :request, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:telemetry_stop, ref, measurements, metadata})
        end,
        nil
      )

      bucket = create_bucket(unique_bucket())
      put_object(bucket, "telemetry-test.txt", "telemetry data")

      # Flush any telemetry events from setup (put_object)
      receive do
        {:telemetry_stop, ^ref, _, _} -> :ok
      after
        100 -> :ok
      end

      {:ok, _resp} = Req.get("#{@base_url}/#{bucket}/telemetry-test.txt")

      assert_receive {:telemetry_stop, ^ref, measurements, metadata}, 5000
      assert is_integer(measurements.duration)
      assert metadata.operation == :get_object
      assert metadata.bucket == bucket
      assert metadata.key == "telemetry-test.txt"

      :telemetry.detach(handler_id)
      cleanup_bucket(bucket)
    end

    test "put_object emits telemetry events" do
      ref = make_ref()
      parent = self()

      handler_id = "test-telemetry-put-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ex_storage_service, :s3, :request, :stop],
        fn _event, measurements, metadata, _config ->
          send(parent, {:telemetry_stop, ref, measurements, metadata})
        end,
        nil
      )

      bucket = create_bucket(unique_bucket())
      put_object(bucket, "telemetry-put.txt", "put telemetry")

      assert_receive {:telemetry_stop, ^ref, measurements, metadata}, 5000
      assert is_integer(measurements.duration)
      assert metadata.operation == :put_object

      :telemetry.detach(handler_id)
      cleanup_bucket(bucket)
    end
  end
end
