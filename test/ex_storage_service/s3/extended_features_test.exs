defmodule ExStorageService.S3.ExtendedFeaturesTest do
  use ExUnit.Case, async: false

  alias ExStorageService.S3.Presigned
  alias ExStorageService.Storage.Versioning
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Notifications

  @s3_port Application.compile_env(:ex_storage_service, :s3_port, 9001)
  @base_url "http://localhost:#{@s3_port}"

  defp unique_bucket, do: "test-ext-#{:erlang.unique_integer([:positive])}"

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

    # Clean up Concord keys
    Concord.delete("bucket_versioning:#{bucket}")
    Concord.delete("lifecycle:#{bucket}")
    Concord.delete("notification:#{bucket}")
  end

  describe "pre-signed URL generation" do
    test "generates a valid URL with required query parameters" do
      url =
        Presigned.generate_url("my-bucket", "my-key.txt",
          access_key_id: "AKIAIOSFODNN7EXAMPLE",
          secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          region: "us-east-1",
          host: "localhost:9000",
          scheme: "http"
        )

      uri = URI.parse(url)
      assert uri.scheme == "http"
      assert uri.host == "localhost"
      assert uri.path == "/my-bucket/my-key.txt"

      query = URI.decode_query(uri.query)
      assert query["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
      assert query["X-Amz-Credential"] =~ "AKIAIOSFODNN7EXAMPLE"
      assert query["X-Amz-Credential"] =~ "/us-east-1/s3/aws4_request"
      assert query["X-Amz-Expires"] == "3600"
      assert query["X-Amz-SignedHeaders"] == "host"
      assert query["X-Amz-Date"] =~ ~r/^\d{8}T\d{6}Z$/
      assert Map.has_key?(query, "X-Amz-Signature")
      assert String.length(query["X-Amz-Signature"]) == 64
    end

    test "respects custom expiry" do
      url =
        Presigned.generate_url("bucket", "key",
          access_key_id: "AKID",
          secret_access_key: "secret",
          expires: 300
        )

      query = URI.decode_query(URI.parse(url).query)
      assert query["X-Amz-Expires"] == "300"
    end

    test "clamps expiry to maximum of 604800 seconds" do
      url =
        Presigned.generate_url("bucket", "key",
          access_key_id: "AKID",
          secret_access_key: "secret",
          expires: 999_999
        )

      query = URI.decode_query(URI.parse(url).query)
      assert query["X-Amz-Expires"] == "604800"
    end

    test "generates consistent signatures for the same inputs" do
      opts = [
        access_key_id: "AKID",
        secret_access_key: "secret",
        region: "us-east-1",
        now: ~U[2026-01-15 10:00:00Z]
      ]

      url1 = Presigned.generate_url("bucket", "key", opts)
      url2 = Presigned.generate_url("bucket", "key", opts)

      assert url1 == url2
    end

    test "different keys produce different signatures" do
      opts = [
        access_key_id: "AKID",
        secret_access_key: "secret",
        now: ~U[2026-01-15 10:00:00Z]
      ]

      url1 = Presigned.generate_url("bucket", "key1", opts)
      url2 = Presigned.generate_url("bucket", "key2", opts)

      sig1 = URI.decode_query(URI.parse(url1).query)["X-Amz-Signature"]
      sig2 = URI.decode_query(URI.parse(url2).query)["X-Amz-Signature"]

      assert sig1 != sig2
    end
  end

  describe "pre-signed URL validation" do
    test "validates a correctly signed URL" do
      access_key = "AKID_TEST"
      secret_key = "SECRET_TEST"

      now = DateTime.utc_now()

      url =
        Presigned.generate_url("test-bucket", "test-key.txt",
          access_key_id: access_key,
          secret_access_key: secret_key,
          host: "localhost:9000",
          scheme: "http",
          now: now
        )

      uri = URI.parse(url)
      query_string = uri.query

      conn =
        Plug.Test.conn(:get, "/test-bucket/test-key.txt?#{query_string}")
        |> Map.put(:host, "localhost")
        |> Map.put(:port, 9000)
        |> Plug.Conn.fetch_query_params()

      get_secret_fn = fn
        ^access_key -> secret_key
        _ -> nil
      end

      assert {:ok, _conn} = Presigned.validate_presigned(conn, get_secret_fn)
    end

    test "rejects tampered signature" do
      url =
        Presigned.generate_url("bucket", "key",
          access_key_id: "AKID",
          secret_access_key: "secret",
          host: "localhost:9000"
        )

      # Tamper with the signature
      tampered_url =
        String.replace(
          url,
          ~r/X-Amz-Signature=[a-f0-9]+/,
          "X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000"
        )

      uri = URI.parse(tampered_url)

      conn =
        Plug.Test.conn(:get, "#{uri.path}?#{uri.query}")
        |> Map.put(:host, "localhost")
        |> Map.put(:port, 9000)
        |> Plug.Conn.fetch_query_params()

      get_secret_fn = fn
        "AKID" -> "secret"
        _ -> nil
      end

      assert {:error, "SignatureDoesNotMatch"} = Presigned.validate_presigned(conn, get_secret_fn)
    end

    test "rejects expired URL" do
      # Generate URL with timestamp in the past
      past = DateTime.add(DateTime.utc_now(), -7200, :second)

      url =
        Presigned.generate_url("bucket", "key",
          access_key_id: "AKID",
          secret_access_key: "secret",
          host: "localhost:9000",
          expires: 60,
          now: past
        )

      uri = URI.parse(url)

      conn =
        Plug.Test.conn(:get, "#{uri.path}?#{uri.query}")
        |> Map.put(:host, "localhost")
        |> Map.put(:port, 9000)
        |> Plug.Conn.fetch_query_params()

      get_secret_fn = fn
        "AKID" -> "secret"
        _ -> nil
      end

      assert {:error, "Request has expired"} = Presigned.validate_presigned(conn, get_secret_fn)
    end

    test "rejects unknown access key" do
      url =
        Presigned.generate_url("bucket", "key",
          access_key_id: "UNKNOWN_KEY",
          secret_access_key: "secret",
          host: "localhost:9000"
        )

      uri = URI.parse(url)

      conn =
        Plug.Test.conn(:get, "#{uri.path}?#{uri.query}")
        |> Map.put(:host, "localhost")
        |> Map.put(:port, 9000)
        |> Plug.Conn.fetch_query_params()

      get_secret_fn = fn _ -> nil end

      assert {:error, "InvalidAccessKeyId"} = Presigned.validate_presigned(conn, get_secret_fn)
    end
  end

  describe "bucket versioning" do
    test "default versioning state is disabled" do
      bucket = unique_bucket()
      assert Versioning.get_versioning(bucket) == :disabled
    end

    test "enable and suspend versioning" do
      bucket = unique_bucket()

      assert :ok = Versioning.set_versioning(bucket, :enabled)
      assert Versioning.get_versioning(bucket) == :enabled

      assert :ok = Versioning.set_versioning(bucket, :suspended)
      assert Versioning.get_versioning(bucket) == :suspended

      Concord.delete("bucket_versioning:#{bucket}")
    end

    test "cannot set versioning to disabled" do
      bucket = unique_bucket()
      assert {:error, :invalid_state_transition} = Versioning.set_versioning(bucket, :disabled)
    end

    test "put_version creates versions when versioning is enabled" do
      bucket = unique_bucket()
      Versioning.set_versioning(bucket, :enabled)

      meta1 = %{
        content_hash: "hash1",
        size: 10,
        etag: "etag1",
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      meta2 = %{
        content_hash: "hash2",
        size: 20,
        etag: "etag2",
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      {:ok, v1} = Versioning.put_version(bucket, "key.txt", meta1)
      {:ok, v2} = Versioning.put_version(bucket, "key.txt", meta2)

      assert v1 != v2
      assert v1 != "null"
      assert v2 != "null"

      # Latest version should be v2
      {:ok, latest} = Versioning.get_version(bucket, "key.txt", nil)
      assert latest.content_hash == "hash2"
      assert latest.version_id == v2

      # Specific version should work
      {:ok, specific} = Versioning.get_version(bucket, "key.txt", v1)
      assert specific.content_hash == "hash1"

      # List versions
      {:ok, versions} = Versioning.list_versions(bucket, "key.txt")
      assert length(versions) == 2

      # Cleanup
      Concord.delete("bucket_versioning:#{bucket}")
      Concord.delete("obj_ver:#{bucket}:key.txt:#{v1}")
      Concord.delete("obj_ver:#{bucket}:key.txt:#{v2}")
      Concord.delete("obj_ver_list:#{bucket}:key.txt")
      Concord.delete("obj:#{bucket}:key.txt")
    end

    test "delete with versioning creates delete marker" do
      bucket = unique_bucket()
      Versioning.set_versioning(bucket, :enabled)

      meta = %{
        content_hash: "hash1",
        size: 10,
        etag: "etag1",
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      {:ok, v1} = Versioning.put_version(bucket, "key.txt", meta)

      {:ok, marker_id, :delete_marker} = Versioning.delete_version(bucket, "key.txt")
      assert marker_id != v1

      # Latest version should be the delete marker
      {:ok, latest} = Versioning.get_version(bucket, "key.txt", nil)
      assert latest.is_delete_marker == true

      # Original version should still be accessible
      {:ok, original} = Versioning.get_version(bucket, "key.txt", v1)
      assert original.content_hash == "hash1"

      # Cleanup
      Concord.delete("bucket_versioning:#{bucket}")
      Concord.delete("obj_ver:#{bucket}:key.txt:#{v1}")
      Concord.delete("obj_ver:#{bucket}:key.txt:#{marker_id}")
      Concord.delete("obj_ver_list:#{bucket}:key.txt")
      Concord.delete("obj:#{bucket}:key.txt")
    end

    test "suspended versioning uses null version id" do
      bucket = unique_bucket()
      Versioning.set_versioning(bucket, :suspended)

      meta = %{
        content_hash: "hash1",
        size: 10,
        etag: "etag1",
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      {:ok, vid} = Versioning.put_version(bucket, "key.txt", meta)
      assert vid == "null"

      # Cleanup
      Concord.delete("bucket_versioning:#{bucket}")
      Concord.delete("obj_ver:#{bucket}:key.txt:null")
      Concord.delete("obj_ver_list:#{bucket}:key.txt")
      Concord.delete("obj:#{bucket}:key.txt")
    end

    test "versioning API via HTTP" do
      bucket = create_bucket(unique_bucket())

      # Get versioning — should be empty/disabled
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?versioning")
      assert resp.status == 200
      assert String.contains?(resp.body, "VersioningConfiguration")
      refute String.contains?(resp.body, "<Status>")

      # Enable versioning
      versioning_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <VersioningConfiguration>
        <Status>Enabled</Status>
      </VersioningConfiguration>
      """

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}?versioning",
          body: versioning_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 200

      # Verify versioning is enabled
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?versioning")
      assert resp.status == 200
      assert String.contains?(resp.body, "<Status>Enabled</Status>")

      cleanup_bucket(bucket)
    end
  end

  describe "lifecycle policies" do
    test "put and get lifecycle rules" do
      bucket = unique_bucket()

      rules = [
        %{
          id: "expire-logs",
          prefix: "logs/",
          status: "Enabled",
          expiration_days: 30
        },
        %{
          id: "expire-tmp",
          prefix: "tmp/",
          status: "Enabled",
          expiration_days: 1
        }
      ]

      assert :ok = Lifecycle.put_rules(bucket, rules)

      {:ok, stored_rules} = Lifecycle.get_rules(bucket)
      assert length(stored_rules) == 2
      assert Enum.any?(stored_rules, &(&1.id == "expire-logs"))
      assert Enum.any?(stored_rules, &(&1.id == "expire-tmp"))

      Concord.delete("lifecycle:#{bucket}")
    end

    test "delete lifecycle rules" do
      bucket = unique_bucket()

      Lifecycle.put_rules(bucket, [
        %{id: "test", prefix: "", status: "Enabled", expiration_days: 7}
      ])

      Lifecycle.delete_rules(bucket)
      assert {:error, :not_found} = Lifecycle.get_rules(bucket)
    end

    test "should_expire? evaluates rules correctly" do
      now = ~U[2026-03-12 12:00:00Z]
      old_date = DateTime.add(now, -31 * 86_400, :second) |> DateTime.to_iso8601()
      recent_date = DateTime.add(now, -1 * 86_400, :second) |> DateTime.to_iso8601()

      rules = [
        %{prefix: "logs/", status: "Enabled", expiration_days: 30}
      ]

      # Old object with matching prefix should expire
      assert Lifecycle.should_expire?("logs/app.log", %{created_at: old_date}, rules, now)

      # Recent object with matching prefix should not expire
      refute Lifecycle.should_expire?("logs/app.log", %{created_at: recent_date}, rules, now)

      # Old object with non-matching prefix should not expire
      refute Lifecycle.should_expire?("data/file.txt", %{created_at: old_date}, rules, now)
    end

    test "should_expire? with empty prefix matches all objects" do
      now = ~U[2026-03-12 12:00:00Z]
      old_date = DateTime.add(now, -10 * 86_400, :second) |> DateTime.to_iso8601()

      rules = [%{prefix: "", status: "Enabled", expiration_days: 7}]

      assert Lifecycle.should_expire?("any/key.txt", %{created_at: old_date}, rules, now)
    end

    test "disabled rules are ignored" do
      now = ~U[2026-03-12 12:00:00Z]
      old_date = DateTime.add(now, -31 * 86_400, :second) |> DateTime.to_iso8601()

      rules = [%{prefix: "", status: "Disabled", expiration_days: 1}]

      refute Lifecycle.should_expire?("key.txt", %{created_at: old_date}, rules, now)
    end

    test "lifecycle API via HTTP" do
      bucket = create_bucket(unique_bucket())

      # Get lifecycle — should be 404 when none configured
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?lifecycle")
      assert resp.status == 404

      # Put lifecycle rules
      lifecycle_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <LifecycleConfiguration>
        <Rule>
          <ID>expire-logs</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
        </Rule>
      </LifecycleConfiguration>
      """

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}?lifecycle",
          body: lifecycle_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 200

      # Get lifecycle rules
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?lifecycle")
      assert resp.status == 200
      assert String.contains?(resp.body, "expire-logs")
      assert String.contains?(resp.body, "30")

      # Delete lifecycle
      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}?lifecycle")
      assert resp.status == 204

      cleanup_bucket(bucket)
    end
  end

  describe "bucket notifications" do
    test "put and get notification config" do
      bucket = unique_bucket()

      configs = [
        %{
          id: "notify-create",
          events: ["s3:ObjectCreated:*"],
          endpoint: "https://example.com/webhook",
          enabled: true
        }
      ]

      assert :ok = Notifications.put_config(bucket, configs)

      {:ok, stored} = Notifications.get_config(bucket)
      assert length(stored) == 1
      assert hd(stored).id == "notify-create"
      assert hd(stored).endpoint == "https://example.com/webhook"

      Concord.delete("notification:#{bucket}")
    end

    test "delete notification config" do
      bucket = unique_bucket()

      Notifications.put_config(bucket, [
        %{id: "test", events: [], endpoint: "http://x", enabled: true}
      ])

      Notifications.delete_config(bucket)
      assert {:error, :not_found} = Notifications.get_config(bucket)
    end

    test "event_matches? with wildcard patterns" do
      assert Notifications.event_matches?(["s3:ObjectCreated:*"], "s3:ObjectCreated:Put")
      assert Notifications.event_matches?(["s3:ObjectCreated:*"], "s3:ObjectCreated:Copy")
      assert Notifications.event_matches?(["s3:ObjectRemoved:*"], "s3:ObjectRemoved:Delete")
      refute Notifications.event_matches?(["s3:ObjectCreated:*"], "s3:ObjectRemoved:Delete")
    end

    test "event_matches? with exact patterns" do
      assert Notifications.event_matches?(["s3:ObjectCreated:Put"], "s3:ObjectCreated:Put")
      refute Notifications.event_matches?(["s3:ObjectCreated:Put"], "s3:ObjectCreated:Copy")
    end

    test "build_event creates proper event structure" do
      event =
        Notifications.build_event("my-bucket", "my-key.txt", "s3:ObjectCreated:Put", %{
          "size" => 42
        })

      assert is_list(event["Records"])
      record = hd(event["Records"])
      assert record["eventName"] == "s3:ObjectCreated:Put"
      assert record["s3"]["bucket"]["name"] == "my-bucket"
      assert record["s3"]["object"]["key"] == "my-key.txt"
      assert record["s3"]["object"]["size"] == 42
      assert record["eventSource"] == "ex-storage-service"
    end

    test "notification API via HTTP" do
      bucket = create_bucket(unique_bucket())

      # Get notification — should return empty config
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?notification")
      assert resp.status == 200
      assert String.contains?(resp.body, "NotificationConfiguration")

      # Put notification config
      notification_xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <NotificationConfiguration>
        <TopicConfiguration>
          <Id>notify-all</Id>
          <Topic>https://example.com/webhook</Topic>
          <Event>s3:ObjectCreated:*</Event>
          <Event>s3:ObjectRemoved:*</Event>
        </TopicConfiguration>
      </NotificationConfiguration>
      """

      {:ok, resp} =
        Req.put("#{@base_url}/#{bucket}?notification",
          body: notification_xml,
          headers: [{"content-type", "application/xml"}]
        )

      assert resp.status == 200

      # Get notification config
      {:ok, resp} = Req.get("#{@base_url}/#{bucket}?notification")
      assert resp.status == 200
      assert String.contains?(resp.body, "notify-all")
      assert String.contains?(resp.body, "https://example.com/webhook")

      # Delete notification
      {:ok, resp} = Req.delete("#{@base_url}/#{bucket}?notification")
      assert resp.status == 204

      cleanup_bucket(bucket)
    end
  end
end
