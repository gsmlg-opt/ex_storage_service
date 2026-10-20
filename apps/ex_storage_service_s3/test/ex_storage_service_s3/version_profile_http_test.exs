defmodule ExStorageServiceS3.VersionProfileHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("version-profile-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("version-profile-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "version-profile-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)
    {:ok, %{version_id: old}} = ObjectService.put(bucket, "key", "historical", "text/plain", %{})
    {:ok, %{version_id: latest}} = ObjectService.put(bucket, "key", "current", "text/plain", %{})

    on_exit(fn ->
      case Versioning.list_versions(bucket, "key") do
        {:ok, versions} ->
          Enum.each(versions, &ObjectService.delete(bucket, "key", &1.version_id))

        _ ->
          :ok
      end

      Metadata.delete_bucket(bucket)

      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)
    end)

    %{bucket: bucket, credentials: credentials, old: old, latest: latest}
  end

  test "current and explicit version GET HEAD range and 304 identify the served version", ctx do
    for {query, version, bytes} <- [
          {"", ctx.latest, "current"},
          {"?versionId=#{ctx.old}", ctx.old, "historical"}
        ] do
      path = "/#{ctx.bucket}/key" <> query
      full = signed(ctx, :get, path)
      assert full.status == 200
      assert full.body == bytes
      assert full.headers["x-amz-version-id"] == [version]
      head = signed(ctx, :head, path)
      assert head.status == 200
      assert head.body in [nil, ""]
      assert head.headers["x-amz-version-id"] == [version]
      range = signed(ctx, :get, path, [{"range", "bytes=1-3"}])
      assert range.status == 206
      assert range.body == binary_part(bytes, 1, 3)
      assert range.headers["x-amz-version-id"] == [version]
      unchanged = signed(ctx, :get, path, [{"if-none-match", hd(full.headers["etag"])}])
      assert unchanged.status == 304
      assert unchanged.headers["x-amz-version-id"] == [version]
    end
  end

  test "explicit delete markers use 405 with identity and Last-Modified while latest uses 404",
       ctx do
    {:ok, %{version_id: marker}} = ObjectService.delete(ctx.bucket, "key", nil)

    for method <- [:get, :head] do
      current = signed(ctx, method, "/#{ctx.bucket}/key")
      assert current.status == 404
      assert current.headers["x-amz-delete-marker"] == ["true"]
      assert current.headers["x-amz-version-id"] == [marker]
      historical = signed(ctx, method, "/#{ctx.bucket}/key?versionId=#{marker}")
      assert historical.status == 405
      assert historical.headers["x-amz-delete-marker"] == ["true"]
      assert historical.headers["x-amz-version-id"] == [marker]
      assert [date] = historical.headers["last-modified"]
      assert {:ok, _} = ExStorageServiceS3.Handlers.Shared.parse_http_date(date)
      if method == :head, do: assert(historical.body in [nil, ""])
    end

    assert signed(ctx, :get, "/#{ctx.bucket}/key?versionId=#{ctx.old}").body == "historical"

    assert {:ok, %{version_id: ^marker, delete_marker: true}} =
             ObjectService.head(ctx.bucket, "key")
  end

  test "missing explicit versions report NoSuchVersion and HEAD remains empty", ctx do
    response = signed(ctx, :get, "/#{ctx.bucket}/key?versionId=missing-version")
    assert response.status == 404
    assert response.body =~ "<Code>NoSuchVersion</Code>"
    head = signed(ctx, :head, "/#{ctx.bucket}/key?versionId=missing-version")
    assert head.status == 404
    assert head.body in [nil, ""]
    assert signed(ctx, :get, "/#{ctx.bucket}/key").body == "current"
  end

  test "GET bucket versions is a version listing and never an ordinary object list", ctx do
    response = signed(ctx, :get, "/#{ctx.bucket}?versions&max-keys=1")
    assert response.status == 200
    assert response.body =~ "<ListVersionsResult"
    assert response.body =~ "<VersionId>#{ctx.latest}</VersionId>"
    assert response.body =~ "<IsLatest>true</IsLatest>"
    assert response.body =~ "<IsTruncated>true</IsTruncated>"
    assert response.body =~ "<NextKeyMarker>key</NextKeyMarker>"
    assert response.body =~ "<NextVersionIdMarker>#{ctx.latest}</NextVersionIdMarker>"
    refute response.body =~ "<Contents>"
  end

  defp signed(ctx, method, path, headers \\ []) do
    Req.request!(
      method: method,
      url: @base <> path,
      headers: headers,
      retry: false,
      decode_body: false,
      aws_sigv4: [
        access_key_id: ctx.credentials.access_key_id,
        secret_access_key: ctx.credentials.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end
end
