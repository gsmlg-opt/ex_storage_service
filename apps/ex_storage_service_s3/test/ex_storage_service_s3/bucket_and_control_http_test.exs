defmodule ExStorageServiceS3.BucketAndControlHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.{Multipart, Versioning}

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("bucket-control-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("bucket-control-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "bucket-control-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)
      Metadata.delete_bucket(bucket)
    end)

    %{credentials: credentials, bucket: bucket}
  end

  test "signed bucket deletion rejects historical versions and remaining delete markers",
       context do
    %{credentials: credentials, bucket: bucket} = context
    assert :ok = Versioning.set_versioning(bucket, :enabled)

    assert {:ok, %{version_id: old}} =
             ObjectService.put(bucket, "key", "history", "text/plain", %{})

    assert {:ok, %{version_id: marker}} = ObjectService.delete(bucket, "key", nil)
    assert {:ok, before} = Versioning.list_versions(bucket, "key")

    response = signed(credentials, :delete, "/#{bucket}")
    assert response.status == 409
    assert response.body =~ "<Code>BucketNotEmpty</Code>"
    assert {:ok, ^before} = Versioning.list_versions(bucket, "key")
    assert {:ok, %{version_id: ^old}} = ObjectService.head(bucket, "key", old, [])

    assert {:ok, _} = ObjectService.delete(bucket, "key", old)
    response = signed(credentials, :delete, "/#{bucket}")
    assert response.status == 409
    assert response.body =~ "<Code>BucketNotEmpty</Code>"
    assert {:ok, %{delete_marker: true, version_id: ^marker}} = ObjectService.head(bucket, "key")

    assert {:ok, _} = ObjectService.delete(bucket, "key", marker)
    assert signed(credentials, :delete, "/#{bucket}").status == 204
    assert {:error, :not_found} = Metadata.get_bucket(bucket)
  end

  test "malformed read dates are ignored safely and malformed ranges cannot serve object bytes",
       context do
    %{credentials: credentials, bucket: bucket} = context
    assert {:ok, _} = ObjectService.put(bucket, "key", "unchanged", "text/plain", %{})
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")

    for method <- [:get, :head],
        header <- ["if-modified-since", "if-unmodified-since"],
        date <- [
          "invalid",
          "Thu, 31 Feb 2026 00:00:00 GMT",
          "Thu, 01 Jan 2026 25:00:00 GMT",
          String.duplicate("9", 256)
        ] do
      response = signed(credentials, method, "/#{bucket}/key", [{header, date}])
      assert response.status == 200
      assert response.body == if(method == :head, do: "", else: "unchanged")
    end

    for range <- [
          "bytes=-",
          "bytes=1-0",
          "bytes=-0",
          "bytes=999-",
          "items=0-1",
          "bytes=0-1,3-4",
          "bytes=" <> String.duplicate("9", 256) <> "-"
        ] do
      response = signed(credentials, :get, "/#{bucket}/key", [{"range", range}])
      assert response.status == 416
      assert response.body == ""
      assert response.headers["content-range"] == ["bytes */9"]
    end

    assert signed(credentials, :get, "/#{bucket}/key").body == "unchanged"
    assert {:ok, ^versions} = Versioning.list_versions(bucket, "key")
  end

  test "signed oversized multipart completion preserves uploaded parts and existing object",
       context do
    %{credentials: credentials, bucket: bucket} = context
    assert {:ok, _} = ObjectService.put(bucket, "key", "existing", "text/plain", %{})
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert {:ok, upload} = Multipart.init_upload(bucket, "key")
    assert {:ok, etag} = Multipart.store_part(bucket, upload, 1, "retained-part")
    assert {:ok, parts} = Multipart.list_parts(bucket, upload)

    xml =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>"

    oversized = xml <> String.duplicate(" ", 16 * 1024 * 1024)

    response = signed(credentials, :post, "/#{bucket}/key?uploadId=#{upload}", [], oversized)
    assert response.status == 413
    assert response.body =~ "<Code>EntityTooLarge</Code>"
    assert {:ok, ^parts} = Multipart.list_parts(bucket, upload)
    assert {:ok, ^versions} = Versioning.list_versions(bucket, "key")
    assert signed(credentials, :get, "/#{bucket}/key").body == "existing"

    for query <- ["uploads", "uploadId=#{upload}"] do
      response =
        signed(
          credentials,
          :post,
          "/#{bucket}/key?#{query}",
          [{"x-amz-copy-source", "/#{bucket}/key"}],
          xml
        )

      assert response.status == 501
      assert {:ok, ^parts} = Multipart.list_parts(bucket, upload)
      assert {:ok, ^versions} = Versioning.list_versions(bucket, "key")
    end

    assert :ok = Multipart.abort_upload(bucket, upload)
  end

  defp signed(credentials, method, path, headers \\ [], body \\ nil) do
    Req.request!(
      method: method,
      url: @base <> path,
      headers: headers,
      body: body,
      retry: false,
      decode_body: false,
      aws_sigv4: [
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end
end
