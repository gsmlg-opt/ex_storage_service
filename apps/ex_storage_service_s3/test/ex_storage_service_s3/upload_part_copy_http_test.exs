defmodule ExStorageServiceS3.UploadPartCopyHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.{Multipart, Versioning}
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  test "independent signed part copies require source version permission and pin ranges and conditions" do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    on_exit(fn -> Application.put_env(:ex_storage_service, :s3_auth_enabled, previous) end)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("part-copy-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    bucket = "part-copy-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)

    {:ok, %{version_id: version, metadata: source_metadata}} =
      ObjectService.put(bucket, "source", "abcdef", "text/plain", %{})

    {:ok, upload} = Multipart.init_upload(bucket, "destination")
    grant(user.id, "s3:PutObject", "arn:aws:s3:::#{bucket}/destination")
    path = "/#{bucket}/destination?partNumber=1&uploadId=#{upload}"
    source = "/#{bucket}/source?versionId=#{version}"
    headers = [{"x-amz-copy-source", source}, {"x-amz-copy-source-range", "bytes=1-3"}]

    assert signed(credentials, path, headers).status == 403
    assert {:ok, []} = Multipart.list_parts(bucket, upload)
    grant(user.id, "s3:GetObject", "arn:aws:s3:::#{bucket}/source")
    assert signed(credentials, path, headers).status == 403
    grant(user.id, "s3:GetObjectVersion", "arn:aws:s3:::#{bucket}/source")

    assert signed(credentials, path, [{"x-amz-copy-source-if-match", "\"stale\""} | headers]).status ==
             412

    assert {:ok, []} = Multipart.list_parts(bucket, upload)

    assert signed(
             credentials,
             path,
             List.keystore(
               headers,
               "x-amz-copy-source-range",
               0,
               {"x-amz-copy-source-range", "bytes=99-100"}
             )
           ).status == 416

    response =
      signed(credentials, path, [
        {"x-amz-copy-source-if-match", "\"#{source_metadata.etag}\""} | headers
      ])

    assert response.status == 200
    assert response.body =~ "CopyPartResult"
    assert Req.Response.get_header(response, "x-amz-copy-source-version-id") == [version]
    assert {:ok, [%{etag: etag, size: 3}]} = Multipart.list_parts(bucket, upload)
    assert {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    assert prepared.content_hash == Base.encode16(:crypto.hash(:sha256, "bcd"), case: :lower)

    # Knowledge of an upload ID does not authorize reading or mutating another key.
    grant(user.id, "s3:PutObject", "arn:aws:s3:::#{bucket}/wrong")
    wrong = "/#{bucket}/wrong?partNumber=1&uploadId=#{upload}"
    assert signed(credentials, wrong, headers).status == 404
    assert {:ok, [%{etag: ^etag}]} = Multipart.list_parts(bucket, upload)
  end

  defp signed(credentials, path, headers) do
    Req.put!(@base <> path,
      body: "",
      retry: false,
      headers: headers,
      aws_sigv4: [
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end

  defp grant(user, action, resource) do
    {:ok, policy} =
      Policy.create_policy("part-copy", [
        %{effect: :allow, actions: [action], resources: [resource]}
      ])

    :ok = Policy.attach_policy(user, policy.id)
  end
end
