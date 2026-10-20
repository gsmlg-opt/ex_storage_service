defmodule ExStorageServiceS3.TaggingAuthorizationHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.{Multipart, Versioning}

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("tag-auth-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    bucket = "tag-auth-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      case Policy.get_user_policies(user.id) do
        {:ok, policies} ->
          Enum.each(policies, fn policy ->
            Policy.detach_policy(user.id, policy.id)
            Policy.delete_policy(policy.id)
          end)

        _ ->
          :ok
      end

      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)
    end)

    %{user: user, credentials: credentials, bucket: bucket}
  end

  test "tagged PUT needs destination PutObjectTagging and denied replacement preserves history",
       ctx do
    :ok = Versioning.set_versioning(ctx.bucket, :enabled)
    {:ok, before} = ObjectService.put(ctx.bucket, "key", "original", "text/plain", %{})
    grant(ctx, ["s3:PutObject"], "key")
    headers = [{"x-amz-tagging", "owner=upload"}]
    assert signed(ctx, :put, "key", "replacement", headers).status == 403
    assert {:ok, %{version_id: version}} = ObjectService.head(ctx.bucket, "key")
    assert version == before.version_id
    assert {:ok, [_]} = Versioning.list_versions(ctx.bucket, "key")

    grant(ctx, ["s3:PutObjectTagging"], "another-key")
    assert signed(ctx, :put, "key", "replacement", headers).status == 403
    grant(ctx, ["s3:PutObjectTagging"], "key")
    assert signed(ctx, :put, "key", "replacement", headers).status == 200

    assert {:ok, %{metadata: %{tags: %{"owner" => "upload"}}}} =
             ObjectService.head(ctx.bucket, "key")

    grant(ctx, ["s3:PutObjectTagging"], "key", :deny)
    assert signed(ctx, :put, "key", "denied", headers).status == 403
    assert {:ok, [_, _]} = Versioning.list_versions(ctx.bucket, "key")
  end

  test "tagged multipart initiation needs tag grant before creating an upload", ctx do
    grant(ctx, ["s3:PutObject"], "key")
    assert {:ok, before} = Multipart.list_uploads(ctx.bucket)
    headers = [{"x-amz-tagging", "owner=multipart"}]
    assert signed(ctx, :post, "key?uploads", "", headers).status == 403
    assert {:ok, ^before} = Multipart.list_uploads(ctx.bucket)
    grant(ctx, ["s3:PutObjectTagging"], "key")
    response = signed(ctx, :post, "key?uploads", "", headers)
    assert response.status == 200
    [_, upload] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, response.body)

    assert {:ok, %{attributes: %{tags: %{"owner" => "multipart"}}}} =
             Multipart.get_upload(ctx.bucket, upload)

    assert :ok = Multipart.abort_upload(ctx.bucket, upload)
  end

  test "COPY checks actual source tags against destination grant, with version read independent",
       ctx do
    :ok = Versioning.set_versioning(ctx.bucket, :enabled)

    {:ok, old} =
      ObjectService.put(ctx.bucket, "source", "historical", "text/plain", %{},
        attributes: %{tags: %{"owner" => "source"}}
      )

    {:ok, _} = ObjectService.put(ctx.bucket, "source", "latest", "text/plain", %{})
    grant(ctx, ["s3:PutObject"], "destination")
    grant(ctx, ["s3:GetObjectVersion"], "source")
    headers = [{"x-amz-copy-source", "/#{ctx.bucket}/source?versionId=#{old.version_id}"}]
    assert signed(ctx, :put, "destination", "", headers).status == 403
    assert {:error, :object_not_found} = ObjectService.head(ctx.bucket, "destination")
    grant(ctx, ["s3:PutObjectTagging"], "source")
    assert signed(ctx, :put, "destination", "", headers).status == 403
    grant(ctx, ["s3:PutObjectTagging"], "destination")
    assert signed(ctx, :put, "destination", "", headers).status == 200

    assert {:ok, %{metadata: %{tags: %{"owner" => "source"}}}} =
             ObjectService.head(ctx.bucket, "destination")

    grant(ctx, ["s3:PutObjectTagging"], "destination", :deny)
    assert signed(ctx, :put, "destination", "", headers).status == 403
    assert {:ok, [_]} = Versioning.list_versions(ctx.bucket, "destination")
  end

  test "REPLACE supplied tags need destination grant while empty REPLACE and untagged copies do not",
       ctx do
    {:ok, _} =
      ObjectService.put(ctx.bucket, "source", "body", "text/plain", %{},
        attributes: %{tags: %{"owner" => "source"}}
      )

    {:ok, _} = ObjectService.put(ctx.bucket, "plain", "body", "text/plain", %{})
    grant(ctx, ["s3:PutObject", "s3:GetObject"], "*")

    source = [
      {"x-amz-copy-source", "/#{ctx.bucket}/source"},
      {"x-amz-tagging-directive", "REPLACE"}
    ]

    tagged = source ++ [{"x-amz-tagging", "owner=replacement"}]
    assert signed(ctx, :put, "destination", "", tagged).status == 403
    assert {:error, :object_not_found} = ObjectService.head(ctx.bucket, "destination")
    assert signed(ctx, :put, "empty", "", source).status == 200
    assert {:ok, %{metadata: %{tags: tags}}} = ObjectService.head(ctx.bucket, "empty")
    assert tags == %{}

    assert signed(ctx, :put, "untagged", "", [{"x-amz-copy-source", "/#{ctx.bucket}/plain"}]).status ==
             200

    grant(ctx, ["s3:PutObjectTagging"], "destination")
    assert signed(ctx, :put, "destination", "", tagged).status == 200

    assert {:ok, %{metadata: %{tags: %{"owner" => "replacement"}}}} =
             ObjectService.head(ctx.bucket, "destination")
  end

  test "legacy PutObject allow does not gain tagging or multipart privileges", ctx do
    {:ok, policy} =
      Policy.create_policy("legacy-tag-auth", [
        %{effect: :allow, actions: ["s3:PutObject"], resources: ["arn:ess:::#{ctx.bucket}/key"]}
      ])

    :ok = Concord.put("policy:#{policy.id}", Map.delete(policy, :semantics))
    :ok = Policy.attach_policy(ctx.user.id, policy.id)
    assert signed(ctx, :put, "key", "plain", []).status == 200
    assert signed(ctx, :put, "key", "tagged", [{"x-amz-tagging", "owner=legacy"}]).status == 403
    grant(ctx, ["s3:PutObjectTagging"], "key")
    assert signed(ctx, :put, "key", "tagged", [{"x-amz-tagging", "owner=legacy"}]).status == 200

    assert signed(ctx, :post, "key?uploads", "", [{"x-amz-tagging", "owner=legacy"}]).status ==
             403
  end

  test "tag count is omitted without tag read permission while object GET still succeeds", ctx do
    :ok = Versioning.set_versioning(ctx.bucket, :enabled)

    {:ok, object} =
      ObjectService.put(ctx.bucket, "key", "body", "text/plain", %{},
        attributes: %{tags: %{"owner" => "protected"}}
      )

    grant(ctx, ["s3:GetObject", "s3:GetObjectVersion"], "key")

    for path <- ["key", "key?versionId=#{object.version_id}"] do
      response = signed(ctx, :get, path, "", [])
      assert response.status == 200
      assert response.body == "body"
      refute Map.has_key?(response.headers, "x-amz-tagging-count")
    end

    grant(ctx, ["s3:GetObjectTagging"], "key")
    assert signed(ctx, :get, "key", "", []).headers["x-amz-tagging-count"] == ["1"]

    refute Map.has_key?(
             signed(ctx, :get, "key?versionId=#{object.version_id}", "", []).headers,
             "x-amz-tagging-count"
           )

    grant(ctx, ["s3:GetObjectVersionTagging"], "key")

    assert signed(ctx, :get, "key?versionId=#{object.version_id}", "", []).headers[
             "x-amz-tagging-count"
           ] == ["1"]

    grant(ctx, ["s3:GetObjectTagging", "s3:GetObjectVersionTagging"], "key", :deny)
    refute Map.has_key?(signed(ctx, :get, "key", "", []).headers, "x-amz-tagging-count")

    refute Map.has_key?(
             signed(ctx, :get, "key?versionId=#{object.version_id}", "", []).headers,
             "x-amz-tagging-count"
           )
  end

  defp grant(ctx, actions, key, effect \\ :allow) do
    {:ok, policy} =
      Policy.create_policy("tag-auth", [
        %{effect: effect, actions: actions, resources: ["arn:aws:s3:::#{ctx.bucket}/#{key}"]}
      ])

    :ok = Policy.attach_policy(ctx.user.id, policy.id)
  end

  defp signed(ctx, method, path, body, headers) do
    Req.request!(
      method: method,
      url: "#{@base}/#{ctx.bucket}/#{path}",
      body: body,
      headers: headers,
      retry: false,
      aws_sigv4: [
        access_key_id: ctx.credentials.access_key_id,
        secret_access_key: ctx.credentials.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end
end
