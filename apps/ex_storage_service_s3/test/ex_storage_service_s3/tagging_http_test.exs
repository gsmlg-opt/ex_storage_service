defmodule ExStorageServiceS3.TaggingHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceS3.Handlers.Tagging
  alias ExStorageService.CloudCache.Config
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  defmodule UpstreamSpy do
    def init(agent), do: agent

    def call(conn, agent) do
      Agent.update(agent, &[{conn.method, conn.request_path} | &1])
      Plug.Conn.send_resp(conn, 500, "unexpected upstream request")
    end
  end

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("tagging-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("tagging-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "tagging-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)
    {:ok, %{version_id: old}} = ObjectService.put(bucket, "key", "old", "text/plain", %{})
    {:ok, %{version_id: latest}} = ObjectService.put(bucket, "key", "current", "text/plain", %{})

    on_exit(fn ->
      Config.delete_config(bucket)

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

    %{credentials: credentials, policy: policy, bucket: bucket, old: old, latest: latest}
  end

  test "bucket and version tag CRUD never replace bytes or mint extra versions", ctx do
    path = "/#{ctx.bucket}"
    assert signed(ctx, :get, path <> "?tagging").status == 404

    assert signed(ctx, :put, path <> "?tagging", Tagging.document(%{"owner" => "研发"})).status ==
             204

    assert signed(ctx, :get, path <> "?tagging").body =~ "研发"
    assert {:ok, before} = Versioning.list_versions(ctx.bucket, "key")
    current = path <> "/key?tagging"
    historical = current <> "&versionId=#{ctx.old}"
    assert signed(ctx, :put, current, Tagging.document(%{"version" => "latest"})).status == 200
    assert signed(ctx, :put, historical, Tagging.document(%{"version" => "old"})).status == 200
    assert signed(ctx, :get, current).body =~ "latest"
    assert signed(ctx, :get, historical).body =~ "old"
    assert signed(ctx, :get, path <> "/key").body == "current"
    assert signed(ctx, :get, path <> "/key?versionId=#{ctx.old}").body == "old"
    assert signed(ctx, :delete, historical).status == 204
    assert signed(ctx, :get, historical).body =~ "<TagSet></TagSet>"
    assert signed(ctx, :get, current).body =~ "latest"
    assert signed(ctx, :delete, current).status == 204
    assert signed(ctx, :get, current).body =~ "<TagSet></TagSet>"
    assert signed(ctx, :get, path <> "/key").body == "current"
    assert signed(ctx, :delete, path <> "?tagging").status == 204
    assert signed(ctx, :get, path <> "?tagging").status == 404
    assert {:ok, after_tags} = Versioning.list_versions(ctx.bucket, "key")

    assert Enum.map(after_tags, &Map.delete(&1, :tags)) ==
             Enum.map(before, &Map.delete(&1, :tags))

    assert {:ok, %{version_id: version}} = ObjectService.head(ctx.bucket, "key")
    assert version == ctx.latest
  end

  test "bucket tag limits and missing object versions fail without changing accepted state",
       ctx do
    path = "/#{ctx.bucket}?tagging"
    tags = Map.new(1..50, &{"key#{&1}", "value"})
    assert signed(ctx, :put, path, Tagging.document(tags)).status == 204

    assert signed(ctx, :put, path, Tagging.document(Map.put(tags, "extra", "value"))).status ==
             400

    assert {:ok, ^tags} = ExStorageService.Storage.Tags.get_bucket(ctx.bucket)

    for method <- [:get, :put, :delete] do
      body = if method == :put, do: Tagging.document(%{}), else: nil
      missing = signed(ctx, method, "/#{ctx.bucket}/missing?tagging", body)
      assert missing.status == 404
      assert missing.body =~ "<Code>NoSuchKey</Code>"

      missing_version =
        signed(ctx, method, "/#{ctx.bucket}/key?tagging&versionId=missing-version", body)

      assert missing_version.status == 404
      assert missing_version.body =~ "<Code>NoSuchVersion</Code>"
    end

    assert signed(ctx, :get, "/#{ctx.bucket}/key").body == "current"
  end

  test "unsupported gateway object tagging rejects before any upstream request or local mutation",
       ctx do
    spy = start_supervised!({Agent, fn -> [] end})

    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {UpstreamSpy, spy}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)
    assert {:ok, before} = Versioning.list_versions(ctx.bucket, "key")

    assert :ok =
             Config.set_config(ctx.bucket, %{
               enabled: true,
               provider: :s3_compat,
               endpoint: "http://localhost:#{port}",
               bucket: "upstream",
               access_key_id: "test",
               secret_access_key: "test",
               cache_enabled: true
             })

    on_exit(fn -> Config.delete_config(ctx.bucket) end)

    for method <- [:get, :put, :delete], version <- ["", "&versionId=#{ctx.old}"] do
      body = if method == :put, do: Tagging.document(%{"owner" => "new"}), else: nil
      response = signed(ctx, method, "/#{ctx.bucket}/key?tagging" <> version, body)
      assert response.status == 501
      assert response.body =~ "<Code>NotImplemented</Code>"
    end

    assert Agent.get(spy, & &1) == []
    assert {:ok, ^before} = Versioning.list_versions(ctx.bucket, "key")
    assert :ok = Config.delete_config(ctx.bucket)
    assert signed(ctx, :get, "/#{ctx.bucket}/key").body == "current"
    assert signed(ctx, :get, "/#{ctx.bucket}/key?versionId=#{ctx.old}").body == "old"
  end

  test "version tagging permissions are separate from current tags and object reads", ctx do
    {:ok, _} =
      Policy.update_policy(ctx.policy.id, %{
        statements: [
          %{
            effect: :allow,
            actions: ["s3:GetObjectTagging", "s3:PutObjectTagging", "s3:DeleteObjectTagging"],
            resources: ["arn:aws:s3:::#{ctx.bucket}/*"]
          }
        ]
      })

    path = "/#{ctx.bucket}/key?tagging"
    assert signed(ctx, :put, path, Tagging.document(%{"owner" => "current"})).status == 200
    assert signed(ctx, :get, path).status == 200
    assert signed(ctx, :get, path <> "&versionId=#{ctx.old}").status == 403
    assert signed(ctx, :put, path <> "&versionId=#{ctx.old}", Tagging.document(%{})).status == 403
    assert signed(ctx, :delete, path <> "&versionId=#{ctx.old}").status == 403
    assert signed(ctx, :get, "/#{ctx.bucket}/key").status == 403
    assert signed(ctx, :put, "/#{ctx.bucket}?tagging", Tagging.document(%{})).status == 403

    {:ok, _} =
      Policy.update_policy(ctx.policy.id, %{
        statements: [
          %{
            effect: :allow,
            actions: [
              "s3:GetObjectVersionTagging",
              "s3:PutObjectVersionTagging",
              "s3:DeleteObjectVersionTagging"
            ],
            resources: ["arn:aws:s3:::#{ctx.bucket}/*"]
          }
        ]
      })

    assert signed(
             ctx,
             :put,
             path <> "&versionId=#{ctx.old}",
             Tagging.document(%{"owner" => "old"})
           ).status == 200

    assert signed(ctx, :get, path <> "&versionId=#{ctx.old}").status == 200
    assert signed(ctx, :get, path).status == 403
    assert signed(ctx, :delete, path <> "&versionId=#{ctx.old}").status == 204
  end

  test "malformed, oversized, duplicate and excessive tags preserve the last accepted set", ctx do
    path = "/#{ctx.bucket}/key?tagging"
    assert signed(ctx, :put, path, Tagging.document(%{"retained" => "yes"})).status == 200

    for {xml, status} <- [
          {"<Tagging><TagSet><Tag><Key>x</Key><Value>a</Value></Tag><Tag><Key>x</Key><Value>b</Value></Tag></TagSet></Tagging>",
           400},
          {Tagging.document(Map.new(1..11, &{"key#{&1}", "value"})), 400},
          {"<!DOCTYPE Tagging [<!ENTITY secret SYSTEM 'file:///not-readable'>]><Tagging><TagSet/></Tagging>",
           400},
          {"<Tagging><Unknown/></Tagging>", 400},
          {"<Tagging custom='value'><TagSet/></Tagging>", 400},
          {Tagging.document(%{}) <> String.duplicate(" ", 131_072), 413}
        ] do
      assert signed(ctx, :put, path, xml).status == status
      assert signed(ctx, :get, path).body =~ "retained"
      assert signed(ctx, :get, "/#{ctx.bucket}/key").body == "current"
    end
  end

  test "tagging a delete marker never removes its version", ctx do
    {:ok, %{version_id: marker}} = ObjectService.delete(ctx.bucket, "key", nil)
    path = "/#{ctx.bucket}/key?tagging"
    assert signed(ctx, :get, path).status == 404

    for method <- [:get, :put, :delete] do
      assert signed(
               ctx,
               method,
               path <> "&versionId=#{marker}",
               if(method == :put, do: Tagging.document(%{}), else: nil)
             ).status == 405
    end

    assert {:ok, %{version_id: ^marker, delete_marker: true}} =
             ObjectService.head(ctx.bucket, "key")
  end

  defp signed(ctx, method, path, body \\ nil) do
    Req.request!(
      method: method,
      url: @base <> path,
      body: body,
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
