defmodule ExStorageServiceS3.AttributesProfileHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("attrs-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("attrs-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "attrs-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      for key <- ["source", "copied", "replaced"], do: ObjectService.delete(bucket, key, nil)
      Metadata.delete_bucket(bucket)

      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)
    end)

    %{bucket: bucket, credentials: credentials}
  end

  test "PUT preserves genuine gzip bytes, metadata, tags, attributes and verified checksum",
       ctx do
    bytes = :zlib.gzip("real gzip payload")
    checksum = Base.encode64(:crypto.hash(:sha256, bytes))

    attrs = [
      {"content-type", "application/x-test"},
      {"content-encoding", "gzip"},
      {"cache-control", "max-age=42"},
      {"content-language", "zh"},
      {"content-disposition", "attachment; filename=test.gz"},
      {"x-amz-meta-owner", "source"},
      {"x-amz-tagging", "owner=source"},
      {"x-amz-checksum-sha256", checksum}
    ]

    response = signed(ctx, :put, "source", bytes, attrs)
    assert response.status == 200
    assert response.headers["x-amz-checksum-sha256"] == [checksum]

    for method <- [:get, :head] do
      response = signed(ctx, method, "source", nil, [{"x-amz-checksum-mode", "ENABLED"}])
      assert response.status == 200
      if method == :get, do: assert(response.body == bytes)

      for {name, value} <- attrs, name != "x-amz-tagging" do
        assert response.headers[name] == [value]
      end

      assert response.headers["x-amz-tagging-count"] == ["1"]
    end

    tags = signed(ctx, :get, "source?tagging")
    assert tags.body =~ "<Value>source</Value>"
  end

  test "COPY and REPLACE independently preserve or replace metadata and tags", ctx do
    assert signed(ctx, :put, "source", "source", [
             {"content-type", "text/plain"},
             {"x-amz-meta-owner", "original"},
             {"x-amz-tagging", "owner=original"},
             {"cache-control", "max-age=42"}
           ]).status == 200

    source = "/#{ctx.bucket}/source"
    assert signed(ctx, :put, "copied", nil, [{"x-amz-copy-source", source}]).status == 200
    assert signed(ctx, :head, "copied").headers["x-amz-meta-owner"] == ["original"]
    assert signed(ctx, :get, "copied?tagging").body =~ "<Value>original</Value>"

    headers = [
      {"x-amz-copy-source", source},
      {"x-amz-metadata-directive", "REPLACE"},
      {"x-amz-tagging-directive", "REPLACE"},
      {"content-type", "application/new"},
      {"x-amz-meta-owner", "replacement"},
      {"x-amz-tagging", "owner=replacement"}
    ]

    assert signed(ctx, :put, "replaced", nil, headers).status == 200
    replaced = signed(ctx, :head, "replaced")
    assert replaced.headers["content-type"] == ["application/new"]
    assert replaced.headers["x-amz-meta-owner"] == ["replacement"]
    refute replaced.headers["cache-control"] == ["max-age=42"]
    assert {:ok, %{metadata: replaced_metadata}} = ObjectService.head(ctx.bucket, "replaced")
    refute {"cache-control", "max-age=42"} in replaced_metadata.system_headers
    assert signed(ctx, :get, "replaced?tagging").body =~ "<Value>replacement</Value>"
    assert signed(ctx, :get, "replaced").body == "source"

    assert signed(ctx, :put, "replaced", nil, [
             {"x-amz-copy-source", source},
             {"x-amz-metadata-directive", "bogus"}
           ]).status == 400

    assert signed(ctx, :get, "replaced").body == "source"
  end

  test "invalid upload tags cannot publish bytes", ctx do
    assert signed(ctx, :put, "source", "old").status == 200

    assert signed(ctx, :put, "source", "new", [{"x-amz-tagging", "duplicate=1&duplicate=2"}]).status ==
             400

    assert signed(ctx, :get, "source").body == "old"
  end

  defp signed(ctx, method, key, body \\ nil, headers \\ []) do
    Req.request!(
      method: method,
      url: @base <> "/" <> ctx.bucket <> "/" <> key,
      body: body,
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
