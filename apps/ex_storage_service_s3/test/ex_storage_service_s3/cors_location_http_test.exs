defmodule ExStorageServiceS3.CorsLocationHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"
  @cors "<CORSConfiguration><CORSRule><ID>browser</ID><AllowedOrigin>https://*.example.test</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedHeader>x-amz-*</AllowedHeader><AllowedHeader>content-type</AllowedHeader><ExposeHeader>etag</ExposeHeader><ExposeHeader>x-amz-version-id</ExposeHeader><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    region = Application.get_env(:ex_storage_service, :s3_region)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("cors-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("cors-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "cors-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, _} = ObjectService.put(bucket, "key", "private", "text/plain", %{})

    on_exit(fn ->
      ObjectService.delete(bucket, "key", nil)
      Metadata.delete_bucket(bucket)

      for {key, value} <- [s3_auth_enabled: previous, s3_region: region] do
        if is_nil(value),
          do: Application.delete_env(:ex_storage_service, key),
          else: Application.put_env(:ex_storage_service, key, value)
      end

      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)
    end)

    %{credentials: credentials, bucket: bucket, policy: policy}
  end

  test "persisted CORS scopes preflight and actual headers without authenticating actual reads",
       ctx do
    path = "/#{ctx.bucket}"
    assert signed(ctx, :get, path <> "?cors").status == 404
    assert signed(ctx, :put, path <> "?cors", @cors).status == 200
    assert signed(ctx, :get, path <> "?cors").body =~ "<ID>browser</ID>"

    headers = [
      {"origin", "https://app.example.test"},
      {"access-control-request-method", "PUT"},
      {"access-control-request-headers", "X-Amz-Meta-Owner, Content-Type"}
    ]

    response =
      Req.request!(method: :options, url: @base <> path <> "/key", headers: headers, retry: false)

    assert response.status == 200
    assert response.headers["access-control-allow-origin"] == ["https://app.example.test"]
    assert response.headers["access-control-max-age"] == ["60"]

    assert Enum.all?(
             ~w(origin access-control-request-method access-control-request-headers),
             &(&1 in vary(response))
           )

    assert hd(response.headers["access-control-allow-headers"]) =~ "x-amz-meta-owner"

    missing =
      Req.request!(
        method: :options,
        url: @base <> "/missing-cors-bucket/key",
        headers: headers,
        retry: false
      )

    assert missing.status == 404
    assert missing.body =~ "<Code>NoSuchBucket</Code>"

    malformed =
      Req.request!(
        method: :options,
        url: @base <> path <> "/key",
        headers:
          List.keystore(
            headers,
            "access-control-request-method",
            0,
            {"access-control-request-method", "get"}
          ),
        retry: false
      )

    assert malformed.status == 403
    refute Map.has_key?(malformed.headers, "access-control-allow-origin")

    no_method =
      Req.request!(
        method: :options,
        url: @base <> path <> "/key",
        headers: [{"origin", "https://app.example.test"}],
        retry: false
      )

    assert no_method.status == 403

    for changed <- [
          List.keystore(headers, "origin", 0, {"origin", "https://example.test.evil"}),
          List.keystore(
            headers,
            "access-control-request-method",
            0,
            {"access-control-request-method", "DELETE"}
          ),
          List.keystore(
            headers,
            "access-control-request-headers",
            0,
            {"access-control-request-headers", "x-private"}
          )
        ] do
      denied =
        Req.request!(
          method: :options,
          url: @base <> path <> "/key",
          headers: changed,
          retry: false
        )

      assert denied.status == 403
      refute Map.has_key?(denied.headers, "access-control-allow-origin")
      assert "origin" in vary(denied)
    end

    origin = [{"origin", "https://app.example.test"}]
    unauthorized = Req.get!(@base <> path <> "/key", headers: origin, retry: false)
    assert unauthorized.status == 403
    assert unauthorized.headers["access-control-allow-origin"] == ["https://app.example.test"]
    response = signed(ctx, :get, path <> "/key", nil, origin)
    assert response.body == "private"
    assert response.headers["access-control-allow-origin"] == ["https://app.example.test"]
    assert hd(response.headers["access-control-expose-headers"]) =~ "etag"
    assert "origin" in vary(response)
    other = signed(ctx, :get, path <> "/key", nil, [{"origin", "https://evil.test"}])
    assert other.status == 200
    refute Map.has_key?(other.headers, "access-control-allow-origin")
    assert signed(ctx, :delete, path <> "?cors").status == 204
    assert signed(ctx, :get, path <> "?cors").status == 404

    assert Req.request!(
             method: :options,
             url: @base <> path <> "/key",
             headers: headers,
             retry: false
           ).status == 403
  end

  test "bad, oversized and unauthorized CORS updates preserve persisted rules", ctx do
    path = "/#{ctx.bucket}?cors"
    assert signed(ctx, :put, path, @cors).status == 200
    assert {:ok, original} = Concord.get("bucket_cors:#{ctx.bucket}")

    for {body, status} <- [
          {String.replace(
             @cors,
             "<AllowedMethod>PUT</AllowedMethod>",
             "<AllowedMethod>OPTIONS</AllowedMethod>"
           ), 400},
          {"<!DOCTYPE CORSConfiguration><CORSConfiguration/>", 400},
          {"<CORSConfiguration><Unknown/></CORSConfiguration>", 400},
          {@cors <> String.duplicate(" ", 65_536), 413}
        ] do
      assert signed(ctx, :put, path, body).status == status
      assert {:ok, ^original} = Concord.get("bucket_cors:#{ctx.bucket}")
    end

    {:ok, _} =
      Policy.update_policy(ctx.policy.id, %{
        statements: [
          %{
            effect: :allow,
            actions: ["s3:GetBucketCORS"],
            resources: ["arn:aws:s3:::#{ctx.bucket}"]
          }
        ]
      })

    assert signed(ctx, :get, path).status == 200
    assert signed(ctx, :put, path, @cors).status == 403
    assert signed(ctx, :delete, path).status == 403
    assert {:ok, ^original} = Concord.get("bucket_cors:#{ctx.bucket}")
  end

  test "configured location and region headers agree without tightening signing regions", ctx do
    Application.put_env(:ex_storage_service, :s3_region, "us-east-1")
    response = signed(ctx, :get, "/#{ctx.bucket}?location")
    assert response.status == 200
    assert response.body =~ "></LocationConstraint>"
    assert response.headers["x-amz-bucket-region"] == ["us-east-1"]
    Application.put_env(:ex_storage_service, :s3_region, "eu-west-1")
    response = signed(ctx, :get, "/#{ctx.bucket}?location")
    assert response.body =~ ">eu-west-1</LocationConstraint>"
    assert response.headers["x-amz-bucket-region"] == ["eu-west-1"]
    assert signed(ctx, :head, "/#{ctx.bucket}").headers["x-amz-bucket-region"] == ["eu-west-1"]
    assert signed(ctx, :get, "/#{ctx.bucket}/key").status == 200
  end

  defp signed(ctx, method, path, body \\ nil, headers \\ []) do
    Req.request!(
      method: method,
      url: @base <> path,
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

  defp vary(response) do
    response.headers
    |> Map.get("vary", [])
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.map(&(String.trim(&1) |> String.downcase()))
  end
end
