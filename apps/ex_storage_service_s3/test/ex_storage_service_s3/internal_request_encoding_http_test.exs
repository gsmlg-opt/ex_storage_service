defmodule ExStorageServiceS3.InternalRequestEncodingHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("encoding-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)

    {:ok, policy} =
      Policy.create_policy("encoding-http-#{suffix}", Policy.full_access_statements())

    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "encoding-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      for key <- ["gzip", "deflate"], do: ObjectService.delete(bucket, key, nil)
      Metadata.delete_bucket(bucket)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)

      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
    end)

    %{bucket: bucket, credentials: credentials}
  end

  test "authenticated gzip and deflate PUT retain raw bytes and Content-Encoding on HEAD and GET",
       ctx do
    payload = String.duplicate("encoded object bytes\0\xff", 32)

    for {encoding, bytes} <- [{"gzip", :zlib.gzip(payload)}, {"deflate", :zlib.compress(payload)}] do
      refute bytes == payload
      etag = "\"#{Base.encode16(:crypto.hash(:md5, bytes), case: :lower)}\""

      headers = [
        {"content-type", "application/octet-stream"},
        {"content-encoding", encoding},
        {"content-length", Integer.to_string(byte_size(bytes))},
        {"x-amz-meta-encoding", "user metadata"}
      ]

      assert %{status: 200, headers: put_headers} = signed(ctx, :put, encoding, bytes, headers)
      assert put_headers["etag"] == [etag]

      for method <- [:head, :get] do
        response = signed(ctx, method, encoding)
        assert response.status == 200
        assert response.headers["content-encoding"] == [encoding]
        assert response.headers["content-length"] == [Integer.to_string(byte_size(bytes))]
        assert response.headers["content-type"] == ["application/octet-stream"]
        assert response.headers["etag"] == [etag]
        assert response.headers["x-amz-meta-encoding"] == ["user metadata"]
        refute Map.has_key?(response.headers, "x-amz-meta-content-encoding")
        assert response.body == if(method == :get, do: bytes, else: "")
      end
    end
  end

  defp signed(ctx, method, key, body \\ nil, headers \\ []) do
    Req.request!(
      method: method,
      url: @base <> "/" <> ctx.bucket <> "/" <> key,
      body: body,
      headers: headers,
      retry: false,
      redirect: false,
      raw: true,
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
