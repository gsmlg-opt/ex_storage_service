defmodule ExStorageServiceS3.ReadSemanticsHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("read-http-#{suffix}")
    {:ok, key} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("read-http-#{suffix}", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "read-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, object} = ObjectService.put(bucket, "key", "object bytes", "text/plain", %{})

    on_exit(fn ->
      Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(key.access_key_id)
      User.delete_user(user.id)
    end)

    %{key: key, path: "/#{bucket}/key", object: object}
  end

  test "signed and independently presigned response overrides survive full and ranged GET", ctx do
    query =
      URI.encode_query(
        %{
          "response-content-type" => "application/x-custom",
          "response-content-language" => "fr",
          "response-cache-control" => "private",
          "response-content-disposition" => "attachment; filename=\"a b.txt\"",
          "response-content-encoding" => "identity",
          "response-expires" => "Thu, 01 Jan 2037 00:00:00 GMT"
        },
        :rfc3986
      )

    url = @base <> ctx.path <> "?" <> query
    response = signed(ctx.key, :get, url)
    assert response.status == 200
    assert response.body == "object bytes"
    assert response.headers["content-type"] == ["application/x-custom"]
    assert response.headers["content-language"] == ["fr"]
    assert response.headers["content-disposition"] == ["attachment; filename=\"a b.txt\""]
    assert response.headers["cache-control"] == ["private"]
    assert response.headers["content-encoding"] == ["identity"]
    assert response.headers["expires"] == ["Thu, 01 Jan 2037 00:00:00 GMT"]

    presigned =
      Req.Utils.aws_sigv4_url(
        access_key_id: ctx.key.access_key_id,
        secret_access_key: ctx.key.secret_access_key,
        region: "us-east-1",
        service: :s3,
        datetime: DateTime.utc_now(),
        method: :get,
        url: @base <> ctx.path,
        query: URI.query_decoder(query) |> Enum.to_list(),
        expires: 3600
      )
      |> URI.to_string()

    range = Req.get!(presigned, headers: [{"range", "bytes=1-3"}], retry: false)
    assert range.status == 206
    assert range.body == "bje"
    assert range.headers["content-type"] == ["application/x-custom"]
    assert Req.get!(presigned <> "&response-content-type=text/plain", retry: false).status == 403
  end

  test "override injection and duplicate values never become response headers", ctx do
    for query <- [
          "response-content-type=x%0D%0AX-Evil%3Ayes",
          "response-content-type=a&response-content-type=b"
        ] do
      response = signed(ctx.key, :get, @base <> ctx.path <> "?" <> query)
      assert response.status == 400
      refute Map.has_key?(response.headers, "x-evil")
    end
  end

  test "GET and HEAD evaluate strong/weak ETags and date precedence", ctx do
    original = signed(ctx.key, :get, @base <> ctx.path)
    [etag] = original.headers["etag"]

    for method <- [:get, :head] do
      for headers <- [
            [{"if-match", "\"wrong\""}],
            [{"if-match", "W/" <> etag}],
            [{"if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT"}]
          ] do
        response = signed(ctx.key, method, @base <> ctx.path, headers)
        assert response.status == 412
        if method == :head, do: assert(response.body == "")
      end

      assert signed(ctx.key, method, @base <> ctx.path, [
               {"if-match", etag},
               {"if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT"}
             ]).status == 200

      assert signed(ctx.key, method, @base <> ctx.path, [
               {"if-none-match", "\"other\", W/" <> etag}
             ]).status == 304

      assert signed(ctx.key, method, @base <> ctx.path, [{"if-none-match", "*"}]).status == 304

      assert signed(ctx.key, method, @base <> ctx.path, [
               {"if-none-match", "\"other\""},
               {"if-modified-since", "Thu, 01 Jan 2037 00:00:00 GMT"}
             ]).status == 200
    end
  end

  defp signed(key, method, url, headers \\ []) do
    Req.request!(
      method: method,
      url: url,
      headers: headers,
      retry: false,
      aws_sigv4: [
        access_key_id: key.access_key_id,
        secret_access_key: key.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end
end
