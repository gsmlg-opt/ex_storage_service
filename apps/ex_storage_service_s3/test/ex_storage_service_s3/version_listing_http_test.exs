defmodule ExStorageServiceS3.VersionListingHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"
  @special "z&< 雪+%"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("version-list-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("version-list-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "version-list-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)
    {:ok, %{version_id: old}} = ObjectService.put(bucket, "a", "historical", "text/plain", %{})
    {:ok, %{version_id: latest}} = ObjectService.put(bucket, "a", "current", "text/plain", %{})
    {:ok, %{version_id: marker}} = ObjectService.delete(bucket, "a", nil)
    keys = ["dir/a/one", "dir/a/two", "dir/b/one", "dir/z", @special]

    for key <- keys,
        do: assert({:ok, _} = ObjectService.put(bucket, key, "value", "text/plain", %{}))

    on_exit(fn ->
      for key <- ["a" | keys] do
        case Versioning.list_versions(bucket, key) do
          {:ok, versions} ->
            Enum.each(versions, &ObjectService.delete(bucket, key, &1.version_id))

          _ ->
            :ok
        end
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

    %{
      bucket: bucket,
      credentials: credentials,
      policy: policy,
      old: old,
      latest: latest,
      marker: marker
    }
  end

  test "single-entry pages expose all versions and markers exactly once", ctx do
    documents = pages(ctx, %{"max-keys" => "1"})
    assert length(documents) == 8
    ids = Enum.flat_map(documents, &values(&1, "//VersionId/text()"))
    assert length(Enum.uniq(ids)) == 8
    assert Enum.take(ids, 3) == [ctx.marker, ctx.latest, ctx.old]
    assert values(hd(documents), "//DeleteMarker/IsLatest/text()") == ["true"]
    assert values(Enum.at(documents, 1), "//Version/IsLatest/text()") == ["false"]

    assert Enum.flat_map(documents, &values(&1, "//Version/Key/text()")) |> List.last() ==
             @special

    assert values(List.last(documents), "//IsTruncated/text()") == ["false"]
  end

  test "prefix delimiter common-prefix pages and zero limits preserve marker contracts", ctx do
    documents = pages(ctx, %{"prefix" => "dir/", "delimiter" => "/", "max-keys" => "1"})
    assert length(documents) == 3
    assert values(hd(documents), "//Version/Key/text()") == []
    assert values(hd(documents), "//CommonPrefixes/Prefix/text()") == ["dir/a/"]
    assert values(hd(documents), "//NextKeyMarker/text()") == ["dir/a/"]
    assert values(hd(documents), "//NextVersionIdMarker/text()") == []
    assert values(Enum.at(documents, 1), "//CommonPrefixes/Prefix/text()") == ["dir/b/"]
    assert values(List.last(documents), "//Version/Key/text()") == ["dir/z"]
    zero = request(ctx, %{"max-keys" => "0"})
    assert zero.status == 200
    assert values(zero.body, "//VersionId/text()") == []
    assert values(zero.body, "//IsTruncated/text()") == ["false"]
  end

  test "URL encoding covers keys prefixes delimiters and key markers with XML escaping", ctx do
    response = request(ctx, %{"prefix" => @special, "encoding-type" => "url"})
    expected = URI.encode(@special, &URI.char_unreserved?/1)
    assert response.status == 200
    assert values(response.body, "//Version/Key/text()") == [expected]
    assert values(response.body, "/ListVersionsResult/Prefix/text()") == [expected]
    assert values(response.body, "//EncodingType/text()") == ["url"]

    response =
      request(ctx, %{
        "prefix" => "dir/",
        "delimiter" => "/",
        "max-keys" => "1",
        "encoding-type" => "url"
      })

    assert values(response.body, "//Delimiter/text()") == ["%2F"]
    assert values(response.body, "//NextKeyMarker/text()") == ["dir%2Fa%2F"]
    response = request(ctx, %{"prefix" => @special})
    assert values(response.body, "//Version/Key/text()") == [@special]
    assert response.body =~ "z&amp;&lt;"
  end

  test "invalid markers and parameters reject while list-version permission remains distinct",
       ctx do
    for params <- [
          %{"max-keys" => "-1"},
          %{"max-keys" => "1junk"},
          %{"max-keys" => ""},
          %{"version-id-marker" => ctx.old},
          %{"key-marker" => "a", "version-id-marker" => "unknown"},
          %{"encoding-type" => "base64"}
        ] do
      response = request(ctx, params)
      assert response.status == 400
      assert response.body =~ "<Code>InvalidArgument</Code>"
    end

    {:ok, _} =
      Policy.update_policy(ctx.policy.id, %{
        statements: [
          %{effect: :allow, actions: ["s3:ListBucket"], resources: ["arn:aws:s3:::#{ctx.bucket}"]}
        ]
      })

    assert request(ctx, %{}).status == 403

    {:ok, _} =
      Policy.update_policy(ctx.policy.id, %{
        statements: [
          %{
            effect: :allow,
            actions: ["s3:ListBucketVersions"],
            resources: ["arn:aws:s3:::#{ctx.bucket}"]
          }
        ]
      })

    assert request(ctx, %{}).status == 200
    assert signed(ctx, "/#{ctx.bucket}").status == 403
  end

  test "suspended null overwrites and markers expose one null version without replacing numbered history",
       ctx do
    assert :ok = Versioning.set_versioning(ctx.bucket, :suspended)

    assert {:ok, %{version_id: "null"}} =
             ObjectService.put(ctx.bucket, "a", "first null", "text/plain", %{})

    assert {:ok, %{version_id: "null"}} =
             ObjectService.put(ctx.bucket, "a", "second null", "text/plain", %{})

    current = signed(ctx, "/#{ctx.bucket}/a")
    assert current.body == "second null"
    assert current.headers["x-amz-version-id"] == ["null"]
    assert {:ok, %{version_id: "null"}} = ObjectService.delete(ctx.bucket, "a", nil)
    response = request(ctx, %{"prefix" => "a"})
    assert response.status == 200

    assert values(response.body, "//VersionId/text()") == [
             "null",
             ctx.marker,
             ctx.latest,
             ctx.old
           ]

    assert values(response.body, "//DeleteMarker/IsLatest/text()") == ["true", "false"]
  end

  defp pages(ctx, params, remaining \\ 20) do
    assert remaining > 0, "version pagination did not terminate"
    response = request(ctx, params)
    assert response.status == 200

    if values(response.body, "//IsTruncated/text()") == ["true"] do
      [key] = values(response.body, "//NextKeyMarker/text()")
      params = Map.put(params, "key-marker", key) |> Map.delete("version-id-marker")

      params =
        case values(response.body, "//NextVersionIdMarker/text()") do
          [id] -> Map.put(params, "version-id-marker", id)
          [] -> params
        end

      [response.body | pages(ctx, params, remaining - 1)]
    else
      [response.body]
    end
  end

  defp request(ctx, params),
    do:
      signed(
        ctx,
        "/#{ctx.bucket}?" <> URI.encode_query(Map.put(params, "versions", ""), :rfc3986)
      )

  defp signed(ctx, path) do
    Req.get!(@base <> path,
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

  defp values(body, xpath) do
    {document, []} = :xmerl_scan.string(:binary.bin_to_list(body), quiet: true)

    :xmerl_xpath.string(String.to_charlist(xpath), document)
    |> Enum.map(&:unicode.characters_to_binary(elem(&1, 4)))
  end
end
