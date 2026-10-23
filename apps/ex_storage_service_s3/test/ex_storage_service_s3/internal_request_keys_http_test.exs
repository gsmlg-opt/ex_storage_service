defmodule ExStorageServiceS3.InternalRequestKeysHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceS3.XML

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"
  @keys [
    "delete-ascii",
    "delete-雪",
    "plus+a",
    "plus a",
    "100%done",
    "unusual/binary% +?#/雪/bytes.bin"
  ]
  @version_key "versions/雪+ %😀"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("internal-keys-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("internal-keys-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "internal-keys-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      for key <- [@version_key | @keys] do
        {:ok, versions} = Versioning.list_versions(bucket, key)
        Enum.each(versions, &ObjectService.delete(bucket, key, &1.version_id))
      end

      Metadata.delete_bucket(bucket)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)

      if is_nil(previous),
        do: Application.delete_env(:ex_storage_service, :s3_auth_enabled),
        else: Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
    end)

    %{credentials: credentials, bucket: bucket}
  end

  test "DeleteObjects accepts UTF-8 XML and deletes the exact Unicode and literal keys", ctx do
    for key <- @keys do
      assert {:ok, _} = ObjectService.put(ctx.bucket, key, "value", "text/plain", %{})
    end

    body = delete_xml(Enum.map(@keys, &{&1, nil}))
    response = delete_objects(ctx, body)
    assert response.status == 200
    refute response.body =~ "<Error>"

    for key <- @keys do
      assert response.body =~ "<Deleted><Key>#{XML.escape(key)}</Key>"
      assert {:error, :object_not_found} = ObjectService.head(ctx.bucket, key)
    end
  end

  test "quiet UTF-8 DeleteObjects removes only the requested historical version", ctx do
    assert :ok = Versioning.set_versioning(ctx.bucket, :enabled)

    assert {:ok, %{version_id: old}} =
             ObjectService.put(ctx.bucket, @version_key, "historical", "text/plain", %{})

    assert {:ok, %{version_id: latest}} =
             ObjectService.put(ctx.bucket, @version_key, "current", "text/plain", %{})

    body =
      delete_xml([{@version_key, old}], true)
      |> String.replace_prefix(~s(<?xml version="1.0" encoding="UTF-8"?>), "")

    response = delete_objects(ctx, body)
    assert response.status == 200
    refute response.body =~ "<Deleted>"
    refute response.body =~ "<Error>"
    assert {:error, :version_not_found} = ObjectService.head(ctx.bucket, @version_key, old, [])
    assert {:ok, %{version_id: ^latest}} = ObjectService.head(ctx.bucket, @version_key)
    assert signed(ctx, :get, object_path(ctx, @version_key)).body == "current"
  end

  test "invalid UTF-8 and DOCTYPE delete documents preserve every object", ctx do
    assert {:ok, _} = ObjectService.put(ctx.bucket, "delete-ascii", "retained", "text/plain", %{})
    valid = delete_xml([{"delete-ascii", nil}])

    invalid_utf8 =
      String.replace(
        valid,
        "</Delete>",
        "<Object><Key>delete-" <> <<255>> <> "</Key></Object></Delete>"
      )

    doctype =
      String.replace(
        valid,
        "<Delete xmlns=",
        "<!DOCTYPE Delete [<!ENTITY key 'delete-ascii'>]><Delete xmlns="
      )

    for body <- [invalid_utf8, doctype] do
      response = delete_objects(ctx, body)
      assert response.status == 400
      assert response.body =~ "<Code>MalformedXML</Code>"
      assert signed(ctx, :get, object_path(ctx, "delete-ascii")).body == "retained"
    end
  end

  test "ListObjectsV2 preserves plus, space, percent and Unicode in plain and URL listings",
       ctx do
    for key <- @keys do
      response = signed(ctx, :put, object_path(ctx, key), [], "value")
      assert response.status == 200
    end

    plain = list(ctx, %{})
    assert plain.status == 200
    refute plain.body =~ "<EncodingType>"
    assert listed_keys(plain.body) == Enum.sort(Enum.map(@keys, &XML.escape/1))

    encoded = list(ctx, %{"encoding-type" => "url"})
    assert encoded.status == 200
    assert encoded.body =~ "<EncodingType>url</EncodingType>"
    assert encoded.body =~ "<Key>plus%2Ba</Key>"
    assert encoded.body =~ "<Key>plus%20a</Key>"
    assert encoded.body =~ "<Key>100%25done</Key>"
    assert Enum.map(listed_keys(encoded.body), &URI.decode/1) == Enum.sort(@keys)

    for {prefix, key} <- [{"plus+", "plus+a"}, {"plus ", "plus a"}] do
      response = list(ctx, %{"prefix" => prefix})
      assert response.status == 200
      assert listed_keys(response.body) == [key]
    end

    for key <- @keys do
      assert signed(ctx, :get, object_path(ctx, key)).body == "value"
    end
  end

  defp delete_xml(entries, quiet \\ false) do
    objects =
      Enum.map_join(entries, fn {key, version} ->
        version_xml = if version, do: "<VersionId>#{XML.escape(version)}</VersionId>", else: ""
        "<Object><Key>#{XML.escape(key)}</Key>#{version_xml}</Object>"
      end)

    ~s(<?xml version="1.0" encoding="UTF-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">) <>
      "<Quiet>#{quiet}</Quiet>#{objects}</Delete>"
  end

  defp delete_objects(ctx, body) do
    headers = [{"content-md5", :crypto.hash(:md5, body) |> Base.encode64()}]
    signed(ctx, :post, "/#{ctx.bucket}?delete", headers, body)
  end

  defp list(ctx, params) do
    query = URI.encode_query(Map.put(params, "list-type", "2"), :rfc3986)
    signed(ctx, :get, "/#{ctx.bucket}?#{query}")
  end

  defp listed_keys(body), do: for([_, key] <- Regex.scan(~r/<Key>([^<]*)<\/Key>/, body), do: key)

  defp object_path(ctx, key) do
    "/#{ctx.bucket}/" <> URI.encode(key, &(URI.char_unreserved?(&1) or &1 == ?/))
  end

  defp signed(ctx, method, path, headers \\ [], body \\ nil) do
    Req.request!(
      method: method,
      url: @base <> path,
      headers: headers,
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
