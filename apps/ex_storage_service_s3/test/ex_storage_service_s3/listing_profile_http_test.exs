defmodule ExStorageServiceS3.ListingProfileHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.IAM.{User, AccessKey, Policy}
  alias ExStorageService.{Metadata, ObjectService}
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("listing-http-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)
    {:ok, policy} = Policy.create_policy("listing-http", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "listing-http-#{suffix}"
    :ok = Metadata.create_bucket(bucket)

    for key <- ["a/1", "a/2", "b/1", "m +雪", "z&<"] do
      {:ok, _} = ObjectService.put(bucket, key, "value", "text/plain", %{})
    end

    on_exit(fn ->
      for key <- ["a/1", "a/2", "b/1", "m +雪", "z&<"], do: ObjectService.delete(bucket, key, nil)
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

  test "zero and malformed limits are deliberate", ctx do
    zero = get(ctx, %{"list-type" => "2", "max-keys" => "0"})
    assert zero.status == 200
    assert zero.body =~ "<KeyCount>0</KeyCount>"
    refute zero.body =~ "<Contents>"
    assert zero.body =~ "<IsTruncated>false</IsTruncated>"

    for params <- [
          %{"max-keys" => "-1"},
          %{"max-keys" => "2garbage"},
          %{"list-type" => "3"},
          %{"encoding-type" => "bogus"},
          %{"list-type" => "2", "continuation-token" => "nonsense"}
        ] do
      assert get(ctx, params).status == 400
    end
  end

  test "v1 and v2 clamp limits above one thousand without changing the page", ctx do
    for version <- [%{}, %{"list-type" => "2"}], delimiter <- [%{}, %{"delimiter" => "/"}] do
      params = Map.merge(version, delimiter)
      boundary = get(ctx, Map.put(params, "max-keys", "1000"))
      clamped = get(ctx, Map.put(params, "max-keys", "1001"))

      assert boundary.status == 200
      assert clamped.status == 200
      assert boundary.body =~ "<MaxKeys>1000</MaxKeys>"
      assert boundary.body =~ "<IsTruncated>false</IsTruncated>"
      assert clamped.body == boundary.body

      expected =
        if delimiter == %{},
          do: ["a/1", "a/2", "b/1", "m +雪", "z&amp;&lt;"],
          else: ["m +雪", "z&amp;&lt;"]

      assert Regex.scan(~r/<Key>([^<]+)<\/Key>/, clamped.body, capture: :all_but_first) ==
               Enum.map(expected, &[&1])

      prefixes =
        Regex.scan(
          ~r/<CommonPrefixes><Prefix>([^<]+)<\/Prefix><\/CommonPrefixes>/,
          clamped.body,
          capture: :all_but_first
        )

      assert prefixes == if(delimiter == %{}, do: [], else: [["a/"], ["b/"]])
    end
  end

  test "v1 markers and v2 opaque context-bound continuation paginate prefixes", ctx do
    first = get(ctx, %{"delimiter" => "/", "max-keys" => "2"})
    assert first.status == 200
    assert first.body =~ "<NextMarker>b/</NextMarker>"
    refute first.body =~ "<ContinuationToken>"
    second = get(ctx, %{"delimiter" => "/", "max-keys" => "2", "marker" => "b/"})
    assert second.body =~ "<Key>m +雪</Key>"
    refute second.body =~ "<CommonPrefixes>"
    first = get(ctx, %{"list-type" => "2", "delimiter" => "/", "max-keys" => "2"})
    assert first.body =~ "<KeyCount>2</KeyCount>"

    [_, token] =
      Regex.run(~r/<NextContinuationToken>([^<]+)<\/NextContinuationToken>/, first.body)

    refute token == "b/"

    second =
      get(ctx, %{
        "list-type" => "2",
        "delimiter" => "/",
        "max-keys" => "2",
        "continuation-token" => token
      })

    assert second.status == 200
    assert second.body =~ "<Key>m +雪</Key>"

    assert get(ctx, %{"list-type" => "2", "prefix" => "different", "continuation-token" => token}).status ==
             400
  end

  test "encoding is opt-in and start-after applies to keys and common prefixes", ctx do
    response =
      get(ctx, %{
        "list-type" => "2",
        "delimiter" => "/",
        "start-after" => "a/",
        "encoding-type" => "url"
      })

    assert response.status == 200
    assert response.body =~ "<EncodingType>url</EncodingType>"
    assert response.body =~ "<StartAfter>a%2F</StartAfter>"
    assert response.body =~ "<Key>m%20%2B%E9%9B%AA</Key>"
    assert response.body =~ "<Key>z%26%3C</Key>"
    refute response.body =~ "<Prefix>a/</Prefix>"
    plain = get(ctx, %{"list-type" => "2"})
    assert plain.body =~ "<Key>z&amp;&lt;</Key>"
    refute plain.body =~ "<EncodingType>"
  end

  defp get(ctx, params) do
    Req.get!(@base <> "/" <> ctx.bucket <> "?" <> URI.encode_query(params),
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
