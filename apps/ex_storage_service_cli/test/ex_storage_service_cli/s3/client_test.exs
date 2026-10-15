Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.S3.ClientTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, S3.Client, S3.Endpoint, S3.Request}
  alias ExStorageServiceCli.Test.WireServer

  test "unsigned mode is explicit and missing/incomplete credentials never fall through" do
    for context <- [%Context{}, %Context{access_key_id: "only"}] do
      assert {:error, %Error{category: :credentials}} =
               Client.call(context, "ListBuckets", %{})
    end

    {endpoint, _} =
      WireServer.fixture([
        {200, [], "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"}
      ])

    assert {:ok, %{"Buckets" => []}} =
             Client.call(%Context{endpoint: endpoint, unsigned: true}, "ListBuckets", %{})

    assert_receive {:wire_request, %{headers: headers}}
    refute Map.has_key?(headers, "authorization")
  end

  test "session credentials and encoded query/key values reach the wire" do
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"tag\""}], ""}])

    context = %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret",
      session_token: "fixture-token"
    }

    key = "a//../%+ ?#雪"

    assert {:ok, %{"ETag" => "\"tag\""}} =
             Client.call(context, "HeadObject", %{
               "Bucket" => "bucket",
               "Key" => key,
               "VersionId" => "v+ %"
             })

    assert_receive {:wire_request, %{method: "HEAD", target: target, headers: headers}}
    assert target == "/bucket/a//../%25%2B%20%3F%23%E9%9B%AA?versionId=v%2B%20%25"
    assert headers["x-amz-security-token"] == "fixture-token"
    assert headers["authorization"] =~ "x-amz-security-token"
  end

  test "HTTP-200 copy errors are service failures, not successful moves" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"x-amz-request-id", "request-1"}],
         "<Error><Code>SlowDown</Code><Message>fixture-secret&#xA; retry</Message></Error>"}
      ])

    context = %Context{
      endpoint: endpoint,
      access_key_id: "fixture-access",
      secret_access_key: "fixture-secret"
    }

    assert {:error,
            %Error{
              category: :service,
              code: "SlowDown",
              http_status: 200,
              request_id: "request-1",
              message: message
            }} =
             Client.call(context, "CopyObject", %{
               "Bucket" => "destination",
               "Key" => "key",
               "CopySource" => "/source/key"
             })

    refute message =~ "fixture-secret"
    refute message =~ "\e"
  end

  test "illegal XML controls are protocol failures" do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], "<Error><Code>SlowDown</Code><Message>\e[31m</Message></Error>"}
      ])

    assert {:error, %Error{category: :protocol}} =
             Client.call(%Context{endpoint: endpoint, unsigned: true}, "CopyObject", %{
               "Bucket" => "destination",
               "Key" => "key",
               "CopySource" => "/source/key"
             })
  end

  test "regional creation is modeled and endpoint styles preserve path boundaries" do
    context = %Context{endpoint: "https://objects.test:9443/base", addressing: :virtual}

    request = %Request{
      operation: "GetObject",
      method: "GET",
      path: "/bucket/a//../%25",
      query: [{"versionId", "v+"}]
    }

    assert {:ok, "https://bucket.objects.test:9443/base/a//../%25?versionId=v%2B"} =
             Endpoint.url(context, request, %{"Bucket" => "bucket"})

    assert {:error, %Error{category: :configuration}} =
             Endpoint.url(context, request, %{"Bucket" => "bad_bucket"})
  end
end
