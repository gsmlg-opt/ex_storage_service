defmodule ExStorageServiceS3.OperationRoutingTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceS3.Router

  defp request(method, path, body \\ "") do
    method |> Plug.Test.conn(path, body) |> Router.call(Router.init([]))
  end

  setup do
    bucket = "routing-#{System.unique_integer([:positive])}"
    assert request(:put, "/#{bucket}").status in [200, 201]
    assert request(:put, "/#{bucket}/key", "original").status == 200
    %{bucket: bucket}
  end

  test "unsupported subresources cannot overwrite or delete bytes", %{bucket: bucket} do
    assert :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :enabled)
    assert :ok = ExStorageService.Storage.Lifecycle.put_rules(bucket, [%{id: "preserved"}])
    assert :ok = ExStorageService.Notifications.put_config(bucket, [%{id: "preserved"}])
    prefixes = ~w(bucket_versioning lifecycle notification bucket_cors bucket_tagging cloud_cache)
    configurations = Map.new(prefixes, &{&1, Concord.get("#{&1}:#{bucket}")})
    assert {:ok, versions} = ExStorageService.Storage.Versioning.list_versions(bucket, "key")
    assert {:ok, configuration} = ExStorageService.Metadata.get_bucket(bucket)

    for selector <- ["acl", "retention", "legal-hold", "versions", "website", "policy"] do
      assert request(:put, "/#{bucket}/key?#{selector}", "corrupt").status == 501
      assert request(:delete, "/#{bucket}/key?#{selector}").status == 501
      assert request(:get, "/#{bucket}/key").resp_body == "original"
      assert {:ok, ^versions} = ExStorageService.Storage.Versioning.list_versions(bucket, "key")
      assert {:ok, ^configuration} = ExStorageService.Metadata.get_bucket(bucket)

      for prefix <- prefixes do
        assert Concord.get("#{prefix}:#{bucket}") == configurations[prefix]
      end
    end
  end

  test "tagging operations change only tags and never fall through to object CRUD", %{
    bucket: bucket
  } do
    alias ExStorageServiceS3.Handlers.Tagging
    assert request(:put, "/#{bucket}/key?tagging", "corrupt").status == 400

    assert request(:put, "/#{bucket}/key?tagging", Tagging.document(%{"owner" => "test"})).status ==
             200

    assert request(:get, "/#{bucket}/key?tagging").resp_body =~ "<Key>owner</Key>"
    assert request(:get, "/#{bucket}/key").resp_body == "original"
    assert request(:delete, "/#{bucket}/key?tagging").status == 204
    assert request(:get, "/#{bucket}/key?tagging").resp_body =~ "<TagSet></TagSet>"
    assert request(:get, "/#{bucket}/key").resp_body == "original"
  end

  test "conflicting and duplicate selectors fail before mutation", %{bucket: bucket} do
    for query <- [
          "acl&tagging",
          "uploadId=one&uploadId=two",
          "partNumber=1",
          "uploads&uploadId=one",
          "versionId="
        ] do
      assert request(:put, "/#{bucket}/key?#{query}", "corrupt").status == 400
      assert request(:get, "/#{bucket}/key").resp_body == "original"
    end
  end

  test "benign SDK query parameters do not prevent ordinary CRUD", %{bucket: bucket} do
    assert request(:put, "/#{bucket}/key?x-id=PutObject", "updated").status == 200
    assert request(:get, "/#{bucket}/key?x-id=GetObject").resp_body == "updated"
  end

  test "HEAD errors are bodyless", %{bucket: bucket} do
    response = request(:head, "/#{bucket}/key?acl")
    assert response.status == 501
    assert response.resp_body == ""
  end

  test "oversized keys and custom metadata fail before mutation", %{bucket: bucket} do
    key = String.duplicate("k", 1025)
    assert request(:put, "/#{bucket}/#{key}", "bad").status == 400

    conn =
      Plug.Test.conn(:put, "/#{bucket}/key", "bad")
      |> Plug.Conn.put_req_header("x-amz-meta-large", String.duplicate("v", 2049))
      |> Router.call(Router.init([]))

    assert conn.status == 400
    assert request(:get, "/#{bucket}/key").resp_body == "original"
  end

  test "copy headers cannot select multipart initiation or completion", %{bucket: bucket} do
    alias ExStorageService.Storage.{Multipart, Versioning}
    assert {:ok, upload} = Multipart.init_upload(bucket, "key")
    assert {:ok, etag} = Multipart.store_part(bucket, upload, 1, "retained-part")
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert {:ok, parts} = Multipart.list_parts(bucket, upload)

    body =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>"

    for {method, query} <- [
          post: "uploads",
          post: "uploadId=#{upload}",
          get: "uploadId=#{upload}"
        ] do
      response =
        Plug.Test.conn(method, "/#{bucket}/key?#{query}", body)
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/#{bucket}/key")
        |> Router.call(Router.init([]))

      assert response.status == 501
      assert {:ok, ^parts} = Multipart.list_parts(bucket, upload)
      assert {:ok, ^versions} = Versioning.list_versions(bucket, "key")
      assert request(:get, "/#{bucket}/key").resp_body == "original"
    end

    conn = Plug.Test.conn(:put, "/#{bucket}/key", "corrupt")

    conn = %{
      conn
      | req_headers: [
          {"x-amz-copy-source", "/#{bucket}/key"},
          {"x-amz-copy-source", "/#{bucket}/other"} | conn.req_headers
        ]
    }

    assert Router.call(conn, Router.init([])).status == 400
    assert {:ok, ^parts} = Multipart.list_parts(bucket, upload)
    assert {:ok, ^versions} = Versioning.list_versions(bucket, "key")
    assert request(:get, "/#{bucket}/key").resp_body == "original"
    assert :ok = Multipart.abort_upload(bucket, upload)
  end

  test "copy source rejects duplicate and empty version identities", %{bucket: bucket} do
    for query <- ["versionId=", "versionId=one&versionId=two"] do
      conn =
        Plug.Test.conn(:put, "/#{bucket}/destination", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/#{bucket}/key?#{query}")
        |> Router.call(Router.init([]))

      assert conn.status == 400
      assert request(:get, "/#{bucket}/destination").status == 404
    end
  end
end
