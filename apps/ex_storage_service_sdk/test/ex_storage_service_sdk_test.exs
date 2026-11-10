Code.require_file("support/wire_server.exs", __DIR__)

defmodule ExStorageServiceSdkTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceSdk.{Config, Credentials, Error}
  alias ExStorageServiceSdk.Test.WireServer

  @key "dir//naïve file+plus%25 percent.txt"

  defp client(endpoint, opts \\ []) do
    {:ok, config} =
      ExStorageServiceSdk.new(
        Keyword.merge(
          [endpoint: endpoint, access_key_id: "AKIDEXAMPLE", secret_access_key: "secret"],
          opts
        )
      )

    config
  end

  defp xml(body), do: ~s(<?xml version="1.0" encoding="UTF-8"?>) <> body

  describe "new/1" do
    test "captures endpoint, region, path style and static credentials" do
      assert {:ok,
              %Config{
                endpoint: "http://minio.local:9100",
                region: "eu-west-1",
                addressing: :path,
                access_key_id: "a",
                credentials: %Credentials{secret_access_key: "s"}
              }} =
               ExStorageServiceSdk.new(
                 endpoint: "http://minio.local:9100/",
                 region: "eu-west-1",
                 access_key_id: "a",
                 secret_access_key: "s"
               )

      assert {:ok, %Config{addressing: :virtual}} =
               ExStorageServiceSdk.new(endpoint: "https://s3.test", path_style: false)
    end

    test "rejects invalid endpoints and incomplete credentials without echoing secrets" do
      assert {:error, %Error{category: :configuration}} =
               ExStorageServiceSdk.new(endpoint: "ftp://host")

      assert {:error, %Error{category: :configuration}} = ExStorageServiceSdk.new([])

      assert {:error, %Error{category: :credentials} = error} =
               ExStorageServiceSdk.new(endpoint: "http://h", secret_access_key: "top-secret")

      refute inspect(error) =~ "top-secret"
      refute inspect(client("http://h")) =~ "secret"
    end
  end

  describe "objects" do
    test "exact keys are path-encoded once and raw bytes survive round trip" do
      bytes = <<0, 255, " {\n  \"a\" : 1 }\n", 31, 139>>

      {endpoint, _} =
        WireServer.fixture([
          {200, [{"ETag", ~s("put-tag")}], ""},
          {200, [{"ETag", ~s("put-tag")}, {"Content-Type", "application/octet-stream"}], bytes}
        ])

      s3 = client(endpoint)

      assert {:ok, %{status: 200, etag: ~s("put-tag")}} =
               ExStorageServiceSdk.put_object(s3, "bucket", @key, bytes, %{
                 "ContentType" => "application/octet-stream"
               })

      assert_receive {:wire_request, %{method: "PUT", target: target, body: ^bytes}}
      assert target == "/bucket/dir//na%C3%AFve%20file%2Bplus%2525%20percent.txt"

      assert {:ok, %{status: 200, body: ^bytes, etag: ~s("put-tag")}} =
               ExStorageServiceSdk.get_object(s3, "bucket", @key)

      assert_receive {:wire_request, %{method: "GET", target: ^target}}
    end

    test "bodies larger than one transport chunk upload intact" do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", ~s("big")}], ""}])
      body = :crypto.strong_rand_bytes(5 * 65_536 + 7)

      assert {:ok, %{etag: ~s("big")}} =
               ExStorageServiceSdk.put_object(client(endpoint), "bucket", "big", body)

      assert_receive {:wire_request, %{method: "PUT", body: ^body}}, 5_000
    end

    test "empty objects read back as empty binaries" do
      {endpoint, _} = WireServer.fixture([{200, [{"ETag", ~s("empty")}], ""}])

      assert {:ok, %{status: 200, body: "", etag: ~s("empty")}} =
               ExStorageServiceSdk.get_object(client(endpoint), "bucket", "empty")
    end

    test "a range read is a successful 206 and an unsatisfiable range is a 416 error" do
      {endpoint, _} =
        WireServer.fixture([
          {206, [{"Content-Range", "bytes 2-4/10"}, {"ETag", ~s("t")}], "234"},
          {416, [{"Content-Type", "application/xml"}],
           xml("<Error><Code>InvalidRange</Code><Message>bad range</Message></Error>")}
        ])

      s3 = client(endpoint)

      assert {:ok, %{status: 206, body: "234", result: %{"ContentRange" => "bytes 2-4/10"}}} =
               ExStorageServiceSdk.get_object(s3, "bucket", "k", %{"Range" => "bytes=2-4"})

      assert_receive {:wire_request, %{headers: %{"range" => "bytes=2-4"}}}

      assert {:error, %Error{category: :service, http_status: 416, code: "InvalidRange"}} =
               ExStorageServiceSdk.get_object(s3, "bucket", "k", %{"Range" => "bytes=20-30"})
    end

    test "missing objects are 404 for GET and bodiless HEAD" do
      {endpoint, _} =
        WireServer.fixture([
          {404, [], xml("<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>")},
          {404, [], ""}
        ])

      s3 = client(endpoint)

      assert {:error, %Error{category: :service, http_status: 404, code: "NoSuchKey"}} =
               ExStorageServiceSdk.get_object(s3, "bucket", "missing")

      assert {:error, %Error{category: :service, http_status: 404, code: "HTTP404"}} =
               ExStorageServiceSdk.head_object(s3, "bucket", "missing")
    end

    test "conditional writes send their predicate and surface 412 without replay" do
      {endpoint, _} =
        WireServer.fixture([
          {412, [],
           xml("<Error><Code>PreconditionFailed</Code><Message>exists</Message></Error>")}
        ])

      assert {:error, %Error{http_status: 412, code: "PreconditionFailed"}} =
               ExStorageServiceSdk.put_object(client(endpoint), "bucket", "k", "v", %{
                 "IfNoneMatch" => "*"
               })

      assert_receive {:wire_request, %{headers: %{"if-none-match" => "*"}}}
      refute_receive {:wire_request, _}, 50
    end

    test "conditional deletes and heads pass If-Match" do
      {endpoint, _} = WireServer.fixture([{204, [], ""}, {200, [{"ETag", ~s("e")}], ""}])
      s3 = client(endpoint)

      assert {:ok, %{status: 204}} =
               ExStorageServiceSdk.delete_object(s3, "bucket", "k", %{"IfMatch" => ~s("e")})

      assert_receive {:wire_request, %{method: "DELETE", headers: %{"if-match" => ~s("e")}}}

      assert {:ok, %{status: 200, etag: ~s("e")}} =
               ExStorageServiceSdk.head_object(s3, "bucket", "k", %{"IfMatch" => ~s("e")})

      assert_receive {:wire_request, %{method: "HEAD", headers: %{"if-match" => ~s("e")}}}
    end

    test "copy sends encoded source, conditions and directives" do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], xml(~s(<CopyObjectResult><ETag>"copy"</ETag></CopyObjectResult>))}
        ])

      assert {:ok, %{status: 200, result: %{"CopyObjectResult" => %{"ETag" => ~s("copy")}}}} =
               ExStorageServiceSdk.copy_object(client(endpoint), "dest", "to", %{
                 "CopySource" => %{"Bucket" => "src", "Key" => @key},
                 "CopySourceIfMatch" => ~s("source-tag"),
                 "MetadataDirective" => "REPLACE",
                 "TaggingDirective" => "COPY",
                 "Metadata" => %{"owner" => "me"}
               })

      assert_receive {:wire_request, %{method: "PUT", target: "/dest/to", headers: headers}}

      assert headers["x-amz-copy-source"] ==
               "/src/dir//na%C3%AFve%20file%2Bplus%2525%20percent.txt"

      assert headers["x-amz-copy-source-if-match"] == ~s("source-tag")
      assert headers["x-amz-metadata-directive"] == "REPLACE"
      assert headers["x-amz-tagging-directive"] == "COPY"
      assert headers["x-amz-meta-owner"] == "me"
    end

    test "HTTP 200 carrying an S3 error document is a failure, not a copy" do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], xml("<Error><Code>InternalError</Code><Message>late</Message></Error>")}
        ])

      assert {:error, %Error{code: "InternalError"}} =
               ExStorageServiceSdk.copy_object(client(endpoint), "dest", "to", %{
                 "CopySource" => %{"Bucket" => "src", "Key" => "k"}
               })
    end

    test "an object larger than max_body_bytes fails instead of growing unbounded" do
      {endpoint, _} = WireServer.fixture([{200, [], String.duplicate("x", 64)}])

      assert {:error, %Error{code: :body_too_large}} =
               ExStorageServiceSdk.get_object(client(endpoint), "bucket", "k", %{},
                 max_body_bytes: 16
               )
    end
  end

  describe "listing" do
    test "url-encoded keys are decoded and continuation tokens are followed" do
      first =
        xml("""
        <ListBucketResult><Name>bucket</Name><EncodingType>url</EncodingType>
        <IsTruncated>true</IsTruncated><NextContinuationToken>tok+1/=</NextContinuationToken>
        <Contents><Key>dir//na%C3%AFve+file%2Bplus%2525+percent.txt</Key><Size>1</Size></Contents>
        </ListBucketResult>
        """)

      second =
        xml("""
        <ListBucketResult><Name>bucket</Name><EncodingType>url</EncodingType>
        <IsTruncated>false</IsTruncated>
        <Contents><Key>z</Key><Size>2</Size></Contents>
        </ListBucketResult>
        """)

      {endpoint, _} = WireServer.fixture([{200, [], first}, {200, [], second}])

      keys =
        client(endpoint)
        |> ExStorageServiceSdk.pages("ListObjectsV2", %{"Bucket" => "bucket"})
        |> Enum.flat_map(fn {:ok, page} -> Enum.map(page["Contents"], & &1["Key"]) end)

      assert keys == [@key, "z"]

      assert_receive {:wire_request, %{target: target}}
      assert target =~ "encoding-type=url"
      assert_receive {:wire_request, %{target: target}}
      assert target =~ "continuation-token=tok%2B1%2F%3D"
    end

    test "list_objects_v2 decodes strictly percent-encoded keys (ESS style)" do
      page =
        xml("""
        <ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>
        <Contents><Key>a%20b%2Bc%25</Key></Contents></ListBucketResult>
        """)

      {endpoint, _} = WireServer.fixture([{200, [], page}])

      assert {:ok, %{"Contents" => [%{"Key" => "a b+c%"}]}} =
               ExStorageServiceSdk.list_objects_v2(client(endpoint), "bucket", %{"Prefix" => "a"})
    end
  end

  describe "multipart" do
    test "parts upload, complete in part order and abort" do
      {endpoint, _} =
        WireServer.fixture([
          {200, [],
           xml(
             "<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"
           )},
          {200, [{"ETag", ~s("p2")}], ""},
          {200, [{"ETag", ~s("p1")}], ""},
          {200, [],
           xml(
             ~s(<CompleteMultipartUploadResult><ETag>"final-2"</ETag></CompleteMultipartUploadResult>)
           )},
          {204, [], ""}
        ])

      s3 = client(endpoint)

      assert {:ok, %{"UploadId" => "up-1"}} =
               ExStorageServiceSdk.create_multipart_upload(s3, "bucket", "big")

      assert {:ok, %{etag: ~s("p2")}} =
               ExStorageServiceSdk.upload_part(s3, "bucket", "big", "up-1", 2, "second")

      assert {:ok, %{etag: ~s("p1")}} =
               ExStorageServiceSdk.upload_part(s3, "bucket", "big", "up-1", 1, "first")

      assert {:ok, %{result: %{"ETag" => ~s("final-2")}}} =
               ExStorageServiceSdk.complete_multipart_upload(s3, "bucket", "big", "up-1", [
                 {2, ~s("p2")},
                 {1, ~s("p1")}
               ])

      assert {:ok, _} = ExStorageServiceSdk.abort_multipart_upload(s3, "bucket", "big", "up-1")

      assert_receive {:wire_request, %{method: "POST", target: "/bucket/big?uploads="}}

      assert_receive {:wire_request,
                      %{target: "/bucket/big?partNumber=2&uploadId=up-1", body: "second"}}

      assert_receive {:wire_request,
                      %{target: "/bucket/big?partNumber=1&uploadId=up-1", body: "first"}}

      assert_receive {:wire_request,
                      %{method: "POST", target: "/bucket/big?uploadId=up-1", body: body}}

      assert [_, part1, part2] = String.split(body, "<PartNumber>")
      assert part1 =~ ~r/^1</
      assert part2 =~ ~r/^2</
      assert_receive {:wire_request, %{method: "DELETE", target: "/bucket/big?uploadId=up-1"}}
    end

    test "a completion answered with HTTP 200 and an S3 error is a failure" do
      {endpoint, _} =
        WireServer.fixture([
          {200, [], xml("<Error><Code>InternalError</Code><Message>retry</Message></Error>")}
        ])

      assert {:error, %Error{code: "InternalError"}} =
               ExStorageServiceSdk.complete_multipart_upload(
                 client(endpoint),
                 "bucket",
                 "big",
                 "up-1",
                 [{1, ~s("p1")}]
               )
    end
  end

  describe "configuration capture" do
    test "credential_provider runs before each signed request" do
      {endpoint, _} = WireServer.fixture([{200, [], ""}, {200, [], ""}])
      counter = :counters.new(1, [])

      provider = fn ->
        :counters.add(counter, 1, 1)
        n = :counters.get(counter, 1)
        Credentials.new(%{access_key_id: "AK#{n}", secret_access_key: "s#{n}"}, %{})
      end

      {:ok, s3} = ExStorageServiceSdk.new(endpoint: endpoint, credential_provider: provider)

      assert {:ok, _} = ExStorageServiceSdk.head_object(s3, "b", "k")
      assert {:ok, _} = ExStorageServiceSdk.head_object(s3, "b", "k")

      assert_receive {:wire_request, %{headers: %{"authorization" => first}}}
      assert_receive {:wire_request, %{headers: %{"authorization" => second}}}
      assert first =~ "Credential=AK1/"
      assert second =~ "Credential=AK2/"
    end

    test "a custom transport receives the request and its structured failure is returned" do
      defmodule FailingTransport do
        @behaviour ExStorageServiceSdk.Transport
        def request(url, request, _opts) do
          send(self(), {:transport_request, url, request.method})
          {:error, Error.new(:transport, "offline", code: :econnrefused)}
        end

        def consume(_response, _acc, _reducer, _opts), do: raise("unused")
        def close(_response), do: :ok
      end

      {:ok, s3} =
        ExStorageServiceSdk.new(
          endpoint: "http://storage.invalid:9000",
          unsigned: true,
          transport: FailingTransport
        )

      assert {:error, %Error{category: :transport, code: :econnrefused}} =
               ExStorageServiceSdk.delete_object(s3, "b", "k", %{}, max_attempts: 1)

      assert_received {:transport_request, "http://storage.invalid:9000/b/k", "DELETE"}
    end
  end
end
