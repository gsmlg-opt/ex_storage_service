defmodule ExStorageServiceS3.AuthorizationHardeningTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{Policy, User}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Multipart
  alias ExStorageServiceS3.{MultipartHandlers, Plugs.Authorize}

  defmodule DeniedUpstream do
    def init(opts), do: opts

    def call(conn, owner) do
      send(owner, {:denied_upstream_request, conn.method})
      Plug.Conn.send_resp(conn, 403, "upstream denied")
    end
  end

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    {:ok, user} = User.create_user("authorization-#{System.unique_integer([:positive])}")
    bucket = "authorization-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)

      case Policy.get_user_policies(user.id) do
        {:ok, policies} -> Enum.each(policies, &Policy.delete_policy(&1.id))
        _ -> :ok
      end

      Concord.delete("user_policies:#{user.id}")
      User.delete_user(user.id)
      Metadata.delete_bucket(bucket)
    end)

    %{user: user, bucket: bucket}
  end

  test "copy needs independent source read and destination write grants", %{
    user: user,
    bucket: bucket
  } do
    grant(user.id, ["s3:PutObject"], ["arn:aws:s3:::#{bucket}/*"])

    operation = %{
      name: :copy_object,
      bucket: bucket,
      key: "destination",
      version_id: nil,
      copy_source: %{bucket: "protected", key: "source", version_id: nil}
    }

    assert %{status: 403, halted: true} = authorize(user.id, operation)
    grant(user.id, ["s3:GetObject"], ["arn:aws:s3:::protected/source"])
    refute authorize(user.id, operation).halted
  end

  test "copying an explicit version requires GetObjectVersion", %{user: user, bucket: bucket} do
    grant(user.id, ["s3:PutObject", "s3:GetObject"], ["*"])

    operation = %{
      name: :upload_part_copy,
      bucket: bucket,
      key: "destination",
      version_id: nil,
      copy_source: %{bucket: "protected", key: "source", version_id: "version"}
    }

    assert %{status: 403} = authorize(user.id, operation)
    grant(user.id, ["s3:GetObjectVersion"], ["arn:aws:s3:::protected/source"])
    refute authorize(user.id, operation).halted
  end

  test "presigned identities still require current policies", %{user: user, bucket: bucket} do
    operation = %{name: :get_object, bucket: bucket, key: "private", version_id: nil}
    conn = request(user.id, operation) |> Plug.Conn.assign(:presigned_auth, true)
    assert %{status: 403} = Authorize.call(conn, [])
  end

  test "all multipart endpoints bind upload id to its original key", %{bucket: bucket} do
    {:ok, upload_id} = Multipart.init_upload(bucket, "original")
    {:ok, etag} = Multipart.store_part(bucket, upload_id, 1, "original bytes")

    paths = [
      {:put, "partNumber=1&uploadId=#{upload_id}", "replacement", :upload_part},
      {:get, "uploadId=#{upload_id}", "", :list_parts},
      {:post, "uploadId=#{upload_id}",
       "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>",
       :complete_multipart_upload},
      {:delete, "uploadId=#{upload_id}", "", :abort_multipart_upload}
    ]

    for {method, query, body, handler} <- paths do
      conn =
        Plug.Test.conn(method, "/#{bucket}/different?#{query}", body)
        |> Plug.Conn.fetch_query_params()

      response = apply(MultipartHandlers, handler, [conn, bucket, "different"])
      assert response.status == 404, "#{handler} must reject mismatched key"
      assert response.resp_body =~ "NoSuchUpload"
      assert {:ok, %{key: "original"}} = Multipart.get_upload(bucket, upload_id)
      assert {:ok, [%{etag: ^etag}]} = Multipart.list_parts(bucket, upload_id)
    end

    :ok = Multipart.abort_upload(bucket, upload_id)
  end

  test "batch deletion authorizes each object and reports denied entries", %{
    user: user,
    bucket: bucket
  } do
    {:ok, _} = ObjectService.put(bucket, "allowed", "a", "text/plain", %{})
    {:ok, _} = ObjectService.put(bucket, "denied", "b", "text/plain", %{})
    {:ok, denied} = ObjectService.head(bucket, "denied")
    grant(user.id, ["s3:DeleteObject"], ["arn:aws:s3:::#{bucket}/allowed"])

    body =
      "<Delete><Object><Key>allowed</Key></Object><Object><Key>denied</Key></Object></Delete>"

    conn =
      Plug.Test.conn(:post, "/#{bucket}?delete", body)
      |> Plug.Conn.assign(:user_id, user.id)
      |> Plug.Conn.put_req_header("content-md5", :crypto.hash(:md5, body) |> Base.encode64())

    response = ExStorageServiceS3.Handlers.Object.delete_objects(conn, bucket)
    assert response.status == 200
    assert response.resp_body =~ "<Deleted><Key>allowed</Key>"
    assert response.resp_body =~ "<Error><Key>denied</Key><Code>AccessDenied</Code>"
    assert {:error, :object_not_found} = ObjectService.head(bucket, "allowed")
    assert {:ok, ^denied} = ObjectService.head(bucket, "denied")
    ObjectService.delete(bucket, "denied", nil)
  end

  test "upstream deletion failures preserve local objects for single and batch requests", %{
    bucket: bucket
  } do
    alias ExStorageService.CloudCache.Config, as: CloudConfig

    # A gateway projection uses legacy local metadata; hosted history is a
    # separately guarded mode and must not make this upstream-failure test vacuous.
    :ok =
      Concord.put("obj:#{bucket}:retained", %{
        etag: Base.encode16(:crypto.hash(:md5, "keep"), case: :lower),
        content_hash: Base.encode16(:crypto.hash(:sha256, "keep"), case: :lower),
        size: 4,
        content_type: "text/plain",
        metadata: %{},
        cloud_backed: true
      })

    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {DeniedUpstream, self()}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)

    :ok =
      CloudConfig.set_config(bucket, %{
        enabled: true,
        provider: :s3_compat,
        endpoint: "http://localhost:#{port}",
        region: "us-east-1",
        bucket: "upstream",
        access_key_id: "test",
        secret_access_key: "test",
        cache_enabled: true
      })

    on_exit(fn -> CloudConfig.delete_config(bucket) end)

    conn = Plug.Test.conn(:delete, "/#{bucket}/retained") |> Plug.Conn.fetch_query_params()
    response = ExStorageServiceS3.Handlers.Object.delete_object(conn, bucket, "retained")
    assert response.status == 403
    assert_receive {:denied_upstream_request, "HEAD"}
    assert {:ok, %{cloud_backed: true}} = Concord.get("obj:#{bucket}:retained")

    body = "<Delete><Object><Key>retained</Key></Object></Delete>"

    conn =
      Plug.Test.conn(:post, "/#{bucket}?delete", body)
      |> Plug.Conn.assign(:user_id, "root")
      |> Plug.Conn.put_req_header("content-md5", :crypto.hash(:md5, body) |> Base.encode64())

    response = ExStorageServiceS3.Handlers.Object.delete_objects(conn, bucket)
    assert response.status == 200
    assert response.resp_body =~ "<Code>AccessDenied</Code>"
    assert_receive {:denied_upstream_request, "HEAD"}
    refute response.resp_body =~ "<Deleted>"
    assert {:ok, %{cloud_backed: true}} = Concord.get("obj:#{bucket}:retained")

    :ok = CloudConfig.delete_config(bucket)
    ObjectService.delete(bucket, "retained", nil)
  end

  test "batch integrity and entry limits reject before any deletion", %{bucket: bucket} do
    {:ok, _} = ObjectService.put(bucket, "retained", "keep", "text/plain", %{})
    one = "<Delete><Object><Key>retained</Key></Object></Delete>"

    for {body, digest} <- [
          {one, nil},
          {one, Base.encode64(<<0::128>>)},
          {"<Delete>" <>
             String.duplicate("<Object><Key>retained</Key></Object>", 1001) <> "</Delete>",
           :valid}
        ] do
      conn =
        Plug.Test.conn(:post, "/#{bucket}?delete", body) |> Plug.Conn.assign(:user_id, "root")

      digest = if digest == :valid, do: :crypto.hash(:md5, body) |> Base.encode64(), else: digest
      conn = if digest, do: Plug.Conn.put_req_header(conn, "content-md5", digest), else: conn
      response = ExStorageServiceS3.Handlers.Object.delete_objects(conn, bucket)
      assert response.status == 400
      assert {:ok, %{delete_marker: false}} = ObjectService.head(bucket, "retained")
    end

    ObjectService.delete(bucket, "retained", nil)
  end

  test "explicit batch versions require DeleteObjectVersion and preserve latest versions", %{
    user: user,
    bucket: bucket
  } do
    :ok = ExStorageService.Storage.Versioning.set_versioning(bucket, :enabled)
    {:ok, %{version_id: old}} = ObjectService.put(bucket, "object", "old", "text/plain", %{})
    {:ok, %{version_id: latest}} = ObjectService.put(bucket, "object", "new", "text/plain", %{})
    grant(user.id, ["s3:DeleteObject"], ["arn:aws:s3:::#{bucket}/object"])
    body = "<Delete><Object><Key>object</Key><VersionId>#{old}</VersionId></Object></Delete>"

    request = fn ->
      Plug.Test.conn(:post, "/#{bucket}?delete", body)
      |> Plug.Conn.assign(:user_id, user.id)
      |> Plug.Conn.put_req_header("content-md5", :crypto.hash(:md5, body) |> Base.encode64())
      |> ExStorageServiceS3.Handlers.Object.delete_objects(bucket)
    end

    assert request.().resp_body =~ "<Code>AccessDenied</Code>"
    assert {:ok, _} = ObjectService.head(bucket, "object", old, [])
    grant(user.id, ["s3:DeleteObjectVersion"], ["arn:aws:s3:::#{bucket}/object"])
    response = request.()
    assert response.resp_body =~ "<VersionId>#{old}</VersionId>"
    assert {:error, :version_not_found} = ObjectService.head(bucket, "object", old, [])
    assert {:ok, %{version_id: ^latest}} = ObjectService.head(bucket, "object")
  end

  test "multipart completion retains initiation attributes and honors atomic create-only conditions",
       %{bucket: bucket} do
    initiate =
      Plug.Test.conn(:post, "/#{bucket}/attributes?uploads")
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> Plug.Conn.put_req_header("content-encoding", "gzip")
      |> Plug.Conn.put_req_header("x-amz-meta-owner", "original")

    response = MultipartHandlers.create_multipart_upload(initiate, bucket, "attributes")
    [_, upload] = Regex.run(~r/<UploadId>([^<]+)<\/UploadId>/, response.resp_body)
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "encoded bytes")

    assert complete_request(bucket, "attributes", upload, etag, [{"if-none-match", "*"}]).status ==
             200

    assert {:ok, %{metadata: metadata}} = ObjectService.head(bucket, "attributes")
    assert metadata.content_type == "text/plain"
    assert metadata.content_encoding == "gzip"
    assert metadata.metadata == %{"owner" => "original"}

    parent = self()

    uploads =
      for _ <- 1..32 do
        {:ok, upload} = Multipart.init_upload(bucket, "raced")
        {:ok, etag} = Multipart.store_part(bucket, upload, 1, "candidate")
        {upload, etag}
      end

    tasks =
      for {upload, etag} <- uploads do
        Task.async(fn ->
          send(parent, {:completion_ready, self()})
          receive do: (:complete -> :ok)
          complete_request(bucket, "raced", upload, etag, [{"if-none-match", "*"}]).status
        end)
      end

    for _ <- 1..32 do
      assert_receive {:completion_ready, _pid}, 5000
    end

    Enum.each(tasks, &send(&1.pid, :complete))
    results = Task.await_many(tasks, 30_000)
    assert Enum.count(results, &(&1 == 200)) == 1
    assert Enum.count(results, &(&1 == 412)) == 31, inspect(Enum.frequencies(results))
    Enum.each(uploads, fn {upload, _} -> Multipart.abort_upload(bucket, upload) end)
  end

  defp complete_request(bucket, key, upload, etag, headers) do
    body =
      "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>#{etag}</ETag></Part></CompleteMultipartUpload>"

    conn =
      Plug.Test.conn(:post, "/#{bucket}/#{key}?uploadId=#{upload}", body)
      |> Plug.Conn.fetch_query_params()

    conn =
      Enum.reduce(headers, conn, fn {name, value}, current ->
        Plug.Conn.put_req_header(current, name, value)
      end)

    MultipartHandlers.complete_multipart_upload(conn, bucket, key)
  end

  defp authorize(user_id, operation),
    do: operation |> then(&request(user_id, &1)) |> Authorize.call([])

  defp request(user_id, operation) do
    Plug.Test.conn(:put, "/#{operation.bucket}/#{operation.key}")
    |> Plug.Conn.assign(:user_id, user_id)
    |> Plug.Conn.assign(:s3_operation, operation)
  end

  defp grant(user_id, actions, resources) do
    {:ok, policy} =
      Policy.create_policy("authorization", [
        %{effect: :allow, actions: actions, resources: resources}
      ])

    :ok = Policy.attach_policy(user_id, policy.id)
  end
end
