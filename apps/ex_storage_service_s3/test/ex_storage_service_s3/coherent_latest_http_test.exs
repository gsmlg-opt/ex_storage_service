defmodule ExStorageServiceS3.CoherentLatestHTTPTest do
  use ExUnit.Case, async: false

  alias ExStorageService.IAM.{AccessKey, Policy, User}
  alias ExStorageService.Metadata.{Keys, ObjectCommit}
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.Versioning

  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  # Only scheduling is injected. Every value comes from the real Concord
  # backend, and the competing deletion uses the normal object-domain commit.
  defmodule InterleavingBackend do
    def get(key, opts) do
      result =
        ExStorageService.Metadata.Backend.Concord.get(key, Keyword.delete(opts, :engine))

      hook =
        Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
          if state.key == key and state.hook do
            {state.hook, %{state | hook: nil}}
          else
            {nil, state}
          end
        end)

      if hook, do: hook.()
      result
    end
  end

  # Exercise the existing injectable object-domain boundary over real signed
  # HTTP without adding a scheduling hook to production adapters. The ordinary
  # Router/GET/HEAD/COPY paths are verified separately below.
  defmodule ControlledReadServer do
    import Plug.Conn
    alias ExStorageServiceS3.Handlers.Shared

    def init(opts), do: opts

    def call(conn, opts) do
      conn = fetch_query_params(conn)
      conn = ExStorageServiceS3.Auth.SigV4.call(conn, [])

      if conn.halted do
        conn
      else
        {:ok, operation} = ExStorageServiceS3.Operation.resolve(conn)

        conn =
          conn
          |> assign(:s3_operation, operation)
          |> ExStorageServiceS3.Plugs.Authorize.call([])

        if conn.halted, do: conn, else: serve(conn, operation, opts)
      end
    end

    defp serve(conn, operation, opts) do
      case ObjectService.head(operation.bucket, operation.key, operation.version_id,
             metadata_opts: [backend: InterleavingBackend, engine: Keyword.fetch!(opts, :engine)]
           ) do
        {:ok, %{metadata: metadata, version_id: version_id, delete_marker: false}} ->
          conn =
            conn
            |> Shared.put_s3_headers("controlled-latest-read")
            |> put_resp_header("etag", "\"#{metadata.etag}\"")
            |> put_resp_header("content-length", Integer.to_string(metadata.size))
            |> put_resp_header("content-type", metadata.content_type)
            |> put_resp_header("x-amz-version-id", version_id)
            |> put_resp_header("x-amz-meta-selected", metadata.metadata["selected"])

          if conn.method == "HEAD" do
            send_resp(conn, 200, "")
          else
            {:ok, source} = ObjectService.open_source(metadata)
            Shared.send_blob_source(conn, 200, source, request_id: "controlled-latest-read")
          end

        {:error, :object_not_found} ->
          Shared.error_response(
            conn,
            "NoSuchKey",
            "Missing object",
            conn.request_path,
            "controlled-latest-read"
          )

        {:error, reason} ->
          Shared.storage_error_response(conn, reason, conn.request_path, "controlled-latest-read")
      end
    end
  end

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("coherent-read-#{suffix}")
    {:ok, credentials} = AccessKey.create_access_key(user.id)

    {:ok, policy} =
      Policy.create_policy("coherent-read-#{suffix}", Policy.full_access_statements())

    :ok = Policy.attach_policy(user.id, policy.id)
    bucket = "coherent-read-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)

    {:ok, first} =
      ObjectService.put(bucket, "key", "predecessor bytes", "text/plain", %{"selected" => "first"})

    {:ok, second} =
      ObjectService.put(bucket, "key", "current object bytes", "application/octet-stream", %{
        "selected" => "second"
      })

    on_exit(fn ->
      Application.put_env(:ex_storage_service, :s3_auth_enabled, previous)
      Policy.detach_policy(user.id, policy.id)
      Policy.delete_policy(policy.id)
      AccessKey.delete_key(credentials.access_key_id)
      User.delete_user(user.id)

      for object_key <- ["key", "copy"] do
        {:ok, versions} = Versioning.list_versions(bucket, object_key)
        Enum.each(versions, &ObjectService.delete(bucket, object_key, &1.version_id))
      end

      Metadata.delete_bucket(bucket)
    end)

    %{credentials: credentials, bucket: bucket, first: first, second: second}
  end

  for method <- [:get, :head] do
    expected_body = if method == :get, do: "predecessor bytes", else: ""

    test "signed #{method} retries current deletion after head capture and preserves response identity",
         context do
      method = unquote(method)
      parent = self()

      hook = fn ->
        {:ok, _} = ObjectService.delete(context.bucket, "key", context.second.version_id)
        send(parent, :current_deleted_after_head_capture)
      end

      {:ok, engine} =
        Agent.start_link(fn -> %{key: Keys.object_head(context.bucket, "key"), hook: hook} end)

      server = start_supervised!({Bandit, plug: {ControlledReadServer, engine: engine}, port: 0})
      {:ok, {_, port}} = ThousandIsland.listener_info(server)

      response =
        signed(context.credentials, method, "http://localhost:#{port}/#{context.bucket}/key")

      assert_receive :current_deleted_after_head_capture
      assert response.status == 200
      assert response.headers["x-amz-version-id"] == [context.first.version_id]
      assert response.headers["etag"] == ["\"#{context.first.metadata.etag}\""]

      assert response.headers["content-length"] == [
               Integer.to_string(byte_size("predecessor bytes"))
             ]

      assert response.headers["content-type"] == ["text/plain"]
      assert response.headers["x-amz-meta-selected"] == ["first"]
      assert response.body == unquote(expected_body)
    end
  end

  test "actual bucket deletion and recreation cannot switch an in-flight selected generation",
       context do
    parent = self()

    hook = fn ->
      {:ok, _} = ObjectService.delete(context.bucket, "key", context.second.version_id)
      {:ok, _} = ObjectService.delete(context.bucket, "key", context.first.version_id)
      :ok = Metadata.delete_bucket(context.bucket)
      :ok = Metadata.create_bucket(context.bucket)
      {:ok, _} = ObjectService.put(context.bucket, "key", "new generation", "text/plain", %{})
      send(parent, :bucket_recreated_after_version_capture)
    end

    {:ok, engine} =
      Agent.start_link(fn ->
        %{key: Keys.object_version(context.bucket, "key", context.second.version_id), hook: hook}
      end)

    server = start_supervised!({Bandit, plug: {ControlledReadServer, engine: engine}, port: 0})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    response = signed(context.credentials, :get, "http://localhost:#{port}/#{context.bucket}/key")
    assert_receive :bucket_recreated_after_version_capture
    assert response.status == 409
    assert response.body =~ "<Code>ConditionalRequestConflict</Code>"

    assert signed(context.credentials, :get, @base <> "/#{context.bucket}/key").body ==
             "new generation"
  end

  test "cross-bucket domain and signed copies keep source and destination generations independent",
       context do
    destination = context.bucket <> "-destination"
    :ok = Metadata.create_bucket(destination)

    on_exit(fn ->
      for key <- ["domain-copy", "historical-copy", "http-copy"] do
        {:ok, versions} = Versioning.list_versions(destination, key)
        Enum.each(versions, &ObjectService.delete(destination, key, &1.version_id))
      end

      Metadata.delete_bucket(destination)
    end)

    assert {:error, :bucket_generation_changed} =
             ObjectService.copy(context.bucket, "key", destination, "domain-copy",
               metadata_opts: [bucket_generation: "stale-destination"]
             )

    assert {:ok, %{metadata: %{etag: etag}}} =
             ObjectService.copy(context.bucket, "key", destination, "domain-copy")

    assert etag == context.second.metadata.etag

    assert signed(context.credentials, :get, @base <> "/#{destination}/domain-copy").body ==
             "current object bytes"

    assert {:ok, _} =
             ObjectService.copy(context.bucket, "key", destination, "historical-copy",
               source_version_id: context.first.version_id
             )

    assert signed(context.credentials, :get, @base <> "/#{destination}/historical-copy").body ==
             "predecessor bytes"

    assert signed(context.credentials, :put, @base <> "/#{destination}/http-copy", [
             {"x-amz-copy-source", "/#{context.bucket}/key"}
           ]).status == 200

    assert signed(context.credentials, :get, @base <> "/#{destination}/http-copy").body ==
             "current object bytes"
  end

  test "ordinary signed Router GET HEAD COPY and latest tags preserve the selected version",
       context do
    path = "/#{context.bucket}/key"
    {:ok, _} = ObjectService.delete(context.bucket, "key", context.second.version_id)
    {:ok, _} = ObjectCommit.set_tags(context.bucket, "key", nil, %{"selected" => "first"})

    for method <- [:get, :head] do
      response = signed(context.credentials, method, @base <> path)
      assert response.status == 200
      assert response.headers["x-amz-version-id"] == [context.first.version_id]
      assert response.headers["etag"] == ["\"#{context.first.metadata.etag}\""]

      assert response.headers["content-length"] == [
               Integer.to_string(byte_size("predecessor bytes"))
             ]

      assert response.body == if(method == :get, do: "predecessor bytes", else: "")
    end

    tags = signed(context.credentials, :get, @base <> path <> "?tagging")
    assert tags.status == 200
    assert tags.body =~ "<Key>selected</Key><Value>first</Value>"

    copy =
      signed(context.credentials, :put, @base <> "/#{context.bucket}/copy", [
        {"x-amz-copy-source", path}
      ])

    assert copy.status == 200

    assert signed(context.credentials, :get, @base <> "/#{context.bucket}/copy").body ==
             "predecessor bytes"

    explicit =
      signed(
        context.credentials,
        :get,
        @base <> path <> "?versionId=#{context.second.version_id}"
      )

    assert explicit.status == 404
    assert explicit.body =~ "<Code>NoSuchVersion</Code>"
  end

  defp signed(credentials, method, url, headers \\ []) do
    Req.request!(
      method: method,
      url: url,
      body: "",
      headers: headers,
      retry: false,
      aws_sigv4: [
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key,
        region: "us-east-1",
        service: :s3
      ]
    )
  end
end
