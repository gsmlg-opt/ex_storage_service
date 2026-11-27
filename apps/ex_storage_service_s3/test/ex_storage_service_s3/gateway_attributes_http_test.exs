defmodule ExStorageServiceS3.GatewayAttributesHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.CloudCache.Config
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageServiceS3.Handlers.Object.CloudCopy
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  defmodule Upstream do
    import Plug.Conn
    def init(agent), do: agent

    def call(conn, agent) do
      case conn.method do
        "PUT" ->
          {:ok, body, conn} = read_body(conn)

          status =
            Agent.get_and_update(agent, fn state ->
              current = Map.get(state, conn.request_path)
              match = get_req_header(conn, "if-match")
              none_match = get_req_header(conn, "if-none-match")

              state =
                Map.update(state, :put_requests, [conn.req_headers], &[conn.req_headers | &1])

              if (none_match == ["*"] and current != nil) or
                   (match != [] and (current == nil or match != [etag(current)])) do
                {412, state}
              else
                {200, Map.put(state, conn.request_path, %{body: body, headers: conn.req_headers})}
              end
            end)

          send_resp(conn, status, "")

        method when method in ["HEAD", "GET"] ->
          Agent.update(
            agent,
            &Map.put(&1, :checksum_mode, get_req_header(conn, "x-amz-checksum-mode"))
          )

          case Agent.get(agent, &Map.get(&1, conn.request_path)) do
            nil ->
              send_resp(conn, 404, "")

            object ->
              headers =
                Enum.filter(object.headers, fn {name, _} ->
                  name in ~w(content-type content-encoding cache-control content-disposition content-language expires) or
                    String.starts_with?(name, "x-amz-meta-") or
                    String.starts_with?(name, "x-amz-checksum-")
                end)

              conn =
                Enum.reduce(headers, conn, fn {name, value}, acc ->
                  put_resp_header(acc, name, value)
                end)

              conn
              |> put_resp_header("etag", etag(object))
              |> put_resp_header("content-length", to_string(byte_size(object.body)))
              |> send_resp(200, if(method == "HEAD", do: "", else: object.body))
          end
      end
    end

    defp etag(object),
      do: "\"" <> Base.encode16(:crypto.hash(:md5, object.body), case: :lower) <> "\""
  end

  setup do
    previous = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, false)
    on_exit(fn -> Application.put_env(:ex_storage_service, :s3_auth_enabled, previous) end)
    bucket = "gateway-attrs-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    agent = start_supervised!({Agent, fn -> %{} end})

    server =
      start_supervised!(
        {Gaoxy.HTTP, plug: {Upstream, agent}, port: 0, limits: %{max_bytes: 4_194_304}}
      )

    %{local: {:ok, {_, port}}} = Gaoxy.info(server, :http)

    :ok =
      Config.set_config(bucket, %{
        enabled: true,
        provider: :s3_compat,
        endpoint: "http://localhost:#{port}",
        region: "us-east-1",
        bucket: "remote",
        access_key_id: "access",
        secret_access_key: "secret",
        cache_enabled: true
      })

    on_exit(fn -> Config.delete_config(bucket) end)
    %{bucket: bucket, agent: agent}
  end

  test "verified upload checksums and raw encoded attributes reach upstream and return on HEAD",
       context do
    body = :zlib.gzip("raw gateway bytes")
    checksum = Base.encode64(<<:erlang.crc32(body)::unsigned-big-32>>)
    url = @base <> "/#{context.bucket}/object"

    response =
      Req.put!(url,
        retry: false,
        body: body,
        headers: [
          {"content-type", "application/x-gateway"},
          {"content-encoding", "gzip"},
          {"cache-control", "private"},
          {"content-disposition", "attachment"},
          {"content-language", "fr"},
          {"expires", "Thu, 01 Jan 2037 00:00:00 GMT"},
          {"x-amz-meta-owner", "initial"},
          {"x-amz-checksum-crc32", checksum}
        ]
      )

    assert response.status == 200
    assert Req.Response.get_header(response, "x-amz-checksum-crc32") == [checksum]
    saved = Agent.get(context.agent, &Map.fetch!(&1, "/remote/object"))
    assert saved.body == body
    assert {"x-amz-checksum-crc32", checksum} in saved.headers
    assert {"content-encoding", "gzip"} in saved.headers
    assert {"content-language", "fr"} in saved.headers
    head = Req.head!(url, retry: false)
    assert head.status == 200
    assert Req.Response.get_header(head, "x-amz-checksum-crc32") == [checksum]
    assert Req.Response.get_header(head, "content-type") == ["application/x-gateway"]
    assert Req.Response.get_header(head, "content-encoding") == ["gzip"]
    assert Req.Response.get_header(head, "x-amz-meta-owner") == ["initial"]
    assert Agent.get(context.agent, & &1.checksum_mode) == ["ENABLED"]
  end

  test "gateway PUT enforces upstream preconditions without changing rejected local state",
       context do
    alias ExStorageService.CloudCache.LocalStore

    url = @base <> "/#{context.bucket}/conditional"
    first = Req.put!(url, body: "first", retry: false, headers: [{"if-none-match", "*"}])
    assert first.status == 200
    [etag] = Req.Response.get_header(first, "etag")
    assert {:ok, original} = Metadata.get_object_meta(context.bucket, "conditional")
    assert {:ok, cached} = LocalStore.get(context.bucket, "conditional")
    assert File.read!(cached) == "first"

    Agent.update(context.agent, fn state ->
      Map.update!(state, "/remote/conditional", &%{&1 | body: "external"})
    end)

    for headers <- [[{"if-none-match", "*"}], [{"if-match", etag}]] do
      rejected = Req.put!(url, body: "rejected", retry: false, headers: headers)
      assert rejected.status == 412
      assert rejected.body =~ "<Code>PreconditionFailed</Code>"
      assert {:ok, ^original} = Metadata.get_object_meta(context.bucket, "conditional")
      assert File.read!(cached) == "first"
      assert Agent.get(context.agent, &Map.fetch!(&1, "/remote/conditional")).body == "external"
    end

    requests = Agent.get(context.agent, & &1.put_requests)
    assert Enum.any?(requests, &({"if-none-match", "*"} in &1))
    assert Enum.any?(requests, &({"if-match", etag} in &1))

    external_etag = "\"" <> Base.encode16(:crypto.hash(:md5, "external"), case: :lower) <> "\""

    accepted =
      Req.put!(url, body: "updated", retry: false, headers: [{"if-match", external_etag}])

    assert accepted.status == 200
    assert Agent.get(context.agent, &Map.fetch!(&1, "/remote/conditional")).body == "updated"
    assert {:ok, updated} = Metadata.get_object_meta(context.bucket, "conditional")
    refute updated.etag == original.etag
  end

  test "copy to gateway recomputes a source composite checksum and preserves system headers",
       context do
    source_bucket = "gateway-attrs-source-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(source_bucket)
    {:ok, result} = ObjectService.put(source_bucket, "source", "source bytes", "text/plain", %{})
    old = Base.encode64(:crypto.hash(:sha256, "part checksums")) <> "-2"
    metadata = Map.put(result.metadata, :checksums, %{"sha256" => old})

    attrs = %{
      content_type: "text/plain",
      metadata: %{"owner" => "copy"},
      checksums: metadata.checksums,
      system_headers: [{"cache-control", "private"}]
    }

    assert {:ok, _} =
             CloudCopy.copy(
               %{bucket: source_bucket, key: "source", version_id: nil},
               context.bucket,
               "copied",
               metadata,
               attrs,
               "gateway-copy-attrs-#{System.unique_integer([:positive])}"
             )

    upstream = Agent.get(context.agent, &Map.fetch!(&1, "/remote/copied"))
    expected = Base.encode64(:crypto.hash(:sha256, "source bytes"))
    assert upstream.body == "source bytes"
    assert {"x-amz-checksum-sha256", expected} in upstream.headers
    refute {"x-amz-checksum-sha256", old} in upstream.headers
    assert {"cache-control", "private"} in upstream.headers
    assert {"x-amz-meta-owner", "copy"} in upstream.headers
  end
end
