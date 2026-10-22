defmodule ExStorageServiceS3.GatewayReadHTTPTest do
  use ExUnit.Case, async: false
  alias ExStorageService.CloudCache.Config
  alias ExStorageService.Metadata
  @base "http://localhost:#{Application.compile_env(:ex_storage_service, :s3_port, 9001)}"

  defmodule Upstream do
    import Plug.Conn
    def init(agent), do: agent

    def call(conn, agent) do
      state =
        Agent.get_and_update(agent, fn state ->
          {state,
           Map.update!(state, :requests, &[{conn.method, get_req_header(conn, "if-match")} | &1])}
        end)

      cond do
        state.status != 200 ->
          send_resp(conn, state.status, "")

        conn.method == "GET" and get_req_header(conn, "if-match") != [state.etag] ->
          send_resp(conn, 412, "")

        true ->
          conn
          |> put_resp_header("etag", state.etag)
          |> put_resp_header("content-length", Integer.to_string(byte_size(state.body)))
          |> put_resp_header("content-type", "application/octet-stream")
          |> put_resp_header("last-modified", "Sun, 18 Oct 2026 00:00:00 GMT")
          |> put_resp_header("cache-control", state.cache_control)
          |> put_resp_header(
            "x-amz-checksum-crc32",
            Base.encode64(<<:erlang.crc32(state.body)::unsigned-big-32>>)
          )
          |> put_resp_header("x-amz-meta-custom", state.custom)
          |> respond(state)
      end
    end

    defp respond(%{method: "GET"} = conn, %{interrupt: true, body: body}) do
      source =
        ExStorageService.BlobStore.Source.stateful_stream(
          fn conn, reducer ->
            {:cont, next} = reducer.(binary_part(body, 0, 3), conn)
            {:error, :acceptance_upstream_interruption, next}
          end,
          byte_size(body)
        )

      ExStorageServiceS3.Handlers.Shared.send_blob_source(conn, 200, source,
        request_id: "truncated-upstream"
      )
    end

    defp respond(conn, state),
      do: send_resp(conn, 200, if(conn.method == "HEAD", do: "", else: state.body))
  end

  setup do
    bucket = "gateway-read-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)

    agent =
      start_supervised!(
        {Agent,
         fn ->
           %{
             body: "0123456789",
             etag: "\"first\"",
             status: 200,
             requests: [],
             custom: "initial",
             cache_control: "private"
           }
         end}
      )

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
    %{agent: agent, bucket: bucket, url: "#{@base}/#{bucket}/object"}
  end

  test "cold ranges, warm cache and out-of-band replacement expose one current identity",
       context do
    assert {:ok, range} = Req.get(context.url, headers: [{"range", "bytes=3-6"}])
    assert range.status == 206
    assert range.body == "3456"
    assert range.headers["content-range"] == ["bytes 3-6/10"]
    assert range.headers["etag"] == ["\"first\""]
    assert range.headers["x-amz-meta-custom"] == ["initial"]
    assert Req.Response.get_header(range, "x-amz-checksum-crc32") == []
    assert {:error, :not_found} = Metadata.get_object_meta(context.bucket, "object")
    assert {:ok, %{body: "0123456789", status: 200}} = Req.get(context.url)
    requests = Agent.get(context.agent, & &1.requests)
    assert Enum.count(requests, &(elem(&1, 0) == "HEAD")) == 2
    assert Enum.count(requests, &(elem(&1, 0) == "GET")) == 1
    assert {"GET", ["\"first\""]} in requests

    Agent.update(
      context.agent,
      &%{
        &1
        | body: "replacement",
          etag: "\"second\"",
          custom: "changed",
          cache_control: "no-store"
      }
    )

    assert {:ok, current} = Req.get(context.url)
    assert current.status == 200
    assert current.body == "replacement"
    assert current.headers["etag"] == ["\"second\""]
    assert current.headers["content-length"] == ["11"]
    assert current.headers["x-amz-meta-custom"] == ["changed"]
    assert current.headers["cache-control"] == ["no-store"]
    assert {:ok, head} = Req.head(context.url)
    assert head.status == 200
    assert head.headers["etag"] == ["\"second\""]
    assert head.headers["content-length"] == ["11"]
  end

  test "gateway cache and real upstream errors expose finite operational outcomes", context do
    :ok = ExStorageService.Metrics.setup()
    :ok = ExStorageService.Metrics.reset()
    assert {:ok, %{status: 200, body: "0123456789"}} = Req.get(context.url)
    assert {:ok, %{status: 200, body: "0123456789"}} = Req.get(context.url)
    output = ExStorageService.Metrics.format_metrics()
    assert output =~ ~s(gateway_cache_operations_total{operation="read",result="miss"} 1)
    assert output =~ ~s(gateway_cache_operations_total{operation="read",result="hit"} 1)
    assert output =~ ~s(gateway_upstream_requests_total{operation="get",result="ok"} 1)

    for status <- [403, 503] do
      Agent.update(context.agent, &%{&1 | status: status})
      assert {:ok, %{status: ^status}} = Req.get(context.url, retry: false)
    end

    output = ExStorageService.Metrics.format_metrics()
    assert output =~ ~s(gateway_upstream_requests_total{operation="head",result="denied"} 1)
    assert output =~ ~s(gateway_upstream_requests_total{operation="head",result="unavailable"} 1)
    refute output =~ context.bucket
    refute output =~ "localhost"
    refute output =~ "secret"
  end

  test "deleted, denied and unavailable upstream never serve a stale cache", context do
    assert {:ok, %{status: 200}} = Req.get(context.url)

    for {upstream, expected, code} <- [
          {403, 403, "AccessDenied"},
          {503, 503, "ServiceUnavailable"},
          {429, 503, "SlowDown"},
          {404, 404, "NoSuchKey"}
        ] do
      Agent.update(context.agent, &%{&1 | status: upstream})
      assert {:ok, response} = Req.get(context.url, retry: false)
      assert response.status == expected
      assert response.body =~ code
      assert {:ok, head} = Req.head(context.url, retry: false)
      assert head.status == expected
      assert head.body == ""
    end
  end

  test "truncated upstream download returns framed XML without stale object checksum or cache",
       context do
    :ok = ExStorageService.Metrics.setup()
    :ok = ExStorageService.Metrics.reset()
    Agent.update(context.agent, &Map.put(&1, :interrupt, true))

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:ok, response} =
               Req.get(context.url, retry: false, decode_body: false, receive_timeout: 5_000)

      assert response.status == 503
      assert response.body =~ "<Code>ServiceUnavailable</Code>"
      assert response.headers["content-type"] == ["application/xml"]
      assert response.headers["content-length"] == [Integer.to_string(byte_size(response.body))]

      for name <- ["x-amz-checksum-crc32", "etag", "x-amz-meta-custom", "cache-control"] do
        assert Req.Response.get_header(response, name) == [], "stale object header: #{name}"
      end
    end)

    assert ExStorageService.Metrics.format_metrics() =~
             ~s(gateway_upstream_requests_total{operation="get",result="transport_error"} 1)

    assert :miss = ExStorageService.CloudCache.LocalStore.get(context.bucket, "object")
    assert {:error, :not_found} = Metadata.get_object_meta(context.bucket, "object")
  end

  test "cache eviction cannot invalidate an already-open HTTP response", context do
    bytes = :binary.copy("bounded-raw-data-", 524_288)
    hash = :crypto.hash(:sha256, bytes)
    Agent.update(context.agent, &%{&1 | body: bytes, etag: "\"large\""})
    # First fill the cache, then read through an acknowledged HTTP stream.
    assert {:ok, %{status: 200}} = Req.get(context.url)
    assert {:ok, response} = ExStorageService.HTTPClient.fetch(context.url)
    stream = HTTP.Response.stream_pid(response)
    assert is_pid(stream)
    send(stream, {:read_chunk, self(), :ack})

    receive do
      {:stream_chunk, ^stream, first, ack} ->
        :ok = ExStorageService.CloudCache.LocalStore.clear(context.bucket)
        send(stream, {:stream_chunk_ack, ack})
        assert finish_hash(stream, :crypto.hash_update(:crypto.hash_init(:sha256), first)) == hash
    after
      5_000 -> flunk("first gateway chunk was not delivered")
    end
  end

  defp finish_hash(stream, hash) do
    receive do
      {:stream_chunk, ^stream, bytes, ack} ->
        next = :crypto.hash_update(hash, bytes)
        send(stream, {:stream_chunk_ack, ack})
        finish_hash(stream, next)

      {:stream_end, ^stream} ->
        :crypto.hash_final(hash)

      {:stream_error, ^stream, reason} ->
        flunk("gateway stream failed: #{inspect(reason)}")
    after
      5_000 -> flunk("gateway stream stalled")
    end
  end

  test "remote-only gateway copy is bounded and never trusts stale source metadata", context do
    alias ExStorageServiceS3.Handlers.Object.CloudCopy
    source = %{bucket: context.bucket, key: "object", version_id: nil}
    {:ok, snapshot} = CloudCopy.head_source(source)
    destination = "gateway-copy-dest-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(destination)

    attributes = %{
      content_type: snapshot.metadata.content_type,
      metadata: snapshot.metadata.metadata,
      system_headers: snapshot.metadata.system_headers
    }

    assert {:ok, result} =
             CloudCopy.copy(
               source,
               destination,
               "copied",
               snapshot.metadata,
               attributes,
               "gateway-copy-#{System.unique_integer([:positive])}"
             )

    assert result.metadata.size == 10
    assert {:ok, %{body: "0123456789"}} = Req.get("#{@base}/#{destination}/copied")
    assert {:error, :not_found} = Metadata.get_object_meta(context.bucket, "object")

    assert {:ok, %{status: 200}} =
             Req.put("#{@base}/#{destination}/http-copy",
               body: "",
               headers: [{"x-amz-copy-source", "/#{context.bucket}/object"}]
             )

    assert {:ok, http_copy} = Req.get("#{@base}/#{destination}/http-copy")
    assert http_copy.status == 200
    assert http_copy.body == "0123456789"
    assert http_copy.headers["x-amz-meta-custom"] == ["initial"]
    assert http_copy.headers["cache-control"] == ["private"]

    Agent.update(context.agent, &%{&1 | body: "new", etag: "\"changed\""})

    assert {:error, :precondition_failed} =
             CloudCopy.copy(
               source,
               destination,
               "stale",
               snapshot.metadata,
               attributes,
               "gateway-copy-stale-#{System.unique_integer([:positive])}"
             )

    assert {:error, :not_found} = Metadata.get_object_meta(destination, "stale")
  end

  test "UploadPartCopy pins a remote-only gateway range and persists negotiated checksums",
       context do
    alias ExStorageService.Storage.Multipart
    destination = "gateway-part-dest-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(destination)

    {:ok, upload} =
      Multipart.init_upload(destination, "part", %{
        checksum_algorithm: "SHA256",
        checksum_type: "COMPOSITE"
      })

    response =
      Req.put!("#{@base}/#{destination}/part?uploadId=#{upload}&partNumber=1",
        body: "",
        retry: false,
        headers: [
          {"x-amz-copy-source", "/#{context.bucket}/object"},
          {"x-amz-copy-source-range", "bytes=2-5"}
        ]
      )

    checksum = Base.encode64(:crypto.hash(:sha256, "2345"))
    assert response.status == 200
    assert response.body =~ "<ChecksumSHA256>#{checksum}</ChecksumSHA256>"
    assert Req.Response.get_header(response, "x-amz-checksum-sha256") == [checksum]

    assert {:ok, [%{size: 4, checksums: %{"sha256" => ^checksum}, etag: etag}]} =
             Multipart.list_parts(destination, upload)

    assert {:ok, prepared} = Multipart.prepare_complete_upload(destination, upload, [{1, etag}])
    assert prepared.content_hash == Base.encode16(:crypto.hash(:sha256, "2345"), case: :lower)
    assert {"GET", ["\"first\""]} in Agent.get(context.agent, & &1.requests)
    assert {:error, :not_found} = Metadata.get_object_meta(context.bucket, "object")
  end

  test "conditions and invalid cold ranges are evaluated against upstream identity", context do
    assert {:ok, %{status: 304}} = Req.get(context.url, headers: [{"if-none-match", "\"first\""}])

    assert {:ok, %{status: 304}} =
             Req.get(context.url, headers: [{"if-none-match", "W/\"first\""}])

    assert {:ok, %{status: 412}} = Req.get(context.url, headers: [{"if-match", "\"other\""}])
    assert {:ok, range} = Req.get(context.url, headers: [{"range", "bytes=100-200"}])
    assert range.status == 416
    assert range.headers["content-range"] == ["bytes */10"]
  end
end
