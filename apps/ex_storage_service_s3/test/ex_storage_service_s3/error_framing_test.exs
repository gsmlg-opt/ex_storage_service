defmodule ExStorageServiceS3.ErrorFramingTest do
  use ExUnit.Case, async: false

  alias ExStorageServiceS3.{Auth.SigV4, Handlers.Shared, Plugs.RateLimiter, Router}

  defmodule Server do
    def init(key), do: key

    def call(conn, key) do
      case conn.request_path do
        "/health" ->
          Plug.Conn.send_resp(conn, 200, "ready")

        "/shared" ->
          Shared.error_response(conn, "NoSuchBucket", "Missing bucket.", "/shared", "test")

        "/multipart" ->
          conn
          |> Plug.Conn.fetch_query_params()
          |> ExStorageServiceS3.MultipartHandlers.upload_part("absent", "key")

        "/signature" ->
          SigV4.call(conn, get_secret_fn: fn _ -> nil end)

        "/presigned" ->
          Router.call(conn, Router.init([]))

        "/rate" ->
          conn |> Plug.Conn.assign(:access_key_id, key) |> RateLimiter.call([])

        "/crash" ->
          Router.handle_errors(conn, %{kind: :error, reason: {:private, "secret-path"}, stack: []})
      end
    end
  end

  setup do
    prior_auth = Application.get_env(:ex_storage_service, :s3_auth_enabled)
    prior_rate = Application.get_env(:ex_storage_service, :rate_limit)
    Application.put_env(:ex_storage_service, :s3_auth_enabled, true)

    Application.put_env(:ex_storage_service, :rate_limit,
      enabled: true,
      max_tokens: 1,
      refill_rate: 0
    )

    key = "error-framing-#{System.unique_integer([:positive])}"
    RateLimiter.ensure_table()

    :ets.insert(
      :ex_storage_service_rate_limiter,
      {{:access_key, key}, 0, System.monotonic_time(:millisecond)}
    )

    server = start_supervised!({Bandit, plug: {Server, key}, port: 0})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    on_exit(fn ->
      for {name, value} <- [s3_auth_enabled: prior_auth, rate_limit: prior_rate] do
        if value == nil,
          do: Application.delete_env(:ex_storage_service, name),
          else: Application.put_env(:ex_storage_service, name, value)
      end

      if :ets.info(:ex_storage_service_rate_limiter) != :undefined,
        do: :ets.delete(:ex_storage_service_rate_limiter, {:access_key, key})
    end)

    {:ok, port: port, key: key}
  end

  test "every common error path leaves HEAD bodyless and uses S3 SlowDown503", %{key: key} do
    for {path, status} <- paths() do
      response = Plug.Test.conn(:head, path) |> Server.call(key)
      assert response.status == status
      assert response.resp_body == ""
      refute response.resp_body =~ "secret-path"
    end
  end

  test "an error after object HEAD removes payload headers while preserving version diagnostics" do
    object_headers = [
      {"content-length", "8388608"},
      {"content-encoding", "gzip"},
      {"content-range", "bytes 0-9/8388608"},
      {"content-disposition", "attachment; filename=object"},
      {"content-language", "en"},
      {"cache-control", "public, max-age=86400"},
      {"expires", "Sun, 18 Oct 2026 00:00:00 GMT"},
      {"etag", "\"object-etag\""},
      {"x-amz-checksum-crc32", "AAAAAA=="},
      {"x-amz-checksum-type", "FULL_OBJECT"},
      {"x-amz-meta-private", "object metadata"}
    ]

    for method <- [:get, :head] do
      conn =
        Enum.reduce(object_headers, Plug.Test.conn(method, "/failed-object"), fn {name, value},
                                                                                 conn ->
          Plug.Conn.put_resp_header(conn, name, value)
        end)
        |> Plug.Conn.put_resp_header("x-amz-version-id", "version")
        |> Plug.Conn.put_resp_header("x-amz-delete-marker", "true")
        |> Plug.Conn.put_resp_header("last-modified", "Sun, 18 Oct 2026 00:00:00 GMT")

      response =
        Shared.storage_error_response(
          conn,
          :upstream_length_mismatch,
          "/failed-object",
          "request"
        )

      assert response.status == 503

      for {name, _value} <- object_headers do
        assert Plug.Conn.get_resp_header(response, name) == [], "stale object header: #{name}"
      end

      assert Plug.Conn.get_resp_header(response, "content-type") == ["application/xml"]
      assert Plug.Conn.get_resp_header(response, "x-amz-version-id") == ["version"]
      assert Plug.Conn.get_resp_header(response, "x-amz-delete-marker") == ["true"]

      assert Plug.Conn.get_resp_header(response, "last-modified") == [
               "Sun, 18 Oct 2026 00:00:00 GMT"
             ]

      assert Plug.Conn.get_resp_header(response, "x-amz-request-id") == ["request"]

      if method == :head,
        do: assert(response.resp_body == ""),
        else: assert(response.resp_body =~ "<Code>ServiceUnavailable</Code>")
    end
  end

  test "every early body-bearing error closes HTTP1 instead of consuming another request", %{
    key: key
  } do
    for {path, status} <- paths(),
        framing <- [[{"content-length", "9"}], [{"transfer-encoding", "chunked"}]] do
      conn =
        Enum.reduce(framing, Plug.Test.conn(:put, path), fn {name, value}, conn ->
          Plug.Conn.put_req_header(conn, name, value)
        end)

      response = Server.call(conn, key)
      assert response.status == status
      assert Plug.Conn.get_resp_header(response, "connection") == ["close"]
      refute response.resp_body =~ "secret-path"
    end
  end

  test "real TCP Expect100-continue rejection closes before the next request can be mistaken for a body",
       %{port: port} do
    for {path, status} <- paths() do
      {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2000)

      :ok =
        :gen_tcp.send(
          socket,
          "PUT #{path} HTTP/1.1\r\nHost: localhost:#{port}\r\nContent-Length: 9\r\nExpect: 100-continue\r\n\r\n"
        )

      {headers, body} = receive_headers(socket, "")
      assert headers =~ "HTTP/1.1 #{status}"
      assert String.downcase(headers) =~ "connection: close"

      length =
        Regex.run(~r/\r\ncontent-length: ([0-9]+)/i, headers, capture: :all_but_first)
        |> hd()
        |> String.to_integer()

      receive_body(socket, body, length)
      # No candidate bytes were sent: a peer must not drain the next GET as
      # this request's declared body after sending its final error response.
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2000)
      :gen_tcp.close(socket)

      assert {:ok, %{status: 200, body: "ready"}} =
               Req.get("http://localhost:#{port}/health", retry: false)
    end
  end

  defp paths,
    do: [
      {"/shared", 404},
      {"/multipart?partNumber=1&uploadId=missing", 404},
      {"/signature", 403},
      {"/presigned?X-Amz-Signature=bad", 403},
      {"/rate", 503},
      {"/crash", 500}
    ]

  defp receive_headers(socket, bytes) do
    case :binary.split(bytes, "\r\n\r\n") do
      [headers, body] ->
        {headers <> "\r\n", body}

      [_] ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 2000)
        receive_headers(socket, bytes <> chunk)
    end
  end

  defp receive_body(_socket, body, length) when byte_size(body) == length, do: :ok

  defp receive_body(socket, body, length) when byte_size(body) < length do
    {:ok, chunk} = :gen_tcp.recv(socket, length - byte_size(body), 2000)
    receive_body(socket, body <> chunk, length)
  end
end
