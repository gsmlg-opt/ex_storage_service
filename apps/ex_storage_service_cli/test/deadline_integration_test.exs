Code.require_file("support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.DeadlineIntegrationTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, S3.Client, S3.Request, Transport.HTTPFetch}
  alias ExStorageServiceCli.CLI.{Parser, Runner}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.{Coordinator, RateLimiter}
  @moduletag :tmp_dir

  test "AWS zero read and connect timeouts reach the real adapter without a substitute", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"}
      ])

    assert {:ok, invocation} =
             Parser.parse([
               "s3api",
               "list-buckets",
               "--endpoint-url",
               endpoint,
               "--no-sign-request",
               "--cli-read-timeout",
               "0",
               "--cli-connect-timeout",
               "0"
             ])

    assert {:ok, %Context{timeout: :infinity, connect_timeout: :infinity} = context} =
             Context.aws(invocation.options,
               env: %{},
               config_file: root <> "/missing-config",
               credentials_file: root <> "/missing-credentials"
             )

    assert {:ok, %{"Buckets" => []}} = Client.call(context, "ListBuckets", %{})
  end

  test "native durations resolve independent defaults units and disabled limits" do
    assert {:ok, invocation} = Parser.parse(["ls", "--no-sign-request"])

    assert {:ok, %{timeout: :infinity, read_idle_timeout: 600_000, write_idle_timeout: 600_000}} =
             Context.native(nil, invocation.options, %{})

    assert {:ok, invocation} =
             Parser.parse([
               "ls",
               "--no-sign-request",
               "--conn-read-deadline",
               "1m2.5s",
               "--conn-write-deadline",
               "75ms"
             ])

    assert {:ok, %{read_idle_timeout: 62_500, write_idle_timeout: 75}} =
             Context.native(nil, invocation.options, %{})

    assert {:ok, invocation} =
             Parser.parse([
               "ls",
               "--no-sign-request",
               "--conn-read-deadline",
               "0",
               "--conn-write-deadline",
               "0s"
             ])

    assert {:ok, %{read_idle_timeout: nil, write_idle_timeout: nil}} =
             Context.native(nil, invocation.options, %{})

    for value <- ["-1s", "invalid", "1", "0.1ms", "5000000s"] do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse(["ls", "--conn-read-deadline", value])
    end

    assert {:error, %Error{category: :syntax}} =
             Parser.parse(["s3", "ls", "--conn-read-deadline", "1s"])
  end

  test "native legacy and modeled dispatch both enforce real read inactivity", %{tmp_dir: root} do
    for command <- [["ls"], ["version", "info", "s3://bucket"]] do
      {endpoint, _} =
        WireServer.start(fn listener, owner ->
          for _ <- 1..if(command == ["ls"], do: 1, else: 3) do
            {:ok, socket} = :gen_tcp.accept(listener, 5000)
            WireServer.request(socket)
            send(owner, :stalled)
            assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2500)
            :gen_tcp.close(socket)
          end
        end)

      args =
        command ++
          [
            "--config-dir",
            root,
            "--endpoint",
            endpoint,
            "--no-sign-request",
            "--conn-read-deadline",
            "80ms",
            "--conn-write-deadline",
            "1s"
          ]

      if command == ["ls"] do
        context = native(endpoint, "80ms", "1s")
        client = ExStorageServiceCli.S3Client.new(Map.from_struct(context))

        assert {:error, %Error{code: :read_idle_timeout}} =
                 ExStorageServiceCli.S3Client.list_buckets(client)
      else
        capture_io(:stderr, fn ->
          assert {:error, %Error{code: :read_idle_timeout}} = Runner.run(args)
        end)
      end

      assert_receive :stalled
    end
  end

  test "read inactivity resets while raw bytes trickle beyond one idle interval" do
    {endpoint, _} =
      peer(fn socket, _owner ->
        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n"
          )

        for _ <- 1..8 do
          Process.sleep(35)
          :ok = :gen_tcp.send(socket, "x")
        end
      end)

    context = native(endpoint, "120ms", "20ms")

    assert {:ok, response} =
             Client.call(context, "GetObject", %{"Bucket" => "b", "Key" => "k"}, mode: :stream)

    assert response.timeout == :infinity

    assert {:ok, "xxxxxxxx", %{bytes: 8}} =
             HTTPFetch.consume(response, "", fn bytes, acc -> {:ok, acc <> bytes} end)

    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  @tag timeout: 120_000
  test "infinite adapter body reads survive the unchanged global sixty-second stream limit" do
    assert HTTP.Config.streaming_timeout() == 60_000
    started = System.monotonic_time(:millisecond)

    {endpoint, _} =
      peer(fn socket, _owner ->
        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx"
          )

        Process.sleep(65_000)
        :ok = :gen_tcp.send(socket, "y")
      end)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(),
               mode: :stream,
               timeout: :infinity,
               connect_timeout: :infinity,
               read_idle_timeout: 120_000
             )

    owner = self()

    assert {:ok, "xy", %{bytes: 2}} =
             HTTPFetch.consume(response, "", fn bytes, acc ->
               if acc == "",
                 do:
                   send(
                     owner,
                     {:first_body_bytes, bytes, System.monotonic_time(:millisecond) - started}
                   )

               {:ok, acc <> bytes}
             end)

    assert_receive {:first_body_bytes, "x", first_elapsed}
    assert first_elapsed < 5_000
    assert System.monotonic_time(:millisecond) - started >= 65_000
    assert HTTP.Config.streaming_timeout() == 60_000
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "native body read stalls preserve typed idle errors and release resources" do
    {endpoint, _} =
      peer(fn socket, owner ->
        :ok =
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nx"
          )

        send(owner, {:peer_closed, :gen_tcp.recv(socket, 0, 2500)})
      end)

    pool = start_supervised!(Coordinator)
    context = %{native(endpoint, "80ms", "1s") | coordinator: pool}

    assert {:ok, response} =
             Client.call(context, "GetObject", %{"Bucket" => "b", "Key" => "k"}, mode: :stream)

    assert {:error, %Error{code: :read_idle_timeout}} =
             HTTPFetch.consume(response, 0, fn chunk, n -> {:ok, n + byte_size(chunk)} end)

    assert_receive {:peer_closed, {:error, :closed}}, 1000
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "native write inactivity is independent and closes a genuinely stalled socket" do
    {endpoint, server} =
      WireServer.start(fn listener, owner ->
        {:ok, socket} = :gen_tcp.accept(listener, 5000)
        :inet.setopts(socket, recbuf: 65_536)
        WireServer.head(socket)
        send(owner, :upload_started)

        receive do
          :finish -> :ok
        after
          5000 -> :ok
        end

        :inet.setopts(socket, recbuf: 1_048_576)
        send(owner, {:upload_peer_closed, drain(socket)})
        :gen_tcp.close(socket)
      end)

    context = native(endpoint, "2s", "80ms")
    pool = start_supervised!(Coordinator)
    context = %{context | coordinator: pool}

    assert {:error, %Error{code: :write_idle_timeout}} =
             Client.call(
               context,
               "UploadPart",
               %{"Bucket" => "b", "Key" => "k", "UploadId" => "u", "PartNumber" => 1},
               body: :binary.copy("x", 16 * 1024 * 1024),
               max_attempts: 1
             )

    assert_receive :upload_started
    send(server, :finish)
    assert_receive {:upload_peer_closed, {:error, :closed}}, 5000
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
  end

  test "infinite request timeout has no rate-limit arithmetic or cleanup failure" do
    limiter = start_supervised!(RateLimiter)
    {endpoint, _} = WireServer.fixture([{200, [], "abcdefgh"}])

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(),
               timeout: :infinity,
               connect_timeout: :infinity,
               limit_download: 32,
               rate_limiter: limiter,
               mode: :stream
             )

    assert {:ok, "abcdefgh", _} =
             HTTPFetch.consume(response, "", fn chunk, acc -> {:ok, acc <> chunk} end)

    refute Process.alive?(response.cancellation)
  end

  test "explicit finite adapter timeout still caps a native request" do
    {endpoint, _} =
      peer(fn socket, _owner -> assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2500) end)

    assert {:error, %Error{code: :request_timeout}} =
             Client.call(native(endpoint, "2s", "2s"), "ListBuckets", %{},
               timeout: 70,
               max_attempts: 1
             )
  end

  defp native(endpoint, read, write) do
    assert {:ok, invocation} =
             Parser.parse([
               "ls",
               "--endpoint",
               endpoint,
               "--no-sign-request",
               "--conn-read-deadline",
               read,
               "--conn-write-deadline",
               write
             ])

    assert {:ok, context} = Context.native(nil, invocation.options, %{})
    context
  end

  test "infinite upload pacing and retry keep exact bytes and bounded cleanup" do
    limiter = start_supervised!(RateLimiter)

    {endpoint, _} =
      WireServer.fixture([
        {503, [], "<Error><Code>SlowDown</Code><Message>Retry</Message></Error>"},
        {200, [], "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>"}
      ])

    assert {:ok, %{"Buckets" => []}} =
             Client.call(native(endpoint, "1s", "1s"), "ListBuckets", %{},
               max_attempts: 2,
               jitter: fn _ -> 0 end
             )

    {upload, _} = WireServer.fixture([{200, [], ""}])

    assert {:ok, _} =
             HTTPFetch.request(upload, %{request() | method: "PUT", body: "abcdefgh"},
               timeout: :infinity,
               limit_upload: 32,
               rate_limiter: limiter
             )

    assert_receive {:wire_request, %{body: "abcdefgh"}}
  end

  test "an explicit overall retry budget caps native infinity without arithmetic failures" do
    {endpoint, _} =
      peer(fn socket, _owner -> assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2500) end)

    assert {:error, %Error{code: :request_timeout}} =
             Client.call(native(endpoint, "2s", "2s"), "ListBuckets", %{},
               overall_timeout: 80,
               max_attempts: 1
             )
  end

  test "idle timeout retries retain replay safety and uncertain write outcomes" do
    alias ExStorageServiceCli.S3.Retry

    for code <- [:read_idle_timeout, :write_idle_timeout] do
      error = Error.new(:transport, "fixture", code: code)
      assert Retry.disposition("GetObject", error, []) == :retry
      assert Retry.disposition("UploadPart", error, []) == :retry
      assert Retry.disposition("PutObject", error, []) == :indeterminate
      assert Retry.disposition("GetObject", error, replayable: false) == :stop
    end
  end

  defp drain(socket) do
    case :gen_tcp.recv(socket, 0, 1000) do
      {:ok, _} -> drain(socket)
      result -> result
    end
  end

  defp request, do: %Request{method: "GET", path: "/", headers: [], body: ""}

  defp peer(callback) do
    WireServer.start(fn listener, owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 5000)
      WireServer.request(socket)
      callback.(socket, owner)
      :gen_tcp.close(socket)
    end)
  end
end
