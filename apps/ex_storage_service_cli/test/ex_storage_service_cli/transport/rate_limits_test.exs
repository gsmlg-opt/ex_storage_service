Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transport.RateLimitsTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCli.{
    Error,
    S3.Client,
    S3.Request,
    S3.Response,
    Transport.HTTPFetch
  }

  alias ExStorageServiceCli.CLI.Runtime
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.RateLimiter
  @moduletag :tmp_dir

  test "finite signed uploads keep exact bytes, explicit framing and invocation ownership", %{
    tmp_dir: root
  } do
    body = :crypto.strong_rand_bytes(2048)
    {endpoint, _} = WireServer.fixture([{200, [{"ETag", "\"fixture\""}], ""}])

    assert :ok =
             Runtime.with_invocation(fn runtime ->
               assert {:ok, context} =
                        Runtime.aws_context(
                          runtime,
                          %{endpoint_url: endpoint, limit_upload: 1024},
                          home: root,
                          env: %{
                            "AWS_ACCESS_KEY_ID" => "fixture-access",
                            "AWS_SECRET_ACCESS_KEY" => "fixture-secret"
                          }
                        )

               assert context.options[:rate_limiter] == runtime.rate_limiter
               assert context.endpoint == endpoint
               started = System.monotonic_time(:millisecond)

               assert {:ok, %{"ETag" => "\"fixture\""}} =
                        Client.call(
                          context,
                          "PutObject",
                          %{
                            "Bucket" => "bucket",
                            "Key" => "key",
                            "Body" => body
                          },
                          allow_streaming: true
                        )

               assert System.monotonic_time(:millisecond) - started >= 900
               assert_receive {:wire_request, %{body: ^body, headers: headers}}
               assert headers["content-length"] == "2048"
               refute headers["transfer-encoding"]
               assert headers["authorization"] =~ "AWS4-HMAC-SHA256"

               assert headers["x-amz-content-sha256"] ==
                        Base.encode16(:crypto.hash(:sha256, body), case: :lower)

               send(self(), {:limiter, runtime.rate_limiter})
               :ok
             end)

    assert_receive {:limiter, limiter}
    refute Process.alive?(limiter)
  end

  test "existing acknowledged upload streams preserve explicit length and source bytes" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    bytes = :crypto.strong_rand_bytes(70_000)

    {:ok, source} =
      HTTP.Stream.from_enumerable([
        binary_part(bytes, 0, 35_000),
        binary_part(bytes, 35_000, 35_000)
      ])

    {endpoint, _} = WireServer.fixture([{200, [], ""}])
    request = %Request{method: "PUT", body: source, headers: [{"Content-Length", "70000"}]}

    assert {:ok, _} =
             HTTPFetch.request(endpoint, request, rate_limiter: limiter, limit_upload: "100KiB")

    assert_receive {:wire_request, %{body: ^bytes, headers: %{"content-length" => "70000"}}}
  end

  test "raw gzip downloads are charged before bounded sink delivery" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    body = :zlib.gzip(:crypto.strong_rand_bytes(2048))
    {endpoint, _} = WireServer.fixture([{200, [{"Content-Encoding", "gzip"}], body}])
    started = System.monotonic_time(:millisecond)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, %Request{method: "GET"},
               mode: :stream,
               rate_limiter: limiter,
               limit_download: 1024
             )

    assert {:ok, chunks, %{bytes: size}} =
             HTTPFetch.consume(response, [], fn bytes, chunks ->
               assert byte_size(bytes) <= 1024
               {:ok, [bytes | chunks]}
             end)

    assert System.monotonic_time(:millisecond) - started >= 900
    assert IO.iodata_to_binary(Enum.reverse(chunks)) == body
    assert size == byte_size(body)
    refute Process.alive?(response.controller)
  end

  @tag timeout: 120_000
  test "paced download acknowledgement can exceed the unchanged global sixty-second limit" do
    assert HTTP.Config.streaming_timeout() == 60_000
    limiter = start_supervised!({RateLimiter, owner: self()})
    body = :binary.copy(<<0, 255, 43, 37>>, 128)
    {endpoint, _} = WireServer.fixture([{200, [], body}])
    started = System.monotonic_time(:millisecond)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, %Request{method: "GET"},
               mode: :stream,
               timeout: :infinity,
               read_idle_timeout: 600_000,
               rate_limiter: limiter,
               limit_download: 8
             )

    assert {:ok, chunks, %{bytes: 512}} =
             HTTPFetch.consume(response, [], fn bytes, chunks ->
               assert byte_size(bytes) <= 8
               {:ok, [bytes | chunks]}
             end)

    assert IO.iodata_to_binary(Enum.reverse(chunks)) == body
    assert System.monotonic_time(:millisecond) - started >= 60_000
    assert HTTP.Config.streaming_timeout() == 60_000
    assert %{waiting: 0} = GenServer.call(limiter, :stats)
    refute Process.alive?(response.body)
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "an unlimited acknowledgement wait retains the finite overall transfer deadline" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    {endpoint, _} = WireServer.fixture([{200, [], "two bytes"}])

    assert {:ok, response} =
             HTTPFetch.request(endpoint, %Request{method: "GET"},
               mode: :stream,
               timeout: 250,
               rate_limiter: limiter,
               limit_download: 1
             )

    assert {:error, %Error{category: :transport, code: :request_timeout}} =
             HTTPFetch.consume(response, 0, fn bytes, count ->
               {:ok, count + byte_size(bytes)}
             end)

    assert %{waiting: 0} = GenServer.call(limiter, :stats)
    refute Process.alive?(response.body)
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "cancellation stops paced delivery and preserves the borrowed signal" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    signal = HTTP.AbortController.new()
    on_exit(fn -> if Process.alive?(signal), do: Agent.stop(signal) end)
    {endpoint, _} = WireServer.fixture([{200, [], "two bytes"}])

    assert {:ok, response} =
             HTTPFetch.request(endpoint, %Request{method: "GET"},
               mode: :stream,
               timeout: :infinity,
               signal: signal,
               rate_limiter: limiter,
               limit_download: 1
             )

    owner = self()

    consumer =
      Task.async(fn ->
        HTTPFetch.consume(response, 0, fn bytes, count ->
          send(owner, {:delivered, bytes})
          {:ok, count + byte_size(bytes)}
        end)
      end)

    assert_receive {:delivered, "t"}
    await_waiter(limiter, 1)
    HTTP.AbortController.abort(signal)
    assert {:error, %Error{category: :interrupted, code: :aborted}} = Task.await(consumer, 1000)
    assert %{waiting: 0} = GenServer.call(limiter, :stats)
    assert Process.alive?(signal)
    assert HTTP.AbortController.aborted?(signal)
    refute Process.alive?(response.body)
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "separate simultaneous requests share one endpoint upload burst" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    body = String.duplicate("x", 1024)

    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        for _ <- 1..2 do
          {:ok, socket} = :gen_tcp.accept(listener, 5000)
          request = WireServer.request(socket)
          send(owner, {:wire_request, request})
          WireServer.response(socket, 200, [], "")
          :gen_tcp.close(socket)
        end
      end)

    started = System.monotonic_time(:millisecond)

    tasks =
      for _ <- 1..2 do
        Task.async(fn ->
          HTTPFetch.request(endpoint, %Request{method: "PUT", body: body},
            rate_limiter: limiter,
            rate_endpoint: endpoint,
            limit_upload: 1024
          )
        end)
      end

    for task <- tasks, do: assert({:ok, _} = Task.await(task, 5000))
    assert System.monotonic_time(:millisecond) - started >= 900
    for _ <- 1..2, do: assert_receive({:wire_request, %{body: ^body}})
    assert %{waiting: 0, buckets: 1} = GenServer.call(limiter, :stats)
  end

  test "finite control responses at different paths share the direct adapter origin budget" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    body = String.duplicate("x", 1024)
    {endpoint, _} = WireServer.fixture([{200, [], body}, {200, [], body}])
    opts = [rate_limiter: limiter, limit_download: 1024]
    started = System.monotonic_time(:millisecond)

    assert {:ok, %Response{body: ^body, rate_limit: nil}} =
             HTTPFetch.request(endpoint <> "/first", %Request{method: "GET"}, opts)

    assert {:ok, %Response{body: ^body}} =
             HTTPFetch.request(endpoint <> "/second", %Request{method: "GET"}, opts)

    assert System.monotonic_time(:millisecond) - started >= 900
  end

  test "upload proxy dies with its owner and removes its pending rate waiter" do
    limiter = start_supervised!({RateLimiter, owner: self()})
    parent = self()

    owner =
      spawn(fn ->
        limit = %{pid: limiter, endpoint: :endpoint, direction: :upload, rate: 1, opts: []}
        {:ok, stream, guardian} = RateLimiter.upload("two", limit)
        send(parent, {:proxy, stream, guardian})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:proxy, stream, guardian}
    send(stream, {:read_chunk, self(), :ack})
    assert_receive {:stream_chunk, ^stream, "t", reference}
    send(stream, {:stream_chunk_ack, reference})
    await_waiter(limiter, 1)
    stream_monitor = Process.monitor(stream)
    guardian_monitor = Process.monitor(guardian)
    send(owner, :stop)
    assert_receive {:DOWN, ^stream_monitor, :process, ^stream, _}, 1000
    assert_receive {:DOWN, ^guardian_monitor, :process, ^guardian, _}, 1000
    await_waiter(limiter, 0)
  end

  test "limits without invocation ownership fail before sending any request" do
    {endpoint, _} = WireServer.fixture([{200, [], ""}])

    assert {:error, %Error{code: :rate_owner_required}} =
             HTTPFetch.request(endpoint, %Request{method: "GET"}, limit_download: "1KiB")

    refute_receive {:wire_request, _}
  end

  defp await_waiter(limiter, expected, attempts \\ 100) do
    case GenServer.call(limiter, :stats) do
      %{waiting: ^expected} ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(1)
        await_waiter(limiter, expected, attempts - 1)

      result ->
        flunk("Unexpected limiter state #{inspect(result)}")
    end
  end
end
