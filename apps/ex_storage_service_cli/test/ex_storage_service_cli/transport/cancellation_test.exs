Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.Transport.CancellationTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, S3.Request, S3.Response, Transport.HTTPFetch}
  alias ExStorageServiceCli.Test.WireServer
  alias ExStorageServiceCli.Transfer.Coordinator

  test "normal control requests leave a borrowed controller unchanged and reusable" do
    borrowed = signal()
    original = Agent.get(borrowed, & &1)
    {endpoint, _} = WireServer.fixture([{200, [], "first"}, {200, [], "second"}])

    for body <- ["first", "second"] do
      assert {:ok, %Response{body: ^body, controller: nil, cancellation: nil}} =
               HTTPFetch.request(endpoint, request(), signal: borrowed)

      assert Process.alive?(borrowed)
      assert Agent.get(borrowed, & &1) == original
    end
  end

  test "closing one of two requests sharing a signal does not abort the other request" do
    borrowed = signal()
    {endpoint, _} = WireServer.fixture([{200, [], "first"}, {200, [], "second"}])
    assert {:ok, first} = HTTPFetch.request(endpoint, request(), signal: borrowed, mode: :stream)
    assert {:ok, second} = HTTPFetch.request(endpoint, request(), signal: borrowed, mode: :stream)
    assert first.controller != borrowed
    assert second.controller != borrowed
    assert first.controller != second.controller
    assert :ok = HTTPFetch.close(first)
    assert :ok = HTTPFetch.close(first)
    refute HTTP.AbortController.aborted?(borrowed)

    assert {:ok, "second", _} =
             HTTPFetch.consume(second, "", fn chunk, acc -> {:ok, acc <> chunk} end)

    refute Process.alive?(first.controller)
    refute Process.alive?(first.cancellation)
    refute Process.alive?(second.controller)
    refute Process.alive?(second.cancellation)
    refute HTTP.AbortController.aborted?(borrowed)
  end

  test "already cancelled borrowed signals reject before socket I/O" do
    borrowed = signal()
    HTTP.AbortController.abort(borrowed)
    {endpoint, _} = WireServer.fixture([{200, [], "never requested"}])

    assert {:error, %Error{category: :interrupted, code: :aborted}} =
             HTTPFetch.request(endpoint, request(), signal: borrowed)

    refute_receive {:wire_request, _}
    assert Process.alive?(borrowed)
  end

  test "a cancelled response cannot transfer ownership to a reader" do
    alias ExStorageServiceCli.Transport.Cancellation
    {:ok, cancellation, controller} = Cancellation.start(self(), nil, nil)

    try do
      assert :ok = HTTP.AbortController.abort(controller)
      assert {:error, :aborted} = Cancellation.adopt(cancellation)
    after
      Cancellation.close(cancellation)
    end

    refute Process.alive?(controller)
  end

  test "borrowed cancellation interrupts a real peer stalled before headers" do
    borrowed = signal()
    pool = start_supervised!(Coordinator)
    {endpoint, _} = stalled(:headers)
    parent = self()

    worker =
      spawn(fn ->
        {:ok, lease} = Coordinator.acquire(pool, 65_536)
        send(parent, {:result, HTTPFetch.request(endpoint, request(lease), signal: borrowed)})
      end)

    monitor = Process.monitor(worker)
    assert_receive :peer_waiting
    assert :ok = HTTP.AbortController.abort(borrowed)
    assert_receive {:result, {:error, %Error{category: :interrupted}}}, 1000
    assert_receive {:peer_closed, {:error, :closed}}, 1000
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    assert Process.alive?(borrowed)
  end

  test "coordinator cancellation owns its controller while a peer is stalled midstream" do
    borrowed = signal()
    pool = start_supervised!(Coordinator)
    {:ok, lease} = Coordinator.acquire(pool, 65_536)
    {endpoint, _} = stalled(:body)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(lease), signal: borrowed, mode: :stream)

    assert_receive :peer_waiting
    assert :ok = Coordinator.cancel(pool)

    assert {:error, %Error{category: :interrupted}} =
             HTTPFetch.consume(response, 0, fn bytes, count -> {:ok, count + byte_size(bytes)} end)

    assert_receive {:peer_closed, {:error, :closed}}, 1000
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    refute HTTP.AbortController.aborted?(borrowed)
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "borrowed cancellation interrupts an acknowledged stream without changing signal ownership" do
    borrowed = signal()
    {endpoint, _} = stalled(:body)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(), signal: borrowed, mode: :stream)

    assert :ok = HTTP.AbortController.abort(borrowed)

    assert {:error, %Error{category: :interrupted}} =
             HTTPFetch.consume(response, 0, fn bytes, count -> {:ok, count + byte_size(bytes)} end)

    assert_receive {:peer_closed, {:error, :closed}}, 1000
    assert Process.alive?(borrowed)
    assert Agent.get(borrowed, & &1.request_id) == nil
    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
  end

  test "request owner death before headers cancels socket and releases its lease" do
    borrowed = signal()
    pool = start_supervised!(Coordinator)
    {endpoint, _} = stalled(:headers)

    worker =
      spawn(fn ->
        {:ok, lease} = Coordinator.acquire(pool, 65_536)
        HTTPFetch.request(endpoint, request(lease), signal: borrowed)
      end)

    assert_receive :peer_waiting
    assert %{active: 1} = Coordinator.stats(pool)
    Process.exit(worker, :kill)
    assert_receive {:peer_closed, {:error, :closed}}, 1500
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    refute HTTP.AbortController.aborted?(borrowed)
  end

  test "unconsumed response owner death closes owned stream, monitor and controller" do
    borrowed = signal()
    pool = start_supervised!(Coordinator)
    {endpoint, _} = stalled(:body)
    parent = self()

    worker =
      spawn(fn ->
        {:ok, lease} = Coordinator.acquire(pool, 65_536)

        {:ok, response} =
          HTTPFetch.request(endpoint, request(lease), signal: borrowed, mode: :stream)

        send(parent, {:response, response})

        receive do
          :finish -> :ok
        end
      end)

    assert_receive {:response, response}, 1000

    monitors =
      for pid <- [response.body, response.controller, response.cancellation],
          do: {pid, Process.monitor(pid)}

    Process.exit(worker, :kill)
    for {pid, monitor} <- monitors, do: assert_receive({:DOWN, ^monitor, :process, ^pid, _}, 1500)
    assert_receive {:peer_closed, {:error, :closed}}, 1000
    assert %{active: 0, bytes: 0} = Coordinator.stats(pool)
    refute HTTP.AbortController.aborted?(borrowed)
  end

  test "a different consumer adopts ownership and its death cleans resources" do
    borrowed = signal()
    {endpoint, _} = stalled(:body)

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(), signal: borrowed, mode: :stream)

    parent = self()

    consumer =
      spawn(fn ->
        HTTPFetch.consume(response, nil, fn _, _ ->
          send(parent, :consuming)

          receive do
            :continue -> {:ok, nil}
          end
        end)
      end)

    assert_receive :consuming, 1000
    monitor = Process.monitor(response.cancellation)
    Process.exit(consumer, :kill)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 1500
    assert_receive {:peer_closed, {:error, :closed}}, 1000
    refute Process.alive?(response.body)
    refute Process.alive?(response.controller)
    refute HTTP.AbortController.aborted?(borrowed)
  end

  test "consumer exceptions and legacy borrowed responses never abort borrowed controllers" do
    borrowed = signal()
    {endpoint, _} = WireServer.fixture([{200, [], "payload"}])

    assert {:ok, response} =
             HTTPFetch.request(endpoint, request(), signal: borrowed, mode: :stream)

    assert_raise RuntimeError, "consumer failed", fn ->
      HTTPFetch.consume(response, nil, fn _, _ ->
        HTTP.AbortController.abort(response.controller)
        Process.sleep(50)
        raise "consumer failed"
      end)
    end

    stream = response.body
    refute_received {:stream_error, ^stream, _}
    refute_received {:stream_end, ^stream}

    refute Process.alive?(response.controller)
    refute Process.alive?(response.cancellation)
    refute HTTP.AbortController.aborted?(borrowed)
    assert :ok = HTTPFetch.close(%Response{controller: borrowed, owns_controller: false})
    refute HTTP.AbortController.aborted?(borrowed)
    assert Process.alive?(borrowed)
  end

  defp request(lease \\ nil), do: %Request{method: "GET", path: "/", lease: lease}

  defp signal do
    controller = HTTP.AbortController.new()
    on_exit(fn -> if Process.alive?(controller), do: Agent.stop(controller) end)
    controller
  end

  defp stalled(stage) do
    WireServer.start(fn listener, owner ->
      {:ok, socket} = :gen_tcp.accept(listener, 5000)
      WireServer.request(socket)

      if stage == :body,
        do:
          :gen_tcp.send(
            socket,
            "HTTP/1.1 200 Fixture\r\nContent-Length: 100000\r\nConnection: close\r\n\r\nx"
          )

      send(owner, :peer_waiting)
      send(owner, {:peer_closed, :gen_tcp.recv(socket, 0, 5000)})
      :gen_tcp.close(socket)
    end)
  end
end
