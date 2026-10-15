defmodule ExStorageServiceCli.Transport.Cancellation do
  @moduledoc "Request-owned cancellation and lifetime monitoring; borrowed signals are read-only."
  use GenServer
  alias ExStorageServiceCli.Transfer.Coordinator

  def start(owner, signal, lease) do
    case GenServer.start(__MODULE__, {owner, signal, lease}) do
      {:ok, pid} -> {:ok, pid, GenServer.call(pid, :controller)}
      {:error, _} -> {:error, :invalid_signal}
    end
  end

  def fetch(pid, url, options), do: GenServer.call(pid, {:fetch, url, options}, :infinity)

  def adopt(pid), do: GenServer.call(pid, {:adopt, self()})

  def close(pid) do
    GenServer.stop(pid, :normal, 3000)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({owner, signal, lease}) do
    if signal == nil or is_pid(signal) do
      Process.flag(:trap_exit, true)
      controller = HTTP.AbortController.new()
      if cancelled?(signal), do: HTTP.AbortController.abort(controller)
      timer = Process.send_after(self(), :poll, 25)

      {:ok,
       %{
         controller: controller,
         owner: owner,
         owner_monitor: Process.monitor(owner),
         signal: signal,
         signal_monitor: if(signal, do: Process.monitor(signal)),
         lease: lease,
         stream: nil,
         request: nil,
         waiter: nil,
         timer: timer
       }}
    else
      {:stop, :invalid_signal}
    end
  end

  @impl true
  def handle_call(:controller, _from, state), do: {:reply, state.controller, state}

  def handle_call({:adopt, owner}, _from, state) do
    if cancelled?(state.controller) do
      {:reply, {:error, :aborted}, state}
    else
      Process.demonitor(state.owner_monitor, [:flush])
      {:reply, :ok, %{state | owner: owner, owner_monitor: Process.monitor(owner)}}
    end
  end

  def handle_call({:fetch, url, options}, from, state) do
    if cancelled?(state.signal) or HTTP.AbortController.aborted?(state.controller) do
      {:reply, {:error, :aborted}, state}
    else
      try do
        # The monitor process owns the Promise before I/O starts, so owner death
        # cannot leave an untracked request between allocation and registration.
        promise = HTTP.fetch(url, options)
        {:noreply, %{state | request: promise.task, waiter: from}}
      rescue
        _ -> {:reply, {:error, :invalid_request}, state}
      catch
        :exit, _ -> {:reply, {:error, :transport_failure}, state}
      end
    end
  end

  @impl true
  def handle_info({reference, response}, %{request: %Task{ref: reference}} = state) do
    Process.demonitor(reference, [:flush])
    stream = if is_struct(response, HTTP.Response), do: HTTP.Response.stream_pid(response)
    GenServer.reply(state.waiter, response)
    {:noreply, %{state | request: nil, waiter: nil, stream: stream}}
  end

  def handle_info({:DOWN, reference, :process, _, _}, state)
      when reference == state.owner_monitor,
      do: {:stop, :normal, state}

  def handle_info({:DOWN, reference, :process, _, _}, state)
      when reference == state.signal_monitor do
    abort(state.controller)
    {:noreply, %{state | signal: nil, signal_monitor: nil}}
  end

  def handle_info({:DOWN, reference, :process, _, _}, %{request: %Task{ref: reference}} = state) do
    GenServer.reply(state.waiter, {:error, :transport_failure})
    {:noreply, %{state | request: nil, waiter: nil}}
  end

  def handle_info({:EXIT, controller, _}, %{controller: controller} = state),
    do: {:stop, :normal, state}

  def handle_info(:poll, state) do
    if cancelled?(state.signal), do: abort(state.controller)
    {:noreply, %{state | timer: Process.send_after(self(), :poll, 25)}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state) do
    Process.cancel_timer(state.timer)
    Coordinator.release(state.lease)
    abort(state.controller)
    result = if state.request, do: Task.shutdown(state.request, :brutal_kill)
    stream = state.stream || completed_stream(result)
    monitor = if is_pid(stream), do: Process.monitor(stream)
    if is_pid(stream), do: send(stream, {:read_chunk, self(), :ack})
    if monitor, do: drain(stream, monitor, System.monotonic_time(:millisecond) + 1000)
    if Process.alive?(state.controller), do: Agent.stop(state.controller, :normal, 1000)
    :ok
  end

  defp completed_stream({:ok, %HTTP.Response{} = response}),
    do: HTTP.Response.stream_pid(response)

  defp completed_stream(_), do: nil

  # AbortController has no subscription API. This reads only its cancellation
  # bit; a dead or unresponsive borrowed signal fails closed within one poll.
  defp cancelled?(nil), do: false

  defp cancelled?(controller) do
    Agent.get(controller, & &1.aborted, 25)
  catch
    :exit, _ -> true
  end

  defp abort(controller) do
    if Process.alive?(controller), do: HTTP.AbortController.abort(controller)
  catch
    :exit, _ -> :ok
  end

  defp drain(stream, monitor, deadline) do
    receive do
      {:DOWN, ^monitor, :process, ^stream, _} ->
        :ok

      {:stream_chunk, ^stream, _, reference} ->
        send(stream, {:stream_chunk_ack, reference})
        drain(stream, monitor, deadline)

      {:stream_chunk, ^stream, _} ->
        drain(stream, monitor, deadline)

      {:stream_error, ^stream, _} ->
        drain(stream, monitor, deadline)

      {:stream_end, ^stream} ->
        drain(stream, monitor, deadline)

      {:stream_trailers, ^stream, _} ->
        drain(stream, monitor, deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        if Process.alive?(stream), do: Process.exit(stream, :kill)
    end

    Process.demonitor(monitor, [:flush])
  end
end
