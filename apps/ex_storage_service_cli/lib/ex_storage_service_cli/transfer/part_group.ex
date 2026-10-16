defmodule ExStorageServiceCli.Transfer.PartGroup do
  @moduledoc "Owns concurrent multipart workers and synchronously drains their registered requests."
  use GenServer
  alias ExStorageServiceCli.{Error, Transfer.Coordinator}

  def start(signal, coordinator) do
    with {:ok, group} <- GenServer.start(__MODULE__, {self(), signal, coordinator}) do
      {supervisor, signal} = GenServer.call(group, :resources)
      {:ok, group, supervisor, signal}
    end
  end

  # Registration is synchronous and must precede network I/O. A stopped group
  # cannot acquire another request between worker shutdown and multipart abort.
  def register(nil, _request), do: :ok

  def register(group, request) do
    GenServer.call(group, {:register, request})
  catch
    :exit, _ -> closed()
  end

  def cancel(group), do: GenServer.call(group, :cancel, :infinity)
  def reason(group), do: GenServer.call(group, :reason)

  def close(group) do
    GenServer.call(group, :close, :infinity)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({owner, borrowed, coordinator}) do
    Process.flag(:trap_exit, true)
    {:ok, tasks} = Task.Supervisor.start_link(max_children: 64)
    signal = HTTP.AbortController.new()
    timer = Process.send_after(self(), :poll, 25)

    {:ok,
     %{
       owner: Process.monitor(owner),
       tasks: tasks,
       signal: signal,
       borrowed: borrowed,
       coordinator: coordinator,
       requests: %{},
       error: nil,
       closed: false,
       timer: timer
     }}
  end

  @impl true
  def handle_call(:resources, _from, state), do: {:reply, {state.tasks, state.signal}, state}
  def handle_call(:reason, _from, state), do: {:reply, state.error, state}

  def handle_call({:register, _request}, _from, %{closed: true} = state),
    do: {:reply, closed(), state}

  def handle_call({:register, request}, _from, state) do
    monitor = Process.monitor(request)
    {:reply, :ok, %{state | requests: Map.put(state.requests, monitor, request)}}
  end

  def handle_call(:cancel, _from, state), do: {:reply, :ok, cancel_workers(state)}
  def handle_call(:close, _from, state), do: {:stop, :normal, :ok, state}

  @impl true
  def handle_info({:DOWN, reference, :process, _pid, _reason}, %{owner: reference} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, reference, :process, _pid, _reason}, state),
    do: {:noreply, %{state | requests: Map.delete(state.requests, reference)}}

  def handle_info({:EXIT, tasks, _reason}, %{tasks: tasks} = state), do: {:stop, :normal, state}

  def handle_info(:poll, state) do
    state =
      case cancellation(state) do
        nil -> state
        error -> cancel_workers(%{state | error: error})
      end

    {:noreply, %{state | timer: Process.send_after(self(), :poll, 25)}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state) do
    Process.cancel_timer(state.timer)
    abort(state.signal)
    if Process.alive?(state.tasks), do: Supervisor.stop(state.tasks, :normal, :infinity)
    # Worker death alone does not await Cancellation.terminate/2. Stop every
    # registered owner synchronously so no owned request can outlive abort.
    Enum.each(state.requests, fn {monitor, request} ->
      if Process.alive?(request) do
        try do
          GenServer.stop(request, :normal, :infinity)
        catch
          :exit, _ -> :ok
        end
      end

      Process.demonitor(monitor, [:flush])
    end)

    if Process.alive?(state.signal), do: Agent.stop(state.signal)
    :ok
  end

  defp cancel_workers(%{closed: true} = state), do: state

  defp cancel_workers(state) do
    abort(state.signal)

    for child <- Task.Supervisor.children(state.tasks),
        do: Task.Supervisor.terminate_child(state.tasks, child)

    %{state | closed: true}
  end

  defp cancellation(%{closed: true}), do: nil

  defp cancellation(state) do
    case Coordinator.check(state.coordinator) do
      {:error, error} ->
        error

      :ok ->
        if state.borrowed != nil and HTTP.AbortController.aborted?(state.borrowed),
          do: Error.new(:interrupted, "Multipart upload cancelled", code: :cancelled)
    end
  catch
    :exit, _ -> Error.new(:interrupted, "Multipart ownership ended", code: :cancelled)
  end

  defp abort(signal) do
    if Process.alive?(signal), do: HTTP.AbortController.abort(signal)
  catch
    :exit, _ -> :ok
  end

  defp closed,
    do:
      {:error,
       Error.new(:interrupted, "Multipart request group has ended", code: :part_group_closed)}
end
