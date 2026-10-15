defmodule ExStorageServiceCli.Transfer.Coordinator do
  @moduledoc "Invocation-wide request-count and byte reservations with owned cancellation."
  use GenServer
  alias ExStorageServiceCli.Error

  defmodule Lease do
    @moduledoc false
    defstruct [:coordinator, :reference]
  end

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  def acquire(coordinator, bytes, opts \\ []) do
    reference = make_ref()

    try do
      GenServer.call(
        coordinator,
        {:acquire, reference, bytes, opts[:cleanup] == true},
        opts[:timeout] || 60_000
      )
    catch
      :exit, _ ->
        GenServer.cast(coordinator, {:release, reference})

        {:error,
         Error.new(:transport, "Request admission expired or became unavailable",
           code: :admission_timeout
         )}
    end
  end

  def release(%Lease{} = lease),
    do: GenServer.cast(lease.coordinator, {:release, lease.reference})

  def release(nil), do: :ok

  def track(%Lease{} = lease, controller) do
    GenServer.call(lease.coordinator, {:track, lease.reference, controller})
  catch
    :exit, _ -> failure(:interrupted, :request_lease_expired, "Request ownership ended")
  end

  def track(nil, _controller), do: :ok
  def cancel(coordinator, signal \\ :sigint), do: GenServer.call(coordinator, {:cancel, signal})
  def stats(coordinator), do: GenServer.call(coordinator, :stats)
  def check(nil), do: :ok
  def check(coordinator), do: GenServer.call(coordinator, :check)

  @doc "Lazy, supervised object work; result queues remain bounded by active tasks."
  def stream(items, task_supervisor, function, opts \\ []) do
    Task.Supervisor.async_stream_nolink(task_supervisor, items, function,
      max_concurrency: opts[:max_concurrency] || 4,
      ordered: false,
      timeout: opts[:timeout] || :infinity,
      on_timeout: :kill_task
    )
  end

  @impl true
  def init(opts) do
    maximum = opts[:max_requests] || 8
    budget = opts[:memory_budget] || 67_108_864
    queue = opts[:max_waiters] || 64

    if is_integer(maximum) and maximum in 1..1024 and is_integer(budget) and budget >= 65_536 and
         is_integer(queue) and queue in 1..1024 do
      {:ok,
       %{
         maximum: maximum,
         budget: budget,
         queue_limit: queue,
         held: %{},
         waiting: %{},
         order: :queue.new(),
         bytes: 0,
         high_requests: 0,
         high_bytes: 0,
         cancelled: nil
       }}
    else
      {:stop, :invalid_resource_limits}
    end
  end

  @impl true
  def handle_call({:acquire, reference, bytes, cleanup}, from, state) do
    cond do
      not is_integer(bytes) or bytes < 0 or bytes > state.budget ->
        {:reply,
         failure(:configuration, :request_budget, "Request exceeds the invocation byte budget"),
         state}

      state.cancelled != nil and not cleanup ->
        {:reply, interrupted(state.cancelled), state}

      map_size(state.waiting) >= state.queue_limit ->
        {:reply, failure(:client, :request_queue_full, "Request admission queue is full"), state}

      true ->
        item = %{
          from: from,
          bytes: bytes,
          cleanup: cleanup,
          monitor: Process.monitor(elem(from, 0)),
          controller: nil
        }

        state = %{
          state
          | waiting: Map.put(state.waiting, reference, item),
            order: :queue.in(reference, state.order)
        }

        {:noreply, grant(state)}
    end
  end

  def handle_call({:track, reference, controller}, _from, state) do
    case state.held[reference] do
      nil ->
        {:reply, failure(:interrupted, :request_lease_expired, "Request ownership ended"), state}

      item ->
        if state.cancelled != nil and not item.cleanup do
          abort(controller)
          {:reply, interrupted(state.cancelled), state}
        else
          item = %{item | controller: controller}
          {:reply, :ok, %{state | held: Map.put(state.held, reference, item)}}
        end
    end
  end

  def handle_call({:cancel, signal}, _from, state) do
    for {_reference, item} <- state.held, not item.cleanup do
      send(elem(item.from, 0), {:ess_cancel, signal})
      abort(item.controller)
    end

    for {_reference, item} <- state.waiting do
      GenServer.reply(item.from, interrupted(signal))
      Process.demonitor(item.monitor, [:flush])
    end

    {:reply, :ok, %{state | cancelled: signal, waiting: %{}, order: :queue.new()}}
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       maximum: state.maximum,
       active: map_size(state.held),
       bytes: state.bytes,
       queued: map_size(state.waiting),
       high_requests: state.high_requests,
       high_bytes: state.high_bytes
     }, state}
  end

  def handle_call(:check, _from, %{cancelled: nil} = state), do: {:reply, :ok, state}
  def handle_call(:check, _from, state), do: {:reply, interrupted(state.cancelled), state}

  @impl true
  def handle_cast({:release, reference}, state),
    do: {:noreply, release(state, reference) |> grant()}

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    reference =
      Enum.find_value(state.held, fn {ref, item} -> if item.monitor == monitor, do: ref end) ||
        Enum.find_value(state.waiting, fn {ref, item} -> if item.monitor == monitor, do: ref end)

    {:noreply, if(reference, do: release(state, reference) |> grant(), else: state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    for {_, item} <- state.held, do: abort(item.controller)
    :ok
  end

  defp grant(state) do
    case :queue.out(state.order) do
      {:empty, _} ->
        state

      {{:value, reference}, rest} ->
        case state.waiting[reference] do
          nil ->
            grant(%{state | order: rest})

          item ->
            if map_size(state.held) < state.maximum and state.bytes + item.bytes <= state.budget do
              lease = %Lease{coordinator: self(), reference: reference}
              GenServer.reply(item.from, {:ok, lease})
              bytes = state.bytes + item.bytes
              held = Map.put(state.held, reference, item)

              grant(%{
                state
                | held: held,
                  waiting: Map.delete(state.waiting, reference),
                  order: rest,
                  bytes: bytes,
                  high_bytes: max(state.high_bytes, bytes),
                  high_requests: max(state.high_requests, map_size(held))
              })
            else
              state
            end
        end
    end
  end

  defp release(state, reference) do
    case Map.pop(state.held, reference) do
      {nil, _held} ->
        case Map.pop(state.waiting, reference) do
          {nil, _} ->
            state

          {item, waiting} ->
            Process.demonitor(item.monitor, [:flush])
            %{state | waiting: waiting}
        end

      {item, held} ->
        Process.demonitor(item.monitor, [:flush])
        abort(item.controller)
        %{state | held: held, bytes: state.bytes - item.bytes}
    end
  end

  defp abort(controller) when is_pid(controller) do
    if Process.alive?(controller), do: HTTP.AbortController.abort(controller)
    :ok
  catch
    :exit, _ -> :ok
  end

  defp abort(_controller), do: :ok
  defp interrupted(:sigterm), do: failure(:terminated, :cancelled, "Invocation terminated")
  defp interrupted(_signal), do: failure(:interrupted, :cancelled, "Invocation interrupted")
  defp failure(category, code, message), do: {:error, Error.new(category, message, code: code)}
end
