defmodule ExStorageServiceCli.Credentials.Owner do
  @moduledoc "Invocation-owned, bounded single-flight credential refresh with no persistent secrets."
  use GenServer
  alias ExStorageServiceCli.Credentials.{Record, Remote}

  def start_link(loader, opts \\ []) when is_function(loader, 0),
    do: GenServer.start_link(__MODULE__, {self(), loader, opts})

  def resolve(owner, timeout \\ 10_000) do
    request = make_ref()

    try do
      GenServer.call(owner, {:resolve, request}, timeout)
    catch
      :exit, _ ->
        GenServer.cast(owner, {:cancel, request})
        Remote.failure(:credential_refresh_unavailable)
    end
  end

  @impl true
  def init({owner, loader, opts}) do
    {:ok,
     %{
       owner: Process.monitor(opts[:invocation_owner] || owner),
       loader: loader,
       clock: opts[:clock] || (&DateTime.utc_now/0),
       advisory: opts[:advisory_timeout] || opts[:expiry_skew] || 900,
       mandatory: opts[:mandatory_timeout] || opts[:expiry_skew] || 600,
       attempt_timeout: opts[:refresh_attempt_timeout],
       next_refresh: nil,
       limit: opts[:max_waiters] || 32,
       result: nil,
       refresh: nil,
       waiters: %{}
     }}
  end

  @impl true
  def handle_call({:resolve, request}, from, state) do
    mode = refresh_mode(state, state.clock.())

    cond do
      mode == :none or (mode == :advisory and state.refresh != nil) ->
        {:reply, state.result, state}

      map_size(state.waiters) >= state.limit ->
        {:reply, Remote.failure(:credential_refresh_busy), state}

      true ->
        monitor = Process.monitor(elem(from, 0))
        state = %{state | waiters: Map.put(state.waiters, monitor, {from, request})}

        if state.refresh do
          {:noreply, state}
        else
          owner = self()
          loader = state.loader

          {pid, monitor} =
            spawn_monitor(fn ->
              result =
                try do
                  loader.()
                rescue
                  _ -> Remote.failure(:credential_refresh_failed)
                catch
                  _, _ -> Remote.failure(:credential_refresh_failed)
                end

              send(owner, {:refreshed, self(), result})
            end)

          interval =
            state.attempt_timeout ||
              if(match?({:ok, %Record{provenance: %{provider: :sso}}, _}, state.result),
                do: 60,
                else: 0
              )

          next_refresh = if interval > 0, do: DateTime.add(state.clock.(), interval, :second)
          {:noreply, %{state | refresh: {pid, monitor, mode}, next_refresh: next_refresh}}
        end
    end
  end

  @impl true
  def handle_info({:refreshed, pid, result}, %{refresh: {pid, monitor, _mode}} = state) do
    Process.demonitor(monitor, [:flush])

    result =
      case result do
        {:ok, %Record{expires_at: nil}, _settings} ->
          result

        {:ok, %Record{expires_at: expires}, _settings} ->
          if DateTime.compare(expires, state.clock.()) == :gt,
            do: result,
            else: Remote.failure(:expired_credentials)

        {:ok, nil, %{unsigned: true}} ->
          result

        {:error, %ExStorageServiceCli.Error{}} ->
          result

        _ ->
          Remote.failure(:credential_refresh_failed)
      end

    {:noreply, finish_refresh(state, result)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner: monitor} = state),
    do: {:stop, :normal, state}

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %{refresh: {_pid2, monitor, _mode}} = state
      ) do
    error = Remote.failure(:credential_refresh_failed)
    {:noreply, finish_refresh(state, error)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state),
    do: {:noreply, abandon(%{state | waiters: Map.delete(state.waiters, monitor)})}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{refresh: {pid, _monitor, _mode}}), do: Process.exit(pid, :kill)
  def terminate(_reason, _state), do: :ok

  @impl true
  def format_status(_status), do: %{state: "credential refresh state redacted"}

  defp reply_waiters(state, result) do
    Enum.each(state.waiters, fn {monitor, {from, _request}} ->
      Process.demonitor(monitor, [:flush])
      GenServer.reply(from, result)
    end)

    %{state | waiters: %{}}
  end

  @impl true
  def handle_cast({:cancel, request}, state) do
    waiters =
      Enum.reduce(state.waiters, state.waiters, fn {monitor, {_from, reference}}, acc ->
        if reference == request do
          Process.demonitor(monitor, [:flush])
          Map.delete(acc, monitor)
        else
          acc
        end
      end)

    {:noreply, abandon(%{state | waiters: waiters})}
  end

  defp abandon(%{waiters: waiters, refresh: {pid, monitor, _mode}} = state)
       when map_size(waiters) == 0 do
    Process.demonitor(monitor, [:flush])
    Process.exit(pid, :kill)
    %{state | refresh: nil}
  end

  defp abandon(state), do: state

  defp finish_refresh(%{refresh: {_pid, _monitor, mode}} = state, result) do
    success = match?({:ok, _, _}, result)

    fallback =
      not success and mode == :advisory and valid?(state.result, state.clock.()) and
        remaining(state.result, state.clock.()) >= state.mandatory and
        not match?({:error, %{code: :expired_credentials}}, result)

    reply = if fallback, do: state.result, else: result
    state = if success, do: %{state | result: result}, else: state
    reply_waiters(%{state | refresh: nil}, reply)
  end

  defp refresh_mode(%{result: {:ok, nil, %{unsigned: true}}}, _now), do: :none
  defp refresh_mode(%{result: {:ok, %Record{expires_at: nil}, _}}, _now), do: :none

  defp refresh_mode(state, now) do
    remaining = remaining(state.result, now)

    cond do
      remaining <= 0 -> :mandatory
      state.next_refresh != nil and DateTime.compare(now, state.next_refresh) == :lt -> :none
      remaining < state.mandatory -> :mandatory
      remaining < state.advisory -> :advisory
      true -> :none
    end
  end

  defp valid?({:ok, %Record{}, _} = result, now), do: remaining(result, now) > 0
  defp valid?(_result, _now), do: false

  defp remaining({:ok, %Record{expires_at: nil}, _}, _now), do: :infinity
  defp remaining({:ok, %Record{expires_at: expires}, _}, now), do: DateTime.diff(expires, now)
  defp remaining(_result, _now), do: 0
end
