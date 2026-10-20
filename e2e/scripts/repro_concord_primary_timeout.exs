# Run only in a new isolated VM; no applications or storage roots are started.
# This reproducer intentionally exits 0 when the current API defect is observed.
# A future fixed implementation should make the blocked-primary observation differ.
defmodule ESSPrimaryTimeoutMock do
  use GenServer

  def init({group_id, replica_id, mode}) do
    {:ok, _} =
      Registry.register(ViewstampedReplication.Registry, {:replica, group_id, replica_id}, nil)

    {:ok, {replica_id, mode}}
  end

  def handle_call(:primary, _from, {_id, :blocked_primary} = state),
    do: {:noreply, state}

  def handle_call(:primary, _from, {id, :blocked_read} = state),
    do: {:reply, {:ok, id}, state}

  def handle_call({:linearizable_read, _operation}, _from, state),
    do: {:noreply, state}
end

defmodule ESSPrimaryTimeoutRepro do
  def observe(fun) do
    started = System.monotonic_time(:millisecond)

    result =
      try do
        {:returned, fun.()}
      catch
        :exit, {:timeout, {GenServer, :call, [_pid, request, timeout]}} ->
          {:timeout_exit, request, timeout}

        kind, _reason ->
          {:unexpected_catch, kind}
      end

    {result, System.monotonic_time(:millisecond) - started}
  end

  def run do
    # Fail if this VM already owns application infrastructure: the runner must
    # create an isolated VM, never inject mocks into a running ESS instance.
    nil = Process.whereis(ViewstampedReplication.Registry)

    {:ok, registry} =
      Registry.start_link(
        keys: :unique,
        name: ViewstampedReplication.Registry
      )

    group_id = {:ess_primary_timeout_repro, System.unique_integer([:positive])}
    replica_id = :owned_mock
    member = struct(ViewstampedReplication.Member, id: replica_id, endpoint: node())
    opts = [replica_id: replica_id, replicas: [member], timeout: 50]

    {:ok, mock} =
      GenServer.start(
        ESSPrimaryTimeoutMock,
        {group_id, replica_id, :blocked_primary}
      )

    try do
      # Control: direct actual-read timeout has its documented error boundary.
      {direct, direct_ms} =
        observe(fn ->
          ViewstampedReplication.Replica.read(mock, :probe, timeout: 50)
        end)

      IO.inspect(%{
        case: :direct_read_control,
        requested_timeout_ms: 50,
        result: direct,
        elapsed_ms: direct_ms
      })

      {:returned, {:error, :quorum_unavailable}} = direct
      true = direct_ms >= 40 and direct_ms < 2_000

      # Defect: public read's preliminary primary lookup ignores the 50 ms
      # option and leaks an exit at the implicit GenServer.call default 5000.
      {public, public_ms} =
        observe(fn ->
          ViewstampedReplication.read(group_id, :probe, opts)
        end)

      IO.inspect(%{
        case: :blocked_primary_public_read,
        requested_timeout_ms: 50,
        result: public,
        elapsed_ms: public_ms
      })

      {:timeout_exit, :primary, 5_000} = public
      true = public_ms >= 4_500 and public_ms < 15_000
      IO.puts("REPRODUCED: primary discovery ignores read timeout and leaks timeout exit")
    after
      if Process.alive?(mock), do: GenServer.stop(mock)
      if Process.alive?(registry), do: Supervisor.stop(registry)
    end
  end
end

ESSPrimaryTimeoutRepro.run()
