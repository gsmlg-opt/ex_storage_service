defmodule PureWriteProbe do
  alias ViewstampedReplication.{Configuration, Log, LogEntry, Member, Protocol, Request}
  alias ViewstampedReplication.Protocol.State

  @functions [{Protocol, :assert_invariants!, 1}, {Protocol, :refresh_applied_request_index, 2},
              {Protocol, :index_applied_requests, 3}, {Protocol, :retained_prefix_mismatch, 4},
              {Log, :append, 2}, {Log, :fetch, 2}]

  def assert!(true), do: :ok
  def assert!(false), do: raise("probe assertion failed")

  def apps, do: Application.started_applications() |> Enum.map(&elem(&1, 0)) |> Enum.sort()

  def recovered(count) do
    config = Configuration.new!(group_id: :pure_write_probe, replica_id: :a,
      members: [%Member{id: :a, endpoint: :local_probe}])
    operation = {:atomic_probe, <<0::256>>}
    entries = for n <- 1..count, do: %LogEntry{view_number: 0, op_number: n,
      client_id: :stable_probe_client, request_number: n, operation: operation}
    {:ok, log} = Log.new(entries)
    recovered = %{configuration_hash: Configuration.hash(config), replica_id: :a,
      hard_state: %{view_number: 0, last_normal_view: 0, status: :normal},
      log: log, commit_number: count, applied_number: count,
      client_table: %{stable_probe_client: %{request_number: count, status: :applied, result: :ok}}}
    {state, _recovery_effects} = Protocol.step(State.new(config), {:storage_recovered, recovered})
    assert!(state.status == :normal and state.op_number == count and state.applied_number == count)
    assert!(map_size(state.applied_request_index.stable_probe_client) == count)
    Protocol.assert_invariants!(state)
    {state, operation}
  end

  def measured(state, event) do
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    start = System.monotonic_time(:microsecond)
    {next, effects} = Protocol.step(state, event)
    elapsed = System.monotonic_time(:microsecond) - start
    {:reductions, after_reductions} = Process.info(self(), :reductions)
    {next, effects, %{microseconds: elapsed, reductions: after_reductions - before_reductions}}
  end

  def sample(state, operation, number) do
    request = %Request{client_id: :stable_probe_client, request_number: number, operation: operation}
    {pending, effects, submit} = measured(state, {:client_request, :probe_route, request})
    [entry] = for {:apply, entry} <- effects, do: entry
    assert!(entry.op_number == number and entry.request_number == number)
    essential = Enum.flat_map(effects, fn
      {:persist, {:append, _}} -> [:append]
      {:persist, {:set_commit_number, _}} -> [:commit]
      {:apply, _} -> [:apply]
      _ -> []
    end)
    assert!(essential == [:append, :commit, :apply])
    assert!(pending.commit_number == number and pending.applying_number == number)
    {complete, ack_effects, apply_ack} = measured(pending, {:state_machine_applied, number, :ok})
    essential_ack = Enum.flat_map(ack_effects, fn
      {:persist, {:applied, n, table}} ->
        assert!(n == number and table.stable_probe_client.request_number == number)
        [:persist_applied]
      {:reply, :probe_route, reply} ->
        assert!(reply.request_number == number and reply.result == :ok)
        [:reply]
      _ -> []
    end)
    assert!(essential_ack == [:persist_applied, :reply])
    assert!(complete.op_number == number and complete.commit_number == number and
      complete.applied_number == number and complete.applying_number == nil)
    assert!(complete.pending_clients == %{} and map_size(complete.client_table) == 1)
    {complete, %{request_number: number, submit: submit, apply_ack: apply_ack}}
  end

  def trace(on) do
    Enum.each(@functions, &:erlang.trace_pattern(&1, on, [:local, :call_time]))
    :erlang.trace(self(), on, [:call])
  end

  def timings do
    Enum.map(@functions, fn mfa ->
      {:call_time, entries} = :erlang.trace_info(mfa, :call_time)
      selected = Enum.filter(entries, fn {pid, _, _, _} -> pid == self() end)
      %{function: inspect(mfa), calls: Enum.sum(Enum.map(selected, &elem(&1, 1))),
        microseconds: Enum.sum(Enum.map(selected, fn {_, _, seconds, micros} -> seconds * 1_000_000 + micros end))}
    end)
  end

  def run do
    initial_apps = apps()
    rows = for profiled <- [false, true], count <- [1_000, 2_000, 4_000] do
      {state, operation} = recovered(count)
      :erlang.garbage_collect(self())
      if profiled, do: trace(true)
      {final, samples} = Enum.map_reduce((count + 1)..(count + 30), state, fn number, acc ->
        {next, sample} = sample(acc, operation, number)
        {sample, next}
      end) |> then(fn {samples, final} -> {final, samples} end)
      calls = if profiled, do: timings(), else: []
      if profiled, do: trace(false)
      Protocol.assert_invariants!(final)
      last_number = count + 30
      duplicate = %Request{client_id: :stable_probe_client, request_number: last_number, operation: operation}
      {same, dedup_effects} = Protocol.step(final, {:client_request, :probe_route, duplicate})
      assert!(same == final and match?([{:reply, :probe_route, _}], dedup_effects))
      forged_entries = List.update_at(final.log.entries, -1, &%{&1 | request_number: last_number - 1})
      forged = %{final | log: %{final.log | entries: forged_entries}}
      guard = try do
        Protocol.step(forged, {:client_request, :probe_route, duplicate})
        :accepted
      rescue
        error in ArgumentError -> Exception.message(error)
      end
      assert!(is_binary(guard) and String.contains?(guard, "client_request_applied_more_than_once"))
      %{profiled: profiled, retained_entries_at_start: count, sample_count: length(samples), samples: samples,
        call_time: calls, final_op_number: final.op_number,
        guards_passed: true, effect_order_checked: true, duplicate_deduplicated: true,
        forged_duplicate_history_rejected: true, stable_client_count: map_size(final.client_table)}
    end
    final_apps = apps()
    assert!(initial_apps == final_apps)
    assert!(Enum.all?([:concord, :viewstamped_replication, :ex_turso, :ess], &(&1 not in final_apps)))
    output = %{kind: "pure_protocol_cpu_diagnostic_not_acceptance", rows: rows,
      elixir: System.version(), otp: System.otp_release(), erts: to_string(:erlang.system_info(:version)),
      schedulers: :erlang.system_info(:schedulers_online),
      apps_before: Enum.map(initial_apps, &to_string/1), apps_after: Enum.map(final_apps, &to_string/1),
      no_applications_started_during_probe: true,
      applications_never_called: ["Concord", "VSR replica", "ESS"],
      boundary: "Protocol.step only; effects inspected in order, persistence/network/apply effects not executed; deterministic successful apply acknowledgements supplied; no durability or ESS performance claim",
      beam: to_string(:code.which(Protocol))}
    File.write!("/tmp/ess-vsr-write-cpu-iU3PleL2/result.json", JSON.encode!(output))
    IO.puts("pure protocol diagnostic passed")
  end
end

PureWriteProbe.run()
