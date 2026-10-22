# ESS dependency diagnostic only; never promotes A28 or changes protocol state.
Code.require_file(Path.expand("e2e/scripts/index_setup_diagnostics.exs"))

defmodule ESSConcord315Probe do
  alias Concord.StateMachine.Core
  alias Concord.StateMachine.Core.Context
  alias ViewstampedReplication.Protocol

  @read_specs [
    {{Protocol, :step, 2}, [{[:_, {:read_request, :_, :_}], [], []}]},
    {{Protocol, :assert_applied_requests_are_unique!, 1}, true},
    {{Protocol, :applied_requests_indexed?, 3}, true},
    {{Protocol, :assert_contiguous_log!, 2}, true},
    {{Protocol, :assert_committed_prefix!, 2}, true},
    {{Concord.StateMachine, :materialize, 1}, true},
    {{Core, :snapshot, 1}, true}
  ]
  @txn_specs [
    {{Core, :apply_with_changes, 4}, true},
    {{Concord.StateMachine, :materialize, 1}, true},
    {{Concord.StateMachine, :materialize, 2}, [{[:_, :all], [], []}]},
    {{Concord.StateMachine.Observer, :changed_keys, 3}, [{[:all, :_, :_], [], []}]}
  ]

  def run(output) do
    false = File.exists?(output)
    pure = pure_transactions()
    ESSIndexSetup.start_core!()
    vsr = Application.fetch_env!(:concord, :vsr)
    0 = Keyword.fetch!(vsr, :command_version)
    [_] = Keyword.fetch!(vsr, :members)
    replica = ViewstampedReplication.Replica.whereis(vsr[:group_id], vsr[:replica_id])
    true = is_pid(replica)
    true = is_pid(Process.whereis(Concord.Sync.Dispatcher))
    :ok = Concord.put("probe-fixed", "constant")
    apps = applications()

    {_, rows} = Enum.reduce([1_000, 2_000, 4_000], {0, []}, fn count, {previous, rows} ->
      for index <- (previous + 1)..count do
        :ok = Concord.put("probe-unrelated-#{index}", "unrelated")
      end

      before_read = :sys.get_state(replica)
      {samples, reads} = profile(replica, @read_specs, fn ->
        for _ <- 1..30 do
          {:reductions, before} = Process.info(replica, :reductions)
          {duration, {:ok, "constant"}} = :timer.tc(fn -> Concord.get("probe-fixed") end)
          {:reductions, after_read} = Process.info(replica, :reductions)
          %{microseconds: duration, replica_reductions: after_read - before}
        end
      end)
      after_read = :sys.get_state(replica)
      for field <- [:log, :client_table, :op_number, :applied_number, :commit_number] do
        true = Map.fetch!(before_read.protocol_state, field) === Map.fetch!(after_read.protocol_state, field)
      end
      true = before_read.state_machine_state === after_read.state_machine_state
      true = Enum.all?(reads, fn {_name, calls} -> calls == 0 end)

      specification = %{
        compare: [{:exists, "probe-fixed", :==, true}],
        success: [{:put, "probe-txn-target", "value", %{}}, {:put, "probe-txn-outcome", "committed", %{}}],
        failure: [{:put, "probe-wrong-branch", "must-not-exist", %{}}]
      }
      {transactions, transaction_trace} = profile(replica, @txn_specs, fn ->
        for _ <- 1..30 do
          {:reductions, before} = Process.info(replica, :reductions)
          {duration, {:ok, %Concord.Txn.Result{succeeded: true}}} =
            :timer.tc(fn -> Concord.Txn.commit(specification) end)
          {:reductions, after_txn} = Process.info(replica, :reductions)
          %{microseconds: duration, replica_reductions: after_txn - before}
        end
      end)
      30 = transaction_trace["Concord.StateMachine.Core.apply_with_changes/4"]
      for {name, calls} <- transaction_trace, name != "Concord.StateMachine.Core.apply_with_changes/4", do: 0 = calls
      {:ok, "value"} = Concord.get("probe-txn-target")
      {:ok, "committed"} = Concord.get("probe-txn-outcome")
      {:error, :not_found} = Concord.get("probe-wrong-branch")
      {:ok, original} = Concord.Txn.commit(specification, idempotency_key: "probe-retry-#{count}")
      {:ok, ^original} = Concord.Txn.commit(specification, idempotency_key: "probe-retry-#{count}")
      {:ok, ^original} = Concord.Txn.resolve("probe-retry-#{count}")
      current = :sys.get_state(replica)
      for field <- [:store, :current, :history, :leases] do
        true = Map.new(:ets.tab2list(Concord.StorageScope.table(field))) === Map.fetch!(current.state_machine_state, field)
      end
      true = apps == applications()
      row = %{
        unrelated_keys: count,
        retained_entries: length(current.protocol_state.log.entries),
        public_read_samples: samples,
        public_read_p50_reductions: median(samples, :replica_reductions),
        public_read_native_calls: reads,
        public_atomic_samples: transactions,
        public_atomic_p50_reductions: median(transactions, :replica_reductions),
        public_atomic_native_calls: transaction_trace,
        read_state_unchanged: true, exact_compatibility_views: true,
        chosen_branch_and_idempotent_result_verified: true
      }
      IO.puts(JSON.encode!(Map.drop(row, [:public_read_samples, :public_atomic_samples])))
      {count, rows ++ [row]}
    end)

    File.write!(output, JSON.encode!(%{
      status: "released_runtime_boundaries_verified", diagnostic_only: true,
      acceptance_promotion: false, source_sha256: System.fetch_env!("ESS_INDEX_SOURCE_SHA256"),
      source_head: System.fetch_env!("ESS_INDEX_SOURCE_HEAD"),
      versions: Map.new([:concord, :viewstamped_replication, :ex_turso], &{&1, to_string(Application.spec(&1, :vsn))}),
      command_version: 0, applications_unchanged_during_measurements: true,
      pure_transactions: pure, rows: rows,
      scope: "public ready singleton runtime and map-form atomic transactions; pure Protocol.step remains exhaustive; no scale acceptance or historical-timeout claim"
    }) <> "\n", [:exclusive])
  end

  defp pure_transactions do
    for version <- [0, 1] do
      context = %Context{op_number: 1, timestamp_ms: 1_761_000_000_000}
      initial = Core.init()
      {:ok, direct, direct_changes} = Core.apply_with_changes(version, context, {:put, "target", "value"}, initial)
      specification = %{compare: [], success: [{:put, "target", "value", %{}}], failure: []}
      {{:ok, %Concord.Txn.Result{succeeded: true}}, transaction, changes} =
        Core.apply_with_changes(version, context, {:txn, specification}, initial)
      true = direct.store === transaction.store
      expected = [current: "target", store: "target"]
      ^expected = Enum.sort(direct_changes)
      ^expected = Enum.sort(changes)
      %{command_version: version, exact_store_equal: true, transaction_rows: inspect(changes)}
    end
  end

  defp profile(replica, specs, operation) do
    {:module, :trace} = Code.ensure_loaded(:trace)
    session = :trace.session_create(:ess_concord315_diagnostic, self(), [])
    try do
      for {{module, _, _} = mfa, match} <- specs do
        {:module, ^module} = Code.ensure_loaded(module)
        1 = :trace.function(session, mfa, match, [:local])
      end
      # Native call events honor argument matchspecs; call_time does not.
      # Arity-only messages keep protocol/state arguments out of the tracer.
      1 = :trace.process(session, replica, true, [:call, :arity])
      result = operation.()
      reference = :trace.delivered(session, replica)
      counts = collect_calls(replica, reference, %{})
      rows = Map.new(specs, fn {{module, function, arity} = mfa, _} ->
        {"#{inspect(module)}.#{function}/#{arity}", Map.get(counts, mfa, 0)}
      end)
      {result, rows}
    after
      true = :trace.session_destroy(session)
    end
  end

  defp collect_calls(replica, reference, counts) do
    receive do
      {:trace, ^replica, :call, mfa} ->
        collect_calls(replica, reference, Map.update(counts, mfa, 1, &(&1 + 1)))
      {:trace_delivered, ^replica, ^reference} -> counts
    after
      5_000 -> raise "owned probe trace delivery did not complete"
    end
  end

  defp median(samples, field), do: samples |> Enum.map(&Map.fetch!(&1, field)) |> Enum.sort() |> Enum.at(14)
  defp applications, do: Application.started_applications() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
end

[output] = System.argv()
ESSConcord315Probe.run(output)
