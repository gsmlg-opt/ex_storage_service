# Positive admission diagnostics for the published 3.1.6 cohort, never A28 acceptance.
Code.require_file("index_setup_diagnostics.exs", __DIR__)
Code.require_file("index_acceptance_bounds.exs", __DIR__)

defmodule ESSIndexConcord316Probe.Backend do
  def get(key, opts) do
    result = ExStorageService.Metadata.Backend.Concord.get(key, opts)
    ESSIndexAcceptance.Bounds.record_read(key, result)
  end
end

defmodule ESSIndexConcord316Probe do
  alias Concord.StateMachine.Core
  alias Concord.StateMachine.Core.Context
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.{BucketLifecycle, ListIndex, ObjectCommit}
  alias ViewstampedReplication.{Configuration, Log, LogEntry, Member, Protocol, Reply, Request}
  alias ViewstampedReplication.Protocol.State

  @history_specs [
    {Protocol, :step, 2},
    {Protocol, :assert_invariants!, 1},
    {Protocol, :assert_transition!, 2},
    {Protocol, :assert_applied_requests_are_unique!, 1},
    {Protocol, :assert_committed_prefix!, 2},
    {Protocol, :index_applied_requests, 3},
    {Log, :canonical, 1},
    {Log, :indexed, 1}
  ]

  def run("minimal", output) do
    before_apps = Application.started_applications() |> Enum.sort()
    context = %Context{op_number: 1, timestamp_ms: 1_761_000_000_000}
    specification = %{compare: [], success: [{:put, "target", "value", %{}}], failure: []}

    {{:ok, %{succeeded: true}}, transaction, changes} =
      Core.apply_with_changes(0, context, {:txn, specification}, Core.init())

    {:ok, direct, _} = Core.apply_with_changes(0, context, {:put, "target", "value"}, Core.init())
    true = transaction.store == direct.store
    true = is_list(changes) and changes != []

    rows =
      for count <- [1_000, 10_000] do
        {state, advance} = Protocol.runtime(fixture(count))
        %ViewstampedReplication.Log.Indexed{} = state.log
        ^count = map_size(state.log.entries)
        ^count = map_size(state.applied_request_index.writer)

        {{final, _advance, samples}, profile} =
          profile(@history_specs, [self()], fn ->
            Enum.reduce(1..30, {state, advance, []}, fn offset, {_state, next, samples} ->
              number = count + offset

              request = %Request{
                client_id: :writer,
                request_number: number,
                operation: {:put, "constant", number}
              }

              {:reductions, before_reductions} = Process.info(self(), :reductions)

              {microseconds, {applied, after_apply}} =
                :timer.tc(fn ->
                  {pending, effects, after_request} = next.({:client_request, :route, request})
                  ^number = pending.commit_number
                  ^number = pending.applying_number

                  [
                    {:persist, {:append, %LogEntry{op_number: ^number} = entry}},
                    {:persist, {:set_commit_number, ^number}},
                    {:apply, applied_entry}
                  ] = Enum.filter(effects, &match?({kind, _} when kind in [:persist, :apply], &1))

                  ^entry = applied_entry

                  {applied, effects, after_apply} =
                    after_request.({:state_machine_applied, number, :ok})

                  ^number = applied.applied_number
                  nil = applied.applying_number

                  [
                    {:persist, {:applied, ^number, _}},
                    {:emit_telemetry, [:viewstamped_replication, :request, :stop], _, _},
                    {:reply, :route, %Reply{result: :ok}}
                  ] = effects

                  {^applied, [{:reply, :retry, %Reply{result: :ok}}], after_retry} =
                    after_apply.({:client_request, :retry, request})

                  {applied, after_retry}
                end)

              {:reductions, after_reductions} = Process.info(self(), :reductions)

              {applied, after_apply,
               [
                 %{microseconds: microseconds, reductions: after_reductions - before_reductions}
                 | samples
               ]}
            end)
          end)

        true = Enum.all?(profile, fn {_mfa, calls} -> calls == 0 end)
        final_count = count + 30
        ^final_count = final.applied_number
        ^final_count = map_size(final.log.entries)
        ^final_count = map_size(final.applied_request_index.writer)
        ^final = Protocol.assert_invariants!(final)

        # A direct closure read is a cold fallback, not Replica's ready read path.
        # Keep it outside the hot-write measurement and prove trace sensitivity.
        {{read_state, [{:read, :read, {:get, "constant"}}], _}, control} =
          profile(@history_specs, [self()], fn ->
            {_owned, next} = Protocol.runtime(final)
            next.({:read_request, :read, {:get, "constant"}})
          end)

        ^final = read_state
        true = control["ViewstampedReplication.Protocol.step/2"] > 0
        true = control["ViewstampedReplication.Log.canonical/1"] > 0

        %{
          retained_before: count,
          retained_after: final_count,
          samples: Enum.reverse(samples),
          forbidden_history_calls: profile,
          cold_read_control_history_calls: control,
          invariants_verified_outside_measurement: true
        }
      end

    ^before_apps = Application.started_applications() |> Enum.sort()

    write(output, %{
      mode: "minimal",
      applications_unchanged: true,
      transaction_change_tracking: "exact_changes",
      transaction_store_equal: true,
      rows: rows
    })
  end

  def run("production", output) do
    ESSIndexSetup.start_core!()
    vsr = Application.fetch_env!(:concord, :vsr)
    0 = vsr[:command_version]
    [_] = vsr[:members]
    :ok = Metadata.create_bucket("admission-target")
    :ok = Metadata.create_bucket("admission-unrelated")

    {:ok, {hash, etag, size}} =
      ExStorageService.Storage.Engine.put_object("fixture", "shared", "shared")

    metadata = %{content_hash: hash, etag: etag, size: size, created_at: "2026-10-22T00:00:00Z"}

    setup =
      for {bucket, count} <- [{"admission-target", 128}, {"admission-unrelated", 128}] do
        ESSIndexSetup.phase(bucket, count, fn number ->
          ObjectCommit.put(bucket, key(number), metadata,
            operation_id: "#{bucket}-#{number}",
            bucket_versioning: true,
            backend: ESSIndexSetup.Backend
          )
        end)
      end

    before_reads = native_inventory()

    pages =
      for {kind, opts, maximum} <- [
            {:first, [max_keys: 10], 10},
            {:delimiter, [max_keys: 1, delimiter: "/"], 1},
            {:absent, [max_keys: 1, prefix: "missing/"], 1}
          ] do
        sample =
          ESSIndexAcceptance.Bounds.measure(
            fn ->
              {:ok, page} =
                Metadata.list_objects(
                  "admission-target",
                  [backend: ESSIndexConcord316Probe.Backend] ++ opts
                )

              case kind do
                :first -> 10 = length(page.keys)
                :delimiter -> ["group/"] = page.common_prefixes
                :absent -> [] = page.keys
              end

              {:ok, page}
            end,
            maximum,
            trace_specs:
              ESSIndexAcceptance.Bounds.default_specs() ++
                Enum.map(@history_specs, &{&1, :history_traversal, :forbidden})
          )

        %{kind: kind, sample: sample}
      end

    ^before_reads = native_inventory()
    {:ok, %{version_id: "null"}} = ObjectCommit.get_head("admission-target", key(1))
    :ok = ExStorageService.Storage.Versioning.set_versioning("admission-target", :enabled)

    for version <- ["v1", "v2"] do
      {:ok, _} =
        ObjectCommit.put("admission-target", "versions", metadata,
          version_id: version,
          bucket_versioning: true
        )
    end

    {:ok, _} = ObjectCommit.delete_version("admission-target", "versions", "v2")
    {:ok, %{version_id: "v1"}} = ObjectCommit.get_head("admission-target", "versions")
    {:ok, _} = ObjectCommit.delete_marker("admission-target", "deleted", version_id: "marker")
    {:ok, %{delete_marker: true}} = ObjectCommit.get_head("admission-target", "deleted")
    {:ok, snapshot} = Concord.get_all(consistency: :linearizable)

    inventories =
      for {bucket, visible, versions, commits} <- [
            {"admission-target", 129, 130, 132},
            {"admission-unrelated", 128, 128, 128}
          ] do
        generation = BucketLifecycle.generation(Map.fetch!(snapshot, "bucket:#{bucket}"))
        root = Map.fetch!(snapshot, ListIndex.root_key(bucket, generation))
        :ok = ListIndex.ready(%{value: root}, generation)
        ^visible = root.visible_count
        ^versions = root.version_count
        ^commits = root.commit_sequence
        0 = root.active_upload_count

        trees =
          for {kind, field} <- [
                current: :current_root,
                versions: :versions_root,
                uploads: :uploads_root
              ],
              do: {ListIndex.namespace(bucket, root, kind), Map.fetch!(root, field)}

        %{
          bucket: bucket,
          visible: visible,
          versions: versions,
          commits: commits,
          fixture: ESSIndexAcceptance.Bounds.fixture!(snapshot, trees)
        }
      end

    write(output, %{
      mode: "production",
      command_version: 0,
      setup: setup,
      pages: pages,
      native_before_reads: before_reads,
      native_log_unchanged_during_reads: true,
      native_final: native_inventory(),
      inventories: inventories,
      bounds: ESSIndexAcceptance.Bounds.declaration()
    })
  end

  defp native_inventory do
    vsr = Application.fetch_env!(:concord, :vsr)
    pid = ViewstampedReplication.Replica.whereis(vsr[:group_id], vsr[:replica_id])
    state = :sys.get_state(pid).protocol_state
    %ViewstampedReplication.Log.Indexed{} = state.log
    canonical = Log.canonical(state.log)

    %{
      op_number: state.op_number,
      applied_number: state.applied_number,
      retained_entries: length(canonical.entries),
      log_base: canonical.base_op_number
    }
  end

  defp fixture(count) do
    configuration =
      Configuration.new!(
        group_id: :concord316_admission,
        replica_id: :replica,
        members: [%Member{id: :replica, endpoint: :pure_fixture}]
      )

    entries =
      for number <- 1..count,
          do: %LogEntry{
            view_number: 0,
            op_number: number,
            client_id: :writer,
            request_number: number,
            operation: {:put, "constant", <<0>>}
          }

    {:ok, log} = Log.new(entries)

    %{
      State.new(configuration)
      | status: :normal,
        log: log,
        op_number: count,
        commit_number: count,
        applied_number: count,
        client_table: %{writer: %{request_number: count, status: :applied, result: :ok}}
    }
  end

  defp profile(specs, pids, operation) do
    tracer = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(tracer)
    session = :trace.session_create(:concord316_admission, tracer, [])

    try do
      for {module, _, _} = mfa <- specs do
        {:module, ^module} = Code.ensure_loaded(module)
        1 = :trace.function(session, mfa, true, [:local, :call_count])
      end

      for pid <- pids, do: 1 = :trace.process(session, pid, true, [:call, :silent])
      result = operation.()

      counts =
        Map.new(specs, fn {module, function, arity} = mfa ->
          {:call_count, calls} = :trace.info(session, mfa, :call_count)
          {"#{inspect(module)}.#{function}/#{arity}", calls}
        end)

      {:message_queue_len, 0} = Process.info(tracer, :message_queue_len)
      {result, counts}
    after
      true = :trace.session_destroy(session)
      send(tracer, :stop)

      receive do
        {:DOWN, ^monitor, :process, ^tracer, :normal} -> :ok
      after
        5_000 -> raise "owned admission tracer did not stop"
      end
    end
  end

  defp key(number), do: "group/" <> String.pad_leading(Integer.to_string(number), 8, "0")

  defp write(output, details) do
    receipt =
      Map.merge(details, %{
        status: "diagnostic_completed",
        diagnostic_only: true,
        acceptance_promotion: false,
        elixir_version: System.version(),
        otp_release: to_string(:erlang.system_info(:otp_release)),
        versions:
          Map.new(
            [:concord, :viewstamped_replication, :ex_turso],
            &{&1, to_string(Application.spec(&1, :vsn))}
          )
      })

    File.write!(output, JSON.encode!(receipt) <> "\n", [:exclusive])
  end
end

[mode, output] = System.argv()
ESSIndexConcord316Probe.run(mode, output)
