# Bounded diagnostic through normal ObjectCommit. This never certifies A28.
Code.require_file("index_setup_diagnostics.exs", __DIR__)
Code.require_file("index_acceptance_bounds.exs", __DIR__)

defmodule ESSIndexSetupProbe do
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.{BucketLifecycle, ListIndex, ObjectCommit}
  alias ExStorageService.Storage.Versioning

  @write_specs [
    {ListIndex, :augment, 4},
    {Concord.Engine.VSR.StateMachine, :apply, 3},
    {Concord.StateMachine.Core, :apply_with_changes, 4},
    {Concord.StateMachine.Core, :representation_conflict_status, 1},
    {Concord.StateMachine, :materialize, 2},
    {Concord.StateMachine.Observer, :committed, 6},
    {ViewstampedReplication.Protocol, :step, 2},
    {ViewstampedReplication.Storage.File, :persist, 2},
    {ViewstampedReplication.Storage.File, :write_checkpoint, 2}
  ]
  @read_specs [
    {ViewstampedReplication.Protocol, :step, 2},
    {ViewstampedReplication.Protocol, :assert_applied_requests_are_unique!, 1},
    {ViewstampedReplication.Protocol, :applied_requests_indexed?, 3},
    {Concord.StateMachine, :materialize, 1},
    {Concord.StateMachine.Core, :snapshot, 1}
  ]

  def run(output, milestones) do
    false = File.exists?(output)
    true = milestones != [] and milestones == Enum.sort(Enum.uniq(milestones))
    true = Enum.all?(milestones, &(&1 in 1..3000))
    ESSIndexSetup.start_core!()
    vsr = Application.fetch_env!(:concord, :vsr)
    0 = Keyword.fetch!(vsr, :command_version)
    [_] = Keyword.fetch!(vsr, :members)
    :ok = Metadata.create_bucket("diagnostic-target")
    :ok = Metadata.create_bucket("diagnostic-unrelated")

    {:ok, {hash, etag, size}} =
      ExStorageService.Storage.Engine.put_object("fixture", "shared", "shared-fixture")

    metadata = %{content_hash: hash, etag: etag, size: size, created_at: "2026-10-20T00:00:00Z"}

    {_, rows} =
      Enum.reduce(milestones, {0, []}, fn count, {previous, rows} ->
        {setup, profile} =
          profile(@write_specs, fn ->
            ESSIndexSetup.phase("probe:#{count}:target", count - previous, fn offset ->
              number = previous + offset

              ObjectCommit.put("diagnostic-target", key(number), metadata,
                operation_id: "target-#{number}",
                bucket_versioning: true,
                backend: ESSIndexSetup.Backend
              )
            end)
          end)

        native = native_inventory()

        {reads, read_profile} =
          profile(@read_specs, fn ->
            for _ <- 1..10 do
              {duration, {:ok, %{version_id: "null"}}} =
                :timer.tc(fn -> ObjectCommit.get_head("diagnostic-target", key(1)) end)

              duration
            end
          end)

        page_profiles = pages(count)
        after_reads = native_inventory()
        invariants = inventory!(count, 0)

        row = %{
          completed_target_puts: count,
          setup: setup,
          intrusive_write_profile: profile,
          latest_read_microseconds: reads,
          latest_read_latency: ESSIndexSetup.latency(reads),
          intrusive_read_profile: read_profile,
          page_profiles: page_profiles,
          native_before_quiescent_reads: native,
          native_after_reads: after_reads,
          native_log_unchanged_during_reads: native == after_reads,
          invariants: invariants
        }

        checkpoint(output, rows ++ [row], "running")
        {count, rows ++ [row]}
      end)

    count = List.last(milestones)

    unrelated =
      ESSIndexSetup.phase("probe:unrelated", count, fn number ->
        ObjectCommit.put("diagnostic-unrelated", key(number), metadata,
          operation_id: "unrelated-#{number}",
          bucket_versioning: true,
          backend: ESSIndexSetup.Backend
        )
      end)

    native_after_unrelated = native_inventory()
    after_unrelated_pages = pages(count)
    :ok = Versioning.set_versioning("diagnostic-target", :enabled)

    for number <- 1..3 do
      {:ok, _} =
        ObjectCommit.put("diagnostic-target", "versions", metadata,
          version_id: "v#{number}",
          bucket_versioning: true
        )
    end

    {:ok, _} = ObjectCommit.delete_version("diagnostic-target", "versions", "v3")
    {:ok, _} = ObjectCommit.delete_marker("diagnostic-target", "deleted", version_id: "marker")
    {:ok, %{version_id: "v2"}} = ObjectCommit.get_head("diagnostic-target", "versions")
    {:error, :not_found} = ObjectCommit.get_version("diagnostic-target", "versions", "v3")
    {:ok, %{delete_marker: true}} = ObjectCommit.get_head("diagnostic-target", "deleted")
    final = inventory!(count, count, true)

    checkpoint(output, rows, "diagnostic_completed", %{
      unrelated_setup: unrelated,
      native_after_unrelated: native_after_unrelated,
      pages_after_unrelated: after_unrelated_pages,
      final_invariants: final,
      native_final: native_inventory(),
      runtime: ESSIndexSetup.runtime()
    })
  end

  defp key(number), do: "group/" <> String.pad_leading(Integer.to_string(number), 8, "0")

  defp pages(count) do
    for {kind, opts} <- [
          first: [max_keys: 10],
          delimiter: [max_keys: 1, delimiter: "/"],
          absent: [max_keys: 1, prefix: "missing/"]
        ] do
      {measurements, native} =
        profile(@read_specs, fn ->
          ESSIndexSetup.phase("probe:#{count}:#{kind}", 10, fn _ ->
            {:ok, result} =
              Metadata.list_objects("diagnostic-target", [backend: ESSIndexSetup.Backend] ++ opts)

            case kind do
              :first -> true = length(result.keys) == min(count, 10)
              :delimiter -> ["group/"] = result.common_prefixes
              :absent -> [] = result.keys
            end

            Process.put(:index_probe_page_stats, result.index_stats)
            {:ok, result}
          end)
        end)

      %{
        kind: kind,
        measurements: measurements,
        native: native,
        last_index_stats: Process.delete(:index_probe_page_stats)
      }
    end
  end

  defp inventory!(target_count, unrelated_count, special \\ false) do
    {:ok, snapshot} = Concord.get_all(consistency: :linearizable)

    for {bucket, count} <- [
          {"diagnostic-target", target_count},
          {"diagnostic-unrelated", unrelated_count}
        ] do
      generation = BucketLifecycle.generation(Map.fetch!(snapshot, "bucket:#{bucket}"))
      root = Map.fetch!(snapshot, ListIndex.root_key(bucket, generation))
      :ok = ListIndex.ready(%{value: root}, generation)
      addition = if special and bucket == "diagnostic-target", do: 1, else: 0
      true = root.visible_count == count + addition
      true = root.version_count == count + 3 * addition
      true = root.commit_sequence == count + 5 * addition
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
        visible_count: root.visible_count,
        version_count: root.version_count,
        commit_sequence: root.commit_sequence,
        fixture: ESSIndexAcceptance.Bounds.fixture!(snapshot, trees)
      }
    end
  end

  defp native_inventory do
    # Diagnostic-only materialization, outside every timed/native read block.
    replica = replica!()
    state = :sys.get_state(replica, 5_000).protocol_state

    %{
      op_number: state.op_number,
      applied_number: state.applied_number,
      retained_entries: length(state.log.entries),
      retained_entry_serialized_bytes:
        Enum.reduce(state.log.entries, 0, &(:erlang.external_size(&1) + &2)),
      log_base: state.log.base_op_number
    }
  end

  defp profile(specs, operation) do
    {:module, :trace} = Code.ensure_loaded(:trace)

    roles = [
      {self(), "client"},
      {Process.whereis(Concord.Engine.VSR), "engine"},
      {replica!(), "replica"}
    ]

    tracer = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(tracer)
    session = :trace.session_create(:ess_index_setup_profile, tracer, [])

    try do
      for {module, _, _} = mfa <- specs do
        {:module, ^module} = Code.ensure_loaded(module)
        1 = :trace.function(session, mfa, true, [:local, :call_time])
      end

      for {pid, _} <- roles, do: 1 = :trace.process(session, pid, true, [:call, :silent])
      result = operation.()

      measurements =
        Map.new(specs, fn {module, function, arity} = mfa ->
          {:call_time, entries} = :trace.info(session, mfa, :call_time)

          rows =
            for {pid, calls, seconds, microseconds} <- entries do
              {_, role} = List.keyfind(roles, pid, 0)
              %{role: role, calls: calls, microseconds: seconds * 1_000_000 + microseconds}
            end

          {"#{inspect(module)}.#{function}/#{arity}", rows}
        end)

      {:message_queue_len, 0} = Process.info(tracer, :message_queue_len)
      {result, measurements}
    after
      true = :trace.session_destroy(session)
      send(tracer, :stop)

      receive do
        {:DOWN, ^monitor, :process, ^tracer, :normal} -> :ok
      after
        5_000 -> raise "owned index diagnostic tracer did not stop"
      end
    end
  end

  defp replica! do
    vsr = Application.fetch_env!(:concord, :vsr)
    replica = ViewstampedReplication.Replica.whereis(vsr[:group_id], vsr[:replica_id])
    true = is_pid(replica) and Process.alive?(replica)
    replica
  end

  defp checkpoint(output, rows, status, details \\ %{}) do
    receipt =
      Map.merge(details, %{
        status: status,
        diagnostic_only: true,
        acceptance_promotion: false,
        source_sha256: System.fetch_env!("ESS_INDEX_SOURCE_SHA256"),
        head: System.fetch_env!("ESS_INDEX_SOURCE_HEAD"),
        concord_version: to_string(Application.spec(:concord, :vsn)),
        command_version: Application.fetch_env!(:concord, :vsr)[:command_version],
        elixir_version: System.version(),
        otp_release: to_string(:erlang.system_info(:otp_release)),
        rows: rows,
        observation_scope:
          "Silent selected call-time profiling is intrusive diagnostic evidence. Offline inventories are outside timed blocks; no acceptance measurement is claimed."
      })

    File.write!(output <> ".tmp", JSON.encode!(receipt) <> "\n")
    File.rename!(output <> ".tmp", output)
  end
end

[output, sizes] = System.argv()
milestones = sizes |> String.split(",") |> Enum.map(&String.to_integer/1)
ESSIndexSetupProbe.run(output, milestones)
