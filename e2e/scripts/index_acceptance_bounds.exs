# Test-only A28 bounds. No application is started by loading this file.
defmodule ESSIndexAcceptance.Bounds do
  @encoded_key_bytes 160
  @height 16
  @node_bytes 2_048
  @hydrated_bytes 4_096
  @node_heap_bytes 8_192
  @hydrated_heap_bytes 16_384
  @heap_bytes 128 * 1_024 * 1_024
  @gc_events [:gc_minor_start, :gc_minor_end, :gc_major_start, :gc_major_end, :gc_max_heap_size]

  def limits(0),
    do: %{reads: 8, bytes: 34_816, output: 16_384, output_heap: 40_960, heap: @heap_bytes}

  def limits(maximum) when maximum in 1..1000 do
    %{
      reads: (2 * @height + 1) * (maximum + 1) + 8,
      bytes: 78_080 * (maximum + 1) + 34_816,
      output: 8_192 * (maximum + 2),
      output_heap: 20_480 * (maximum + 2),
      heap: @heap_bytes
    }
  end

  def declaration do
    %{
      encoded_key_bytes: @encoded_key_bytes,
      height: @height,
      fixture_node_bytes: @node_bytes,
      hydrated_bytes: @hydrated_bytes,
      fixture_node_heap_bytes: @node_heap_bytes,
      hydrated_heap_bytes: @hydrated_heap_bytes,
      production_node_bytes: 32_768,
      production_terminal_bytes: 8_192,
      production_max_children: 256,
      request_heap_bytes: @heap_bytes,
      stable_attempts: 1,
      query_boundaries: ["Concord.Engine.query/2", "Concord.Engine.VSR.query/2"],
      native_scope:
        "owned request PID and single configured local VSR Replica PID; every Core query must be get_record",
      heap_derivation: %{
        page: 20_480 * 1002,
        seek_path: 2 * @height * @node_heap_bytes,
        input: 32_768,
        fixed: 1_048_576,
        transient_gc_multiplier: 3,
        runtime_reserve: 32 * 1_024 * 1_024,
        derived_maximum: 99_147_776
      },
      peak_heap_method: "isolated process total_heap_size plus GC block sizes and 1ms sampling"
    }
  end

  # The offline snapshot is already intentionally materialized for size accounting.
  # Traverse only its exact tree links, before any measured request starts.
  def fixture!(snapshot, trees) when is_map(snapshot) do
    initial = %{
      height: 0,
      encoded_key_bytes: 0,
      node_bytes: 0,
      hydrated_bytes: 0,
      nodes: 0,
      terminals: 0
    }

    Enum.reduce(trees, initial, fn {namespace, root}, stats ->
      walk(snapshot, namespace, root, 0, 0, [], stats)
    end)
  end

  defp walk(_snapshot, _namespace, nil, _height, _bytes, _ancestors, stats), do: stats

  defp walk(snapshot, namespace, id, height, bytes, ancestors, stats) do
    ensure!(is_binary(id) and byte_size(id) == 16 and id not in ancestors, :fixture_link)
    height = height + 1
    ensure!(height <= @height, :fixture_height)
    node = Map.fetch!(snapshot, namespace <> Base.url_encode64(id, padding: false))
    %{schema: 1, label: label, terminal: terminal, children: children} = node
    ensure!(is_binary(label) and is_list(children) and length(children) <= 256, :fixture_node)
    bytes = bytes + byte_size(label)
    node_bytes = :erlang.external_size(node)
    ensure!(bytes <= @encoded_key_bytes, :fixture_encoded_key)
    ensure!(node_bytes <= @node_bytes, :fixture_node_bytes)
    ensure!(flat_bytes(node) <= @node_heap_bytes, :fixture_node_heap_bytes)

    stats = %{
      stats
      | height: max(stats.height, height),
        encoded_key_bytes: max(stats.encoded_key_bytes, bytes),
        node_bytes: max(stats.node_bytes, node_bytes),
        nodes: stats.nodes + 1
    }

    stats =
      if is_nil(terminal) do
        stats
      else
        value = Map.fetch!(snapshot, Map.fetch!(terminal, :metadata_key))
        hydrated_bytes = :erlang.external_size(value)
        ensure!(hydrated_bytes <= @hydrated_bytes, :fixture_hydrated_bytes)
        ensure!(flat_bytes(value) <= @hydrated_heap_bytes, :fixture_hydrated_heap_bytes)

        %{
          stats
          | hydrated_bytes: max(stats.hydrated_bytes, hydrated_bytes),
            terminals: stats.terminals + 1
        }
      end

    Enum.reduce(children, stats, fn {_edge, child}, acc ->
      walk(snapshot, namespace, child, height, bytes, [id | ancestors], acc)
    end)
  end

  def record_read(key, result) do
    current = Process.get(:index_acceptance_reads, %{count: 0, bytes: 0})
    bytes = :erlang.external_size(result)

    case result do
      {:ok, %{value: value}} ->
        bound = if String.contains?(key, ":list_index:"), do: @node_bytes, else: @hydrated_bytes
        # Descriptors also live under list_index but remain below this fixture bound.
        ensure!(:erlang.external_size(value) <= bound, :returned_fixture_record_bytes)
        ensure!(bytes <= bound + 256, :returned_fixture_envelope_bytes)
        heap_bound = if bound == @node_bytes, do: @node_heap_bytes, else: @hydrated_heap_bytes
        ensure!(flat_bytes(value) <= heap_bound, :returned_fixture_record_heap)

      _ ->
        :ok
    end

    updated = %{count: current.count + 1, bytes: current.bytes + bytes}

    case Process.get(:index_acceptance_limits) do
      nil ->
        :ok

      limits ->
        ensure!(updated.count <= limits.reads, :read_bound)
        ensure!(updated.bytes <= limits.bytes, :returned_byte_bound)
    end

    Process.put(:index_acceptance_reads, updated)
    result
  end

  def default_specs do
    queries = [
      {{Concord.Engine, :query, 2}, :engine_query, :query},
      {{Concord.Engine.VSR, :query, 2}, :vsr_query, :query},
      {{Concord.StateMachine.Core, :query, 3}, :core_query, :query},
      {{Concord.StateMachine.Core, :do_query, 3}, :core_dispatch, :query}
    ]

    forbidden = [
      {ExStorageService.Metadata.Backend.Concord, :get_all, [0, 1]},
      {ExStorageService.Metadata.Backend.Concord, :prefix_scan, [1, 2]},
      {ExStorageService.Metadata.Backend.Concord, :scan, [1, 2]},
      {ExStorageService.Metadata.Backend.Concord, :list_page, [3, 4]},
      {Concord, :get_all, [0, 1]},
      {Concord, :get_all_with_ttl, [0, 1]},
      {Concord, :prefix_scan, [1, 2]},
      {Concord.KV, :list, [0, 1]},
      {Concord.KV, :history, [1, 2]},
      {Concord.Engine, :command, [1, 2]},
      {Concord.StateMachine, :query, [2]},
      {Concord.StateMachine, :materialize, [1]},
      {Concord.StateMachine.Core, :snapshot, [1]}
    ]

    queries ++
      for {module, function, arities} <- forbidden, arity <- arities do
        {{module, function, arity}, :forbidden_api, :forbidden}
      end
  end

  def measure(request, maximum, opts \\ []) do
    owner = self()
    reference = make_ref()

    # Fixture inventory/history and unrelated caller messages must not become
    # collector heap or count against the private trace mailbox bound.
    {collector, monitor} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, measure_isolated(request, maximum, opts)}
          catch
            kind, reason -> {:raised, kind, reason, __STACKTRACE__}
          end

        send(owner, {reference, result})
      end)

    receive do
      {^reference, result} ->
        receive do
          {:DOWN, ^monitor, :process, ^collector, reason} ->
            ensure!(reason == :normal, :collector_exit)
        end

        case result do
          {:ok, sample} -> sample
          {:raised, kind, reason, stacktrace} -> :erlang.raise(kind, reason, stacktrace)
        end

      {:DOWN, ^monitor, :process, ^collector, _reason} ->
        raise "A28 owned collector exited before completion"
    end
  end

  defp measure_isolated(request, maximum, opts) do
    ensure!(
      Code.ensure_loaded?(:trace) and function_exported?(:trace, :session_create, 3),
      :otp28_trace_required
    )

    specs = Keyword.get(opts, :trace_specs, default_specs())
    limits = limits(maximum)
    native_pid = Keyword.get_lazy(opts, :native_pid, &replica!/0)
    ensure!(is_pid(native_pid) and Process.alive?(native_pid), :native_replica_required)
    token = make_ref()
    owner = self()
    word_bytes = :erlang.system_info(:wordsize)

    {worker, monitor} =
      :erlang.spawn_opt(
        fn ->
          receive do
            :measure ->
              Process.put(:index_acceptance_reads, %{count: 0, bytes: 0})
              Process.put(:index_acceptance_limits, limits)

              try do
                {duration, {:ok, result}} = :timer.tc(request)
                %{count: _, bytes: _} = reads = Process.get(:index_acceptance_reads)
                entries = Map.get(result, :keys, Map.get(result, :entries, []))

                sample = %{
                  duration: duration,
                  reads: reads.count,
                  bytes: reads.bytes,
                  output_bytes: :erlang.external_size(result),
                  output_heap_bytes: flat_bytes(result),
                  output_entries: length(entries) + length(result.common_prefixes),
                  index_stats: result.index_stats,
                  final_heap_bytes: heap_bytes(self())
                }

                send(owner, {:sample_ready, self(), sample})
              rescue
                _ -> send(owner, {:sample_failed, self()})
              catch
                _, _ -> send(owner, {:sample_failed, self()})
              end

              receive do
                :finish -> :ok
              end
          end
        end,
        [
          :monitor,
          {:max_heap_size, %{size: div(@heap_bytes, word_bytes), kill: true, error_logger: false}}
        ]
      )

    session = :trace.session_create(:ess_index_bounds, self(), [])

    try do
      install!(session, specs, token)
      1 = :trace.process(session, native_pid, true, [:call, :arity])
      1 = :trace.process(session, worker, true, [:call, :arity, :garbage_collection])

      stats = %{
        events: 0,
        exact: %{},
        forbidden: 0,
        gc_events: 0,
        peak_heap_bytes: heap_bytes(worker),
        sampled_process_memory_bytes: 0
      }

      send(worker, :measure)
      deadline = System.monotonic_time(:millisecond) + 60_000

      {sample, stats} =
        collect(session, worker, native_pid, token, monitor, nil, %{}, stats, limits, deadline)

      sample =
        Map.merge(sample, %{
          peak_heap_bytes: stats.peak_heap_bytes,
          sampled_process_memory_bytes: stats.sampled_process_memory_bytes,
          trace_events: stats.events,
          gc_events: stats.gc_events,
          native_exact_counts: stats.exact,
          forbidden_native_calls: stats.forbidden
        })

      check_sample!(sample, maximum)

      for {_mfa, label, :query} <- specs,
          do: ensure!(Map.get(stats.exact, label, 0) == sample.reads, :native_delegation_count)

      sample
    after
      # Destroy only this session. Never reset legacy or another session's tracing.
      :trace.session_destroy(session)

      if Process.alive?(worker) do
        Process.exit(worker, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^worker, _} -> :ok
        after
          5_000 ->
            raise "A28 owned sample did not stop"
        end
      end

      Process.demonitor(monitor, [:flush])
    end
  end

  defp replica! do
    config = Application.fetch_env!(:concord, :vsr)

    ViewstampedReplication.Replica.whereis(
      Keyword.fetch!(config, :group_id),
      Keyword.fetch!(config, :replica_id)
    )
  end

  defp install!(session, specs, token) do
    for {{module, _function, arity} = mfa, label, kind} <- specs do
      {:module, ^module} = Code.ensure_loaded(module)

      match =
        if kind == :query do
          arguments = [{:get_record, :_} | List.duplicate(:_, arity - 1)]

          [
            {arguments, [],
             [{:message, {:const, {:ess_index_bounds, token, :exact_read, label}}}]},
            {:_, [], [{:message, {:const, {:ess_index_bounds, token, :forbidden_query, label}}}]}
          ]
        else
          [{:_, [], [{:message, {:const, {:ess_index_bounds, token, :forbidden_api, label}}}]}]
        end

      # Exact private do_query/3 is intentional. Installation must match one MFA.
      1 = :trace.function(session, mfa, match, [:local])
    end
  end

  defp collect(
         session,
         worker,
         native_pid,
         token,
         monitor,
         sample,
         barriers,
         stats,
         limits,
         deadline
       ) do
    {:message_queue_len, queued} = Process.info(self(), :message_queue_len)
    ensure!(queued <= 512, :trace_mailbox_bound)
    ensure!(System.monotonic_time(:millisecond) < deadline, :sample_deadline)
    ensure!(stats.events <= 4 * limits.reads + 4_096, :trace_event_bound)
    {:message_queue_len, queued} = Process.info(self(), :message_queue_len)
    ensure!(queued <= 512, :trace_mailbox_stop_condition)
    ensure!(stats.peak_heap_bytes <= limits.heap, :peak_heap_bound)

    receive do
      {:sample_ready, ^worker, value} ->
        # Both tracees are stopped in this session before either delivery barrier.
        # The owned child remains parked; the replica is never stopped or killed.
        :trace.process(session, worker, false, [:call, :garbage_collection])
        :trace.process(session, native_pid, false, [:call])

        barriers =
          Map.new([worker, native_pid], fn pid -> {pid, :trace.delivered(session, pid)} end)

        stats = %{stats | peak_heap_bytes: max(stats.peak_heap_bytes, value.final_heap_bytes)}

        collect(
          session,
          worker,
          native_pid,
          token,
          monitor,
          value,
          barriers,
          stats,
          limits,
          deadline
        )

      {:trace_delivered, pid, ref} when is_map_key(barriers, pid) ->
        ensure!(Map.fetch!(barriers, pid) == ref, :trace_delivery_reference)
        barriers = Map.delete(barriers, pid)

        if map_size(barriers) == 0 and not is_nil(sample),
          do: {sample, stats},
          else:
            collect(
              session,
              worker,
              native_pid,
              token,
              monitor,
              sample,
              barriers,
              stats,
              limits,
              deadline
            )

      {:sample_failed, ^worker} ->
        raise "A28 owned sample failed; no payload emitted"

      {:DOWN, ^monitor, :process, ^worker, _} ->
        raise "A28 owned sample exited before completion"

      {:trace, pid, :call, {_module, _function, _arity},
       {:ess_index_bounds, ^token, :exact_read, label}}
      when pid == worker or pid == native_pid ->
        stats = %{
          stats
          | events: stats.events + 1,
            exact: Map.update(stats.exact, label, 1, &(&1 + 1))
        }

        collect(
          session,
          worker,
          native_pid,
          token,
          monitor,
          sample,
          barriers,
          stats,
          limits,
          deadline
        )

      {:trace, pid, :call, {_module, _function, _arity},
       {:ess_index_bounds, ^token, kind, _label}}
      when (pid == worker or pid == native_pid) and kind in [:forbidden_api, :forbidden_query] ->
        raise "A28 forbidden native boundary"

      {:trace, ^worker, event, info} when event in @gc_events ->
        blocks =
          Keyword.get(info, :heap_block_size, 0) +
            Keyword.get(info, :old_heap_block_size, 0) + Keyword.get(info, :mbuf_size, 0)

        peak = blocks * :erlang.system_info(:wordsize)

        stats = %{
          stats
          | events: stats.events + 1,
            gc_events: stats.gc_events + 1,
            peak_heap_bytes: max(stats.peak_heap_bytes, peak)
        }

        ensure!(event != :gc_max_heap_size, :peak_heap_bound)

        collect(
          session,
          worker,
          native_pid,
          token,
          monitor,
          sample,
          barriers,
          stats,
          limits,
          deadline
        )
    after
      1 ->
        {memory, heap} =
          case Process.info(worker, [:memory, :total_heap_size]) do
            nil -> {0, 0}
            info -> {info[:memory], info[:total_heap_size] * :erlang.system_info(:wordsize)}
          end

        stats = %{
          stats
          | peak_heap_bytes: max(stats.peak_heap_bytes, heap),
            sampled_process_memory_bytes: max(stats.sampled_process_memory_bytes, memory)
        }

        collect(
          session,
          worker,
          native_pid,
          token,
          monitor,
          sample,
          barriers,
          stats,
          limits,
          deadline
        )
    end
  end

  def check_sample!(sample, maximum) do
    limits = limits(maximum)
    ensure!(sample.reads <= limits.reads, :read_bound)
    ensure!(sample.bytes <= limits.bytes, :returned_byte_bound)
    ensure!(sample.output_bytes <= limits.output, :page_output_bound)
    ensure!(sample.output_heap_bytes <= limits.output_heap, :page_output_heap_bound)
    ensure!(sample.output_entries <= maximum, :page_entry_bound)
    ensure!(sample.peak_heap_bytes <= limits.heap, :peak_heap_bound)
    ensure!(sample.forbidden_native_calls == 0, :forbidden_native_boundary)
    sample
  end

  defp flat_bytes(value), do: :erts_debug.flat_size(value) * :erlang.system_info(:wordsize)

  defp heap_bytes(pid) do
    case Process.info(pid, :total_heap_size) do
      {:total_heap_size, words} -> words * :erlang.system_info(:wordsize)
      nil -> 0
    end
  end

  defp ensure!(true, _), do: :ok
  defp ensure!(_, tag), do: raise("A28 " <> Atom.to_string(tag))
end
