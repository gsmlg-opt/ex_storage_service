defmodule ESSClusterStageDiagnosticTrace do
  use GenServer

  @stages [
    {ExStorageService.Storage.Multipart, :prepare_complete_upload, 4, "prepare"},
    {ExStorageService.ObjectService, :commit_existing_blob, 5, "commit_existing"},
    {ExStorageService.Metadata.OperationIntents, :open, 6, "intent_open"},
    {ExStorageService.Cluster.WriteCoordinator, :ensure_blob, 3, "ensure_blob"},
    {ExStorageService.Metadata.ObjectCommit, :put, 4, "object_commit"},
    {ExStorageService.Metadata.Backend.Concord, :transaction, 2, "transaction"}
  ]
  @shared {ExStorageServiceS3.Handlers.Shared, :storage_error_response, 4}
  @stage_names Map.new(@stages, fn {m, f, a, name} -> {{m, f, a}, name} end)
  @allowed_atoms ~w(ok error timeout unknown no_leader cluster_not_ready
    metadata_quorum_unavailable blob_write_quorum_unavailable invalid_replica_ack
    listing_index_migration_required listing_index_unavailable listing_index_changed
    listing_index_corrupt corrupt_index insufficient_eligible_nodes
    cluster_data_plane_disabled all_blob_replicas_unavailable compare_retry_exhausted
    operation_intent_compare_failed local_reconciliation_required read_idle_timeout
    write_idle_timeout request_timeout connect_timeout econnrefused nxdomain closed
    enetunreach ehostunreach upstream_changed invalid_upstream_metadata
    upstream_length_mismatch multipart_completion_conflict precondition_failed
    command_in_progress invalid_upload no_such_upload bucket_not_found not_found
    unavailable conflict compare_failed quorum_unavailable)a
  @limit 4096
  @duration_ms 20_000

  def start(path), do: GenServer.start(__MODULE__, path, name: __MODULE__)

  def stop do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _ -> GenServer.call(__MODULE__, :stop, 30_000)
    end
  end

  # Trace messages can contain private return terms. Keep them out of OTP crash
  # and status reports if output I/O or the diagnostic process fails.
  @impl true
  def format_status(status) do
    Map.new(status, fn {key, _value} -> {key, if(key == :log, do: [], else: :redacted)} end)
  end

  @impl true
  def init(path) do
    # proc_lib reports queued trace terms separately from format_status/1.
    # Suppress reports only for this disposable tracer process.
    :ok = Logger.put_process_level(self(), :none)
    receipt_path = path <> ".stopped.json"

    with true <- Path.type(path) == :absolute,
         {:error, :enoent} <- File.lstat(receipt_path),
         {:ok, fd} <- File.open(path, [:write, :exclusive, :binary]) do
      case File.chmod(path, 0o600) do
        :ok ->
          initialize_trace(fd, receipt_path)

        _ ->
          File.close(fd)
          {:stop, :private_output_failed}
      end
    else
      _ -> {:stop, :private_output_unavailable}
    end
  end

  defp initialize_trace(fd, receipt_path) do
    session = :trace.session_create(__MODULE__, self(), [])

    try do
      Enum.each(@stages, fn {m, f, a, _name} ->
        {:module, ^m} = Code.ensure_loaded(m)

        1 =
          :trace.function(session, {m, f, a}, [{List.duplicate(:_, a), [], [{:return_trace}]}], [
            :local
          ])
      end)

      {:module, shared_module} = Code.ensure_loaded(elem(@shared, 0))
      true = shared_module == elem(@shared, 0)

      1 =
        :trace.function(session, @shared, [{[:_, :"$1", :_, :_], [], [{:message, :"$1"}]}], [
          :local
        ])

      :trace.process(session, :all, true, [:call, :arity, :monotonic_timestamp])
      :trace.process(session, self(), false, [:all])
      timer = Process.send_after(self(), :deadline, @duration_ms)

      {:ok,
       %{
         fd: fd,
         receipt_path: receipt_path,
         session: session,
         timer: timer,
         starts: %{},
         aliases: %{},
         seen: 0,
         written: 0,
         unmatched: 0,
         draining: nil,
         waiters: [],
         stop_reason: nil
       }}
    catch
      _, _ ->
        :trace.session_destroy(session)
        File.close(fd)
        {:stop, :trace_setup_failed}
    end
  end

  @impl true
  def handle_call(:stop, from, state) do
    {:noreply, begin_stop(%{state | waiters: [from | state.waiters]}, "explicit")}
  end

  @impl true
  def handle_info(:deadline, state), do: {:noreply, begin_stop(state, "deadline")}

  def handle_info({:trace_delivered, :all, ref}, %{draining: ref} = state) do
    :ok = :file.sync(state.fd)
    :ok = File.close(state.fd)

    receipt = %{
      stopped: true,
      session_destroyed: true,
      delivery_drained: true,
      output_closed: true,
      reason: state.stop_reason,
      trace_events: state.seen,
      output_events: state.written,
      unmatched_returns: state.unmatched,
      event_limit: @limit,
      duration_limit_ms: @duration_ms
    }

    {:ok, receipt_fd} = File.open(state.receipt_path, [:write, :exclusive, :binary])
    :ok = File.chmod(state.receipt_path, 0o600)
    :ok = IO.binwrite(receipt_fd, [JSON.encode_to_iodata!(receipt), "\n"])
    :ok = :file.sync(receipt_fd)
    :ok = File.close(receipt_fd)
    Enum.each(state.waiters, &GenServer.reply(&1, {:ok, receipt}))
    {:stop, :normal, %{state | fd: nil}}
  end

  def handle_info({:trace_ts, pid, :call, @shared, reason, timestamp}, state) do
    consume(state, fn current ->
      emit(current, pid, "storage_error", timestamp, nil, %{
        class: "error",
        reason: safe_reason(reason, 2)
      })
    end)
  end

  def handle_info({:trace_ts, pid, :call, mfa, timestamp}, state)
      when is_map_key(@stage_names, mfa) do
    consume(state, fn current ->
      key = {pid, mfa}
      %{current | starts: Map.update(current.starts, key, [timestamp], &[timestamp | &1])}
    end)
  end

  def handle_info({:trace_ts, pid, :return_from, mfa, value, timestamp}, state)
      when is_map_key(@stage_names, mfa) do
    consume(state, fn current ->
      key = {pid, mfa}

      case Map.get(current.starts, key, []) do
        [started | rest] ->
          starts =
            if rest == [],
              do: Map.delete(current.starts, key),
              else: Map.put(current.starts, key, rest)

          emit(
            %{current | starts: starts},
            pid,
            Map.fetch!(@stage_names, mfa),
            timestamp,
            timestamp - started,
            classify(value)
          )

        [] ->
          %{current | unmatched: current.unmatched + 1}
      end
    end)
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp consume(%{seen: seen} = state, _fun) when seen >= @limit,
    do: {:noreply, begin_stop(state, "event_limit")}

  defp consume(state, fun) do
    state = fun.(%{state | seen: state.seen + 1})
    state = if state.seen >= @limit, do: begin_stop(state, "event_limit"), else: state
    {:noreply, state}
  end

  defp emit(state, pid, stage, timestamp, duration, result) do
    alias_id = Map.get(state.aliases, pid, map_size(state.aliases) + 1)

    duration_us =
      if is_integer(duration),
        do: System.convert_time_unit(duration, :native, :microsecond),
        else: nil

    event = %{
      stage: stage,
      process: alias_id,
      duration_us: duration_us,
      monotonic_us: System.convert_time_unit(timestamp, :native, :microsecond),
      result: result
    }

    :ok = IO.binwrite(state.fd, [JSON.encode_to_iodata!(event), "\n"])
    %{state | aliases: Map.put(state.aliases, pid, alias_id), written: state.written + 1}
  end

  defp classify(:ok), do: %{class: "ok"}

  defp classify({:ok, %{succeeded: succeeded}}) when is_boolean(succeeded),
    do: %{class: "ok", compare_succeeded: succeeded}

  defp classify({:ok, _}), do: %{class: "ok"}
  defp classify({:error, reason}), do: %{class: "error", reason: safe_reason(reason, 2)}
  defp classify(value), do: %{class: "other", reason: safe_reason(value, 2)}

  defp safe_reason(value, _depth) when value in @allowed_atoms, do: Atom.to_string(value)

  defp safe_reason(value, depth) when is_tuple(value) and tuple_size(value) <= 3 and depth > 0,
    do: %{tuple: value |> Tuple.to_list() |> Enum.map(&safe_reason(&1, depth - 1))}

  defp safe_reason(_value, _depth), do: "other"

  defp begin_stop(%{draining: ref} = state, _reason) when not is_nil(ref), do: state

  defp begin_stop(state, reason) do
    Process.cancel_timer(state.timer)
    true = :trace.session_destroy(state.session)
    ref = :erlang.trace_delivered(:all)
    %{state | session: nil, draining: ref, stop_reason: reason}
  end

  @impl true
  def terminate(_reason, state) do
    if state.session, do: :trace.session_destroy(state.session)
    if state.fd, do: File.close(state.fd)
    :ok
  end
end
