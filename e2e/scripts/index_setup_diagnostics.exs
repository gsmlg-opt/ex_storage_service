# Setup observations only. No traces or observer run during Bounds.measure/3.
defmodule ESSIndexSetup do
  @window 100
  @output_limit 64 * 1024 * 1024

  def phase(name, count, operation) when is_integer(count) and count > 0 do
    started = System.monotonic_time(:microsecond)
    Process.put(:index_setup_backend, %{})
    Process.put(:index_setup_output_bytes, Process.get(:index_setup_output_bytes, 0))
    emit(%{phase: name, event: "start", planned: count, completed: 0})

    {_, remaining} =
      Enum.reduce(1..count, {0, []}, fn ordinal, {_, samples} ->
        {duration, result} = :timer.tc(fn -> operation.(ordinal) end)
        {:ok, _} = result
        samples = [duration | samples]

        if rem(ordinal, @window) == 0 or ordinal == count do
          emit(%{
            phase: name,
            event: "progress",
            planned: count,
            completed: ordinal,
            elapsed_us: System.monotonic_time(:microsecond) - started,
            window: latency(samples),
            backend: Process.get(:index_setup_backend)
          })

          {ordinal, []}
        else
          {ordinal, samples}
        end
      end)

    [] = remaining
    duration = System.monotonic_time(:microsecond) - started
    backend = Process.delete(:index_setup_backend)
    backend_us = Enum.reduce(backend, 0, fn {_, metric}, sum -> sum + metric.microseconds end)

    result = %{
      phase: name,
      event: "complete",
      planned: count,
      completed: count,
      elapsed_us: duration,
      backend: backend,
      # This includes observer overhead; it is not an isolated CPU/planner measurement.
      outside_backend_us: max(duration - backend_us, 0)
    }

    emit(result)
    result
  end

  def backend(kind, request, call) do
    {duration, result} = :timer.tc(call)

    if metrics = Process.get(:index_setup_backend) do
      category = Atom.to_string(kind) <> ":" <> category(request)
      bytes = :erlang.external_size(result)
      conflict = if match?({:ok, %{succeeded: false}}, result), do: 1, else: 0
      initial = %{calls: 0, microseconds: 0, maximum_us: 0, returned_bytes: 0, conflicts: 0}

      updated =
        Map.update(metrics, category, add(initial, duration, bytes, conflict), fn previous ->
          add(previous, duration, bytes, conflict)
        end)

      Process.put(:index_setup_backend, updated)
    end

    result
  end

  def observe(name, operation) do
    emit(%{phase: name, event: "start"})
    {duration, result} = :timer.tc(operation)
    emit(%{phase: name, event: "complete", elapsed_us: duration})
    {duration, result}
  end

  def emit(row) do
    row = Map.merge(row, %{monotonic_us: System.monotonic_time(:microsecond), runtime: runtime()})
    encoded = JSON.encode!(row) <> "\n"
    total = Process.get(:index_setup_output_bytes, 0) + byte_size(encoded)

    if byte_size(encoded) > 65_536 or total > @output_limit,
      do: raise("index progress output cap")

    Process.put(:index_setup_output_bytes, total)

    case System.get_env("ESS_INDEX_PROGRESS") do
      nil -> IO.binwrite(encoded)
      path -> File.write!(path, encoded, [:append])
    end
  end

  def latency(samples) do
    ordered = Enum.sort(samples)
    count = length(ordered)

    %{
      count: count,
      p50_us: Enum.at(ordered, div(count - 1, 2)),
      p95_us: Enum.at(ordered, min(div(count * 95, 100), count - 1)),
      maximum_us: List.last(ordered)
    }
  end

  def start_core! do
    workers =
      Map.new(
        ~w(multipart_gc content_gc cas_gc packer lifecycle cross_cluster_replication repair scrub)a,
        &{&1, false}
      )

    configured = Application.get_env(:ex_storage_service, :instance_config, [])

    Application.put_env(
      :ex_storage_service,
      :instance_config,
      Keyword.put(configured, :workers, workers)
    )

    {:ok, config} = ExStorageService.InstanceConfig.from_application_env()
    true = Enum.all?(config.workers, fn {_worker, enabled} -> enabled == false end)
    {:ok, _} = Application.ensure_all_started(:ex_storage_service)
  end

  def runtime do
    vsr = Application.get_env(:concord, :vsr, [])

    replica =
      if Process.whereis(ViewstampedReplication.Registry) do
        ViewstampedReplication.Replica.whereis(vsr[:group_id], vsr[:replica_id])
      end

    {collections, reclaimed_words, _} = :erlang.statistics(:garbage_collection)

    %{
      vm_total_bytes: :erlang.memory(:total),
      vm_binary_bytes: :erlang.memory(:binary),
      gc_collections: collections,
      gc_reclaimed_words: reclaimed_words,
      run_queue_lengths: :erlang.statistics(:run_queue_lengths),
      client: process_info(self()),
      engine: process_info(Process.whereis(Concord.Engine.VSR)),
      replica: process_info(replica),
      persistence: persistence()
    }
  end

  defp add(previous, duration, bytes, conflict) do
    %{
      calls: previous.calls + 1,
      microseconds: previous.microseconds + duration,
      maximum_us: max(previous.maximum_us, duration),
      returned_bytes: previous.returned_bytes + bytes,
      conflicts: previous.conflicts + conflict
    }
  end

  defp category("ess:v2:list_index:" <> key) do
    if String.ends_with?(key, ":root"), do: "index_root", else: "index_node"
  end

  defp category("bucket:" <> _), do: "bucket"
  defp category("ess:v2:object_head:" <> _), do: "head"
  defp category("ess:v2:object_version:" <> _), do: "version"
  defp category(value) when is_binary(value), do: "other"
  defp category(_), do: "specification"

  defp process_info(nil), do: nil

  defp process_info(pid) do
    case Process.info(pid, [:memory, :reductions, :message_queue_len, :total_heap_size, :status]) do
      nil -> nil
      values -> Map.new(values)
    end
  end

  defp persistence do
    root = Application.get_env(:concord, :data_dir)

    if is_binary(root) do
      # Fixed two-file observation, never recurse through objects or materialize metadata.
      Map.new(["replica.wal", "checkpoint.vsr"], fn filename ->
        path = Path.join([root, "vsr", Atom.to_string(node()), filename])

        size =
          case File.stat(path) do
            {:ok, stat} -> stat.size
            {:error, :enoent} -> nil
            {:error, reason} -> raise "index persistence observation failed: #{inspect(reason)}"
          end

        {filename, size}
      end)
    end
  end
end

defmodule ESSIndexSetup.Backend do
  alias ExStorageService.Metadata.Backend.Concord, as: Native

  for {name, arity} <- [get: 2, put: 3, delete: 2, prefix_scan: 2, scan: 2, list_page: 4] do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(name)(unquote_splicing(args)) do
      ESSIndexSetup.backend(unquote(name), unquote(hd(args)), fn ->
        Native.unquote(name)(unquote_splicing(args))
      end)
    end
  end

  def get_all(opts), do: ESSIndexSetup.backend(:get_all, nil, fn -> Native.get_all(opts) end)

  def transaction(spec, opts),
    do: ESSIndexSetup.backend(:transaction, spec, fn -> Native.transaction(spec, opts) end)

  def resolve_transaction(id, opts),
    do:
      ESSIndexSetup.backend(:resolve_transaction, id, fn ->
        Native.resolve_transaction(id, opts)
      end)

  def resolve_operation(key, opts), do: get(key, opts)
end
