# Disposable bounded diagnostic; records only classified returns, never arguments.
root = System.fetch_env!("E2E_RESOURCE_ROOT")
Code.require_file("/home/gao/Workspace/gsmlg-opt/ex_storage_service/e2e/scripts/resource_error_trace.exs")
spawn(fn ->
  targets = [
    {ExStorageServiceS3.Handlers.Object.CloudCopy, :copy, 7},
    {ExStorageServiceS3.Handlers.Object.CloudCopy, :stage_source, 3},
    {ExStorageServiceS3.Handlers.Object.CloudCopy, :put_hosted, 6},
    {ExStorageService.CloudCache.Client, :get_file, 3},
    {ExStorageService.ObjectService, :put_from_reader, 7},
    {ExStorageService.ObjectService, :protect_staged, 3},
    {ExStorageService.ObjectService, :open_operation_intent, 3},
    {ExStorageService.ObjectService, :commit_staged_blob, 4},
    {ExStorageService.ObjectService, :commit_ready_blob, 5},
    {ExStorageService.BlobStore.LocalCAS, :stage_from_reader, 3},
    {ExStorageService.Storage.StagingRecovery, :protect, 3}
  ]
  wait = fn wait, remaining ->
    cond do
      Enum.all?(targets, &Code.ensure_loaded?(elem(&1, 0))) -> :ok
      remaining == 0 -> raise "diagnostic trace module loading failed"
      true -> Process.sleep(25); wait.(wait, remaining - 1)
    end
  end
  wait.(wait, 2400)
  :ok = ESSResourceErrorTrace.start(Path.join(root, "storage-errors.jsonl"))
  path = Path.join(root, "copy-returns.jsonl")
  session = :trace.session_create(:ess_copy_diagnostic, self(), [])
  Enum.each(targets, fn target ->
    1 = :trace.function(session, target, [{:_, [], [{:return_trace}, {:exception_trace}]}], [:local])
  end)
  :trace.process(session, :all, true, [:call, :arity, :monotonic_timestamp])
  classify = fn classify, value, depth ->
    cond do
      depth > 5 -> "depth_limit"
      is_atom(value) -> Atom.to_string(value)
      is_integer(value) -> value
      is_tuple(value) and tuple_size(value) <= 8 -> Enum.map(Tuple.to_list(value), &classify.(classify, &1, depth + 1))
      true -> "redacted"
    end
  end
  File.write!(path, JSON.encode!(%{coverage: "started"}) <> "\n", [:exclusive])
  loop = fn loop, count ->
    receive do
      {:trace_ts, pid, type, {mod, function, arity}, result, stamp} when type in [:return_from, :exception_from] ->
        shape = case result do
          :ok -> "ok"
          {:ok, _} -> "ok"
          {:ok, _, _} -> "ok"
          {:error, reason} -> ["error", classify.(classify, reason, 0)]
          {:error, reason, _state} -> ["error", classify.(classify, reason, 0)]
          _ -> classify.(classify, result, 0)
        end
        File.write!(path, JSON.encode!(%{ordinal: count + 1, pid: inspect(pid), type: type, module: mod, function: function, arity: arity, monotonic_native: stamp, wall_ms: System.system_time(:millisecond), result: shape}) <> "\n", [:append])
        if count < 4095, do: loop.(loop, count + 1)
      _ -> loop.(loop, count)
    after
      900_000 -> :ok
    end
  end
  try do
    loop.(loop, 0)
  after
    :trace.session_destroy(session)
  end
end)
