# Private RPC for disposable A31 services. Only finite diagnostics leave the VM.
[action, request_path, result_path] = System.argv()
target = String.to_atom(System.fetch_env!("E2E_GATEWAY_CONTROL_TARGET"))
true = Node.connect(target)
request = JSON.decode!(File.read!(request_path))

source = """
request = #{inspect(request)}
case #{inspect(action)} do
  "snapshot" ->
    roots = [Application.fetch_env!(:ex_storage_service, :tmp_root),
      Application.fetch_env!(:ex_storage_service, :data_root)]
    files = Enum.flat_map(roots, &Path.wildcard(Path.join([&1, "**", "*"])))
      |> Enum.uniq()
      |> Enum.filter(fn path -> File.regular?(path) and
        (String.starts_with?(path, hd(roots) <> "/") or String.ends_with?(path, ".tmp")) end)
    queues = for pid <- Process.list(), {:message_queue_len, n} <- [Process.info(pid, :message_queue_len)], do: n
    %{staging_files: length(files), staging_bytes: Enum.reduce(files, 0, fn path, sum -> sum + File.stat!(path).size end),
      process_count: :erlang.system_info(:process_count), queue_max: Enum.max(queues, fn -> 0 end),
      queue_total: Enum.sum(queues)}
  "fill-disk" ->
    root = Application.fetch_env!(:ex_storage_service, :blob_root)
    path = Path.join(root, "acceptance-filler")
    {:ok, io} = File.open(path, [:write, :raw, :binary])
    fill = fn fill, count ->
      # At most 16MiB is ever attempted, exclusively in an 8MiB tmpfs.
      if count >= 16, do: raise("isolated disk did not reach its configured bound")
      case :file.write(io, :binary.copy(<<0>>, 1024 * 1024)) do
        :ok -> fill.(fill, count + 1)
        {:error, :enospc} -> %{enospc: true, complete_chunks: count}
        other -> raise("unexpected fill result: \#{inspect(other)}")
      end
    end
    try do fill.(fill, 0) after File.close(io) end
  "free-disk" ->
    :ok = File.rm(Path.join(Application.fetch_env!(:ex_storage_service, :blob_root), "acceptance-filler"))
    %{removed: true}
end
"""

result =
  case :rpc.call(target, Code, :eval_string, [source], 30_000) do
    {:badrpc, reason} -> raise "fault hardening RPC failed: #{inspect(reason)}"
    {result, _binding} -> result
  end

File.write!(result_path, JSON.encode!(result))
File.chmod!(result_path, 0o600)
IO.puts("fault hardening control #{action}: ok")
