# Local distributed RPC for an exclusively owned disposable gateway process.
# Inputs/output live in mode0600 files; never print credentials or KV records.
[action, request_path, result_path] = System.argv()
target = String.to_atom(System.fetch_env!("E2E_GATEWAY_CONTROL_TARGET"))
true = Node.connect(target)
request = JSON.decode!(File.read!(request_path))

source = """
alias ExStorageService.CloudCache.{Config, WriteThrough, DeleteThrough}
alias ExStorageService.IAM.{AccessKey, Policy, User}
alias ExStorageService.Metadata
alias ExStorageService.BlobStore.LocalCAS
request = #{inspect(request)}
case #{inspect(action)} do
  "seed" ->
    suffix = System.unique_integer([:positive])
    {:ok, user} = User.create_user("gateway-hardening-\#{suffix}")
    {:ok, policy} = Policy.create_policy("gateway-hardening-\#{suffix}", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    {:ok, key} = AccessKey.create_access_key(user.id)
    :ok = Metadata.create_bucket(request["bucket"])
    :ok = Config.set_config(request["bucket"], %{
      enabled: true, provider: :s3_compat, endpoint: request["proxy_endpoint"],
      region: "us-east-1", bucket: request["remote_bucket"],
      access_key_id: request["remote_access"], secret_access_key: request["remote_secret"],
      cache_enabled: true, cache_max_bytes: 134217728
    })
    %{access_key: key.access_key_id, secret_key: key.secret_access_key}
  "resource-config" ->
    :ok = Metadata.create_bucket(request["local_bucket"])
    {:ok, config} = Config.get_config(request["bucket"])
    :ok = Config.set_config(request["bucket"], Map.put(Map.from_struct(config), :cache_max_bytes, 6 * 1024 * 1024 * 1024))
    %{configured: true, cache_max_bytes: 6 * 1024 * 1024 * 1024}
  "fail-local-commit" ->
    {:ok, config} = Config.get_active_config(request["bucket"])
    options = [before_local_commit: fn -> {:error, :acceptance_local_commit_failure} end]
    operation_id = "acceptance-" <> Integer.to_string(System.unique_integer([:positive]))
    result = case request["method"] do
      "PUT" ->
        {:ok, staged} = LocalCAS.stage(Base.decode64!(request["body_base64"]))
        WriteThrough.put(request["bucket"], request["key"], staged, config,
          %{content_type: "application/octet-stream", metadata: %{"recovery" => "commit-failure"}},
          operation_id, options)
      "DELETE" ->
        DeleteThrough.delete(request["bucket"], request["key"], config, operation_id, options)
    end
    {:error, {:local_reconciliation_required, :acceptance_local_commit_failure}} = result
    %{failed_before_local_commit: true, operation_id: operation_id, runtime_pid: System.pid()}
  "inspect" ->
    backend = ExStorageService.Metadata.Backend.Concord
    journals = Enum.flat_map(["ess:gateway-write:", "ess:gateway-delete:"], fn prefix ->
      {:ok, page} = backend.list_page(prefix, nil, 100, [])
      if page.next_cursor != nil, do: raise("unexpected acceptance journal overflow")
      Enum.map(page.entries, fn %{value: record} ->
        %{key: record.key, state: to_string(record.state), operation_id: record.operation_id,
          path_exists: if(Map.has_key?(record, :path), do: File.regular?(record.path), else: nil)}
      end)
    end)
    object = case Metadata.get_v1_object_meta(request["bucket"], request["key"]) do
      {:ok, metadata} -> %{exists: true, hash: metadata[:content_hash], size: metadata[:size]}
      {:error, :not_found} -> %{exists: false}
      other -> raise("unexpected object inspection: \#{inspect(other)}")
    end
    identity = fn value -> :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16(case: :lower) end
    index = case backend.get("cloud_cache_index:" <> request["bucket"], consistency: :linearizable) do
      {:ok, nil} -> %{}
      {:ok, %{value: value}} when is_map(value) -> value
      other -> raise("unexpected cache index inspection: \#{inspect(other)}")
    end
    cache = case Map.get(index, request["key"]) do
      nil -> %{indexed: false, index_identity: identity.(index)}
      entry ->
        hash = Map.get(entry, :content_hash, Map.get(entry, "content_hash"))
        <<prefix::binary-size(2), rest::binary>> = hash
        path = Path.join([Application.fetch_env!(:ex_storage_service, :data_root), request["bucket"], "cache", prefix, rest])
        file = case File.stat(path) do
          {:ok, %{type: :regular, size: size}} ->
            actual = File.stream!(path, [], 262_144) |> Enum.reduce(:crypto.hash_init(:sha256), fn bytes, acc -> :crypto.hash_update(acc, bytes) end) |> :crypto.hash_final() |> Base.encode16(case: :lower)
            %{exists: true, hash: actual, size: size}
          {:error, :enoent} -> %{exists: false}
          other -> raise("unexpected cache file inspection: \#{inspect(other)}")
        end
        %{indexed: true, index_identity: identity.(index), entry_identity: identity.(entry), file: file}
    end
    %{journals: journals, object: object, cache: cache, runtime_pid: System.pid()}
  "reconcile" ->
    {:ok, writes} = WriteThrough.reconcile_page(nil, 100)
    {:ok, deletes} = DeleteThrough.reconcile_page(nil, 100)
    if writes.next_cursor != nil or deletes.next_cursor != nil, do: raise("acceptance journal overflow")
    %{writes: Enum.map(writes.results, fn {_id, result} -> inspect(result) end),
      deletes: Enum.map(deletes.results, fn {_id, result} -> inspect(result) end)}
end
"""

result =
  case :rpc.call(target, Code, :eval_string, [source], 30_000) do
    {:badrpc, reason} -> raise "gateway hardening RPC failed: #{inspect(reason)}"
    {result, _binding} -> result
    _ -> raise "unexpected gateway hardening RPC result"
  end

File.write!(result_path, JSON.encode!(result))
File.chmod!(result_path, 0o600)
IO.puts("gateway hardening control #{action}: ok")
