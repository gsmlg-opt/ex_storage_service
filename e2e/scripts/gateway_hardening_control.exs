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
  "legacy-gateway-seed" ->
    backend = ExStorageService.Metadata.Backend.Concord
    alias ExStorageService.Metadata.{BucketLifecycle, Keys, ListIndex}
    {:ok, config} = Config.get_active_config(request["bucket"])
    {:ok, context} = ExStorageService.Context.default()
    for fixture <- request["objects"] do
      bytes = Base.decode64!(fixture["body_base64"])
      hash = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
      metadata = %{
        cloud_backed: true, content_hash: hash, size: byte_size(bytes),
        etag: :crypto.hash(:md5, bytes) |> Base.encode16(case: :lower),
        content_type: "application/octet-stream", created_at: "2026-11-02T00:00:00Z",
        upstream_etag: fixture["etag"], upstream_version_id: fixture["version_id"]
      }
      :ok = backend.put("obj:" <> request["bucket"] <> ":" <> fixture["key"], metadata, [])
      {:error, :not_found} = LocalCAS.stat(hash, ExStorageService.Context.direct_blob_store_options(context))
      if fixture["cached"] do
        {:ok, staged} = LocalCAS.stage(bytes)
        {:ok, _path} = ExStorageService.CloudCache.LocalStore.put_file(request["bucket"], fixture["key"],
          staged.path, staged.hash, staged.size, config)
        :ok = LocalCAS.discard(staged)
      end
    end
    old = %{cloud_backed: true, version_id: "old-cloud", size: 8,
      content_hash: :crypto.hash(:sha256, "old-body") |> Base.encode16(case: :lower),
      etag: "old", metadata: %{"retained" => "history"}, created_at: "2026-10-01T00:00:00Z"}
    marker = %{cloud_backed: true, version_id: "old-marker", is_delete_marker: true,
      created_at: "2026-10-02T00:00:00Z"}
    :ok = backend.put("obj_ver:" <> request["bucket"] <> ":conversion-cold:old-cloud", old, [])
    :ok = backend.put("obj_ver:" <> request["bucket"] <> ":conversion-cold:old-marker", marker, [])
    {:ok, bucket} = Metadata.get_bucket(request["bucket"])
    :ok = backend.delete(ListIndex.root_key(request["bucket"], BucketLifecycle.generation(bucket)), [])
    {:error, :listing_index_migration_required} = Metadata.check_object_mutation(request["bucket"])
    %{seeded: true, cached_and_upstream_only: true}
  "legacy-gateway-convert" ->
    alias ExStorageService.Operations.{Backup, ListingIndex}
    {:ok, context} = ExStorageService.Context.default()
    {:ok, _} = Backup.quiesce(context)
    :ok = Backup.assert_quiesced(context)
    {:ok, _} = Backup.create(context, request["backup"])
    {:ok, %{upstream_only_bodies_included: false}} = Backup.verify(request["backup"])
    opts = [sources: [context], backup: request["backup"], batch_size: 2]
    {:ok, build} = ListingIndex.begin(request["bucket"], opts)
    {:ok, _} = ListingIndex.capture(request["bucket"], build.owner_token, request["manifest"], opts)
    for {phase, terminal} <- [{:resume, :validating}, {:validate, :validated}] do
      result = Enum.reduce_while(1..256, :incomplete, fn _, _ ->
        {:ok, progress} = apply(ListingIndex, phase, [request["bucket"], build.owner_token, request["manifest"], opts])
        if progress.phase == terminal, do: {:halt, :complete}, else: {:cont, :incomplete}
      end)
      :complete = result
    end
    {:ok, %{phase: :ready}} = ListingIndex.cutover(request["bucket"], build.owner_token, opts)
    {:ok, %{ready: true, counts: counts}} = ListingIndex.readiness(request["bucket"])
    %{backup_verified: true, quiescence_verified: true, ready: true, counts: counts}
  "legacy-gateway-inspect" ->
    alias ExStorageService.Metadata.{Keys, ListIndex}
    backend = ExStorageService.Metadata.Backend.Concord
    {:ok, %{entries: versions}} = ListIndex.page(request["bucket"], :versions, "")
    {:ok, %{entries: current}} = ListIndex.page(request["bucket"], :current, "")
    retained = for id <- ["old-cloud", "old-marker"] do
      {:ok, %{value: original}} = backend.get("obj_ver:" <> request["bucket"] <> ":conversion-cold:" <> id, consistency: :linearizable)
      {:ok, %{value: converted}} = backend.get(Keys.object_version(request["bucket"], "conversion-cold", id), consistency: :linearizable)
      true = Enum.all?(original, fn {key, value} -> converted[key] == value end)
      id
    end
    cloud_versions = Enum.map(versions, fn {_encoded, summary} ->
      {:ok, %{value: version}} = backend.get(summary.metadata_key, consistency: :linearizable)
      true = version.cloud_backed
      if not Map.get(version, :delete_marker, false) do
        {:ok, nil} = backend.get(Keys.blob(version.content_hash), consistency: :linearizable)
        {:ok, %{entries: []}} = backend.list_page(Keys.blob_location_prefix(version.content_hash), nil, 1, [])
      end
      %{key: summary.key, version_id: summary.version_id, latest: summary.is_latest,
        marker: summary.delete_marker}
    end)
    %{retained: retained, versions: cloud_versions, current: Enum.map(current, fn {key, _} -> key end),
      no_hosted_durability: true}
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
