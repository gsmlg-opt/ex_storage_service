# Test-only RPC into an exclusively owned disposable process. Never log credentials.
[action, request_path, result_path] = System.argv()
target = String.to_atom(System.fetch_env!("E2E_GATEWAY_CONTROL_TARGET"))
true = Node.connect(target)
request = JSON.decode!(File.read!(request_path))

source = """
alias ExStorageService.{Context, Metadata, ObjectService}
alias ExStorageService.IAM.{AccessKey, Policy, User}
alias ExStorageService.Metadata.{Keys, Backend.Concord}
request = #{inspect(request, limit: :infinity, printable_limit: :infinity)}
case #{inspect(action)} do
  "seed" ->
    {:ok, user} = User.create_user("crash-acceptance")
    {:ok, policy} = Policy.create_policy("crash-acceptance", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    {:ok, key} = AccessKey.create_access_key(user.id)
    :ok = Metadata.create_bucket(request["bucket"])
    %{access_key: key.access_key_id, secret_key: key.secret_access_key}
  "crash" ->
    boundary = String.to_existing_atom(request["boundary"])
    {:ok, context} = Context.default()
    staging_dir = Path.join(context.tmp_root, "uploads")
    crash = fn ->
      staging = for name <- File.ls!(staging_dir),
        String.starts_with?(name, "ess1-"),
        do: %{name: name, size: File.stat!(Path.join(staging_dir, name)).size}
      File.write!(request["witness"], JSON.encode!(%{boundary: boundary, pid: System.pid(), staging: staging}))
      System.halt(137)
    end
    opts = [context: context, metadata_opts: [operation_id: request["operation_id"]], side_effects: false]
    opts = if boundary in [:stage, :protection_data_sync, :protection_rename, :protection_directory_sync, :sync, :rename, :directory_sync] do
      Keyword.put(opts, :blob_store_opts, [faults: %{boundary => crash}])
    else
      Keyword.put(opts, :faults, [{boundary, crash}])
    end
    body = Base.decode64!(request["body_base64"])
    {:ok, result} = ObjectService.put(request["bucket"], request["key"], body, "application/octet-stream", %{}, opts)
    if boundary == :response, do: crash.()
    %{unexpected_completion: true, version_id: result.version_id}
  "inspect" ->
    object = case ObjectService.head(request["bucket"], request["key"]) do
      {:ok, result} -> %{exists: true, version_id: result.version_id, hash: result.metadata.content_hash}
      {:error, :object_not_found} -> %{exists: false}
      other -> raise("unexpected head result: \#{inspect(other)}")
    end
    intent = case Concord.get(Keys.operation_intent(request["operation_id"])) do
      {:ok, nil} -> "absent"
      {:ok, %{value: value}} -> to_string(value.state)
      other -> raise("unexpected intent result: \#{inspect(other)}")
    end
    {:ok, context} = Context.default()
    send(ExStorageService.Names.process(context.instance, :engine, ExStorageService.Storage.Engine), :staging_recovery_start)
    staging = for name <- File.ls!(Path.join(context.tmp_root, "uploads")), String.starts_with?(name, "ess1-"), do: name
    %{object: object, intent: intent, pid: System.pid(), staging: staging}
end
"""

case :rpc.call(target, Code, :eval_string, [source], 30_000) do
  {:badrpc, reason} ->
    raise "crash acceptance RPC failed: #{inspect(reason)}"

  {result, _binding} ->
    File.write!(result_path, JSON.encode!(result))
    File.chmod!(result_path, 0o600)
end
