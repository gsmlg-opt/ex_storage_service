# Disposable-cluster IAM mutations and narrowly guarded blob-loss fixtures only.
# Never print credentials or state.
[action, path, bucket] = System.argv()
target = String.to_atom(System.get_env("E2E_CONTROL_TARGET", "ess_data_a@127.0.0.1"))
state = if action == "seed", do: %{}, else: JSON.decode!(File.read!(path))
true = Node.connect(target)

source =
  if action == "remove-multipart-source-local" do
    root = Path.expand(System.fetch_env!("ESS_CLUSTER_E2E_ROOT"))
    true = target == :"ess_data_a@127.0.0.1"
    true = Path.dirname(root) == "/tmp"
    true = Regex.match?(~r/^ess-s3-hardening-cluster-[A-Za-z0-9]{8}$/, Path.basename(root))
    true = Path.expand(path) == Path.join(root, "multipart-source-control.json")
    true = state["schema"] == 1 and state["root"] == root and state["bucket"] == bucket
    true = Regex.match?(~r/^hardening-multipart-[a-f0-9]{16}$/, bucket)
    hash = state["hash"]
    size = state["size"]
    true = is_binary(hash) and Regex.match?(~r/^[a-f0-9]{64}$/, hash)
    true = is_integer(size) and size > 0 and size <= 4096

    verify_blob = """
    alias ExStorageService.Context
    alias ExStorageService.BlobStore.LocalCAS
    root = #{inspect(root)}
    hash = #{inspect(hash)}
    size = #{inspect(size)}
    {:ok, context} = Context.default()
    true = context.config.mode == :cluster and context.config.node_id == expected_id
    true = context.config.node_generation == 1
    true = context.config.replication_factor == 2 and context.config.write_quorum == 2
    false = context.config.allow_degraded_writes
    true = context.data_root == Path.join(root, expected_id)
    true = context.blob_root == Path.join([root, expected_id, "blob"])
    true = to_string(node()) == "ess_" <> String.replace(expected_id, "-", "_") <> "@127.0.0.1"
    path = LocalCAS.blob_path(hash, Context.direct_blob_store_options(context))
    ["/" | components] = Path.split(Path.dirname(path))
    {:ok, %File.Stat{type: :directory}} = File.lstat("/")
    Enum.reduce(components, "/", fn component, current ->
      next = Path.join(current, component)
      {:ok, %File.Stat{type: :directory}} = File.lstat(next)
      next
    end)
    {:ok, %File.Stat{type: :regular, size: ^size}} = File.lstat(path)
    :ok = LocalCAS.verify(hash, Context.direct_blob_store_options(context))
    %{path: path, hash: hash, size: size, node_id: expected_id}
    """

    """
    alias ExStorageService.{Context, ObjectService}
    alias ExStorageService.BlobStore.LocalCAS
    alias ExStorageService.Metadata.BlobLocations
    alias ExStorageService.Storage.Multipart
    state = #{inspect(state)}
    bucket = #{inspect(bucket)}
    hash = #{inspect(hash)}
    size = #{inspect(size)}
    case state["stage"] do
      "object" ->
        true = state["key"] == "copy-source"
        true = is_binary(state["version_id"]) and state["version_id"] != "null"
        {:ok, %{delete_marker: false, version_id: version, metadata: metadata}} =
          ObjectService.head(bucket, state["key"], state["version_id"], [])
        true = version == state["version_id"]
        true = metadata.content_hash == hash and metadata.size == size
      "part" ->
        true = state["key"] == "copied-part-0" and state["part_number"] == 1
        {:ok, upload} = Multipart.get_upload(bucket, state["upload_id"])
        true = upload.key == state["key"] and upload.status == :initiated
        {:ok, [part]} = Multipart.list_parts(bucket, state["upload_id"])
        true = part.part_number == 1 and part.hash == hash and part.size == size
    end
    {:ok, locations} = BlobLocations.list(hash)
    true = Enum.map(locations, & &1.location.node_id) == ["data-a", "data-b"]
    true = Enum.all?(locations, fn record ->
      record.location.state == :ready and record.location.node_generation == 1 and
        record.location.size == size
    end)
    {local, _} = Code.eval_string(#{inspect(verify_blob)}, expected_id: "data-a")
    {peer, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string,
      [#{inspect(verify_blob)}, [expected_id: "data-b"]], 30_000)
    true = local.hash == peer.hash and local.size == peer.size
    {:ok, context} = Context.default()
    :ok = LocalCAS.delete(hash, Context.direct_blob_store_options(context))
    {:error, :enoent} = File.lstat(local.path)
    {:ok, ^locations} = BlobLocations.list(hash)
    %{schema: 1, stage: state["stage"], hash: hash, size: size,
      removed_node: "data-a", verified_peer: "data-b", local_absent: true,
      locations_unchanged: true, replication_factor: 2, write_quorum: 2}
    """
  else
    """
    alias ExStorageService.IAM.{AccessKey, Policy, User}
    action = #{inspect(action)}
    bucket = #{inspect(bucket)}
    state = #{inspect(state)}

    case action do
      "seed" ->
        suffix = System.unique_integer([:positive, :monotonic])
        {:ok, user} = User.create_user("hardening-cluster-\#{suffix}")
        {:ok, allow} = Policy.create_policy("hardening-allow-\#{suffix}", [
          %{effect: :allow, actions: ["s3:GetObject"], resources: ["arn:aws:s3:::\#{bucket}/probe"]},
          %{effect: :allow, actions: ["s3:PutObject"], resources: ["arn:aws:s3:::\#{bucket}/destination"]}
        ])
        {:ok, deny} = Policy.create_policy("hardening-deny-\#{suffix}", [
          %{effect: :deny, actions: ["s3:GetObject"], resources: ["arn:aws:s3:::\#{bucket}/probe"]}
        ])
        :ok = Policy.attach_policy(user.id, allow.id)
        {:ok, key} = AccessKey.create_access_key(user.id)
        %{user_id: user.id, allow_id: allow.id, deny_id: deny.id,
          access_key_id: key.access_key_id, secret_access_key: key.secret_access_key}
      "attach-deny" -> :ok = Policy.attach_policy(state["user_id"], state["deny_id"]); :ok
      "remove-deny-record" -> :ok = Concord.delete("policy:\#{state["deny_id"]}"); :ok
      "corrupt-deny-record" -> :ok = Concord.put("policy:\#{state["deny_id"]}", %{statements: :invalid}); :ok
      "detach-deny" -> :ok = Policy.detach_policy(state["user_id"], state["deny_id"]); :ok
      "suspend" -> {:ok, _} = User.suspend_user(state["user_id"]); :ok
      "activate" -> {:ok, _} = User.activate_user(state["user_id"]); :ok
      "revoke" -> :ok = AccessKey.delete_key(state["access_key_id"]); :ok
      "cleanup" ->
        _ = AccessKey.delete_key(state["access_key_id"])
        _ = Concord.delete("user_policies:\#{state["user_id"]}")
        _ = Policy.delete_policy(state["allow_id"])
        _ = Policy.delete_policy(state["deny_id"])
        _ = User.delete_user(state["user_id"])
        :ok
    end
    """
  end

result =
  case :rpc.call(target, Code, :eval_string, [source], 30_000) do
    {:badrpc, reason} -> raise "cluster hardening control RPC failed: #{inspect(reason)}"
    {result, _binding} -> result
    _ -> raise "unexpected cluster hardening control result"
  end

if action == "seed" do
  File.write!(path, JSON.encode!(result))
  File.chmod!(path, 0o600)
end

if action == "remove-multipart-source-local" do
  File.write!(path <> ".result.json", JSON.encode!(result))
  File.chmod!(path <> ".result.json", 0o600)
end

IO.puts("cluster hardening control #{action}: ok")
