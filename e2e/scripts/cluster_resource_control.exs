# Resource-only RPCs restricted to the shell-owned disposable RF2/W2 fixture.
[action, state_path, index_text, result_path] = System.argv()
root = Path.expand(System.fetch_env!("ESS_CLUSTER_E2E_ROOT"))
true = Regex.match?(~r|^/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}$|, root)
true = Path.expand(state_path) == Path.join(root, "cluster-resource-state.json")
true = Path.dirname(Path.expand(result_path)) == root
true = Regex.match?(~r/^cluster-resource-control-[a-f0-9]{32}\.json$/, Path.basename(result_path))
state = JSON.decode!(File.read!(state_path))
true = state["schema"] == 1 and state["root"] == root
true = Regex.match?(~r/^hardening-resources-[a-f0-9]{16}$/, state["bucket"])
index = String.to_integer(index_text)
true = index in 0..3

guard = """
alias ExStorageService.Context
{:ok, context} = Context.default()
root = #{inspect(root)}
id = context.config.node_id
true = id in ["meta", "data-a", "data-b"]
true = context.config.mode == :cluster and context.config.node_generation == 1
true = context.config.replication_factor == 2 and context.config.write_quorum == 2
false = context.config.allow_degraded_writes
true = context.data_root == Path.join(root, id)
true = context.blob_root == Path.join([root, id, "blob"])
true = to_string(node()) == "ess_" <> String.replace(id, "-", "_") <> "@127.0.0.1"
["/" | components] = Path.split(root)
Enum.reduce(components, "/", fn component, current ->
  next = Path.join(current, component)
  {:ok, %File.Stat{type: :directory}} = File.lstat(next)
  next
end)
"""

rpc = fn target, source ->
  true = Node.connect(target)

  case :rpc.call(target, Code, :eval_string, [guard <> source], 240_000) do
    {value, binding} when is_list(binding) -> value
    {:badrpc, reason} -> raise "cluster resource RPC failed: #{inspect(reason)}"
  end
end

targets = [:"ess_meta@127.0.0.1", :"ess_data_a@127.0.0.1", :"ess_data_b@127.0.0.1"]

transport = """
defmodule ExStorageServiceCluster.ResourceAcceptanceTransport do
  alias ExStorageServiceCluster.Transport.HTTP
  def open_blob(context, _node, hash, range, opts) do
    {url, opts} = Keyword.pop!(opts, :resource_proxy)
    HTTP.open_blob(context, url, hash, range, opts)
  end
  def put_blob(context, _node, source, descriptor, opts) do
    {url, opts} = Keyword.pop!(opts, :resource_proxy)
    HTTP.put_blob(context, url, source, descriptor, opts)
  end
end
"""

result =
  if action == "setup" do
    sampler = File.read!("e2e/scripts/resource_sampler.exs")
    error_trace = File.read!("e2e/scripts/resource_error_trace.exs")

    Enum.map(targets, fn target ->
      rpc.(target, """
      if id != "meta" do
        alias ExStorageService.Cluster.Outbox.Dispatcher
        server = ExStorageService.Names.process(context.instance, :outbox_dispatcher, Dispatcher)
        :ok = :sys.suspend(server)
        true = map_size(:sys.get_state(server).running) == 0
      end
      if id == "data-a", do: Code.compile_string(#{inspect(transport)})
      Code.eval_string(#{inspect(error_trace)})
      :ok = ESSResourceErrorTrace.start(Path.join(context.data_root, "sanitized-upload-errors.jsonl"))
      sample_root = Path.join([root, "resource-samples", id])
      File.mkdir_p!(sample_root)
      System.put_env("E2E_RESOURCE_ROOT", sample_root)
      Code.eval_string(#{inspect(sampler, printable_limit: :infinity)})
      %{node: id, rf: 2, w: 2, sampling_ms: 25}
      """)
    end)
  else
    fixture = Enum.fetch!(state["fixtures"], index)
    hash = fixture["hash"]
    size = fixture["size"]
    true = Regex.match?(~r/^[a-f0-9]{64}$/, hash)
    true = size in [67_108_864, 1_073_741_824]

    source = """
    alias ExStorageService.BlobStore.{LocalCAS, Source}
    alias ExStorageService.Cluster.Membership
    alias ExStorageService.Metadata.{BlobLocations, JobStore, Outbox}
    alias ExStorageServiceCluster.Transport.HTTP
    hash = #{inspect(hash)}
    size = #{inspect(size)}
    fixture = #{inspect(fixture)}
    state = #{inspect(state)}
    action = #{inspect(action)}
    blob_opts = Context.direct_blob_store_options(context)
    case action do
      "verify" ->
        true = id in ["data-a", "data-b"]
        :ok = LocalCAS.verify(hash, blob_opts)
        {:ok, %File.Stat{type: :regular, size: ^size}} = File.lstat(LocalCAS.blob_path(hash, blob_opts))
        {:ok, locations} = BlobLocations.list(hash)
        true = Enum.sort(Enum.map(locations, & &1.location.node_id)) == ["data-a", "data-b"]
        true = Enum.all?(locations, &(&1.location.state == :ready and &1.location.node_generation == 1 and &1.location.size == size))
        %{node: id, hash: hash, size: size, ready_replicas: 2, verified: true}
      "peer-get" ->
        true = id == "data-a"
        {:ok, source} = HTTP.open_blob(context, state["proxy"], hash, nil,
          expected_size: size, expected_node_id: "data-b", expected_node_generation: 1)
        {:ok, {digest, bytes}} = Source.reduce(source, {:crypto.hash_init(:sha256), 0}, fn chunk, {digest, bytes} ->
          {:cont, {:crypto.hash_update(digest, chunk), bytes + byte_size(chunk)}}
        end)
        true = bytes == size and Base.encode16(:crypto.hash_final(digest), case: :lower) == hash
        %{bytes: bytes, hash: hash, authenticated_peer: "data-b"}
      "prepare-repair" ->
        true = id == state["target"]
        :ok = LocalCAS.verify(hash, blob_opts)
        path = LocalCAS.blob_path(hash, blob_opts)
        {:ok, %File.Stat{type: :regular, size: ^size}} = File.lstat(path)
        ["/" | components] = Path.split(Path.dirname(path))
        Enum.reduce(components, "/", fn component, current ->
          next = Path.join(current, component)
          {:ok, %File.Stat{type: :directory}} = File.lstat(next)
          next
        end)
        # Only generated fixture hashes in the private CAS are removed.
        :ok = File.rm(path)
        {:ok, member} = Membership.member(context.config, id)
        :ok = BlobLocations.mark_unhealthy(hash, id, :unavailable, :resource_acceptance,
          node_record: member, expected_generation: 1)
        %{target: id, hash: hash, missing: true}
      kind when kind in ["repair", "external"] ->
        true = id == "data-a"
        job_id = "resource-" <> state["group"] <> "-" <> Integer.to_string(#{index})
        {job_kind, payload} = if kind == "repair" do
          {:repair_blob, %{hash: hash, target_node_id: state["target"], source_node_ids: [state["source"]]}}
        else
          {:ok, %{metadata: object, delete_marker: false}} = ExStorageService.ObjectService.head(
            state["bucket"], fixture["key"], nil, context: context)
          {:cross_cluster_put, %{bucket: state["bucket"], key: fixture["key"], object: object,
            replica: %{endpoint: state["sink"], access_key: state["sink_token"]}}}
        end
        event = %{id: job_id, kind: job_kind, state: :pending, payload: payload}
        :ok = Outbox.enqueue_legacy([event], operation_id: job_id)
        {:ok, [_]} = Outbox.materialize(job_id)
        {:ok, job} = JobStore.claim(job_id, "data-a", System.system_time(:millisecond), 180_000,
          owner_generation: 1)
        if kind == "repair" do
          # Route the real authenticated adapter through a bounded paced proxy.
          :ok = ExStorageService.Cluster.Repair.Handler.perform(job, context,
            transport: ExStorageServiceCluster.ResourceAcceptanceTransport,
            transport_opts: [resource_proxy: state["proxy"]])
        else
          :ok = ExStorageService.CrossClusterReplication.Handler.perform(job, context)
        end
        {:ok, completed} = JobStore.complete(job_id, "data-a", job.fencing_token,
          System.system_time(:millisecond), owner_generation: 1)
        true = completed.state == :completed and completed.attempts == 1
        %{job_id: job_id, kind: to_string(job_kind), completed: true, fenced: true, hash: hash, size: size}
    end
    """

    case action do
      "verify" ->
        Enum.map(Enum.drop(targets, 1), &rpc.(&1, source))

      "prepare-repair" ->
        target =
          if state["target"] == "data-a", do: Enum.at(targets, 1), else: Enum.at(targets, 2)

        rpc.(target, source)

      _ ->
        rpc.(Enum.at(targets, 1), source)
    end
  end

File.write!(result_path, JSON.encode!(result), [:exclusive])
File.chmod!(result_path, 0o600)
IO.puts("cluster resource control #{action}: ok")
