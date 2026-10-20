# Test-only internal RF/W transport boundary. No production configuration switches.
[action, state_path, result_path] = System.argv()
root = Path.expand(System.fetch_env!("ESS_CLUSTER_E2E_ROOT"))
true = Regex.match?(~r|^/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}$|, root)
true = Path.expand(state_path) == Path.join(root, "replication-crash-state.json")
true = Path.dirname(Path.expand(result_path)) == root
true = Regex.match?(~r/^replication-control-[a-f0-9]{32}\.json$/, Path.basename(result_path))
state = JSON.decode!(File.read!(state_path))
true = state["schema"] == 1 and state["root"] == root
true = Regex.match?(~r/^replication-crash-[a-f0-9]{16}$/, state["bucket"])
true = Regex.match?(~r/^[a-f0-9]{64}$/, state["hash"])
true = state["size"] > 0 and state["size"] <= 1024 * 1024
true = state["operation_id"] == state["bucket"] <> "-operation"

guard = """
alias ExStorageService.{Context, ObjectService}
alias ExStorageService.BlobStore.LocalCAS
alias ExStorageService.Metadata.{BlobLocations, Keys}
alias ExStorageService.Metadata.Backend.Concord, as: Backend
{:ok, context} = Context.default()
root = #{inspect(root)}
state = #{inspect(state, limit: :infinity, printable_limit: :infinity)}
id = context.config.node_id
true = id in ["meta", "data-a", "data-b"]
true = context.config.mode == :cluster and context.config.node_generation == 1
true = context.config.replication_factor == 2 and context.config.write_quorum == 2
false = context.config.allow_degraded_writes
true = context.data_root == Path.join(root, id)
true = context.blob_root == Path.join([root, id, "blob"])
true = to_string(node()) == "ess_" <> String.replace(id, "-", "_") <> "@127.0.0.1"
["/" | components] = Path.split(context.blob_root)
Enum.reduce(components, "/", fn component, current ->
  next = Path.join(current, component)
  {:ok, %File.Stat{type: :directory}} = File.lstat(next)
  next
end)
"""

rpc = fn target, source ->
  reply =
    if Node.connect(target),
      do: :rpc.call(target, Code, :eval_string, [guard <> source], 45_000),
      else: {:badrpc, :nodedown}

  case reply do
    {:badrpc, reason} when action == "crash" ->
      {:badrpc, reason}

    {:badrpc, reason} when action == "rejoined" and reason in [:nodedown, :timeout] ->
      {%{id: to_string(target), status: "unavailable"}, []}

    {:badrpc, reason} ->
      raise "replication RPC failed: #{inspect(reason)}"

    {%{} = result, binding} when is_list(binding) ->
      {result, binding}
  end
end

result =
  case action do
    "crash" ->
      source = """
      true = id == "data-b"
      defmodule ExStorageService.Acceptance.ReplicationCrashTransport do
        alias ExStorageServiceCluster.Transport.HTTP
        defdelegate head_blob(context, node, hash, opts), to: HTTP
        def put_blob(context, node, source, descriptor, opts) do
          {:ok, ack} = HTTP.put_blob(context, node, source, descriptor, opts)
          true = node.node_id == "data-a" and ack.node_id == "data-a"
          true = ack.node_generation == 1 and ack.hash == descriptor.hash and ack.size == descriptor.size
          witness = Keyword.fetch!(opts, :acceptance_witness)
          {:ok, io} = File.open(witness, [:write, :raw, :binary, :exclusive])
          :ok = :file.write(io, JSON.encode!(%{boundary: "remote_durable_ack", peer: ack.node_id,
            generation: ack.node_generation, hash: ack.hash, size: ack.size, pid: System.pid()}))
          :ok = :file.sync(io)
          :ok = File.close(io)
          System.halt(137)
        end
      end
      body = Base.decode64!(state["body_base64"])
      true = byte_size(body) == state["size"]
      true = Base.encode16(:crypto.hash(:sha256, body), case: :lower) == state["hash"]
      ObjectService.put(state["bucket"], "uncertain", body, "application/octet-stream", %{"fault" => "replication"},
        context: context, metadata_opts: [operation_id: state["operation_id"]], side_effects: false,
        transport: ExStorageService.Acceptance.ReplicationCrashTransport,
        transport_opts: [acceptance_witness: Path.join(root, "replication-crash-witness.json")])
      raise "remote replication crash boundary was not reached"
      """

      {:badrpc, :nodedown} = rpc.(:"ess_data_b@127.0.0.1", source)
      %{crashed_during_replication: true}

    "resume" ->
      {result, _} =
        rpc.(:"ess_data_b@127.0.0.1", """
        true = id == "data-b" and not context.config.cluster_bootstrap
        body = Base.decode64!(state["body_base64"])
        {:ok, result} = ObjectService.put(state["bucket"], "uncertain", body, "application/octet-stream", %{"fault" => "replication"},
          context: context, metadata_opts: [operation_id: state["operation_id"]], side_effects: false)
        %{version_id: result.version_id, hash: result.metadata.content_hash, size: result.metadata.size}
        """)

      result

    "rejoined" ->
      statuses =
        for id <- ["meta", "data-a", "data-b"] do
          target = String.to_atom("ess_" <> String.replace(id, "-", "_") <> "@127.0.0.1")

          {status, _} =
            rpc.(target, """
            {:ok, status} = ViewstampedReplication.status("ess-cluster-e2e", id)
            {:ok, %{cluster: %{primary_id: primary}}} = Concord.status()
            %{id: id, status: to_string(status.status), commit: status.commit_number,
              applied: status.applied_number, view: status.view_number, primary: primary,
              configuration: inspect(status.configuration_hash), bootstrap: context.config.cluster_bootstrap}
            """)

          status
        end

      [first | _] = statuses

      ready =
        first.status == "normal" and
          Enum.all?(statuses, fn
            %{status: "normal"} = s ->
              s.commit == s.applied and s.commit == first.commit and s.view == first.view and
                s.configuration == first.configuration and s.primary == first.primary and
                (s.id != "data-b" or not s.bootstrap)

            _unavailable ->
              false
          end)

      %{ready: ready, statuses: statuses}

    inspect when inspect in ["inspect-peer", "inspect-origin"] ->
      target =
        if inspect == "inspect-peer", do: :"ess_data_a@127.0.0.1", else: :"ess_data_b@127.0.0.1"

      {result, _} =
        rpc.(target, """
        object = case ObjectService.head(state["bucket"], "uncertain", nil, context: context) do
          {:ok, result} -> %{exists: true, version_id: result.version_id, hash: result.metadata.content_hash, size: result.metadata.size}
          {:error, :object_not_found} -> %{exists: false}
        end
        {:ok, %{value: intent}} = Backend.get(Keys.operation_intent(state["operation_id"]), consistency: :linearizable)
        {:ok, locations} = BlobLocations.list(state["hash"])
        path = LocalCAS.blob_path(state["hash"], Context.direct_blob_store_options(context))
        blob = case File.lstat(path) do
          {:ok, %File.Stat{type: :regular, size: size}} ->
            :ok = LocalCAS.verify(state["hash"], Context.direct_blob_store_options(context))
            %{exists: true, verified: true, size: size}
          {:error, :enoent} -> %{exists: false}
        end
        %{node: id, intent: to_string(intent.state), intent_hash: intent.hash,
          object: object, blob: blob, rf: context.config.replication_factor, w: context.config.write_quorum,
          locations: Enum.map(locations, fn row -> %{node: row.location.node_id,
            generation: row.location.node_generation, state: to_string(row.location.state), size: row.location.size} end)}
        """)

      result
  end

File.write!(result_path, JSON.encode!(result))
File.chmod!(result_path, 0o600)
IO.puts("replication crash control #{action}: ok")
