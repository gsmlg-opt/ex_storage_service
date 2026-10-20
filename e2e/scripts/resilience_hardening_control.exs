# Finite RPCs confined to the shell-owned disposable three-voter fixture.
[action, state_path, result_path] = System.argv()
root = Path.expand(System.fetch_env!("ESS_CLUSTER_E2E_ROOT"))
true = Regex.match?(~r|^/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}$|, root)
true = Path.expand(state_path) == Path.join(root, "resilience-state.json")
true = Path.dirname(Path.expand(result_path)) == root
true = Regex.match?(~r/^resilience-control-[a-f0-9]{32}\.json$/, Path.basename(result_path))
state = JSON.decode!(File.read!(state_path))
true = state["schema"] == 1 and state["root"] == root
true = Regex.match?(~r/^hardening-resilience-[a-f0-9]{16}$/, state["bucket"])

guard = """
alias ExStorageService.Context
root = #{inspect(root)}
{:ok, context} = Context.default()
id = context.config.node_id
true = id in ["meta", "data-a", "data-b"]
true = context.config.mode == :cluster
true = context.config.node_generation == 1
true = context.config.replication_factor == 2 and context.config.write_quorum == 2
false = context.config.allow_degraded_writes
true = context.data_root == Path.join(root, id)
true = context.blob_root == Path.join([root, id, "blob"])
true = to_string(node()) == "ess_" <> String.replace(id, "-", "_") <> "@127.0.0.1"
# Reject symlinked ancestors before any fixture file mutation.
["/" | components] = Path.split(root)
Enum.reduce(components, "/", fn component, current ->
  next = Path.join(current, component)
  {:ok, %File.Stat{type: :directory}} = File.lstat(next)
  next
end)
"""

rpc = fn target, source ->
  topology? = action in ["leader", "elected", "rejoined-data", "rejoined-all"]

  reply =
    if Node.connect(target) do
      started =
        if topology?,
          do: :rpc.call(target, Application, :started_applications, [], 5_000),
          else: :ready

      case started do
        {:badrpc, _reason} = error ->
          error

        apps when is_list(apps) ->
          if Enum.any?(apps, fn {app, _, _} -> app == :ex_storage_service end),
            do: :rpc.call(target, Code, :eval_string, [guard <> source], 45_000),
            else: {:pending, :application_starting}

        :ready ->
          :rpc.call(target, Code, :eval_string, [guard <> source], 45_000)
      end
    else
      {:badrpc, :nodedown}
    end

  case reply do
    {:pending, :application_starting} ->
      %{node_id: to_string(target), status: "unavailable"}

    {:badrpc, reason} when topology? and reason in [:nodedown, :timeout] ->
      %{node_id: to_string(target), status: "unavailable"}

    {:badrpc, {:EXIT, {{:badmatch, {:error, reason}}, _stack}}}
    when topology? and reason in [:cluster_not_ready, :view_change] ->
      %{node_id: to_string(target), status: "unavailable"}

    {:badrpc, reason} ->
      raise "resilience RPC failed: #{inspect(reason)}"

    {result, binding} when is_list(binding) ->
      result
  end
end

data_a = :"ess_data_a@127.0.0.1"
data_b = :"ess_data_b@127.0.0.1"
meta = :"ess_meta@127.0.0.1"

result =
  if action in ["leader", "elected", "rejoined-data", "rejoined-all"] do
    nodes = %{"meta" => meta, "data-a" => data_a, "data-b" => data_b}
    stopped = get_in(state, ["before_loss", "primary"])

    targets =
      if action == "elected",
        do: Map.values(Map.delete(nodes, stopped)),
        else: [meta, data_a, data_b]

    statuses =
      Enum.map(targets, fn target ->
        rpc.(target, """
        {:ok, status} = ViewstampedReplication.status("ess-cluster-e2e", id)
        {:ok, %{cluster: %{primary_id: primary}}} = Concord.status()
        %{node_id: id, status: to_string(status.status), primary: primary,
          view: status.view_number, commit: status.commit_number,
          applied: status.applied_number, configuration: inspect(status.configuration_hash),
          bootstrap: context.config.cluster_bootstrap, generation: context.config.node_generation,
          rf: context.config.replication_factor, w: context.config.write_quorum,
          degraded: context.config.allow_degraded_writes}
        """)
      end)

    [first | _] = statuses

    converged =
      first.status == "normal" and
        Enum.all?(statuses, fn
          %{status: "normal"} = status ->
            status.commit == status.applied and
              status.commit == first.commit and status.view == first.view and
              status.configuration == first.configuration and status.primary == first.primary

          _unavailable ->
            false
        end)

    ready =
      case action do
        "leader" ->
          converged and first.primary in ["meta", "data-a", "data-b"]

        "elected" ->
          converged and first.primary in Map.keys(Map.delete(nodes, stopped)) and
            first.view > state["before_loss"]["view"] and
            first.commit >= state["before_loss"]["commit"] and
            not Node.connect(Map.fetch!(nodes, stopped))

        "rejoined-data" ->
          converged and not Enum.find(statuses, &(&1.node_id == "data-b")).bootstrap

        "rejoined-all" ->
          converged and
            Enum.all?(statuses, &(not &1.bootstrap or &1.node_id not in ["data-b", stopped])) and
            first.view >= state["election"]["view"] and
            first.commit >= state["election"]["commit"]
      end

    %{
      ready: ready,
      primary: Map.get(first, :primary),
      view: Map.get(first, :view),
      commit: Map.get(first, :commit),
      statuses: statuses
    }
  else
    hash = state["hashes"]["repair"]
    size = state["sizes"]["repair"]
    true = Regex.match?(~r/^[a-f0-9]{64}$/, hash)
    true = is_integer(size) and size > 0 and size <= 4096

    peer_source =
      guard <>
        """
        true = id == "data-b"
        :ok = ExStorageService.BlobStore.LocalCAS.verify(#{inspect(hash)}, Context.direct_blob_store_options(context))
        true
        """

    reader_peer_source =
      guard <>
        """
        true = id == "data-b"
        :ok = ExStorageService.BlobStore.LocalCAS.verify(#{inspect(state["hashes"]["reader"])}, Context.direct_blob_store_options(context))
        Enum.each([repair_planner: ExStorageService.Cluster.Repair.Planner,
          outbox_dispatcher: ExStorageService.Cluster.Outbox.Dispatcher], fn {key, module} ->
          :ok = :sys.suspend(ExStorageService.Names.process(context.instance, key, module))
        end)
        true
        """

    reader_resume_source =
      guard <>
        """
        Enum.each([repair_planner: ExStorageService.Cluster.Repair.Planner,
          outbox_dispatcher: ExStorageService.Cluster.Outbox.Dispatcher], fn {key, module} ->
          :ok = :sys.resume(ExStorageService.Names.process(context.instance, key, module))
        end)
        true
        """

    gc_peer_source =
      guard <>
        """
        true = id == "data-b"
        server = ExStorageService.Names.process(context.instance, :cas_gc, ExStorageService.Storage.CasGC)
        reports = Enum.map(1..3, fn _ ->
          {:ok, report} = ExStorageService.Storage.CasGC.run_now(server,
            orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0)
          report
        end)
        Enum.each(#{inspect(Map.values(state["hashes"]))}, fn hash ->
          :ok = ExStorageService.BlobStore.LocalCAS.verify(hash, Context.direct_blob_store_options(context))
        end)
        %{sweeps: length(reports), all_fixture_checksums_verified: true}
        """

    plan_source =
      guard <>
        """
        # Trigger one public bounded shard page; normal durable workers execute it.
        {:ok, report} = ExStorageService.Cluster.Repair.Planner.run_page(context,
          #{inspect(binary_part(hash, 0, 2))}, nil, 100)
        report
        """

    verify_source =
      guard <>
        """
        true = id in ["data-a", "data-b"]
        fixtures = #{inspect(state["hashes"])}
        sizes = #{inspect(state["sizes"])}
        opts = Context.direct_blob_store_options(context)
        blobs = Enum.map(fixtures, fn {name, fixture_hash} ->
          true = Regex.match?(~r/^[a-f0-9]{64}$/, fixture_hash)
          expected_size = Map.fetch!(sizes, name)
          path = ExStorageService.BlobStore.LocalCAS.blob_path(fixture_hash, opts)
          {:ok, %File.Stat{type: :regular, size: ^expected_size}} = File.lstat(path)
          :ok = ExStorageService.BlobStore.LocalCAS.verify(fixture_hash, opts)
          %{fixture: name, hash: fixture_hash, size: expected_size, checksum_verified: true}
        end)
        %{node_id: id, actual_generation: context.config.node_generation,
          blob_root_verified: true, blobs: blobs}
        """

    source = """
    alias ExStorageService.BlobStore.LocalCAS
    alias ExStorageService.Cluster.{Membership, Scrubber}
    alias ExStorageService.Cluster.Repair.Handler
    alias ExStorageService.Metadata.{BlobLocations, JobStore, Keys, Outbox}
    alias ExStorageService.Metadata.Backend.Concord, as: Backend
    hash = #{inspect(hash)}
    size = #{inspect(size)}
    state = #{inspect(state)}
    action = #{inspect(action)}
    true = id == "data-a"
    blob_opts = Context.direct_blob_store_options(context)
    path = LocalCAS.blob_path(hash, blob_opts)
    job_id = "hardening-scrub-" <> state["bucket"]
    case action do
      "remove-read-repair" ->
        reader_hash = state["hashes"]["reader"]
        reader_size = state["sizes"]["reader"]
        :ok = LocalCAS.verify(reader_hash, blob_opts)
        {true, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string,
          [#{inspect(reader_peer_source)}], 10_000)
        Enum.each([repair_planner: ExStorageService.Cluster.Repair.Planner,
          outbox_dispatcher: ExStorageService.Cluster.Outbox.Dispatcher], fn {key, module} ->
          :ok = :sys.suspend(ExStorageService.Names.process(context.instance, key, module))
        end)
        reader_path = LocalCAS.blob_path(reader_hash, blob_opts)
        {:ok, %File.Stat{type: :regular, size: ^reader_size}} = File.lstat(reader_path)
        :ok = File.rm(reader_path)
        %{removed_local_replica: true, peer_checksum_verified: true, expected_size: reader_size}
      "resume-read-repair" ->
        {true, _} = Code.eval_string(#{inspect(reader_resume_source)})
        {true, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string,
          [#{inspect(reader_resume_source)}], 10_000)
        %{planners_resumed: true}
      "read-repair-state" ->
        reader_hash = state["hashes"]["reader"]
        reader_size = state["sizes"]["reader"]
        reader_path = LocalCAS.blob_path(reader_hash, blob_opts)
        exists = File.exists?(reader_path)
        verified = exists and LocalCAS.verify(reader_hash, blob_opts) == :ok
        {:ok, rows} = BlobLocations.list(reader_hash)
        ready = Enum.filter(rows, &(&1.location.state == :ready and
          &1.location.node_generation == 1 and &1.location.size == reader_size))
        %{ready: verified and length(ready) == 2, local_exists: exists,
          local_checksum_verified: verified, ready_count: length(ready),
          generations: Enum.map(ready, & &1.location.node_generation)}
      "gc" ->
        alias ExStorageService.BlobStore.Source
        alias ExStorageService.Metadata.OperationIntents
        alias ExStorageService.Storage.CasGC
        false = ExStorageService.InstanceConfig.worker_enabled?(context.config, :packer)
        unknown_body = "unknown-operation-protected-" <> state["bucket"]
        {:ok, staged} = LocalCAS.stage(unknown_body, blob_opts)
        intent_id = "hardening-unknown-" <> state["bucket"]
        {:ok, _} = OperationIntents.open(intent_id, staged.hash, staged.size, "data-a", 1)
        {:ok, _} = OperationIntents.transition(intent_id, :unknown)
        {:ok, _} = LocalCAS.commit(staged, blob_opts)
        reader_hash = state["hashes"]["reader"]
        {:ok, source} = LocalCAS.open(reader_hash, nil, blob_opts)
        owner = self()
        task = Task.async(fn ->
          Source.reduce(source, {:crypto.hash_init(:sha256), 0}, fn chunk, {digest, bytes} ->
            if bytes == 0 do
              send(owner, {:reader_open, self()})
              receive do :resume_reader -> :ok after 20_000 -> raise "reader resume timed out" end
            end
            {:cont, {:crypto.hash_update(digest, chunk), bytes + byte_size(chunk)}}
          end)
        end)
        receive do {:reader_open, pid} when pid == task.pid -> :ok
          after 5_000 -> raise "reader did not open its source" end
        server = ExStorageService.Names.process(context.instance, :cas_gc, CasGC)
        try do
          Enum.each(1..3, fn _ ->
            {:ok, _report} = CasGC.run_now(server,
              orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0)
          end)
          Enum.each(Map.values(state["hashes"]) ++ [staged.hash], fn fixture_hash ->
            :ok = LocalCAS.verify(fixture_hash, blob_opts)
          end)
        after
          send(task.pid, :resume_reader)
        end
        {:ok, {digest, bytes}} = Task.await(task, 5_000)
        true = bytes == state["sizes"]["reader"]
        true = Base.encode16(:crypto.hash_final(digest), case: :lower) == reader_hash
        {peer, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string,
          [#{inspect(gc_peer_source)}], 20_000)
        %{local_sweeps: 3, peer: peer, shared_keys_versions_and_parts_retained: true,
          active_reader_hash_verified: true, unknown_intent_blob_retained: true, cluster_packer_disabled: true}
      "locations" ->
        hashes = state["hashes"] |> Map.values() |> Enum.uniq()
        rows = Enum.map(hashes, fn current_hash ->
          {:ok, locations} = BlobLocations.list(current_hash)
          true = Enum.sort(Enum.map(locations, & &1.location.node_id)) == ["data-a", "data-b"]
          true = Enum.all?(locations, &(&1.location.state == :ready and &1.location.node_generation == 1))
          %{hash: current_hash, ready_generations: Enum.map(locations, & &1.location.node_generation)}
        end)
        {local_cas, _} = Code.eval_string(#{inspect(verify_source)})
        {peer_cas, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string,
          [#{inspect(verify_source)}], 10_000)
        true = local_cas.node_id == "data-a" and peer_cas.node_id == "data-b"
        true = local_cas.actual_generation == 1 and peer_cas.actual_generation == 1
        %{rf: 2, w: 2, locations: rows, local_cas: local_cas, peer_cas: peer_cas}
      "maintenance" ->
        false = ExStorageService.InstanceConfig.worker_enabled?(context.config, :scrub)
        :ok = LocalCAS.verify(hash, blob_opts)
        {:ok, %File.Stat{type: :regular, size: ^size}} = File.lstat(path)
        ["/" | components] = Path.split(Path.dirname(path))
        Enum.reduce(components, "/", fn component, current ->
          next = Path.join(current, component)
          {:ok, %File.Stat{type: :directory}} = File.lstat(next)
          next
        end)
        peer_source = #{inspect(peer_source)}
        {true, _} = :rpc.call(:"ess_data_b@127.0.0.1", Code, :eval_string, [peer_source], 10_000)
        operation_id = "hardening-maintenance-" <> state["bucket"]
        event = %{id: job_id, kind: :scrub, state: :pending,
          payload: %{hash: hash, target_node_id: "data-a", target_node_generation: 1}}
        :ok = Outbox.enqueue_legacy([event], operation_id: operation_id)
        {:ok, [job]} = Outbox.materialize(operation_id)
        {:ok, [duplicate]} = Outbox.materialize(operation_id)
        true = job.job_id == duplicate.job_id and job.fencing_token == 0
        now = System.system_time(:millisecond)
        {:ok, first} = JobStore.claim(job_id, "data-a", now, 1, owner_generation: 1)
        Process.sleep(2)
        now = System.system_time(:millisecond)
        {:ok, current} = JobStore.claim(job_id, "data-a", now, 30_000, owner_generation: 1)
        true = current.fencing_token == first.fencing_token + 1
        {:error, {:stale_lease, _}} = JobStore.renew(job_id, "data-a", first.fencing_token,
          now, 30_000, owner_generation: 1)
        {:error, {:stale_lease, _}} = JobStore.complete(job_id, "data-a", first.fencing_token,
          now, owner_generation: 1)
        {:ok, locations_before} = BlobLocations.list(hash)
        {:error, _} = Handler.perform(first, context)
        {:ok, ^locations_before} = BlobLocations.list(hash)
        {:ok, io} = File.open(path, [:read, :write, :binary, :raw])
        try do
          {:ok, <<byte>>} = :file.pread(io, 0, 1)
          :ok = :file.pwrite(io, 0, <<Bitwise.bxor(byte, 255)>>)
          :ok = :file.sync(io)
        after
          File.close(io)
        end
        {:error, :checksum_mismatch} = Scrubber.scrub(hash, blob_store_opts: blob_opts)
        :ok = Handler.perform(current, context)
        {:ok, completed} = JobStore.complete(job_id, "data-a", current.fencing_token,
          System.system_time(:millisecond), owner_generation: 1)
        true = completed.state == :completed and completed.attempts == 2
        {:ok, members} = Membership.members(context.config)
        {:ok, shard_owner} = ExStorageService.Cluster.Repair.Planner.owner(binary_part(hash, 0, 2), members)
        owner_target = case shard_owner do
          "data-a" -> :"ess_data_a@127.0.0.1"
          "data-b" -> :"ess_data_b@127.0.0.1"
        end
        {planned, _} = :rpc.call(owner_target, Code, :eval_string, [#{inspect(plan_source)}], 10_000)
        %{duplicate_job_id: true, expired_lease_takeover: true, stale_renew_complete_and_mutation_rejected: true,
          corruption_detected: true, durable_scrub_completed: true, attempts: completed.attempts,
          fencing_token: completed.fencing_token, bounded_shard_plan: planned}
      "repair-state" ->
        {:ok, locations} = BlobLocations.list(hash)
        {:ok, job} = JobStore.get(job_id)
        verified = LocalCAS.verify(hash, blob_opts) == :ok
        ready = verified and job.state == :completed and length(locations) == 2 and
          Enum.all?(locations, &(&1.location.state == :ready and &1.location.node_generation == 1))
        %{ready: ready, local_checksum_verified: verified, scrub_job_completed: job.state == :completed,
          ready_count: Enum.count(locations, &(&1.location.state == :ready))}
      "stale-generation" ->
        {:ok, %{node: member}} = Membership.member(context.config, "data-b")
        true = member.generation == 1
        key = Keys.cluster_node("data-b")
        {:ok, original} = Backend.get(key)
        backup = Path.join(root, "resilience-generation-original.etf")
        :ok = File.write(backup, :erlang.term_to_binary(original), [:exclusive])
        :ok = File.chmod(backup, 0o600)
        updated = Map.put(original.value, :generation, 2)
        {:ok, %{succeeded: true}} = Backend.transaction(%{
          compare: [{:mod_revision, key, :==, original.mod_revision}],
          success: [{:put, key, updated, %{}}], failure: []},
          idempotency_key: "hardening-generation-start-" <> state["bucket"])
        %{registered_generation: 2, actual_process_generation: 1, strict_rf: 2, strict_w: 2}
      "restore-generation" ->
        backup = Path.join(root, "resilience-generation-original.etf")
        {:ok, %File.Stat{type: :regular}} = File.lstat(backup)
        original = :erlang.binary_to_term(File.read!(backup), [:safe])
        key = Keys.cluster_node("data-b")
        {:ok, current} = Backend.get(key)
        true = current.value == Map.put(original.value, :generation, 2)
        {:ok, %{succeeded: true}} = Backend.transaction(%{
          compare: [{:mod_revision, key, :==, current.mod_revision}, {:field, key, [:generation], :==, 2}],
          success: [{:put, key, original.value, %{}}], failure: []},
          idempotency_key: "hardening-generation-restore-" <> state["bucket"])
        %{restored_generation: 1}
    end
    """

    rpc.(data_a, source)
  end

File.write!(result_path, JSON.encode!(result), [:exclusive])
File.chmod!(result_path, 0o600)
IO.puts("resilience hardening control #{action}: ok")
