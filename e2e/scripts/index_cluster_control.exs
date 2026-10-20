# Test-only finite RPC controls. Build tokens and source records stay in private roots.
[action, state_path, result_path] = System.argv()
root = Path.expand(System.fetch_env!("ESS_CLUSTER_E2E_ROOT"))
true = Regex.match?(~r|^/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}$|, root)
true = Path.expand(state_path) == Path.join(root, "index-state.json")
true = Path.dirname(Path.expand(result_path)) == root
true = Regex.match?(~r/^index-control-[a-f0-9]{32}\.json$/, Path.basename(result_path))
state = JSON.decode!(File.read!(state_path))
true = state["schema"] == 1 and state["root"] == root
true = Regex.match?(~r/^hardening-index-[a-f0-9]{16}$/, state["bucket"])
true = Regex.match?(~r/^hardening-index-other-[a-f0-9]{16}$/, state["other_bucket"])

guard = """
alias ExStorageService.Context
alias ExStorageService.Metadata
alias ExStorageService.Metadata.{Keys, ListIndex, ObjectCommit}
alias ExStorageService.Metadata.ListIndex.Backfill
alias ExStorageService.Operations.Backup
root = #{inspect(root)}
input = #{inspect(state)}
bucket = input["bucket"]
{:ok, context} = Context.default()
id = context.config.node_id
true = id in ["meta", "data-a", "data-b"]
true = context.config.mode == :cluster and context.config.node_generation == 1
true = context.config.replication_factor == 2 and context.config.write_quorum == 2
false = context.config.allow_degraded_writes
true = context.data_root == Path.join(root, id)
true = context.blob_root == Path.join([root, id, "blob"])
true = context.tmp_root == Path.join([root, id, "tmp"])
true = context.metadata_root == Path.join([root, id, "metadata"])
true = to_string(node()) == "ess_" <> String.replace(id, "-", "_") <> "@127.0.0.1"
["/" | components] = Path.split(root)
Enum.reduce(components, "/", fn component, current ->
  next = Path.join(current, component)
  {:ok, %File.Stat{type: :directory}} = File.lstat(next)
  next
end)
for path <- [context.data_root, context.blob_root, context.tmp_root, context.metadata_root],
  do: ({:ok, %File.Stat{type: :directory}} = File.lstat(path))
targets = [:"ess_meta@127.0.0.1", :"ess_data_a@127.0.0.1", :"ess_data_b@127.0.0.1"]
sources = Enum.map(targets, fn target ->
  {:ok, source} = :rpc.call(target, Context, :default, [], 5_000)
  true = source.config.erlang_node == target
  true = source.data_root == Path.join(root, source.config.node_id)
  true = source.config.mode == :cluster and source.config.node_generation == 1
  true = source.config.replication_factor == 2 and source.config.write_quorum == 2
  false = source.config.allow_degraded_writes
  source
end)
true = Enum.sort(Enum.map(sources, & &1.config.node_id)) == ["data-a", "data-b", "meta"]
bundle = Path.join(root, "index-backup")
manifest = Path.join(root, "index-manifest")
private_path = Path.join(root, "index-build-private.etf")
opts = [sources: sources, backup: bundle, batch_size: 1]
wait_file = fn wait_file, path, deadline ->
  unless File.exists?(path) do
    true = System.monotonic_time(:millisecond) < deadline
    Process.sleep(25)
    wait_file.(wait_file, path, deadline)
  end
end
"""

operation =
  case action do
    "prepare" ->
      """
      true = id == "meta"
      for source <- sources do
        {:ok, _} = :rpc.call(source.config.erlang_node, Backup, :quiesce, [source], 30_000)
        :ok = :rpc.call(source.config.erlang_node, Backup, :assert_quiesced, [source], 5_000)
      end
      # Raw legacy records are introduced only after all actual writers stop.
      # Their bytes already have two real checksum-verified acknowledged replicas.
      {:ok, donor} = ObjectCommit.get_version(bucket, "versions", hd(input["versions"]))
      donor = Map.drop(donor, [:schema, :bucket, :key, :version_id, :parent_version_id])
      retained_sources = [
        {"obj:" <> bucket <> ":legacy:colon", donor},
        {"obj_ver:" <> bucket <> ":legacy:colon:legacy-indexed", donor},
        {"obj_ver:" <> bucket <> ":legacy:colon:legacy-unindexed", donor},
        {"obj_ver_list:" <> bucket <> ":legacy:colon", ["legacy-indexed"]}
      ]
      for {key, value} <- retained_sources, do: (:ok = Concord.put(key, value))
      {:ok, previous} = ListIndex.status(bucket)
      prior_nodes = Enum.flat_map([:current, :versions, :uploads], fn kind ->
        {:ok, rows} = Concord.prefix_scan(ListIndex.namespace(bucket, previous, kind))
        rows
      end)
      {:ok, backup} = Backup.create(context, bundle, sources: sources)
      {:ok, verified} = Backup.verify(bundle)
      true = backup.backup_id == verified.backup_id
      {:ok, build} = Backfill.begin(bucket, opts)
      {:ok, _} = Backfill.capture(bucket, build.owner_token, manifest, opts)
      {:ok, checkpoint} = Backfill.resume(bucket, build.owner_token, manifest, opts)
      true = checkpoint.phase == :building and checkpoint.processed == 1 and checkpoint.offset > 0
      private = %{owner_token: build.owner_token, checkpoint: checkpoint,
        retained_sources: retained_sources, prior_nodes: prior_nodes, prior_epoch: previous.epoch,
        backup_id: backup.backup_id,
        backup_manifest_hash: :crypto.hash(:sha256, File.read!(Path.join(bundle, "manifest.json")))}
      File.write!(private_path, :erlang.term_to_binary(private), [:sync])
      File.chmod!(private_path, 0o600)
      # Once the durable per-bucket gate exists, restart normal instances/listeners.
      # The same RF/W, generation, node roles and original worker settings remain.
      for source <- sources, source.config.node_role == :data do
        case :rpc.call(source.config.erlang_node, Supervisor, :restart_child,
               [ExStorageService.Supervisor, {ExStorageService, source.instance}], 10_000) do
          {:ok, pid} when is_pid(pid) -> :ok
          {:ok, pid, _info} when is_pid(pid) -> :ok
        end
        for app <- [:ex_storage_service_cluster, :ex_storage_service_s3] do
          {:ok, _} = :rpc.call(source.config.erlang_node, Application, :ensure_all_started, [app], 10_000)
        end
      end
      %{actual_quiescence: true, verified_backup: true, backup_id: backup.backup_id,
        checkpoint_processed: checkpoint.processed, checkpoint_offset: checkpoint.offset,
        prior_epoch_retained: true, actual_source_replication_factor: 2}
      """

    "takeover" ->
      """
      private = :erlang.binary_to_term(File.read!(private_path))
      {:ok, recovered} = Backfill.status(bucket)
      for key <- [:phase, :processed, :offset, :build_digest, :source_manifest_digest],
        do: (true = recovered[key] == private.checkpoint[key])
      {:ok, replacement} = Backfill.takeover(bucket, private.owner_token, opts)
      true = replacement.owner_token != private.owner_token
      {:error, :obsolete_build_owner} = Backfill.resume(bucket, private.owner_token, manifest, opts)
      {:error, :obsolete_build_owner} = Backfill.cutover(bucket, private.owner_token, opts)
      private = Map.put(private, :owner_token, replacement.owner_token)
      File.write!(private_path, :erlang.term_to_binary(private), [:sync])
      File.chmod!(private_path, 0o600)
      %{checkpoint_survived_coordinator_sigkill: true, exact_manifest_and_offset: true,
        former_owner_resume_and_cutover_fenced: true, processed: recovered.processed}
      """

    "resume" ->
      """
      private = :erlang.binary_to_term(File.read!(private_path))
      File.write!(Path.join(root, "index-resume-active"), "active", [:exclusive])
      wait_file.(wait_file, Path.join(root, "index-writer-started"), System.monotonic_time(:millisecond) + 30_000)
      replay = fn replay, remaining ->
        true = remaining > 0
        case Backfill.resume(bucket, private.owner_token, manifest, opts) do
          {:error, :build_conflict} -> replay.(replay, remaining - 1)
          result ->
            {:ok, progress} = result
            if progress.phase == :building, do: replay.(replay, remaining - 1), else: progress
        end
      end
      replayed = replay.(replay, 1_000)
      true = replayed.phase == :validating
      # Keep the target namespace gated until every concurrent HTTP writer finishes.
      wait_file.(wait_file, Path.join(root, "index-writer-finished"), System.monotonic_time(:millisecond) + 30_000)
      validate = fn validate, remaining ->
        true = remaining > 0
        {:ok, progress} = Backfill.validate(bucket, private.owner_token, manifest, opts)
        if progress.phase == :validated, do: progress, else: validate.(validate, remaining - 1)
      end
      validated = validate.(validate, 1_000)
      {:ok, ready} = Backfill.cutover(bucket, private.owner_token, opts)
      true = ready.phase == :ready and ready.processed == replayed.processed
      {:ok, current} = ListIndex.status(bucket)
      true = current.state == :ready and current.legacy_complete and current.epoch != private.prior_epoch
      %{resumed_bounded_batches: true, processed: ready.processed,
        validated: validated.phase == :validated, atomic_cutover: true, new_epoch: true}
      """

    "revision-barrier" ->
      """
      owner = self()
      task = Task.async(fn ->
        tree_fun = fn _reader, initial ->
          unless Process.get(:index_page_barrier_released, false) do
            send(owner, {:page_paused, self()})
            receive do
              :resume_page -> Process.put(:index_page_barrier_released, true)
            after
              30_000 -> raise "owned listing barrier expired"
            end
          end
          {:ok, ListIndex.namespace(bucket, initial, :current), initial.current_root}
        end
        ListIndex.page(bucket, :current, "", limit: 100, tree_fun: tree_fun)
      end)
      receive do
        {:page_paused, pid} ->
          File.write!(Path.join(root, "index-page-paused"), "paused", [:exclusive])
          wait_file.(wait_file, Path.join(root, "index-page-release"), System.monotonic_time(:millisecond) + 30_000)
          send(pid, :resume_page)
      after
        30_000 -> raise "owned listing never reached descriptor barrier"
      end
      {:ok, page} = Task.await(task, 30_000)
      true = Enum.any?(page.entries, fn {key, _} -> key == "revision-after-snapshot" end)
      %{actual_native_page: true, mutation_after_initial_descriptor: true,
        successful_page_contains_new_revision: true}
      """

    "retained" ->
      """
      private = :erlang.binary_to_term(File.read!(private_path))
      for {key, expected} <- private.retained_sources ++ private.prior_nodes do
        {:ok, actual} = Concord.get(key, consistency: :linearizable)
        true = actual == expected
      end
      true = File.regular?(manifest)
      true = File.regular?(Path.join(bundle, "manifest.json"))
      true = :crypto.hash(:sha256, File.read!(Path.join(bundle, "manifest.json"))) == private.backup_manifest_hash
      # No fresh-VM native backup verification bypass is attempted here. The
      # immutable verified boundary is retained; A36/A37 separately require restore.
      %{v1_sources_unchanged: length(private.retained_sources),
        prior_epoch_nodes_unchanged: length(private.prior_nodes),
        verified_backup_id: private.backup_id, backup_manifest_unchanged: true,
        rollback_artifacts_retained: true}
      """

    "topology" ->
      """
      {:ok, status} = ViewstampedReplication.status("ess-cluster-e2e", id)
      {:ok, %{cluster: %{primary_id: primary}}} = Concord.status()
      %{node_id: id, status: to_string(status.status), primary: primary,
        view: status.view_number, commit: status.commit_number, applied: status.applied_number,
        configuration: Base.encode16(status.configuration_hash), bootstrap: context.config.cluster_bootstrap}
      """
  end

# Keep remote failure classifications free of terms, arguments and exception messages.
defmodule ESSIndexRPCFailureDiagnostic do
  @fixed [
    :ok,
    :error,
    true,
    false,
    nil,
    :EXIT,
    :badmatch,
    :badarg,
    :badmap,
    :case_clause,
    :function_clause,
    :timeout,
    :nodedown,
    :noconnection,
    :noproc,
    :normal,
    :shutdown,
    :invalid_build_phase,
    :invalid_validation_phase,
    :obsolete_build_owner,
    :build_conflict,
    :unresolved_build_transaction,
    :manifest_identity_changed,
    :manifest_order_violation,
    :invalid_manifest_file,
    :invalid_manifest_header,
    :invalid_manifest_frame,
    :invalid_manifest_term,
    :manifest_checksum_mismatch,
    :missing_metadata,
    :source_changed,
    :target_changed,
    :part_count_mismatch,
    :index_terminal_mismatch,
    :unexpected_index_terminal,
    :tree_count_mismatch,
    :parts_tree_count_mismatch,
    :index_validation_incomplete,
    :upload_changed,
    :part_changed,
    :normalized_version_changed,
    :head_changed,
    :invalid_part_key,
    :invalid_batch_size,
    :invalid_replication_factor,
    :enoent,
    :eacces,
    :insufficient_verified_blob_sources,
    :missing_verified_blob_metadata,
    :source_blob_probe_unavailable,
    :invalid_blob_identity,
    :invalid_context,
    :invalid_blob_descriptor,
    :blob_descriptor_conflict,
    :blob_location_conflict,
    :blob_size_conflict,
    :local_blob_corrupt,
    :local_blob_probe_failed,
    :local_blob_size_mismatch,
    :invalid_local_blob_evidence,
    :checksum_mismatch,
    :cluster_not_ready,
    :no_leader,
    :view_change,
    :not_found
  ]
  @exceptions [
    MatchError,
    CaseClauseError,
    WithClauseError,
    KeyError,
    BadMapError,
    BadArityError,
    ArgumentError,
    FunctionClauseError,
    UndefinedFunctionError,
    RuntimeError,
    ErlangError,
    Protocol.UndefinedError
  ]
  @modules [
    ExStorageService.Metadata.ListIndex.Backfill,
    ExStorageService.Metadata.ListIndex.Manifest,
    ExStorageService.Metadata.ListIndex,
    ExStorageService.Metadata.ListIndex.Radix,
    ExStorageService.Metadata.Migration,
    ExStorageService.Metadata.Backend.Concord,
    ExStorageService.Operations.Backup,
    Concord,
    Concord.Engine,
    Concord.Engine.VSR,
    Concord.StateMachine.Core,
    Enum,
    File,
    Code,
    :erl_eval,
    :lists,
    :maps,
    :rpc,
    :erpc,
    :proc_lib
  ]

  def emit(action, result) do
    {kind, reason, stack} =
      case result do
        {:badrpc, {:EXIT, {reason, stack}}} when is_list(stack) ->
          {"remote_exit", reason, stack}

        {:badrpc, reason} ->
          {"badrpc", reason, []}

        _ ->
          {"unexpected_rpc_result", result, []}
      end

    {exception, rhs} =
      case reason do
        %MatchError{term: term} -> {"MatchError", term}
        {:badmatch, term} -> {"badmatch", term}
        %{__struct__: module} when module in @exceptions -> {Atom.to_string(module), nil}
        _ -> {"other", reason}
      end

    IO.puts(
      :stderr,
      "A29_RPC_FAILURE " <>
        JSON.encode!(%{
          action:
            if(
              action in [
                "prepare",
                "resume",
                "takeover",
                "revision-barrier",
                "retained",
                "topology"
              ],
              do: action,
              else: "other"
            ),
          kind: kind,
          exception: exception,
          rhs_shape: shape(rhs, 2),
          stack: stack |> Enum.take(8) |> Enum.map(&frame/1)
        })
    )
  end

  defp shape(value, depth) when is_tuple(value) and depth > 0 do
    tag = if tuple_size(value) > 0, do: elem(value, 0), else: nil

    %{
      kind: "tuple",
      arity: if(tuple_size(value) <= 16, do: tuple_size(value), else: "over_16"),
      tag: if(tag in @fixed, do: Atom.to_string(tag), else: "redacted"),
      detail:
        if(tag == :error and tuple_size(value) == 2,
          do: shape(elem(value, 1), depth - 1),
          else: nil
        )
    }
  end

  defp shape(value, _depth) when value in @fixed,
    do: %{kind: "fixed_atom", tag: Atom.to_string(value)}

  defp shape(_value, _depth), do: %{kind: "redacted"}

  defp frame({module, function, arguments, location})
       when module in @modules and is_atom(function) and is_list(location) do
    function = Atom.to_string(function)
    arity = if is_list(arguments), do: length(Enum.take(arguments, 256)), else: arguments
    line = Keyword.get(location, :line)

    %{
      module: Atom.to_string(module),
      function: if(byte_size(function) <= 120, do: function, else: "redacted"),
      arity: if(is_integer(arity) and arity in 0..255, do: arity, else: nil),
      line: if(is_integer(line) and line in 0..1_000_000, do: line, else: nil)
    }
  end

  defp frame(_frame), do: %{module: "redacted"}
end

call = fn target ->
  if Node.connect(target) do
    case :rpc.call(target, Code, :eval_string, [guard <> operation], 60_000) do
      {result, binding} when is_list(binding) ->
        result

      {:badrpc, _} when action == "topology" ->
        %{status: "unavailable"}

      other ->
        ESSIndexRPCFailureDiagnostic.emit(action, other)
        raise "owned index RPC failed; inspect private control log"
    end
  else
    if action == "topology",
      do: %{status: "unavailable"},
      else: raise("owned index node unavailable")
  end
end

result =
  if action == "topology" do
    statuses =
      Enum.map([:"ess_meta@127.0.0.1", :"ess_data_a@127.0.0.1", :"ess_data_b@127.0.0.1"], call)

    [first | _] = statuses

    ready =
      first.status == "normal" and
        Enum.all?(statuses, fn
          %{status: "normal"} = status ->
            status.commit == status.applied and status.commit == first.commit and
              status.view == first.view and status.configuration == first.configuration and
              status.primary == first.primary and
              (status.node_id != "meta" or not status.bootstrap)

          _ ->
            false
        end)

    %{ready: ready, statuses: statuses}
  else
    call.(if action == "prepare", do: :"ess_meta@127.0.0.1", else: :"ess_data_a@127.0.0.1")
  end

File.write!(result_path, JSON.encode!(result), [:sync])
File.chmod!(result_path, 0o600)
