# Supplemental C6 diagnostic only. This is not an authoritative acceptance lane.
# Run exercise and recover in separate fresh BEAM invocations, with the same
# exclusively owned disposable ESS_* roots. Never run beside acceptance workloads.
defmodule ESSC6RealConcordStageProbe do
  alias ExStorageService.{Context, InstanceConfig, Metadata, ObjectService}
  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Metadata.{Keys, ObjectCommit, OperationIntents}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.Models.OperationIntent
  alias ExStorageService.Storage.StagingRecovery

  @bucket "c6-real-concord-cleanup"
  @other_id "c6-unrelated-protected-stage"
  @existing_body "unrelated ready CAS bytes"
  @cases [
    {"sync-enospc", :sync, :enospc, :aborted},
    {"rename-eacces", :rename, :eacces, :aborted},
    {"sync-timeout", :sync, :timeout, :unknown}
  ]

  def run([mode, root]) when mode in ["exercise", "recover"] do
    root = Path.expand(root)
    true = Regex.match?(~r|\A/tmp/ess-c6-real-concord-stage-[A-Za-z0-9_-]+\z|, root)
    ^root = System.fetch_env!("ESS_DATA_ROOT")
    false = Application.started_applications() |> Enum.any?(&(elem(&1, 0) == :concord))

    if mode == "exercise" do
      :ok = File.mkdir(root)
    else
      %{type: :directory} = File.lstat!(root)
      true = File.regular?(Path.join(root, "exercise.json"))
    end

    context = start_core!(root)
    blob_opts = Context.blob_store_options(context)

    result =
      case mode do
        "exercise" -> exercise(context, blob_opts)
        "recover" -> recover(root, blob_opts)
      end

    receipt = Map.merge(result, %{
      mode: mode,
      supplemental_only: true,
      acceptance: false,
      source_sha: "69371bc0c2d00f63fd8f3ff0edc751c28a02d441",
      beam_os_pid: System.pid(),
      beam_proc_start_ticks: proc_start_ticks!(),
      backend: "locked Concord file-backed singleton; actual CAS transactions",
      command_version: 0,
      bootstrap: false,
      metadata_adapters: "production defaults; no metadata/intent stubs",
      automatic_workers: "disabled in this disposable diagnostic",
      proof_limits: [
        "Injected raw LocalCAS failures prove cleanup mechanics, not the original resource failure cause",
        "Normal-exit fresh-BEAM recovery proves file-backed persistence, not crash durability",
        "No claim of A30/full acceptance, restart sweeping, unlink-failure cleanup, or arbitrary-stage reclamation",
        "An aborted intent remains mutable and is not a generic sweep fence"
      ]
    })

    path = Path.join(root, mode <> ".json")
    File.write!(path, JSON.encode!(receipt) <> "\n", [:write, :exclusive])
    IO.puts(JSON.encode!(%{mode: mode, receipt: path, status: "passed", acceptance: false}))
  end

  defp start_core!(root) do
    workers = Map.new(InstanceConfig.worker_defaults(), fn {worker, _} -> {worker, false} end)
    configured = Application.fetch_env!(:ex_storage_service, :instance_config)
    Application.put_env(:ex_storage_service, :instance_config,
      Keyword.merge(configured, workers: workers, auto_start: false))
    {:ok, config} = InstanceConfig.from_application_env()
    false = config.auto_start
    false = config.web_enabled
    false = config.public_s3_enabled
    :standalone = config.mode
    true = Enum.all?(config.workers, fn {_worker, enabled} -> enabled == false end)
    ^root = config.data_root
    Enum.each([blob_root: "blob", tmp_root: "tmp", ra_root: "ra"], fn {key, suffix} ->
      true = Map.fetch!(config, key) == Path.join(root, suffix)
    end)
    metadata_root = Path.join(root, "metadata")
    ^metadata_root = Application.fetch_env!(:concord, :data_dir)
    ^metadata_root = config.metadata_root
    vsr = Application.fetch_env!(:concord, :vsr)
    0 = Keyword.fetch!(vsr, :command_version)
    :file = Keyword.fetch!(vsr, :storage)
    false = Keyword.fetch!(vsr, :bootstrap)
    [_] = Keyword.fetch!(vsr, :members)
    {:ok, _} = Application.ensure_all_started(:ex_storage_service)
    {:ok, nil} = Backend.get("c6-probe-never-written", consistency: :linearizable)
    {:ok, context} = Context.default()
    context
  end

  defp exercise(context, blob_opts) do
    :ok = Metadata.create_bucket(@bucket)
    {:ok, staged} = LocalCAS.stage(@existing_body, blob_opts)
    {:ok, ready} = LocalCAS.commit(staged, blob_opts)

    # Same content hash as a failed writer, but independently owned nonce path
    # and genuine pending intent. Aborting the writer must not sweep this stage.
    other_body = body("sync-enospc")
    {:ok, other} = LocalCAS.stage(other_body, blob_opts)
    {:ok, other} = LocalCAS.protect(other, {:hosted, @other_id}, blob_opts)
    {:ok, %{state: :pending}} = OperationIntents.open(@other_id, other.hash, other.size,
      context.config.node_id, context.config.node_generation)
    true = StagingRecovery.protected?(other.path)

    cases = Enum.map(@cases, fn {id, phase, reason, state} ->
      baseline = paths(blob_opts)
      hook = fn ->
        pending = intent!(id, :pending)
        [owned_path] = paths(blob_opts) -- baseline
        true = StagingRecovery.protected?(owned_path)
        true = File.read!(owned_path) == body(id)
        Process.put({__MODULE__, id}, %{path: owned_path, pending_revision: pending.mod_revision})
        {:error, reason}
      end

      opts = [context: context, side_effects: false, metadata_opts: [operation_id: id],
        blob_store_opts: [faults: %{phase => hook}]]
      expected_error = {:error, {phase, reason}}
      ^expected_error = ObjectService.put(@bucket, id, body(id), "text/plain", %{}, opts)
      %{path: owned_path, pending_revision: pending_revision} = Process.delete({__MODULE__, id})
      stored = intent!(id, state)
      true = stored.mod_revision > pending_revision
      {:error, :not_found} = ObjectCommit.get_head(@bucket, id)
      false = File.exists?(LocalCAS.blob_path(hash(body(id)), blob_opts))

      if state == :aborted do
        false = File.exists?(owned_path)
        ^baseline = paths(blob_opts)
      else
        true = File.read!(owned_path) == body(id)
        true = paths(blob_opts) == Enum.sort([owned_path | baseline])
      end

      true = File.read!(other.path) == other_body
      true = File.read!(ready.path) == @existing_body
      intent!(@other_id, :pending)
      %{operation_id: id, injected_phase: Atom.to_string(phase), original_error_preserved: inspect(expected_error),
        intent_state: Atom.to_string(state), pending_revision: pending_revision, final_revision: stored.mod_revision,
        owned_path: owned_path, owned_stage_retained: state == :unknown,
        object_unpublished: true, unrelated_stage_and_cas_unchanged: true}
    end)

    %{cases: cases, unrelated_stage: other.path, existing_cas: ready.path,
      final_stages: paths(blob_opts)}
  end

  defp recover(root, blob_opts) do
    original = root |> Path.join("exercise.json") |> File.read!() |> JSON.decode!()
    false = original["beam_os_pid"] == System.pid()
    {:ok, _} = Metadata.get_bucket(@bucket)

    Enum.each(@cases, fn {id, _phase, _reason, state} ->
      prior = Enum.find(original["cases"], &(&1["operation_id"] == id))
      %{mod_revision: revision} = intent!(id, state)
      true = revision == prior["final_revision"]
      {:error, :not_found} = ObjectCommit.get_head(@bucket, id)
      false = File.exists?(LocalCAS.blob_path(hash(body(id)), blob_opts))
      if state == :unknown do
        true = File.read!(prior["owned_path"]) == body(id)
      else
        false = File.exists?(prior["owned_path"])
      end
    end)

    intent!(@other_id, :pending)
    true = File.read!(original["unrelated_stage"]) == body("sync-enospc")
    true = File.read!(original["existing_cas"]) == @existing_body
    true = paths(blob_opts) == original["final_stages"]
    %{recovered_from_fresh_beam: true, original_beam_os_pid: original["beam_os_pid"],
      persisted_intent_states: %{sync_enospc: "aborted", rename_eacces: "aborted",
        sync_timeout: "unknown", unrelated: "pending"},
      exact_revisions_preserved: true, stages_and_cas_unchanged: true,
      no_failed_object_publication: true}
  end

  defp intent!(id, state) do
    {:ok, %{value: value} = record} = Backend.get(Keys.operation_intent(id), consistency: :linearizable)
    {:ok, %OperationIntent{operation_id: ^id, state: ^state}} = OperationIntent.cast(value)
    record
  end

  defp paths(opts), do: Path.wildcard(Path.join(Keyword.fetch!(opts, :tmp_dir), "*")) |> Enum.sort()
  defp proc_start_ticks! do
    [_, fields] = File.read!("/proc/self/stat") |> String.split(") ", parts: 2)
    fields |> String.split() |> Enum.fetch!(19)
  end

  defp body(id), do: "bounded real Concord cleanup fixture: " <> id
  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end

ESSC6RealConcordStageProbe.run(System.argv())
