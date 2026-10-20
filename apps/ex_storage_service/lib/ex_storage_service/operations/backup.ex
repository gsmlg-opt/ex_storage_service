defmodule ExStorageService.Operations.Backup do
  @moduledoc """
  Quiesced Concord logical backup and isolated empty-root recovery.

  Call `quiesce/1` on every source, then `create/3`. On each fresh target call
  `prepare/3` before starting its BEAM. Start with auto-start and all listeners
  disabled, `restore/3` once on the primary target, and `install/3` on the other
  targets. `verify_restore/3` and `activate/3` are explicit separate gates.

  Blobs are copied and hashed in bounded chunks. The existing Concord logical
  snapshot format materializes metadata in memory. A gateway backup does not
  include upstream-only object bodies. Actual master keys are provisioned
  separately and never included in a bundle or returned report.
  """

  alias ExStorageService.{Context, InstanceConfig, Names}
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations, Keys}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Operations.BackupManifest, as: Manifest

  @marker ".ess-recovery.json"
  @workers [
    :multipart_gc,
    :content_gc,
    :cas_gc,
    :packer,
    :lifecycle,
    :cross_cluster_replication,
    :repair,
    :scrub
  ]
  @listener_apps [:ex_storage_service_s3, :ex_storage_service_web, :ex_storage_service_cluster]
  @ambiguous [:timeout, :unknown, :cluster_not_ready, :no_leader]

  @doc "Stops this node's listeners, storage workers and notification tasks; metadata stays up."
  def quiesce(%Context{} = context) do
    with :ok <- stop_listeners(),
         :ok <- stop_instance(context),
         :ok <- stop_notifications(),
         :ok <- assert_quiesced(context) do
      {:ok,
       %{
         node_id: context.config.node_id,
         generation: context.config.node_generation,
         quiesced_at: now()
       }}
    end
  end

  @doc "Checks actual processes, rather than treating a data-plane flag as quiescence."
  def assert_quiesced(%Context{} = context) do
    no_instance = is_nil(instance_pid(context))
    no_listeners = Enum.all?(@listener_apps, &(&1 not in started_apps()))
    no_notifications = notification_tasks() == []

    if no_instance and no_listeners and no_notifications, do: :ok, else: {:error, :not_quiesced}
  catch
    :exit, _ -> {:error, :not_quiesced}
  end

  @doc "Creates a private, atomically published backup. All source contexts must be quiesced."
  def create(%Context{} = context, destination, opts \\ []) do
    sources = Keyword.get(opts, :sources, [context])
    stage = Path.expand(destination) <> ".partial-" <> Manifest.random_id()
    started = now()

    with :ok <- valid_sources(context, sources),
         :ok <- absent(destination),
         :ok <- outside_sources(destination, sources),
         :ok <- each_source(sources, :assert_quiesced),
         {:ok, key} <- master_key(opts),
         :ok <- Manifest.directory(stage),
         :ok <- File.chmod(stage, 0o700),
         {:ok, snapshot} <- backup_module(opts).create(backup_opts(opts, stage)),
         :ok <- File.chmod(snapshot, 0o600),
         :ok <- Manifest.sync_file(snapshot),
         {:ok, :valid} <- backup_module(opts).verify(snapshot),
         {:ok, _snapshot, entries} <- Manifest.read_snapshot(snapshot),
         :ok <- Manifest.validate_boundary(entries, context.config.mode),
         {:ok, refs} <- Manifest.references(entries),
         {:ok, artifacts} <- capture_sources(stage, sources, entries),
         {:ok, metadata_artifact} <- artifact(stage, snapshot, "metadata", nil, nil, nil),
         manifest =
           build_manifest(context, sources, started, refs, [metadata_artifact | artifacts]),
         manifest =
           Map.put(manifest, "master_key_sha256", Manifest.hex(:crypto.hash(:sha256, key))),
         :ok <- write_manifest(stage, manifest),
         {:ok, _verified, _} <- Manifest.verify(stage, key),
         :ok <- each_source(sources, :assert_quiesced),
         :ok <- File.rename(stage, destination),
         :ok <- Manifest.sync_directory(Path.dirname(Path.expand(destination))) do
      {:ok,
       %{
         backup_id: manifest["backup_id"],
         path: Path.expand(destination),
         consistency_point: manifest["consistency_point"],
         artifacts: length(artifacts) + 1
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :backup_verification_failed}
    end
  end

  @doc "Verifies artifact checksums, metadata reference coverage, packs and required master key."
  def verify(bundle, opts \\ []) do
    with {:ok, key} <- master_key(opts),
         {:ok, manifest, _entries} <- Manifest.verify(bundle, key) do
      {:ok,
       %{
         backup_id: manifest["backup_id"],
         artifacts: length(manifest["artifacts"]),
         referenced_blobs: map_size(manifest["references"]),
         consistency_point: manifest["consistency_point"],
         upstream_only_bodies_included: false
       }}
    end
  end

  @doc "Offline preflight: refuses nonempty roots and persists quarantine before the target starts."
  def prepare(%Context{} = context, bundle, opts \\ []) do
    with {:ok, key} <- master_key(opts),
         {:ok, manifest, _} <- Manifest.verify(bundle, key),
         :ok <- target_mapping(context, manifest),
         :ok <- empty_roots(context),
         :ok <- Manifest.directory(context.data_root),
         marker = %{
           "schema" => 1,
           "status" => "prepared",
           "backup_id" => manifest["backup_id"],
           "roots" => roots(context),
           "node_id" => context.config.node_id,
           "generation" => context.config.node_generation,
           "prepared_at" => now()
         },
         :ok <- write_marker(context, marker) do
      {:ok, %{backup_id: manifest["backup_id"], status: :quarantined}}
    end
  end

  @doc "Checks a persisted recovery marker before the normal application supervision starts."
  def startup_guard(%InstanceConfig{} = config) do
    context = Context.new(config)

    case read_marker(context) do
      {:error, :enoent} ->
        :ok

      {:ok, marker} ->
        with :ok <- marker_binding(context, marker) do
          if marker["status"] == "activated" do
            approved = marker["workers"]

            if is_map(approved) and
                 Enum.all?(@workers, fn worker ->
                   config.workers[worker] == approved[Atom.to_string(worker)]
                 end),
               do: :ok,
               else: {:error, :recovery_quarantined}
          else
            if not config.auto_start and not config.public_s3_enabled and
                 not config.web_enabled and not config.cluster_data_plane_enabled,
               do: :ok,
               else: {:error, :recovery_quarantined}
          end
        end

      {:error, reason} ->
        {:error, {:invalid_recovery_marker, reason}}
    end
  end

  def quarantined?(%InstanceConfig{} = config) do
    case read_marker(Context.new(config)) do
      {:error, :enoent} -> false
      {:ok, %{"status" => "activated"}} -> false
      _ -> true
    end
  end

  @doc "Returns an explicit all-workers-disabled config for isolated validation only."
  def validation_config(%InstanceConfig{} = config) do
    %{
      config
      | workers: Map.new(@workers, &{&1, false}),
        auto_start: false,
        public_s3_enabled: false,
        web_enabled: false,
        cluster_data_plane_enabled: false
    }
  end

  @doc "Imports logical metadata once. Only a prepared/importing empty recovery target may import."
  def restore(%Context{} = context, bundle, opts \\ []) do
    with :ok <- current_restore_engine(opts),
         :ok <- assert_quiesced(context),
         {:ok, manifest, entries, marker} <- recovery_input(context, bundle, opts),
         true <- marker["status"] in ["prepared", "importing"],
         :ok <- initial_metadata_guard(marker, opts),
         :ok <- write_marker(context, Map.put(marker, "status", "importing")),
         {:ok, snapshot} <- Manifest.safe_path(bundle, manifest["metadata"]),
         :ok <-
           backup_module(opts).restore(
             snapshot,
             Keyword.merge(Keyword.take(opts, [:engine, :timeout]), force: true, verify: true)
           ),
         :ok <- fence_jobs(entries, manifest["backup_id"], opts),
         :ok <- write_marker(context, Map.put(marker, "status", "imported")) do
      install(context, bundle, opts)
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_restore_phase}
    end
  end

  @doc "Installs verified data on a fresh target voter after the one replicated metadata import."
  def install(%Context{} = context, bundle, opts \\ []) do
    with :ok <- assert_quiesced(context),
         {:ok, manifest, entries, marker} <- recovery_input(context, bundle, opts),
         true <- marker["status"] in ["prepared", "imported", "installed", "verified"],
         :ok <- ensure_imported(manifest["backup_id"], opts),
         :ok <- install_artifacts(context, bundle, manifest),
         :ok <- register_target(context, opts),
         :ok <- rebase_journals(context, manifest, entries, opts),
         :ok <- publish_locations(context, manifest, opts),
         :ok <- write_marker(context, Map.put(marker, "status", "installed")) do
      {:ok,
       %{
         backup_id: manifest["backup_id"],
         status: :installed,
         node_id: context.config.node_id,
         generation: context.config.node_generation
       }}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_restore_phase}
    end
  end

  @doc "Rechecks installed bytes and restored metadata without enabling any workers."
  def verify_restore(%Context{} = context, bundle, opts \\ []) do
    with :ok <- assert_quiesced(context),
         {:ok, manifest, entries, marker} <- recovery_input(context, bundle, opts),
         true <- marker["status"] in ["installed", "verified"],
         :ok <- verify_installed(context, bundle, manifest),
         :ok <- verify_metadata(context, manifest, entries, opts),
         :ok <- verify_cluster(context, manifest, opts),
         report = %{
           backup_id: manifest["backup_id"],
           node_id: context.config.node_id,
           generation: context.config.node_generation,
           status: :verified,
           verified_at: now(),
           consistency_point: manifest["consistency_point"],
           prepared_at: marker["prepared_at"]
         },
         :ok <-
           write_marker(
             context,
             Map.merge(marker, %{"status" => "verified", "verified_at" => report.verified_at})
           ) do
      {:ok, report}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_restore_phase}
    end
  end

  @doc "Explicit activation after fresh verification; requires a complete reviewed worker map."
  def activate(%Context{} = context, bundle, opts \\ []) do
    with {:ok, workers} <- approved_workers(opts),
         {:ok, _report} <- verify_restore(context, bundle, opts),
         {:ok, manifest, entries, marker} <- recovery_input(context, bundle, opts),
         :ok <- activation_safety(entries, workers, opts),
         :ok <-
           write_marker(
             context,
             Map.merge(marker, %{
               "status" => "activated",
               "activated_at" => now(),
               "workers" => Map.new(workers, fn {k, v} -> {Atom.to_string(k), v} end)
             })
           ) do
      {:ok, %{backup_id: manifest["backup_id"], status: :activated, workers: workers}}
    end
  end

  defp capture_sources(stage, sources, entries) do
    Enum.reduce_while(sources, {:ok, []}, fn source, {:ok, artifacts} ->
      case capture_source(stage, source, entries) do
        {:ok, captured} -> {:cont, {:ok, captured ++ artifacts}}
        error -> {:halt, error}
      end
    end)
  end

  defp capture_source(_stage, %{config: %{node_role: :metadata}}, _entries), do: {:ok, []}

  defp capture_source(stage, source, entries) do
    # File roots are explicitly operator-mounted, read-only copies or the local
    # quiesced roots. Remote runtime quiescence is checked separately by RPC.
    base = "nodes/" <> Base.url_encode64(source.config.node_id, padding: false)
    root_specs = [{"blob", source.blob_root}, {"tmp", source.tmp_root}]

    with {:ok, captured} <-
           Enum.reduce_while(root_specs, {:ok, []}, fn {kind, root}, {:ok, acc} ->
             excluded = if kind == "blob", do: [source.tmp_root], else: []

             case capture_root(stage, base, kind, root, excluded) do
               {:ok, next} -> {:cont, {:ok, next ++ acc}}
               error -> {:halt, error}
             end
           end),
         {:ok, additional} <- capture_paths(stage, base, source, entries) do
      {:ok, Enum.map(additional ++ captured, &Map.put(&1, "node_id", source.config.node_id))}
    end
  end

  defp capture_root(stage, base, kind, root, excluded) do
    case Manifest.files(root, excluded) do
      {:error, :enoent} ->
        {:ok, []}

      {:ok, files} ->
        Enum.reduce_while(files, {:ok, []}, fn relative, {:ok, acc} ->
          source = Path.join(root, relative)
          destination = Path.join([stage, base, kind, relative])
          artifact_kind = file_kind(kind, relative)

          with :ok <- Manifest.copy(source, destination),
               {:ok, entry} <- artifact(stage, destination, artifact_kind, kind, relative, source) do
            {:cont, {:ok, [entry | acc]}}
          else
            error -> {:halt, error}
          end
        end)

      error ->
        error
    end
  end

  defp capture_paths(stage, base, source, entries) do
    paths =
      Enum.flat_map(entries, fn {key, value} ->
        cond do
          is_map(value) and String.starts_with?(key, "ess:gateway-write:") ->
            if contained?(value.path, source.tmp_root), do: [{"journal", value.path}], else: []

          is_map(value) and String.starts_with?(key, ["obj:", "obj_ver:"]) and
              is_binary(value[:physical_path]) ->
            path = Path.expand(value.physical_path, source.data_root)
            if not contained?(path, source.blob_root), do: [{"legacy", path}], else: []

          true ->
            []
        end
      end)
      |> Enum.uniq()

    Enum.reduce_while(paths, {:ok, []}, fn {kind, source_path}, {:ok, acc} ->
      with true <-
             contained?(source_path, source.data_root) or contained?(source_path, source.tmp_root),
           relative =
             if(kind == "journal",
               do: "recovery-journals/" <> Manifest.hex(:crypto.hash(:sha256, source_path)),
               else: Path.relative_to(source_path, source.data_root)
             ),
           destination = Path.join([stage, base, kind, relative]),
           :ok <- Manifest.copy(source_path, destination),
           {:ok, entry} <-
             artifact(
               stage,
               destination,
               kind,
               if(kind == "journal", do: "tmp", else: "data"),
               relative,
               source_path
             ) do
        {:cont, {:ok, [entry | acc]}}
      else
        {:error, _} = error -> {:halt, error}
        _ -> {:halt, {:error, :unsafe_source_path}}
      end
    end)
  end

  defp artifact(stage, path, kind, target_root, relative, original) do
    with {:ok, hash, size} <- Manifest.digest(path) do
      {:ok,
       %{
         "path" => Path.relative_to(path, stage),
         "sha256" => hash,
         "size" => size,
         "kind" => kind,
         "target_root" => target_root,
         "target_path" => relative,
         "source_path" => original
       }}
    end
  end

  defp build_manifest(context, sources, started, refs, artifacts) do
    %{
      "format" => Manifest.format(),
      "backup_id" => Manifest.random_id(),
      "application_version" => to_string(Application.spec(:ex_storage_service, :vsn)),
      "metadata_schema" => Atom.to_string(context.config.metadata_schema),
      "mode" => Atom.to_string(context.config.mode),
      "quiesced_from" => started,
      "consistency_point" => now(),
      "created_at" => now(),
      "metadata" => Enum.find(artifacts, &(&1["kind"] == "metadata"))["path"],
      "nodes" =>
        Enum.map(sources, fn source ->
          %{
            "id" => source.config.node_id,
            "generation" => source.config.node_generation,
            "role" => Atom.to_string(source.config.node_role),
            "erlang_node" => to_string(source.config.erlang_node)
          }
        end),
      "references" => refs,
      "artifacts" => Enum.sort_by(artifacts, & &1["path"]),
      "exclusions" => [
        "reconstructible gateway cache",
        "upstream-only gateway bodies",
        "Concord leases/history/request cache"
      ]
    }
  end

  defp write_manifest(stage, manifest) do
    with :ok <- Manifest.write_json(Path.join(stage, "manifest.json"), manifest),
         {:ok, hash, _} <- Manifest.digest(Path.join(stage, "manifest.json")),
         :ok <-
           File.write(Path.join(stage, "manifest.sha256"), hash <> "\n", [:exclusive, :sync]),
         :ok <- File.chmod(Path.join(stage, "manifest.sha256"), 0o600) do
      Manifest.sync_directory(stage)
    end
  end

  defp recovery_input(context, bundle, opts) do
    with {:ok, key} <- master_key(opts),
         {:ok, manifest, entries} <- Manifest.verify(bundle, key),
         :ok <- target_mapping(context, manifest),
         {:ok, marker} <- read_marker(context),
         :ok <- marker_binding(context, marker),
         true <- marker["backup_id"] == manifest["backup_id"] do
      {:ok, manifest, entries, marker}
    else
      {:error, _} = error -> error
      _ -> {:error, :recovery_binding_mismatch}
    end
  end

  defp target_mapping(context, manifest) do
    source = Enum.find(manifest["nodes"], &(&1["id"] == context.config.node_id))
    cluster = context.config.mode == :cluster

    cond do
      manifest["mode"] != Atom.to_string(context.config.mode) ->
        {:error, :restore_mode_mismatch}

      not is_map(source) ->
        {:error, :restore_node_not_in_backup}

      source["role"] != Atom.to_string(context.config.node_role) ->
        {:error, :restore_role_mismatch}

      context.config.node_generation <= source["generation"] ->
        {:error, :restore_generation_not_advanced}

      cluster and
          (context.config.replication_factor != 2 or context.config.write_quorum != 2 or
             context.config.allow_degraded_writes) ->
        {:error, :restore_requires_strict_quorum}

      cluster and
          Enum.sort(Enum.map(context.config.cluster_members, & &1.id)) !=
            Enum.sort(Enum.map(manifest["nodes"], & &1["id"])) ->
        {:error, :restore_topology_mismatch}

      cluster and
          Enum.any?(
            manifest["nodes"],
            &(&1["erlang_node"] == to_string(context.config.erlang_node))
          ) ->
        {:error, :restore_runtime_identity_reused}

      true ->
        :ok
    end
  end

  defp empty_roots(context) do
    Manifest.each(Map.values(roots(context)) |> Enum.uniq(), fn root ->
      case Manifest.files(root) do
        {:error, :enoent} -> :ok
        {:ok, []} -> :ok
        {:ok, _} -> {:error, :restore_destination_not_empty}
        error -> error
      end
    end)
  end

  defp install_artifacts(%{config: %{node_role: :metadata}}, _bundle, _manifest), do: :ok

  defp install_artifacts(context, bundle, manifest) do
    Manifest.each(data_artifacts(manifest), fn entry ->
      with {:ok, source} <- Manifest.safe_path(bundle, entry["path"]),
           {:ok, destination} <- destination(context, entry) do
        case Manifest.digest(destination) do
          {:ok, hash, size} ->
            if hash == entry["sha256"] and size == entry["size"],
              do: :ok,
              else: {:error, :restore_destination_conflict}

          {:error, :enoent} ->
            Manifest.install(source, destination, entry["sha256"], entry["size"])

          _ ->
            {:error, :restore_destination_conflict}
        end
      end
    end)
  end

  defp verify_installed(%{config: %{node_role: :metadata}}, _bundle, _manifest), do: :ok

  defp verify_installed(context, _bundle, manifest) do
    Manifest.each(data_artifacts(manifest), fn entry ->
      with {:ok, path} <- destination(context, entry),
           {:ok, hash, size} <- Manifest.digest(path),
           true <- hash == entry["sha256"] and size == entry["size"] do
        :ok
      else
        {:error, _} = error -> error
        _ -> {:error, :restored_artifact_mismatch}
      end
    end)
  end

  defp destination(context, entry) do
    case Map.fetch(roots(context), entry["target_root"]) do
      {:ok, root} -> Manifest.safe_path(root, entry["target_path"])
      :error -> {:error, :invalid_target_root}
    end
  end

  defp data_artifacts(manifest),
    do: Enum.reject(manifest["artifacts"], &(&1["kind"] == "metadata"))

  defp initial_metadata_guard(%{"status" => "importing"}, _opts), do: :ok

  defp initial_metadata_guard(_, opts) do
    with {:ok, entries} <- backend(opts).get_all(read_opts(opts)) do
      if Enum.all?(entries, fn {key, _} ->
           String.starts_with?(key, [
             Keys.cluster_node_prefix(),
             Keys.cluster_status_owner_prefix()
           ])
         end),
         do: :ok,
         else: {:error, :restore_metadata_not_empty}
    end
  end

  defp fence_jobs(entries, backup_id, opts) do
    with :ok <-
           Manifest.each(entries, fn {key, value} ->
             if String.starts_with?(key, Keys.job_prefix()) and is_map(value) and
                  value[:state] == :running do
               update =
                 value
                 |> Map.merge(%{
                   state: :pending,
                   owner_node: nil,
                   owner_generation: nil,
                   lease_until_ms: 0,
                   fencing_token: value.fencing_token + 1
                 })

               fenced_replace(key, update, backup_id, opts)
             else
               :ok
             end
           end) do
      fenced_replace(
        recovery_key(backup_id),
        %{backup_id: backup_id, imported: true},
        backup_id,
        opts
      )
    end
  end

  defp ensure_imported(backup_id, opts) do
    case backend(opts).get(recovery_key(backup_id), read_opts(opts)) do
      {:ok, %{value: %{backup_id: ^backup_id, imported: true}}} -> :ok
      _ -> {:error, :recovery_import_not_complete}
    end
  end

  defp fenced_replace(key, updated, backup_id, opts) do
    with {:ok, current} <- backend(opts).get(key, read_opts(opts)) do
      if current && current.value == updated do
        :ok
      else
        compare =
          if current,
            do: {:mod_revision, key, :==, current.mod_revision},
            else: {:exists, key, :==, false}

        token =
          "restore:" <>
            backup_id <>
            ":" <> Manifest.hex(:crypto.hash(:sha256, :erlang.term_to_binary({key, updated})))

        spec = %{compare: [compare], success: [{:put, key, updated, %{}}], failure: []}
        transaction_opts = Keyword.put(write_opts(opts), :idempotency_key, token)

        case backend(opts).transaction(spec, transaction_opts) do
          {:ok, %{succeeded: true}} ->
            :ok

          {:error, reason} when reason in @ambiguous ->
            case backend(opts).resolve_transaction(token, transaction_opts) do
              {:ok, %{succeeded: true}} -> :ok
              _ -> {:error, :unknown_restore_transaction}
            end

          {:ok, %{succeeded: false}} ->
            {:error, :restore_compare_failed}

          {:error, _} = error ->
            error
        end
      end
    end
  end

  defp rebase_journals(%{config: %{node_role: :metadata}}, _manifest, _entries, _opts), do: :ok

  defp rebase_journals(context, manifest, entries, opts) do
    Manifest.each(entries, fn {key, value} ->
      if String.starts_with?(key, "ess:gateway-write:") do
        case Enum.find(
               manifest["artifacts"],
               &(&1["kind"] == "journal" and &1["source_path"] == value.path)
             ) do
          nil ->
            {:error, :journal_artifact_missing}

          artifact ->
            if artifact["node_id"] == context.config.node_id do
              with {:ok, path} <- destination(context, artifact) do
                fenced_replace(key, Map.put(value, :path, path), manifest["backup_id"], opts)
              end
            else
              :ok
            end
        end
      else
        :ok
      end
    end)
  end

  defp register_target(%{config: %{mode: :standalone}}, _opts), do: :ok

  defp register_target(context, opts) do
    ExStorageService.Cluster.Membership.register(context.config, read_opts(opts))
  end

  defp publish_locations(%{config: %{mode: :standalone}}, _manifest, _opts), do: :ok
  defp publish_locations(%{config: %{node_role: :metadata}}, _manifest, _opts), do: :ok

  defp publish_locations(context, manifest, opts) do
    with {:ok, member} <-
           ExStorageService.Cluster.Membership.member(
             context.config,
             context.config.node_id,
             read_opts(opts)
           ) do
      Manifest.each(manifest["references"], fn {hash, size} ->
        path = ExStorageService.BlobStore.LocalCAS.blob_path(hash, root: context.blob_root)

        case BlobCatalog.get(hash, Keyword.put(read_opts(opts), :backend, backend(opts))) do
          {:ok, descriptor} ->
            with {:ok, ^hash, ^size} <- Manifest.digest(path) do
              BlobLocations.mark_ready(
                hash,
                context.config.node_id,
                context.config.node_generation,
                size,
                Keyword.merge(read_opts(opts),
                  node_record: member,
                  expected_generation: context.config.node_generation,
                  descriptor_record: descriptor,
                  resurrect_absent: true,
                  expected_location_revision: location_revision(hash, context, opts)
                )
              )
            end

          {:error, :not_found} ->
            :ok

          {:error, _} = error ->
            error
        end
      end)
    end
  end

  defp location_revision(hash, context, opts) do
    case backend(opts).get(Keys.blob_location(hash, context.config.node_id), read_opts(opts)) do
      {:ok, %{mod_revision: revision}} -> revision
      _ -> nil
    end
  end

  defp verify_metadata(_context, manifest, entries, opts) do
    Manifest.each(entries, fn {key, source} ->
      with {:ok, %{value: value}} <- backend(opts).get(key, read_opts(opts)) do
        cond do
          String.starts_with?(key, [
            Keys.cluster_node_prefix(),
            Keys.cluster_status_owner_prefix(),
            "ess:v2:blob_location:"
          ]) ->
            :ok

          String.starts_with?(key, Keys.job_prefix()) and source[:state] == :running ->
            expected =
              Map.merge(source, %{
                state: :pending,
                owner_node: nil,
                owner_generation: nil,
                lease_until_ms: 0,
                fencing_token: source.fencing_token + 1
              })

            equal(value, expected)

          String.starts_with?(key, "ess:gateway-write:") ->
            equal(Map.delete(value, :path), Map.delete(source, :path))

          true ->
            if ExStorageService.Operations.RecoveryPolicy.metadata_matches?(
                 key,
                 source,
                 value,
                 Keyword.get(opts, :intent_resolutions, %{})
               ),
               do: :ok,
               else: {:error, :restored_metadata_mismatch}
        end
      else
        _ -> {:error, :restored_metadata_mismatch}
      end
    end)
    |> case do
      :ok -> ensure_imported(manifest["backup_id"], opts)
      error -> error
    end
  end

  defp verify_cluster(%{config: %{mode: :standalone}}, _manifest, _opts), do: :ok

  defp verify_cluster(context, manifest, opts) do
    with {:ok, members} <-
           ExStorageService.Cluster.Membership.members(context.config, read_opts(opts)),
         true <- length(members) == 3,
         true <-
           Enum.all?(members, fn %{node: target} ->
             source = Enum.find(manifest["nodes"], &(&1["id"] == target.node_id))

             (source && target.generation > source["generation"]) and
               to_string(target.erlang_endpoint) != source["erlang_node"]
           end) do
      data = Enum.filter(members, &(&1.node.role == :data))

      Manifest.each(manifest["references"], fn {hash, size} ->
        with {:ok, descriptor} <- backend(opts).get(Keys.blob(hash), read_opts(opts)),
             {:ok, locations} <- BlobLocations.list(hash, read_opts(opts)) do
          if is_nil(descriptor) do
            :ok
          else
            if length(data) == 2 and
                 Enum.all?(data, fn %{node: member} ->
                   Enum.any?(locations, fn %{location: location} ->
                     location.node_id == member.node_id and
                       location.node_generation == member.generation and
                       location.size == size and location.state == :ready
                   end)
                 end),
               do: :ok,
               else: {:error, :restored_replication_not_ready}
          end
        end
      end)
    else
      {:error, _} = error -> error
      _ -> {:error, :restored_topology_not_ready}
    end
  end

  defp approved_workers(opts) do
    workers = Keyword.get(opts, :workers)

    if is_map(workers) and Enum.sort(Map.keys(workers)) == Enum.sort(@workers) and
         Enum.all?(workers, fn {_key, value} -> is_boolean(value) end),
       do: {:ok, workers},
       else: {:error, :explicit_complete_worker_policy_required}
  end

  defp activation_safety(entries, workers, opts) do
    if Enum.any?(workers, fn {_key, enabled} -> enabled end) do
      Manifest.each(entries, fn {key, _value} ->
        if String.starts_with?(key, [
             "ess:gateway-write:",
             "ess:gateway-delete:",
             Keys.operation_intent_prefix(),
             "ess:v2:gc_lock:"
           ]) do
          case backend(opts).get(key, read_opts(opts)) do
            {:ok, nil} ->
              :ok

            {:ok, %{value: value}} ->
              if ExStorageService.Operations.RecoveryPolicy.unresolved?(key, value),
                do: {:error, :unresolved_recovery_work},
                else: :ok

            {:error, _} = error ->
              error
          end
        else
          :ok
        end
      end)
    else
      :ok
    end
  end

  defp marker_binding(context, marker) do
    if marker["schema"] == 1 and marker["roots"] == roots(context) and
         marker["node_id"] == context.config.node_id and
         marker["generation"] == context.config.node_generation and
         marker["status"] in [
           "prepared",
           "importing",
           "imported",
           "installed",
           "verified",
           "activated"
         ] and
         is_binary(marker["backup_id"]),
       do: :ok,
       else: {:error, :invalid_recovery_marker}
  end

  defp read_marker(context) do
    path = Path.join(context.data_root, @marker)

    with :ok <- Manifest.regular(path),
         {:ok, %{size: size}} <- File.stat(path),
         true <- size <= 65_536,
         {:ok, bytes} <- File.read(path),
         {:ok, marker} <- JSON.decode(bytes),
         true <- is_map(marker) do
      {:ok, marker}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_recovery_marker}
    end
  end

  defp write_marker(context, marker),
    do: Manifest.write_json(Path.join(context.data_root, @marker), marker)

  defp roots(context),
    do: %{
      "data" => context.data_root,
      "blob" => context.blob_root,
      "tmp" => context.tmp_root,
      "metadata" => context.metadata_root,
      "ra" => context.ra_root
    }

  defp valid_sources(context, sources) do
    ids = Enum.map(sources, & &1.config.node_id)

    expected =
      if context.config.mode == :cluster,
        do: Enum.map(context.config.cluster_members, & &1.id),
        else: [context.config.node_id]

    if Enum.sort(ids) == Enum.sort(expected) and length(ids) == length(Enum.uniq(ids)),
      do: :ok,
      else: {:error, :incomplete_backup_sources}
  end

  defp each_source(sources, operation) do
    Manifest.each(sources, fn context ->
      if context.config.erlang_node == node() do
        apply(__MODULE__, operation, [context])
      else
        case :rpc.call(context.config.erlang_node, __MODULE__, operation, [context], 30_000) do
          :ok -> :ok
          {:error, _} = error -> error
          _ -> {:error, :source_quiescence_unavailable}
        end
      end
    end)
  end

  defp outside_sources(path, sources) do
    if Enum.any?(sources, fn source ->
         Enum.any?(Map.values(roots(source)), &contained?(path, &1))
       end),
       do: {:error, :backup_inside_source_root},
       else: :ok
  end

  defp contained?(path, root),
    do:
      Path.expand(path) == Path.expand(root) or
        String.starts_with?(Path.expand(path), Path.expand(root) <> "/")

  defp absent(path), do: if(File.exists?(path), do: {:error, :destination_exists}, else: :ok)

  defp file_kind("blob", "packs/" <> relative),
    do: if(String.ends_with?(relative, ".pack"), do: "pack", else: "pack-index")

  defp file_kind(kind, _), do: kind
  defp equal(value, value), do: :ok
  defp equal(_, _), do: {:error, :restored_metadata_mismatch}
  defp now, do: DateTime.utc_now() |> DateTime.to_iso8601()
  defp recovery_key(id), do: "ess:recovery:" <> id
  defp backend(opts), do: Keyword.get(opts, :backend, Backend)
  defp backup_module(opts), do: Keyword.get(opts, :backup_module, Concord.Backup)

  # Version 0 preserves frozen legacy replay and restores values without the
  # revision records required by ESS. Select version 1 before the empty target
  # starts; never switch or reconcile an existing runtime implicitly here.
  defp current_restore_engine(opts) do
    case Concord.Engine.status(Keyword.take(opts, [:engine, :timeout])) do
      {:ok, %{command_version: 1}} -> :ok
      {:ok, _} -> {:error, :restore_requires_command_version_1}
      {:error, _} = error -> error
    end
  end

  defp read_opts(opts),
    do: opts |> Keyword.take([:engine, :timeout]) |> Keyword.put(:consistency, :linearizable)

  defp write_opts(opts), do: Keyword.take(opts, [:engine, :timeout])

  defp backup_opts(opts, path),
    do: Keyword.put(Keyword.take(opts, [:engine, :timeout]), :path, path)

  defp master_key(opts) do
    case Keyword.get(opts, :master_key, Application.get_env(:ex_storage_service, :master_key)) do
      key when is_binary(key) and byte_size(key) > 0 -> {:ok, key}
      _ -> {:error, :master_key_required}
    end
  end

  defp started_apps, do: Application.started_applications() |> Enum.map(&elem(&1, 0))

  defp stop_listeners do
    Manifest.each(@listener_apps, fn app ->
      if app in started_apps(), do: Application.stop(app), else: :ok
    end)
  end

  defp stop_instance(context) do
    case instance_pid(context) do
      nil ->
        :ok

      _ ->
        Supervisor.terminate_child(
          ExStorageService.Supervisor,
          {ExStorageService, context.instance}
        )
    end
  end

  defp notification_tasks do
    if Process.whereis(ExStorageService.NotificationTaskSupervisor),
      do: Task.Supervisor.children(ExStorageService.NotificationTaskSupervisor),
      else: []
  end

  defp instance_pid(context) do
    if Process.whereis(Names.registry()),
      do: GenServer.whereis(Names.instance_supervisor(context.instance)),
      else: nil
  end

  defp stop_notifications do
    Manifest.each(
      notification_tasks(),
      &Task.Supervisor.terminate_child(ExStorageService.NotificationTaskSupervisor, &1)
    )
  end
end
