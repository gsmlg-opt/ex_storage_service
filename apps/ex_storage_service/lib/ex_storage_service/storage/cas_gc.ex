defmodule ExStorageService.Storage.CasGC do
  @moduledoc """
  Garbage collector for the global content-addressable store.

  Conservative candidate → quarantine → delete pipeline (PRD §14):

    1. Build the reachable-hash set from GC roots: `obj:*` / `obj_ver:*`
       object metadata (`content_hash`) and active multipart part records
       (`mpu_part:*` `hash`).
    2. Restore any quarantined blob whose hash became reachable again.
    3. Unreachable active blob files older than the mtime grace become
       `gc:candidate:{hash}` records.
    4. Candidates past `eligible_after` and still unreachable move to
       `cas/gc/quarantine/sha256-{hash}` (blob metadata → `:quarantined`).
    5. Quarantined entries past their second `eligible_after` and still
       unreachable are deleted: file, blob metadata, candidate record.

  Reachability is re-checked at every stage, so a blob that regains a
  reference at any point is never deleted. The legacy `ContentGC` owns
  the legacy bucket-local tree; this module only touches `cas/objects`
  and `cas/gc/quarantine`. Loose fallbacks retained for packed blobs are
  owned by `Packer` cleanup and are never GC candidates.

  Configuration (app env, overridable per `run_now/1` call):
  - `:cas_gc_interval` — sweep interval ms (default 30 min)
  - `:cas_gc_orphan_mtime_grace` — seconds a file must be unmodified before
    it can become a candidate (default 600)
  - `:cas_gc_candidate_grace` — seconds before a candidate may be
    quarantined (default 3600)
  - `:cas_gc_quarantine_grace` — seconds before a quarantined blob may be
    deleted (default 86_400)
  """

  use GenServer

  require Logger

  alias ExStorageService.Context
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.{BlobLocations, GCGuard}
  alias ExStorageService.Storage.CAS

  @scan_page_size 250
  @metadata_prefixes [
    "obj:",
    "obj_ver:",
    "ess:v2:object_version:",
    "mpu_part:",
    "ess:v2:multipart_part:",
    "ess:v2:outbox:",
    "ess:v2:job:",
    "ess:v2:operation_intent:",
    "gc:candidate:",
    "blob:sha256:"
  ]

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Run one sweep now. Options: :orphan_mtime_grace, :candidate_grace, :quarantine_grace, :dry_run."
  def run_now(opts \\ []), do: run_now(__MODULE__, opts)

  def run_now(server, opts) do
    GenServer.call(server, {:run_now, opts}, :infinity)
  end

  @doc "Report what a sweep would do without modifying files or metadata."
  def dry_run(server \\ __MODULE__), do: run_now(server, dry_run: true)

  @doc "Counts of current candidate and quarantined records."
  def stats do
    case scan_prefix("gc:candidate:") do
      {:ok, records} ->
        candidates =
          records
          |> Enum.map(fn {_, v} -> v end)

        %{
          candidates: Enum.count(candidates, &(field(&1, :stage) == :candidate)),
          quarantined: Enum.count(candidates, &(field(&1, :stage) == :quarantined))
        }

      _ ->
        %{candidates: 0, quarantined: 0}
    end
  end

  @impl true
  def init(opts) do
    interval =
      Keyword.get(
        opts,
        :interval,
        Application.get_env(:ex_storage_service, :cas_gc_interval, :timer.minutes(30))
      )

    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:sweep, state) do
    do_sweep([])
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call({:run_now, opts}, _from, state) do
    {:reply, do_sweep(opts), state}
  end

  defp schedule(interval), do: Process.send_after(self(), :sweep, interval)

  ## Sweep

  defp do_sweep(opts) do
    now = System.os_time(:second)
    dry_run? = Keyword.get(opts, :dry_run, false)

    orphan_grace = grace(opts, :orphan_mtime_grace, :cas_gc_orphan_mtime_grace, 600)
    candidate_grace = grace(opts, :candidate_grace, :cas_gc_candidate_grace, 3600)
    quarantine_grace = grace(opts, :quarantine_grace, :cas_gc_quarantine_grace, 86_400)

    case metadata_snapshot() do
      {:ok, all} ->
        reachable = reachable_hashes(all)
        candidates = existing_candidates(all)
        packed = packed_hashes(all)

        report = %{
          reachable: MapSet.size(reachable),
          candidates_created: 0,
          quarantined: 0,
          deleted: 0,
          restored: 0
        }

        report = restore_rereferenced(candidates, reachable, dry_run?, report)

        report =
          advance_candidates(
            candidates,
            reachable,
            packed,
            now,
            quarantine_grace,
            dry_run?,
            report,
            opts
          )

        report =
          create_candidates(
            candidates,
            reachable,
            packed,
            now,
            orphan_grace,
            candidate_grace,
            dry_run?,
            report
          )

        gc_stats = stats()

        :telemetry.execute(
          [:ex_storage_service, :storage, :gc, :stop],
          %{
            candidates: gc_stats.candidates,
            quarantined: gc_stats.quarantined,
            orphans: gc_stats.candidates + gc_stats.quarantined,
            deleted: report.deleted
          },
          %{dry_run: dry_run?}
        )

        if report.candidates_created + report.quarantined + report.deleted + report.restored > 0 do
          Logger.info("CasGC sweep#{if dry_run?, do: " (dry-run)", else: ""}: #{inspect(report)}")
        end

        {:ok, report}

      {:error, reason} ->
        Logger.warning("CasGC: metadata scan failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp grace(opts, key, env_key, default) do
    Keyword.get(opts, key, Application.get_env(:ex_storage_service, env_key, default))
  end

  ## Roots

  @doc false
  def reachable_hashes(all) do
    all
    |> Enum.flat_map(fn
      {"obj:" <> _, %{content_hash: hash}} when is_binary(hash) ->
        [hash]

      {"obj_ver:" <> _, %{content_hash: hash}} when is_binary(hash) ->
        [hash]

      {"ess:v2:object_version:" <> _, %{content_hash: hash}} when is_binary(hash) ->
        [hash]

      {"mpu_part:" <> _, %{hash: hash}} when is_binary(hash) ->
        [hash]

      {"ess:v2:multipart_part:" <> _, %{hash: hash}} when is_binary(hash) ->
        [hash]

      {"ess:v2:outbox:" <> _, operation} when is_map(operation) ->
        operation_hashes(operation)

      {"ess:v2:job:" <> _, job} when is_map(job) ->
        job_hashes(job)

      {"ess:v2:operation_intent:" <> _, intent} when is_map(intent) ->
        operation_intent_hashes(intent)

      _ ->
        []
    end)
    |> MapSet.new()
  end

  defp existing_candidates(all) do
    all
    |> Enum.flat_map(fn
      {"gc:candidate:" <> hash, record} -> [{hash, record}]
      _ -> []
    end)
    |> Map.new()
  end

  defp packed_hashes(all) do
    all
    |> Enum.flat_map(fn
      {"blob:sha256:" <> hash, %{state: :packed}} -> [hash]
      _ -> []
    end)
    |> MapSet.new()
  end

  ## Stage: restore quarantined blobs that regained references

  defp restore_rereferenced(candidates, reachable, dry_run?, report) do
    candidates
    |> Enum.filter(fn {hash, record} ->
      field(record, :stage) == :quarantined and MapSet.member?(reachable, hash)
    end)
    |> Enum.reduce(report, fn {hash, _record}, acc ->
      case fresh_blob_status(hash) do
        {:ok, :packed} ->
          protect_packed_blob(hash, dry_run?)
          acc

        {:ok, :reachable} ->
          restore_referenced_blob(hash, dry_run?)
          %{acc | restored: acc.restored + 1}

        {:ok, :unreferenced} ->
          acc

        {:error, reason} ->
          log_fresh_scan_failure(hash, :restore, reason)
          acc
      end
    end)
  end

  ## Stage: advance existing candidates (quarantine / delete / drop)

  defp advance_candidates(
         candidates,
         reachable,
         packed,
         now,
         quarantine_grace,
         dry_run?,
         report,
         opts
       ) do
    Enum.reduce(candidates, report, fn {hash, record}, acc ->
      cond do
        # A retained loose file for a packed blob belongs to Packer cleanup,
        # never to the unreachable-blob quarantine pipeline.
        MapSet.member?(packed, hash) ->
          protect_packed_blob(hash, dry_run?)
          acc

        # regained a reference before quarantine — drop the candidate
        field(record, :stage) == :candidate and MapSet.member?(reachable, hash) ->
          unless dry_run?, do: Concord.delete(candidate_key(hash))
          acc

        field(record, :stage) == :candidate and now >= field(record, :eligible_after) ->
          if quarantine(hash, now, quarantine_grace, dry_run?, opts) do
            %{acc | quarantined: acc.quarantined + 1}
          else
            acc
          end

        field(record, :stage) == :quarantined and now >= field(record, :eligible_after) and
            not MapSet.member?(reachable, hash) ->
          case finalize_quarantined(hash, dry_run?, opts) do
            :deleted -> %{acc | deleted: acc.deleted + 1}
            :restored -> %{acc | restored: acc.restored + 1}
            :skipped -> acc
          end

        true ->
          acc
      end
    end)
  end

  defp quarantine(hash, now, quarantine_grace, dry_run?, opts) do
    with_gc_lock(hash, opts, fn context, lock ->
      case fresh_blob_status(hash) do
        {:ok, :packed} ->
          protect_packed_blob(hash, dry_run?)
          false

        {:ok, :reachable} ->
          unless dry_run?, do: Concord.delete(candidate_key(hash))
          false

        {:ok, :unreferenced} ->
          with :ok <- prepare_gc_location(hash, context, opts),
               {:ok, _renewed_lock} <-
                 maybe_renew_gc_lock(hash, lock, dry_run?, opts) do
            if dry_run? do
              do_quarantine(hash, now, quarantine_grace, dry_run?)
            else
              do_quarantine(hash, now, quarantine_grace, false)
            end
          else
            {:error, reason} ->
              Logger.warning(
                "CasGC: failed to fence location before quarantine #{hash}: #{inspect(reason)}"
              )

              false
          end

        {:error, reason} ->
          log_fresh_scan_failure(hash, :quarantine, reason)
          false
      end
    end)
    |> case do
      {:ok, result} -> result
      {:error, _reason} -> false
    end
  end

  defp do_quarantine(hash, now, quarantine_grace, dry_run?) do
    if dry_run? do
      true
    else
      case ensure_quarantined(hash) do
        :ok ->
          set_blob_state(hash, :quarantined)

          case Concord.put(candidate_key(hash), %{
                 hash: "sha256:#{hash}",
                 reason: :unreferenced,
                 stage: :quarantined,
                 first_seen_at: now,
                 eligible_after: now + quarantine_grace
               }) do
            result when result in [:ok, {:ok, nil}] ->
              Logger.info("CasGC: quarantined unreferenced blob #{hash}")
              true

            {:ok, _result} ->
              Logger.info("CasGC: quarantined unreferenced blob #{hash}")
              true

            {:error, reason} ->
              Logger.warning(
                "CasGC: failed to persist quarantine state for #{hash}: #{inspect(reason)}"
              )

              false
          end

        {:error, reason} ->
          Logger.warning("CasGC: failed to quarantine #{hash}: #{inspect(reason)}")
          false
      end
    end
  end

  defp finalize_quarantined(hash, dry_run?, opts) do
    case fresh_blob_status(hash) do
      {:ok, :packed} ->
        protect_packed_blob(hash, dry_run?)
        :skipped

      {:ok, :reachable} ->
        restore_referenced_blob(hash, dry_run?)
        :restored

      {:ok, :unreferenced} ->
        case delete_quarantined(hash, dry_run?, opts) do
          :ok -> :deleted
          {:error, _reason} -> :skipped
        end

      {:error, reason} ->
        log_fresh_scan_failure(hash, :delete, reason)
        :skipped
    end
  end

  defp delete_quarantined(hash, dry_run?, opts) do
    if dry_run? do
      :ok
    else
      with_gc_lock(hash, opts, fn context, lock ->
        with {:ok, renewed_lock} <- renew_gc_lock(hash, lock, opts),
             :ok <- ensure_quarantined(hash),
             :ok <- remove_quarantined_file(hash),
             :ok <- remove_gc_location(hash, context, renewed_lock, opts),
             :ok <- normalize_delete_result(Concord.delete("blob:sha256:#{hash}")),
             :ok <- normalize_delete_result(Concord.delete(candidate_key(hash))) do
          Logger.info("CasGC: deleted unreferenced blob #{hash}")
          {:retain_lock, :ok}
        else
          {:error, reason} = error ->
            Logger.warning("CasGC: failed to delete #{hash}: #{inspect(reason)}")
            {:retain_lock, error}
        end
      end)
      |> case do
        {:ok, result} -> result
        {:error, _reason} = error -> error
      end
    end
  end

  defp fresh_blob_status(hash) do
    case metadata_snapshot() do
      {:ok, all} ->
        cond do
          MapSet.member?(packed_hashes(all), hash) -> {:ok, :packed}
          MapSet.member?(reachable_hashes(all), hash) -> {:ok, :reachable}
          true -> {:ok, :unreferenced}
        end

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, other}
    end
  end

  defp protect_packed_blob(hash, dry_run?) do
    unless dry_run? do
      restore_loose_fallback(hash)
      Concord.delete(candidate_key(hash))
    end
  end

  defp restore_referenced_blob(hash, dry_run?) do
    unless dry_run? do
      restore_loose_fallback(hash)
      set_blob_state(hash, :active)
      Concord.delete(candidate_key(hash))
      Logger.info("CasGC: restored re-referenced blob #{hash}")
    end
  end

  defp restore_loose_fallback(hash) do
    qpath = quarantine_path(hash)
    dest = CAS.blob_path(hash)

    cond do
      File.exists?(qpath) and not File.exists?(dest) ->
        with :ok <- File.mkdir_p(Path.dirname(dest)),
             :ok <- File.rename(qpath, dest) do
          :ok
        else
          {:error, reason} ->
            Logger.warning("CasGC: failed to restore #{hash}: #{inspect(reason)}")
            {:error, reason}
        end

      File.exists?(qpath) and File.exists?(dest) ->
        File.rm(qpath)

      true ->
        :ok
    end
  end

  defp log_fresh_scan_failure(hash, action, reason) do
    Logger.warning(
      "CasGC: skipped #{action} for #{hash}; fresh metadata scan failed: #{inspect(reason)}"
    )
  end

  ## Stage: create candidates for unreachable disk blobs

  defp create_candidates(
         candidates,
         reachable,
         packed,
         now,
         orphan_grace,
         candidate_grace,
         dry_run?,
         report
       ) do
    disk_blobs()
    |> Enum.reduce(report, fn {hash, path}, acc ->
      cond do
        MapSet.member?(reachable, hash) ->
          acc

        MapSet.member?(packed, hash) ->
          acc

        Map.has_key?(candidates, hash) ->
          acc

        not older_than?(path, now, orphan_grace) ->
          acc

        true ->
          if packed_blob?(hash) do
            acc
          else
            case create_candidate(hash, now, candidate_grace, dry_run?) do
              :ok -> %{acc | candidates_created: acc.candidates_created + 1}
              {:error, _reason} -> acc
            end
          end
      end
    end)
  end

  defp disk_blobs do
    objects_dir = Path.join([CAS.blob_root(), "objects", "sha256"])

    case File.ls(objects_dir) do
      {:ok, prefixes} ->
        Enum.flat_map(prefixes, fn prefix ->
          prefix_dir = Path.join(objects_dir, prefix)

          case File.ls(prefix_dir) do
            {:ok, files} ->
              Enum.map(files, fn file -> {prefix <> file, Path.join(prefix_dir, file)} end)

            {:error, _} ->
              []
          end
        end)

      {:error, _} ->
        []
    end
  end

  defp older_than?(path, now, grace) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> now - mtime >= grace
      {:error, _} -> false
    end
  end

  defp create_candidate(_hash, _now, _candidate_grace, true), do: :ok

  defp create_candidate(hash, now, candidate_grace, false) do
    case Concord.put(candidate_key(hash), %{
           hash: "sha256:#{hash}",
           reason: :unreferenced,
           stage: :candidate,
           first_seen_at: now,
           eligible_after: now + candidate_grace
         }) do
      result when result in [:ok, {:ok, nil}] ->
        Logger.info("CasGC: marked candidate #{hash}")
        :ok

      {:ok, _result} ->
        Logger.info("CasGC: marked candidate #{hash}")
        :ok

      {:error, reason} = error ->
        Logger.warning("CasGC: failed to mark candidate #{hash}: #{inspect(reason)}")
        error
    end
  end

  defp ensure_quarantined(hash) do
    source = CAS.blob_path(hash)
    quarantine = quarantine_path(hash)

    cond do
      File.regular?(quarantine) ->
        :ok

      File.exists?(quarantine) ->
        {:error, :invalid_quarantine_path}

      File.regular?(source) ->
        with :ok <- File.mkdir_p(Path.dirname(quarantine)),
             :ok <- File.rename(source, quarantine) do
          :ok
        end

      File.exists?(source) ->
        {:error, :invalid_blob_path}

      true ->
        # A prior run may have removed the file before it could clean metadata.
        :ok
    end
  end

  defp remove_quarantined_file(hash) do
    case File.rm(quarantine_path(hash)) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:physical_delete, reason}}
    end
  end

  defp normalize_delete_result(result) when result in [:ok, {:ok, nil}], do: :ok
  defp normalize_delete_result({:ok, _result}), do: :ok
  defp normalize_delete_result({:error, reason}), do: {:error, {:metadata_delete, reason}}

  defp metadata_snapshot do
    Enum.reduce_while(@metadata_prefixes, {:ok, []}, fn prefix, {:ok, records} ->
      case scan_prefix(prefix) do
        {:ok, page_records} -> {:cont, {:ok, page_records ++ records}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp scan_prefix(prefix), do: scan_prefix(prefix, nil, [])

  defp scan_prefix(prefix, cursor, records) do
    case ConcordBackend.list_page(prefix, cursor, @scan_page_size, consistency: :strong) do
      {:ok, %{entries: entries, next_cursor: next_cursor}} ->
        records = Enum.reduce(entries, records, &[{&1.key, &1.value} | &2])

        if is_nil(next_cursor),
          do: {:ok, Enum.reverse(records)},
          else: scan_prefix(prefix, next_cursor, records)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp operation_hashes(operation) do
    pending_events =
      operation
      |> field(:events)
      |> List.wrap()
      |> Enum.filter(&(field(&1, :state) in [nil, :pending, "pending"]))

    hashes = collect_hashes(pending_events, [])

    if is_nil(field(operation, :committed_at)),
      do: collect_hashes(operation, hashes),
      else: hashes
  end

  defp job_hashes(job) do
    if field(job, :state) in [:pending, :running, "pending", "running"],
      do: collect_hashes(field(job, :payload), []),
      else: []
  end

  defp operation_intent_hashes(intent) do
    now_ms = System.system_time(:millisecond)
    deadline = field(intent, :protected_until_ms)
    hash = field(intent, :hash)

    if field(intent, :state) in [:pending, :unknown, "pending", "unknown"] and
         is_integer(deadline) and deadline >= now_ms and is_binary(hash) do
      [hash]
    else
      []
    end
  end

  defp with_gc_lock(hash, opts, callback) do
    with {:ok, context} <- gc_context(opts) do
      if Keyword.get(opts, :dry_run, false) do
        {:ok, callback.(context, %{})}
      else
        claim_gc_lock(hash, context, opts, callback)
      end
    end
  end

  defp claim_gc_lock(hash, context, opts, callback) do
    with now_ms = System.system_time(:millisecond),
         {:ok, lock} <-
           GCGuard.claim(
             hash,
             context.config.node_id,
             context.config.node_generation,
             now_ms,
             gc_lock_lease_ms(opts),
             metadata_opts(opts)
           ) do
      case callback.(context, lock) do
        {:retain_lock, result} ->
          {:ok, result}

        result ->
          _ = GCGuard.release(hash, lock.token, metadata_opts(opts))
          {:ok, result}
      end
    end
  end

  defp maybe_renew_gc_lock(_hash, lock, true, _opts), do: {:ok, lock}
  defp maybe_renew_gc_lock(hash, lock, false, opts), do: renew_gc_lock(hash, lock, opts)

  defp renew_gc_lock(hash, lock, opts) do
    GCGuard.renew(
      hash,
      lock.token,
      System.system_time(:millisecond),
      gc_lock_lease_ms(opts),
      metadata_opts(opts)
    )
  end

  defp gc_lock_lease_ms(opts), do: Keyword.get(opts, :gc_lock_lease_ms, 30_000)

  defp prepare_gc_location(hash, context, opts) do
    case BlobLocations.mark_draining(hash, context.config.node_id, metadata_opts(opts)) do
      :ok -> :ok
      {:error, :blob_location_not_found} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp remove_gc_location(hash, context, lock, opts) do
    BlobLocations.remove_after_gc(
      hash,
      context.config.node_id,
      context.config.node_generation,
      lock.token,
      metadata_opts(opts)
    )
  end

  defp gc_context(opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context -> {:ok, context}
      nil -> Context.default()
    end
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier])

  defp collect_hashes(%{} = value, hashes) do
    Enum.reduce(value, hashes, fn
      {key, hash}, acc
      when key in [:hash, "hash", :content_hash, "content_hash"] and
             is_binary(hash) ->
        [hash | acc]

      {_key, nested}, acc when is_map(nested) or is_list(nested) ->
        collect_hashes(nested, acc)

      _entry, acc ->
        acc
    end)
  end

  defp collect_hashes(values, hashes) when is_list(values),
    do: Enum.reduce(values, hashes, &collect_hashes/2)

  defp collect_hashes(_value, hashes), do: hashes

  defp field(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, to_string(key)))
  defp field(_value, _key), do: nil

  defp set_blob_state(hash, state) do
    case Metadata.get_blob_meta(hash) do
      {:ok, meta} -> Metadata.put_blob_meta(hash, Map.put(meta, :state, state))
      {:error, :not_found} -> :ok
    end
  end

  defp packed_blob?(hash) do
    match?({:ok, %{state: :packed}}, Metadata.get_blob_meta(hash))
  end

  defp candidate_key(hash), do: "gc:candidate:#{hash}"

  defp quarantine_path(hash) do
    Path.join([CAS.blob_root(), "gc", "quarantine", "sha256-#{hash}"])
  end
end
