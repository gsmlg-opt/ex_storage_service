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

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run one sweep now. Options: :orphan_mtime_grace, :candidate_grace, :quarantine_grace, :dry_run."
  def run_now(opts \\ []) do
    GenServer.call(__MODULE__, {:run_now, opts}, :infinity)
  end

  @doc "Report what a sweep would do without modifying files or metadata."
  def dry_run, do: run_now(dry_run: true)

  @doc "Counts of current candidate and quarantined records."
  def stats do
    case Concord.get_all() do
      {:ok, all} ->
        candidates =
          all
          |> Enum.filter(fn {k, _} -> String.starts_with?(k, "gc:candidate:") end)
          |> Enum.map(fn {_, v} -> v end)

        %{
          candidates: Enum.count(candidates, &(&1.stage == :candidate)),
          quarantined: Enum.count(candidates, &(&1.stage == :quarantined))
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

    case Concord.get_all() do
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
            report
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
      {"obj:" <> _, %{content_hash: hash}} when is_binary(hash) -> [hash]
      {"obj_ver:" <> _, %{content_hash: hash}} when is_binary(hash) -> [hash]
      {"ess:v2:object_version:" <> _, %{content_hash: hash}} when is_binary(hash) -> [hash]
      {"mpu_part:" <> _, %{hash: hash}} when is_binary(hash) -> [hash]
      _ -> []
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
      record.stage == :quarantined and MapSet.member?(reachable, hash)
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
         report
       ) do
    Enum.reduce(candidates, report, fn {hash, record}, acc ->
      cond do
        # A retained loose file for a packed blob belongs to Packer cleanup,
        # never to the unreachable-blob quarantine pipeline.
        MapSet.member?(packed, hash) ->
          protect_packed_blob(hash, dry_run?)
          acc

        # regained a reference before quarantine — drop the candidate
        record.stage == :candidate and MapSet.member?(reachable, hash) ->
          unless dry_run?, do: Concord.delete(candidate_key(hash))
          acc

        record.stage == :candidate and now >= record.eligible_after ->
          if quarantine(hash, now, quarantine_grace, dry_run?) do
            %{acc | quarantined: acc.quarantined + 1}
          else
            acc
          end

        record.stage == :quarantined and now >= record.eligible_after and
            not MapSet.member?(reachable, hash) ->
          case finalize_quarantined(hash, dry_run?) do
            :deleted -> %{acc | deleted: acc.deleted + 1}
            :restored -> %{acc | restored: acc.restored + 1}
            :skipped -> acc
          end

        true ->
          acc
      end
    end)
  end

  defp quarantine(hash, now, quarantine_grace, dry_run?) do
    case fresh_blob_status(hash) do
      {:ok, :packed} ->
        protect_packed_blob(hash, dry_run?)
        false

      {:ok, :reachable} ->
        unless dry_run?, do: Concord.delete(candidate_key(hash))
        false

      {:ok, :unreferenced} ->
        do_quarantine(hash, now, quarantine_grace, dry_run?)
        true

      {:error, reason} ->
        log_fresh_scan_failure(hash, :quarantine, reason)
        false
    end
  end

  defp do_quarantine(hash, now, quarantine_grace, dry_run?) do
    unless dry_run? do
      src = CAS.blob_path(hash)
      qpath = quarantine_path(hash)

      if File.exists?(src) do
        File.mkdir_p!(Path.dirname(qpath))
        File.rename!(src, qpath)
      end

      set_blob_state(hash, :quarantined)

      Concord.put(candidate_key(hash), %{
        hash: "sha256:#{hash}",
        reason: :unreferenced,
        stage: :quarantined,
        first_seen_at: now,
        eligible_after: now + quarantine_grace
      })

      Logger.info("CasGC: quarantined unreferenced blob #{hash}")
    end
  end

  defp finalize_quarantined(hash, dry_run?) do
    case fresh_blob_status(hash) do
      {:ok, :packed} ->
        protect_packed_blob(hash, dry_run?)
        :skipped

      {:ok, :reachable} ->
        restore_referenced_blob(hash, dry_run?)
        :restored

      {:ok, :unreferenced} ->
        delete_quarantined(hash, dry_run?)
        :deleted

      {:error, reason} ->
        log_fresh_scan_failure(hash, :delete, reason)
        :skipped
    end
  end

  defp delete_quarantined(hash, dry_run?) do
    unless dry_run? do
      File.rm(quarantine_path(hash))
      Concord.delete("blob:sha256:#{hash}")
      Concord.delete(candidate_key(hash))
      Logger.info("CasGC: deleted unreferenced blob #{hash}")
    end
  end

  defp fresh_blob_status(hash) do
    case Concord.get_all() do
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

    if File.exists?(qpath) and not File.exists?(dest) do
      File.mkdir_p!(Path.dirname(dest))
      File.rename!(qpath, dest)
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
            unless dry_run? do
              Concord.put(candidate_key(hash), %{
                hash: "sha256:#{hash}",
                reason: :unreferenced,
                stage: :candidate,
                first_seen_at: now,
                eligible_after: now + candidate_grace
              })

              Logger.info("CasGC: marked candidate #{hash}")
            end

            %{acc | candidates_created: acc.candidates_created + 1}
          end
      end
    end)
  end

  defp disk_blobs do
    objects_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "objects", "sha256"])

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
      {:error, _} -> true
    end
  end

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
    Path.join([CAS.data_root(), CAS.reserved_root(), "gc", "quarantine", "sha256-#{hash}"])
  end
end
