defmodule ExStorageService.Storage.ContentGC do
  @moduledoc """
  Background process that periodically scans for unreferenced content files
  and removes them to reclaim disk space.

  Content files are stored by their SHA-256 hash. This GC compares files on disk
  against content hashes referenced in metadata, and deletes any orphans.
  """

  use GenServer
  require Logger

  @default_interval :timer.minutes(30)
  @default_orphan_grace_seconds 86_400

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Trigger a GC run manually (useful for testing).
  """
  def run_now(server \\ __MODULE__) do
    GenServer.call(server, :run_now, :infinity)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval)
    orphan_grace_seconds = configured_orphan_grace_seconds(opts)
    schedule_gc(interval)
    {:ok, %{interval: interval, orphan_grace_seconds: orphan_grace_seconds}}
  end

  @impl true
  def handle_info(:run_gc, state) do
    run_once(orphan_grace_seconds: state.orphan_grace_seconds)
    schedule_gc(state.interval)
    {:noreply, state}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    result = run_once(orphan_grace_seconds: state.orphan_grace_seconds)
    {:reply, result, state}
  end

  @doc false
  def run_once(opts \\ []) do
    data_root =
      Keyword.get(
        opts,
        :data_root,
        Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
      )

    orphan_grace_seconds = configured_orphan_grace_seconds(opts)
    backend = Keyword.get(opts, :backend, Concord)

    with {:ok, referenced_hashes} <- get_referenced_hashes(backend) do
      delete_orphans(data_root, referenced_hashes, orphan_grace_seconds)
    else
      {:error, reason} ->
        Logger.warning("ContentGC: metadata scan failed; sweep aborted: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp schedule_gc(interval) do
    Process.send_after(self(), :run_gc, interval)
  end

  defp delete_orphans(data_root, referenced_hashes, orphan_grace_seconds) do
    # Get all content files on disk
    disk_hashes = get_disk_content_hashes(data_root)

    # Find unreferenced files that are also older than the grace period.
    now = System.os_time(:second)

    orphans =
      Enum.filter(disk_hashes, fn {bucket, hash, path} ->
        not MapSet.member?(referenced_hashes, {bucket, hash}) and
          older_than_grace?(path, now, orphan_grace_seconds)
      end)

    # Delete orphans
    deleted =
      Enum.reduce(orphans, 0, fn {_bucket, _hash, path}, count ->
        case File.rm(path) do
          :ok ->
            Logger.info("ContentGC: removed orphan #{path}")
            count + 1

          {:error, :enoent} ->
            count

          {:error, reason} ->
            Logger.warning("ContentGC: failed to remove #{path}: #{inspect(reason)}")
            count
        end
      end)

    if deleted > 0 do
      Logger.info("ContentGC: cleaned up #{deleted} orphaned content files")
    end

    {:ok, deleted}
  end

  # Returns true when the file's last-modified time is older than the grace
  # window (or when it cannot be stat'd, in which case it is safe to reclaim).
  defp older_than_grace?(path, now, orphan_grace_seconds) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> now - mtime > orphan_grace_seconds
      {:error, _} -> false
    end
  end

  defp get_referenced_hashes(backend) do
    case backend.get_all() do
      {:ok, all} ->
        {:ok,
         all
         |> Enum.filter(fn {k, _v} ->
           # Include both current object metadata and versioned object metadata.
           # Content is content-addressed: a file is referenced if ANY metadata
           # key (obj: or obj_ver:) points to its hash. If we only check obj:
           # we would GC content that versioned objects still need.
           String.starts_with?(k, "obj:") or String.starts_with?(k, "obj_ver:")
         end)
         |> Enum.flat_map(fn {k, v} ->
           case k do
             "obj:" <> rest ->
               # key format: "obj:{bucket}:{object_key}"
               case String.split(rest, ":", parts: 2) do
                 [bucket, _key] ->
                   case Map.get(v, :content_hash) do
                     nil -> []
                     hash -> [{bucket, hash}]
                   end

                 _ ->
                   []
               end

             "obj_ver:" <> rest ->
               # key format: "obj_ver:{bucket}:{key}:{version_id}"
               case String.split(rest, ":", parts: 2) do
                 [bucket, _rest] ->
                   case Map.get(v, :content_hash) do
                     nil -> []
                     hash -> [{bucket, hash}]
                   end

                 _ ->
                   []
               end

             _ ->
               []
           end
         end)
         |> MapSet.new()}

      {:error, _} = error ->
        error

      other ->
        {:error, other}
    end
  end

  defp configured_orphan_grace_seconds(opts) do
    Keyword.get_lazy(opts, :orphan_grace_seconds, fn ->
      fallback =
        Application.get_env(
          :ex_storage_service,
          :orphan_grace_seconds,
          @default_orphan_grace_seconds
        )

      case Application.get_env(:ex_storage_service, :instance_config, []) do
        config when is_list(config) -> Keyword.get(config, :orphan_grace_seconds, fallback)
        config when is_map(config) -> Map.get(config, :orphan_grace_seconds, fallback)
        _other -> fallback
      end
    end)
  end

  defp get_disk_content_hashes(data_root) do
    case File.ls(data_root) do
      {:ok, entries} ->
        entries
        # The reserved global-CAS root is not a bucket; its blobs are
        # managed by the Phase 4 CAS GC, never by this legacy sweep.
        |> Enum.reject(&(&1 == ExStorageService.Storage.CAS.reserved_root()))
        |> Enum.flat_map(fn bucket_dir ->
          objects_dir = Path.join([data_root, bucket_dir, "objects"])
          list_content_files(bucket_dir, objects_dir)
        end)

      {:error, _} ->
        []
    end
  end

  defp list_content_files(bucket, objects_dir) do
    case File.ls(objects_dir) do
      {:ok, prefixes} ->
        Enum.flat_map(prefixes, fn prefix ->
          prefix_dir = Path.join(objects_dir, prefix)

          case File.ls(prefix_dir) do
            {:ok, files} ->
              Enum.map(files, fn file ->
                hash = prefix <> file
                path = Path.join(prefix_dir, file)
                {bucket, hash, path}
              end)

            {:error, _} ->
              []
          end
        end)

      {:error, _} ->
        []
    end
  end
end
