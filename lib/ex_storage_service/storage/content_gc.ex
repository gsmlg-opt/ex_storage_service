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

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Trigger a GC run manually (useful for testing).
  """
  def run_now do
    GenServer.call(__MODULE__, :run_now, :infinity)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval)
    schedule_gc(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:run_gc, state) do
    do_gc()
    schedule_gc(state.interval)
    {:noreply, state}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    result = do_gc()
    {:reply, result, state}
  end

  defp schedule_gc(interval) do
    Process.send_after(self(), :run_gc, interval)
  end

  defp do_gc do
    data_root = Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")

    # Get all buckets from metadata
    referenced_hashes = get_referenced_hashes()

    # Get all content files on disk
    disk_hashes = get_disk_content_hashes(data_root)

    # Find unreferenced files
    orphans =
      Enum.filter(disk_hashes, fn {bucket, hash, _path} ->
        not MapSet.member?(referenced_hashes, {bucket, hash})
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

  defp get_referenced_hashes do
    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "obj:") end)
        |> Enum.map(fn {k, v} ->
          # key format: "obj:bucket:object_key"
          case String.split(k, ":", parts: 3) do
            ["obj", bucket, _key] ->
              {bucket, Map.get(v, :content_hash)}

            _ ->
              nil
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      {:error, _} ->
        MapSet.new()
    end
  end

  defp get_disk_content_hashes(data_root) do
    case File.ls(data_root) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn bucket_dir ->
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
