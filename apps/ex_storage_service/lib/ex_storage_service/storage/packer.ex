defmodule ExStorageService.Storage.Packer do
  @moduledoc """
  Cold-storage policy for the global CAS (PRD §20 Phase 6).

  Periodically consolidates **loose, reachable** blobs whose files have
  not been modified for `:pack_cold_after` seconds into pack files via
  `Storage.Pack`. Unreachable blobs are left for `CasGC`. The policy is a
  global age threshold; per-bucket S3 lifecycle `Transition` rules are a
  recorded follow-up.

  Configuration (app env, overridable per `pack_now/1` call):
  - `:packer_interval` — sweep interval ms (default 6 h)
  - `:pack_cold_after` — seconds since last modification (default 30 days)
  - `:pack_min_blobs` — skip packing below this count (default 100)
  - `:pack_max_blobs` / `:pack_max_bytes` — per-pack limits (default 1000 / 1 GiB)
  - `:pack_loose_cleanup_after` — seconds to retain loose fallbacks after packing
    (default 24 hours)
  """

  use GenServer

  require Logger

  alias ExStorageService.Storage.{CAS, CasGC, Pack}

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Run one packing pass now. See moduledoc for options."
  def pack_now(opts \\ []), do: pack_now(__MODULE__, opts)

  def pack_now(server, opts), do: GenServer.call(server, {:pack_now, opts}, :infinity)

  @impl true
  def init(opts) do
    interval =
      Keyword.get(
        opts,
        :interval,
        Application.get_env(:ex_storage_service, :packer_interval, :timer.hours(6))
      )

    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:pack, state) do
    do_pack([])
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call({:pack_now, opts}, _from, state), do: {:reply, do_pack(opts), state}

  defp schedule(interval), do: Process.send_after(self(), :pack, interval)

  defp do_pack(opts) do
    cold_after = conf(opts, :cold_after, :pack_cold_after, 30 * 86_400)
    min_blobs = conf(opts, :min_blobs, :pack_min_blobs, 100)
    max_blobs = conf(opts, :max_blobs, :pack_max_blobs, 1000)
    max_bytes = conf(opts, :max_bytes, :pack_max_bytes, 1024 * 1024 * 1024)
    cleanup_after = conf(opts, :cleanup_after, :pack_loose_cleanup_after, 86_400)

    case Concord.get_all() do
      {:ok, all} ->
        reachable = CasGC.reachable_hashes(all)
        packed_hashes = packed_hashes(all)
        mpu_part_hashes = mpu_part_hashes(all)
        gc_candidate_hashes = gc_candidate_hashes(all)
        now = System.os_time(:second)
        loose_deleted = cleanup_packed_loose_blobs(all, now, cleanup_after)

        candidates =
          loose_blobs()
          |> Enum.filter(fn {hash, path} ->
            MapSet.member?(reachable, hash) and
              not MapSet.member?(packed_hashes, hash) and
              not MapSet.member?(mpu_part_hashes, hash) and
              not MapSet.member?(gc_candidate_hashes, hash) and
              cold?(path, now, cold_after)
          end)
          |> take_batch(max_blobs, max_bytes)

        pack_result =
          if length(candidates) < min_blobs do
            {:ok, %{pack_hash: nil, packed: 0}}
          else
            Pack.pack_blobs(Enum.map(candidates, fn {hash, _path} -> hash end))
          end

        case pack_result do
          {:ok, report} -> {:ok, Map.put(report, :loose_deleted, loose_deleted)}
          {:error, _reason} = error -> error
        end

      {:error, reason} ->
        Logger.warning("Packer: metadata scan failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp conf(opts, key, env_key, default) do
    Keyword.get(opts, key, Application.get_env(:ex_storage_service, env_key, default))
  end

  defp packed_hashes(all) do
    all
    |> Enum.reduce([], fn
      {"blob:sha256:" <> hash, meta}, acc ->
        if get_field(meta, :state) == :packed, do: [hash | acc], else: acc

      _, acc ->
        acc
    end)
    |> MapSet.new()
  end

  defp mpu_part_hashes(all) do
    all
    |> Enum.reduce([], fn
      {"mpu_part:" <> _, meta}, acc ->
        case get_field(meta, :hash) do
          hash when is_binary(hash) -> [hash | acc]
          _ -> acc
        end

      _, acc ->
        acc
    end)
    |> MapSet.new()
  end

  defp gc_candidate_hashes(all) do
    all
    |> Enum.reduce([], fn
      {"gc:candidate:" <> hash, _record}, acc -> [hash | acc]
      _, acc -> acc
    end)
    |> MapSet.new()
  end

  # Only the Concord snapshot captured before this pass is eligible, so blobs
  # packed below cannot be removed by the same pass even with a zero grace.
  defp cleanup_packed_loose_blobs(all, now, cleanup_after) do
    deleted =
      Enum.reduce(all, 0, fn
        {"blob:sha256:" <> hash, meta}, acc ->
          if old_packed_blob?(meta, now, cleanup_after) do
            cleanup_packed_loose_blob(hash, acc)
          else
            acc
          end

        _, acc ->
          acc
      end)

    if deleted > 0, do: Logger.info("Packer: removed #{deleted} retained loose packed blobs")
    deleted
  end

  defp old_packed_blob?(meta, now, cleanup_after) do
    get_field(meta, :state) == :packed and
      is_integer(get_field(meta, :packed_at)) and
      now - get_field(meta, :packed_at) >= cleanup_after
  end

  defp cleanup_packed_loose_blob(hash, count) do
    loose_path = CAS.blob_path(hash)

    with true <- File.exists?(loose_path),
         {:ok, _location} <- Pack.locate(hash) do
      case File.rm(loose_path) do
        :ok ->
          count + 1

        {:error, :enoent} ->
          count

        {:error, reason} ->
          Logger.warning("Packer: failed to remove loose fallback #{hash}: #{inspect(reason)}")
          count
      end
    else
      _ -> count
    end
  end

  defp get_field(map, key) when is_map(map), do: map[key] || map[to_string(key)]
  defp get_field(_, _key), do: nil

  defp loose_blobs do
    objects_dir = Path.join([CAS.blob_root(), "objects", "sha256"])

    case File.ls(objects_dir) do
      {:ok, prefixes} ->
        Enum.flat_map(prefixes, fn prefix ->
          dir = Path.join(objects_dir, prefix)

          case File.ls(dir) do
            {:ok, files} -> Enum.map(files, &{prefix <> &1, Path.join(dir, &1)})
            {:error, _} -> []
          end
        end)

      {:error, _} ->
        []
    end
  end

  defp cold?(path, now, cold_after) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> now - mtime >= cold_after
      {:error, _} -> false
    end
  end

  defp take_batch(candidates, max_blobs, max_bytes) do
    candidates
    |> Enum.reduce_while({[], 0}, fn {_hash, path} = entry, {acc, bytes} ->
      size =
        case File.stat(path) do
          {:ok, %File.Stat{size: s}} -> s
          _ -> 0
        end

      cond do
        length(acc) >= max_blobs -> {:halt, {acc, bytes}}
        bytes + size > max_bytes and acc != [] -> {:halt, {acc, bytes}}
        true -> {:cont, {[entry | acc], bytes + size}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end
end
