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
  """

  use GenServer

  require Logger

  alias ExStorageService.Storage.{CAS, CasGC, Pack}

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Run one packing pass now. See moduledoc for options."
  def pack_now(opts \\ []), do: GenServer.call(__MODULE__, {:pack_now, opts}, :infinity)

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

    case Concord.get_all() do
      {:ok, all} ->
        reachable = CasGC.reachable_hashes(all)
        now = System.os_time(:second)

        candidates =
          loose_blobs()
          |> Enum.filter(fn {hash, path} ->
            MapSet.member?(reachable, hash) and cold?(path, now, cold_after)
          end)
          |> take_batch(max_blobs, max_bytes)

        if length(candidates) < min_blobs do
          {:ok, %{pack_hash: nil, packed: 0}}
        else
          Pack.pack_blobs(Enum.map(candidates, fn {hash, _path} -> hash end))
        end

      {:error, reason} ->
        Logger.warning("Packer: metadata scan failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp conf(opts, key, env_key, default) do
    Keyword.get(opts, key, Application.get_env(:ex_storage_service, env_key, default))
  end

  defp loose_blobs do
    objects_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "objects", "sha256"])

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
