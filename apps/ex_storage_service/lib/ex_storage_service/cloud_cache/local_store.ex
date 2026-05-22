defmodule ExStorageService.CloudCache.LocalStore do
  @moduledoc """
  Local read-cache for cloud-backed buckets.

  Objects fetched from the remote cloud are stored on disk under:
    `{data_root}/{bucket}/cache/{hash_prefix_2}/{hash_rest}`

  An LRU index is maintained in Concord under `"cloud_cache_index:{bucket}"`.
  Each entry tracks the object key → `{content_hash, size, last_accessed_at}`.

  When the total cached size exceeds `cache_max_bytes`, the least-recently-used
  entries are evicted until the cache fits within the limit.
  """

  require Logger

  alias ExStorageService.CloudCache.Config

  @doc """
  Check whether an object is in the local cache.

  Returns `{:ok, file_path}` on a cache hit, or `:miss` if not cached.
  Also updates `last_accessed_at` on a hit.
  """
  @spec get(String.t(), String.t()) :: {:ok, String.t()} | :miss
  def get(bucket, key) do
    case get_index(bucket) do
      {:ok, index} ->
        case Map.get(index, key) do
          nil ->
            :miss

          %{content_hash: hash} = entry ->
            data_root = data_root()
            path = cache_path(data_root, bucket, hash)

            if File.exists?(path) do
              # Update last_accessed_at for LRU tracking
              updated_entry = %{entry | last_accessed_at: now_iso()}
              updated_index = Map.put(index, key, updated_entry)
              put_index(bucket, updated_index)
              {:ok, path}
            else
              # File missing — remove stale index entry
              updated_index = Map.delete(index, key)
              put_index(bucket, updated_index)
              :miss
            end
        end

      _ ->
        :miss
    end
  end

  @doc """
  Store an object in the local cache.

  `data` is the raw binary content. The content hash is computed here.
  After storing, LRU eviction is triggered if needed.

  Returns `{:ok, file_path}` on success, `{:error, reason}` on failure.
  """
  @spec put(String.t(), String.t(), binary(), Config.t()) ::
          {:ok, String.t()} | {:error, term()}
  def put(bucket, key, data, %Config{} = config) when is_binary(data) do
    data_root = data_root()
    content_hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = cache_path(data_root, bucket, content_hash)

    # Write to disk if not already present (content-addressed, so idempotent)
    unless File.exists?(path) do
      File.mkdir_p!(Path.dirname(path))

      case File.write(path, data) do
        :ok -> :ok
        {:error, reason} -> throw({:write_error, reason})
      end
    end

    # Update index
    size = byte_size(data)

    entry = %{
      content_hash: content_hash,
      size: size,
      last_accessed_at: now_iso()
    }

    {:ok, index} = get_index_or_empty(bucket)
    updated_index = Map.put(index, key, entry)
    put_index(bucket, updated_index)

    # Trigger eviction if over budget
    if config.cache_enabled and config.cache_max_bytes > 0 do
      evict_if_needed(bucket, config.cache_max_bytes)
    end

    {:ok, path}
  catch
    {:write_error, reason} ->
      {:error, reason}
  end

  @doc """
  Remove an object from the local cache (index + disk file, if no other
  index entries reference the same content hash).
  """
  @spec delete(String.t(), String.t()) :: :ok
  def delete(bucket, key) do
    case get_index(bucket) do
      {:ok, index} ->
        case Map.get(index, key) do
          nil ->
            :ok

          %{content_hash: hash} ->
            updated_index = Map.delete(index, key)
            put_index(bucket, updated_index)

            # Only remove the file if no other cache entry in this bucket references it
            still_referenced? =
              Enum.any?(updated_index, fn {_k, e} -> e.content_hash == hash end)

            unless still_referenced? do
              data_root = data_root()
              path = cache_path(data_root, bucket, hash)
              File.rm(path)
            end

            :ok
        end

      _ ->
        :ok
    end
  end

  @doc """
  Clear the entire local cache for a bucket (deletes all cache files and index).
  """
  @spec clear(String.t()) :: :ok
  def clear(bucket) do
    data_root = data_root()
    cache_dir = Path.join([data_root, bucket, "cache"])

    case File.rm_rf(cache_dir) do
      {:ok, _} -> :ok
      {:error, reason, _} -> Logger.warning("CloudCache clear failed: #{inspect(reason)}")
    end

    Concord.delete("cloud_cache_index:#{bucket}")
    :ok
  end

  @doc """
  Return cache statistics for a bucket.

  Returns `%{count: N, total_bytes: N, max_bytes: N}`.
  """
  @spec stats(String.t(), non_neg_integer()) :: map()
  def stats(bucket, max_bytes) do
    case get_index(bucket) do
      {:ok, index} ->
        total = Enum.reduce(index, 0, fn {_k, e}, acc -> acc + e.size end)
        %{count: map_size(index), total_bytes: total, max_bytes: max_bytes}

      _ ->
        %{count: 0, total_bytes: 0, max_bytes: max_bytes}
    end
  end

  ## Private

  defp evict_if_needed(bucket, max_bytes) do
    case get_index(bucket) do
      {:ok, index} ->
        total = Enum.reduce(index, 0, fn {_k, e}, acc -> acc + e.size end)

        if total > max_bytes do
          evict_lru(bucket, index, total, max_bytes)
        end

      _ ->
        :ok
    end
  end

  defp evict_lru(bucket, index, total, max_bytes) do
    # Sort by last_accessed_at ascending (oldest first)
    sorted =
      index
      |> Enum.sort_by(fn {_k, e} -> e.last_accessed_at end)

    {remaining_index, freed} =
      Enum.reduce_while(sorted, {index, 0}, fn {key, entry}, {idx, freed_acc} ->
        current_total = total - freed_acc

        if current_total <= max_bytes do
          {:halt, {idx, freed_acc}}
        else
          # Remove this entry
          updated_idx = Map.delete(idx, key)
          data_root = data_root()
          path = cache_path(data_root, bucket, entry.content_hash)

          still_referenced? =
            Enum.any?(updated_idx, fn {_k, e} -> e.content_hash == entry.content_hash end)

          unless still_referenced? do
            File.rm(path)
          end

          Logger.debug("CloudCache LRU evict: #{bucket}/#{key} (#{entry.size} bytes)")
          {:cont, {updated_idx, freed_acc + entry.size}}
        end
      end)

    put_index(bucket, remaining_index)

    if freed > 0 do
      Logger.info("CloudCache LRU eviction: freed #{freed} bytes from #{bucket}")
    end
  end

  defp get_index(bucket) do
    case Concord.get("cloud_cache_index:#{bucket}") do
      {:ok, nil} -> {:ok, %{}}
      {:ok, index} when is_map(index) -> {:ok, atomize_keys(index)}
      {:ok, _} -> {:ok, %{}}
      error -> error
    end
  end

  defp get_index_or_empty(bucket) do
    case get_index(bucket) do
      {:ok, index} -> {:ok, index}
      _ -> {:ok, %{}}
    end
  end

  defp put_index(bucket, index) do
    Concord.put("cloud_cache_index:#{bucket}", index)
  end

  defp cache_path(data_root, bucket, content_hash) do
    <<prefix::binary-size(2), rest::binary>> = content_hash
    Path.join([data_root, bucket, "cache", prefix, rest])
  end

  defp data_root do
    Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
  end

  defp now_iso do
    DateTime.utc_now() |> DateTime.to_iso8601()
  end

  # Ensure all inner maps have atom keys (Concord may return string keys after round-trip)
  defp atomize_keys(index) when is_map(index) do
    Map.new(index, fn {k, v} ->
      entry =
        if is_map(v) do
          %{
            content_hash: v[:content_hash] || v["content_hash"] || "",
            size: v[:size] || v["size"] || 0,
            last_accessed_at: v[:last_accessed_at] || v["last_accessed_at"] || ""
          }
        else
          v
        end

      {k, entry}
    end)
  end
end
