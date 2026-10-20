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

  alias ExStorageService.CloudCache.Config
  alias ExStorageService.Metadata.{BucketLifecycle, ListIndex}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  @attempts 8

  @doc """
  Check whether an object is in the local cache.

  Returns `{:ok, file_path}` on a cache hit, or `:miss` if not cached.
  Also updates `last_accessed_at` on a hit.
  """
  @spec get(String.t(), String.t()) :: {:ok, String.t()} | :miss
  def get(bucket, key) do
    case locked(bucket, fn ->
           with {:ok, index} <- get_index(bucket),
                %{content_hash: hash} = entry <- Map.get(index, key) do
             path = cache_path(data_root(), bucket, hash)

             if File.regular?(path) do
               _ =
                 update_index(bucket, fn current ->
                   case Map.get(current, key) do
                     ^entry -> Map.put(current, key, %{entry | last_accessed_at: now_iso()})
                     _ -> current
                   end
                 end)

               {:ok, path}
             else
               _ = update_index(bucket, &Map.delete(&1, key))
               :miss
             end
           else
             _ -> :miss
           end
         end) do
      {:ok, path} -> {:ok, path}
      _ -> :miss
    end
  end

  @doc "Returns a cache entry only after its upstream identity has been revalidated."
  def get_validated(bucket, key, validator) do
    with {:ok, index} <- get_index(bucket),
         %{validator: ^validator, content_hash: hash} <- Map.get(index, key),
         path = cache_path(data_root(), bucket, hash),
         true <- File.regular?(path) do
      {:ok, path}
    else
      _ -> :miss
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
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)

    stage =
      Path.join(
        data_root(),
        ".cache-stage-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      )

    with :ok <- File.mkdir_p(data_root()),
         :ok <- File.write(stage, data, [:exclusive]) do
      try do
        put_file(bucket, key, stage, hash, byte_size(data), config)
      after
        File.rm(stage)
      end
    end
  end

  @doc "Publishes a complete staged file in the cache without buffering its contents."
  def put_file(bucket, key, source_path, content_hash, size, %Config{} = config, opts \\ []) do
    path = cache_path(data_root(), bucket, content_hash)

    temporary =
      path <> "." <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false) <> ".tmp"

    result =
      locked(bucket, fn ->
        try do
          with :ok <- File.mkdir_p(Path.dirname(path)),
               :ok <- copy_file(source_path, temporary, path, content_hash, size) do
            entry = %{
              content_hash: content_hash,
              size: size,
              validator: Keyword.get(opts, :validator),
              last_accessed_at: now_iso()
            }

            case update_index(bucket, &Map.put(&1, key, entry), config, opts) do
              {:ok, before, after_index} ->
                remove_unreferenced(bucket, before, after_index)
                # Oversized objects may be immediately evicted, while their
                # caller continues serving its already opened staging descriptor.
                remove_unreferenced(bucket, %{key => entry}, after_index)
                {:ok, path}

              {:error, _} = error ->
                # The local lock excludes a concurrent path replacement. Preserve
                # any prior index reference; otherwise this is our unpublished file.
                case get_index(bucket, opts) do
                  {:ok, index} -> remove_unreferenced(bucket, %{key => entry}, index)
                  _ -> :ok
                end

                error
            end
          end
        after
          File.rm(temporary)
        end
      end)

    outcome =
      case result do
        {:ok, _} -> :ok
        {:error, :cache_busy} -> :busy
        _ -> :error
      end

    ExStorageService.Telemetry.cache(:publish, outcome)
    result
  end

  defp copy_file(source, temporary, destination, expected_hash, expected_size) do
    with {:ok, input} <- File.open(source, [:read, :raw, :binary]) do
      try do
        with {:ok, output} <- File.open(temporary, [:write, :raw, :binary]) do
          try do
            with :ok <-
                   copy_verified(
                     input,
                     output,
                     :crypto.hash_init(:sha256),
                     0,
                     expected_hash,
                     expected_size
                   ),
                 :ok <- :file.sync(output),
                 do: File.rename(temporary, destination)
          after
            File.close(output)
          end
        end
      after
        File.close(input)
      end
    end
  end

  defp copy_verified(input, output, context, size, expected_hash, expected_size) do
    case IO.binread(input, 65_536) do
      :eof ->
        hash = Base.encode16(:crypto.hash_final(context), case: :lower)

        if size == expected_size and hash == expected_hash,
          do: :ok,
          else: {:error, :staged_checksum_mismatch}

      {:error, _} = error ->
        error

      bytes when size + byte_size(bytes) > expected_size ->
        {:error, :staged_checksum_mismatch}

      bytes ->
        with :ok <- IO.binwrite(output, bytes),
             do:
               copy_verified(
                 input,
                 output,
                 :crypto.hash_update(context, bytes),
                 size + byte_size(bytes),
                 expected_hash,
                 expected_size
               )
    end
  end

  @doc """
  Remove an object from the local cache (index + disk file, if no other
  index entries reference the same content hash).
  """
  @spec delete(String.t(), String.t()) :: :ok | {:error, term()}
  def delete(bucket, key) do
    locked(bucket, fn ->
      case update_index(bucket, &Map.delete(&1, key)) do
        {:ok, before, after_index} ->
          remove_unreferenced(bucket, before, after_index)
          :ok

        {:error, _} = error ->
          error
      end
    end)
  end

  @doc """
  Clear the entire local cache for a bucket (deletes all cache files and index).
  """
  @spec clear(String.t()) :: :ok | {:error, term()}
  def clear(bucket) do
    locked(bucket, fn ->
      case update_index(bucket, fn _ -> %{} end) do
        {:ok, _, _} ->
          case File.rm_rf(Path.join([data_root(), bucket, "cache"])) do
            {:ok, _} -> :ok
            {:error, reason, _} -> {:error, reason}
          end

        {:error, _} = error ->
          error
      end
    end)
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

  # Serializes only this node's canonical cache file mutations. The metadata
  # transaction separately fences concurrent nodes and bucket/index generations.
  # Never wait for a lock while retaining a request body or a reader descriptor.
  defp locked(bucket, fun) do
    case :global.trans({{__MODULE__, bucket}, self()}, fun, [node()], 0) do
      :aborted -> {:error, :cache_busy}
      result -> result
    end
  end

  defp update_index(bucket, change, config \\ nil, opts \\ [], attempts \\ @attempts)
  defp update_index(_, _, _, _, 0), do: {:error, :cache_index_conflict}

  defp update_index(bucket, change, config, opts, attempts) do
    backend = Keyword.get(opts, :backend, Backend)
    key = "cloud_cache_index:#{bucket}"

    with {:ok, guard} <- BucketLifecycle.guard(backend, bucket, opts),
         [{:put, _, bucket_value, _}] = guard.success,
         generation = BucketLifecycle.generation(bucket_value),
         index_root = ListIndex.root_key(bucket, generation),
         {:ok, root} <- backend.get(index_root, read_opts(opts)),
         :ok <- ListIndex.ready(root, generation),
         {:ok, observed} <- backend.get(key, read_opts(opts)) do
      opts = Keyword.put_new(opts, :bucket_generation, generation)
      before = if observed, do: atomize_keys(observed.value), else: %{}
      after_index = before |> change.() |> fit(config)

      compare =
        if observed,
          do: {:mod_revision, key, :==, observed.mod_revision},
          else: {:exists, key, :==, false}

      spec = %{
        compare: guard.compare ++ [{:mod_revision, index_root, :==, root.mod_revision}, compare],
        success: guard.success ++ [{:put, key, after_index, %{}}],
        failure: []
      }

      case backend.transaction(spec, read_opts(opts)) do
        {:ok, %{succeeded: true}} -> {:ok, before, after_index}
        {:ok, %{succeeded: false}} -> update_index(bucket, change, config, opts, attempts - 1)
        {:error, _} = error -> error
      end
    end
  end

  defp fit(index, %Config{cache_enabled: true, cache_max_bytes: maximum}) when maximum > 0 do
    total = Enum.reduce(index, 0, fn {_, entry}, bytes -> bytes + entry.size end)

    index
    |> Enum.sort_by(fn {key, entry} -> {entry.last_accessed_at, key} end)
    |> Enum.reduce_while({index, total}, fn {key, entry}, {remaining, size} ->
      if size <= maximum,
        do: {:halt, {remaining, size}},
        else: {:cont, {Map.delete(remaining, key), size - entry.size}}
    end)
    |> elem(0)
  end

  defp fit(index, _), do: index

  defp remove_unreferenced(bucket, before, after_index) do
    retained = MapSet.new(after_index, fn {_, entry} -> entry.content_hash end)

    before
    |> Enum.map(fn {_, entry} -> entry.content_hash end)
    |> Enum.uniq()
    |> Enum.reject(&MapSet.member?(retained, &1))
    |> Enum.each(fn hash -> File.rm(cache_path(data_root(), bucket, hash)) end)
  end

  defp get_index(bucket, opts \\ []) do
    backend = Keyword.get(opts, :backend, Backend)

    case backend.get("cloud_cache_index:#{bucket}", read_opts(opts)) do
      {:ok, nil} -> {:ok, %{}}
      {:ok, %{value: index}} when is_map(index) -> {:ok, atomize_keys(index)}
      {:ok, _} -> {:error, :invalid_cache_index}
      error -> error
    end
  end

  defp read_opts(opts),
    do: Keyword.take(opts, [:engine, :timeout, :barrier]) ++ [consistency: :linearizable]

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
            validator: v[:validator] || v["validator"],
            last_accessed_at: v[:last_accessed_at] || v["last_accessed_at"] || ""
          }
        else
          v
        end

      {k, entry}
    end)
  end
end
