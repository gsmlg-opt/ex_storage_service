defmodule ExStorageService.Metadata.ListIndex.Listing do
  @moduledoc false
  alias ExStorageService.Metadata.{Keys, ListIndex}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.ListIndex.Codec

  def objects(bucket, opts) do
    prefix = Keyword.get(opts, :prefix, "")
    cursor = Keyword.get(opts, :continuation_token) || ""
    limit = Keyword.get(opts, :max_keys, 1000)
    reader = reader(opts)

    select = fn _tree_key, summary ->
      select(summary, prefix, Keyword.get(opts, :delimiter), cursor, :current, reader)
    end

    with {:ok, result} <-
           ListIndex.page(
             bucket,
             :current,
             max(prefix, cursor),
             Keyword.merge(opts,
               limit: limit,
               exclusive: cursor >= prefix and cursor != "",
               select: select
             )
           ) do
      entries = Enum.take(result.entries, limit)
      truncated = length(result.entries) > limit

      {:ok,
       %{
         keys: for({key, metadata} <- entries, metadata != :common_prefix, do: {key, metadata}),
         common_prefixes: for({key, :common_prefix} <- entries, do: key),
         is_truncated: truncated,
         next_continuation_token: if(truncated, do: entries |> List.last() |> elem(0)),
         index_stats: result.stats
       }}
    end
  end

  def versions(bucket, query, opts) do
    reader = reader(opts)

    select = fn _tree_key, summary ->
      select(summary, query.prefix, query.delimiter, query.key_marker || "", :versions, reader)
    end

    bound_fun = fn read -> version_bound(read, bucket, query) end

    with {:ok, result} <-
           ListIndex.page(
             bucket,
             :versions,
             "",
             Keyword.merge(opts,
               limit: query.max_keys,
               bound_fun: bound_fun,
               select: select
             )
           ) do
      selected = Enum.take(result.entries, query.max_keys)
      truncated = length(result.entries) > query.max_keys
      last = if truncated, do: List.last(selected)

      {:ok,
       Map.merge(query, %{
         entries: for({_, entry} <- selected, entry != :common_prefix, do: entry),
         common_prefixes: for({key, :common_prefix} <- selected, do: key),
         is_truncated: truncated,
         next_key_marker: if(last, do: elem(last, 0)),
         next_version_id_marker:
           case last do
             {_key, %{version_id: id}} -> id
             _ -> nil
           end,
         index_stats: result.stats
       })}
    end
  end

  defp version_bound(reader, bucket, %{version_id_marker: id} = query) when is_binary(id) do
    with {:ok, %{value: metadata}} <- reader.(Keys.object_version(bucket, query.key_marker, id)),
         {:ok, head} <- reader.(Keys.object_head(bucket, query.key_marker)) do
      latest = head && Map.get(head.value, :version_id) == id
      bound = Codec.version(query.key_marker, id, metadata, !!latest)
      prefix_bound = Codec.key_prefix(query.prefix)
      {:ok, max(bound, prefix_bound), bound >= prefix_bound}
    else
      {:ok, nil} -> {:error, :invalid_argument}
      {:error, _} = error -> error
    end
  end

  defp version_bound(_reader, _bucket, query) do
    prefix = Codec.key_prefix(query.prefix)

    case query.key_marker do
      nil ->
        {:ok, prefix, false}

      marker ->
        bound = Codec.prefix_successor(Codec.version_prefix(marker))
        {:ok, max(prefix, bound), false}
    end
  end

  defp select(summary, prefix, delimiter, marker, kind, reader) do
    if String.starts_with?(summary.key, prefix) do
      case common_prefix(summary.key, prefix, delimiter) do
        nil ->
          hydrate(summary, kind, reader)

        common ->
          successor = successor(common, kind)
          if common <= marker, do: {:skip, successor}, else: {:prefix, common, successor}
      end
    else
      :stop
    end
  end

  defp hydrate(summary, kind, reader) do
    case reader.(summary.metadata_key) do
      {:ok, %{value: metadata}} ->
        entry =
          if kind == :current do
            metadata
          else
            summary
            |> Map.drop([:metadata_key, :etag, :size])
            |> Map.merge(%{metadata: metadata, common_prefix: false})
          end

        {:emit, summary.key, entry}

      {:ok, nil} ->
        {:error, :listing_index_corrupt}

      error ->
        error
    end
  end

  defp common_prefix(_key, _prefix, delimiter) when delimiter in [nil, ""], do: nil

  defp common_prefix(key, prefix, delimiter) do
    rest = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))

    case :binary.match(rest, delimiter) do
      {offset, length} -> prefix <> binary_part(rest, 0, offset + length)
      :nomatch -> nil
    end
  end

  defp successor(prefix, :current), do: Codec.prefix_successor(prefix)

  defp successor(prefix, :versions) do
    case Codec.prefix_successor(prefix) do
      nil -> nil
      next -> Codec.key_prefix(next)
    end
  end

  defp reader(opts) do
    backend = Keyword.get(opts, :backend, Backend)
    read_opts = Keyword.take(opts, [:engine, :timeout, :barrier]) ++ [consistency: :linearizable]
    &backend.get(&1, read_opts)
  end
end
