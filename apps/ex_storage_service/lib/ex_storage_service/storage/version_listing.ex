defmodule ExStorageService.Storage.VersionListing do
  @moduledoc """
  Coherent S3 version pages from the transactional per-bucket index.

  The snapshot adapter remains an offline migration validation oracle. Public
  listing never scans legacy records or the global metadata namespace.
  """
  alias ExStorageService.Metadata.Keys

  def list(bucket, opts \\ []) do
    with {:ok, query} <- options(opts) do
      ExStorageService.Metadata.ListIndex.Listing.versions(bucket, query, opts)
    end
  end

  @doc false
  def page(snapshot, bucket, opts \\ []) do
    with {:ok, query} <- options(opts), do: build(Map.new(snapshot), bucket, query)
  end

  defp options(opts) do
    query =
      Map.new(
        Keyword.take(opts, [
          :prefix,
          :delimiter,
          :max_keys,
          :key_marker,
          :version_id_marker,
          :encoding_type
        ])
      )

    query =
      Map.merge(
        %{
          prefix: "",
          delimiter: nil,
          max_keys: 1000,
          key_marker: nil,
          version_id_marker: nil,
          encoding_type: nil
        },
        query
      )

    query =
      Map.new(query, fn {key, value} ->
        {key, if(value == "" and key != :prefix, do: nil, else: value)}
      end)

    if is_integer(query.max_keys) and query.max_keys >= 0 and is_binary(query.prefix) and
         Enum.all?(
           [query.prefix, query.delimiter, query.key_marker, query.version_id_marker],
           &valid_text?/1
         ) and
         query.encoding_type in [nil, "url"] and
         (is_nil(query.version_id_marker) or not is_nil(query.key_marker)) do
      {:ok, %{query | max_keys: min(query.max_keys, 1000)}}
    else
      {:error, :invalid_argument}
    end
  end

  defp valid_text?(nil), do: true

  defp valid_text?(value),
    do: is_binary(value) and String.valid?(value) and not String.contains?(value, <<0>>)

  defp build(snapshot, bucket, query) do
    if Map.has_key?(snapshot, "bucket:#{bucket}") do
      {v2, heads} = v2_records(snapshot, bucket)
      legacy = legacy_records(snapshot, bucket, MapSet.new(Map.keys(heads) ++ Map.keys(v2)))
      versions = Map.merge(legacy, v2)

      entries =
        versions
        |> Enum.flat_map(fn {key, rows} ->
          latest = latest_id(snapshot, bucket, key, rows, heads)
          rows |> Enum.map(&Map.put(&1, :is_latest, &1.version_id == latest)) |> sort_versions()
        end)

      with :ok <- validate_marker(entries, query) do
        items =
          entries
          |> Enum.filter(&String.starts_with?(&1.key, query.prefix))
          |> group(query)
          |> Enum.sort_by(& &1.key)
          |> after_marker(query)

        selected = Enum.take(items, query.max_keys)
        truncated = query.max_keys > 0 and length(items) > query.max_keys
        last = if truncated, do: List.last(selected)

        {:ok,
         Map.merge(query, %{
           entries: Enum.reject(selected, & &1.common_prefix),
           common_prefixes: for(%{common_prefix: true, key: key} <- selected, do: key),
           is_truncated: truncated,
           next_key_marker: if(last, do: last.key),
           next_version_id_marker: if(last && not last.common_prefix, do: last.version_id)
         })}
      end
    else
      {:error, :bucket_not_found}
    end
  end

  defp v2_records(snapshot, bucket) do
    encoded_bucket = Keys.encode_component(bucket) <> ":"

    Enum.reduce(snapshot, {%{}, %{}}, fn {record_key, value}, {versions, heads} ->
      cond do
        String.starts_with?(record_key, "ess:v2:object_version:" <> encoded_bucket) ->
          rest = String.replace_prefix(record_key, "ess:v2:object_version:" <> encoded_bucket, "")

          case String.split(rest, ":", parts: 2) do
            [encoded_key, id] ->
              case Keys.decode_component(encoded_key) do
                {:ok, key} ->
                  {Map.update(
                     versions,
                     key,
                     [entry(key, id, value)],
                     &[entry(key, id, value) | &1]
                   ), heads}

                :error ->
                  {versions, heads}
              end

            _ ->
              {versions, heads}
          end

        String.starts_with?(record_key, Keys.object_head_prefix() <> encoded_bucket) ->
          encoded_key =
            String.replace_prefix(record_key, Keys.object_head_prefix() <> encoded_bucket, "")

          case Keys.decode_component(encoded_key) do
            {:ok, key} -> {versions, Map.put(heads, key, Map.get(value, :version_id))}
            :error -> {versions, heads}
          end

        true ->
          {versions, heads}
      end
    end)
  end

  defp legacy_records(snapshot, bucket, suppressed) do
    prefix = "obj_ver:#{bucket}:"

    versions =
      Enum.reduce(snapshot, %{}, fn {record_key, value}, result ->
        if String.starts_with?(record_key, prefix) do
          suffix = String.replace_prefix(record_key, prefix, "")

          case String.split(suffix, ":") |> Enum.reverse() do
            [id, _ | _] = parts ->
              key = parts |> tl() |> Enum.reverse() |> Enum.join(":")

              if MapSet.member?(suppressed, key),
                do: result,
                else:
                  Map.update(result, key, [entry(key, id, value)], &[entry(key, id, value) | &1])

            _ ->
              result
          end
        else
          result
        end
      end)

    object_prefix = "obj:#{bucket}:"

    Enum.reduce(snapshot, versions, fn {record_key, value}, result ->
      if String.starts_with?(record_key, object_prefix) do
        key = String.replace_prefix(record_key, object_prefix, "")

        if MapSet.member?(suppressed, key) or Map.has_key?(result, key),
          do: result,
          else: Map.put(result, key, [entry(key, Map.get(value, :version_id, "null"), value)])
      else
        result
      end
    end)
  end

  defp latest_id(snapshot, bucket, key, rows, heads) do
    if Map.has_key?(heads, key) do
      heads[key]
    else
      case Map.get(snapshot, "obj_ver_list:#{bucket}:#{key}", []) do
        [id | _] -> if Enum.any?(rows, &(&1.version_id == id)), do: id, else: newest(rows)
        _ -> newest(rows)
      end
    end
  end

  defp newest(rows), do: rows |> sort_versions() |> hd() |> Map.fetch!(:version_id)

  defp sort_versions(rows) do
    Enum.sort_by(
      rows,
      fn row -> {Map.get(row, :is_latest, false), row.last_modified, row.version_id} end,
      :desc
    )
  end

  defp entry(key, id, metadata) do
    %{
      key: key,
      version_id: id,
      metadata: metadata,
      common_prefix: false,
      delete_marker:
        Map.get(metadata, :delete_marker, Map.get(metadata, :is_delete_marker, false)),
      last_modified: Map.get(metadata, :created_at, Map.get(metadata, :updated_at, ""))
    }
  end

  defp validate_marker(entries, %{key_marker: key, version_id_marker: version})
       when is_binary(version) do
    if Enum.any?(entries, &(&1.key == key and &1.version_id == version)),
      do: :ok,
      else: {:error, :invalid_argument}
  end

  defp validate_marker(_entries, _query), do: :ok

  defp group(entries, %{delimiter: nil}), do: entries

  defp group(entries, query) do
    entries
    |> Enum.map(fn entry ->
      rest = String.replace_prefix(entry.key, query.prefix, "")

      case :binary.match(rest, query.delimiter) do
        {position, length} ->
          %{key: query.prefix <> binary_part(rest, 0, position + length), common_prefix: true}

        :nomatch ->
          entry
      end
    end)
    |> Enum.uniq_by(fn
      %{common_prefix: true, key: key} -> {:prefix, key}
      entry -> {:version, entry.key, entry.version_id}
    end)
  end

  defp after_marker(entries, %{key_marker: nil}), do: entries

  defp after_marker(entries, %{key_marker: marker, version_id_marker: nil}),
    do: Enum.reject(entries, &(&1.key <= marker))

  defp after_marker(entries, %{key_marker: marker, version_id_marker: version}) do
    entries
    |> Enum.drop_while(&(&1.key < marker))
    |> Enum.drop_while(fn
      %{key: ^marker, common_prefix: false, version_id: id} -> id != version
      %{key: ^marker, common_prefix: true} -> true
      _ -> false
    end)
    |> case do
      [%{key: ^marker, common_prefix: false, version_id: ^version} | rest] -> rest
      rest -> rest
    end
  end
end
