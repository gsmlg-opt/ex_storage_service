defmodule ExStorageService.Metadata.ListIndex.Multipart do
  @moduledoc """
  Multipart membership and part trees in the original metadata transaction.

  Existing uploads require an explicitly completed parts-index backfill. New
  uploads initialize an empty tree; ready requests never infer empty legacy
  state from a missing descriptor or scan either historical namespace.
  """

  alias ExStorageService.Metadata.{Keys, ListIndex}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.ListIndex.{Codec, Radix}

  @active [:initiated, :completing]
  @part_fields [
    :part_number,
    :etag,
    :size,
    :hash,
    :uploaded_at,
    :checksums,
    :checksum_algorithm,
    :checksum_type
  ]

  def augment(backend, bucket, root, operations, opts) do
    changes = Enum.map(operations, &operation_entry/1)

    ids =
      changes
      |> Enum.map(fn {key, _} -> upload_identity(key, bucket) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    reader = &backend.get(&1, read_opts(opts))
    tree = Radix.new(reader, ListIndex.namespace(bucket, root, :uploads), root.uploads_root)
    initial = %{tree: tree, replacements: %{}, compare: [], index_operations: []}

    with {:ok, planned} <-
           reduce(ids, initial, fn id, state ->
             plan_upload(backend, bucket, root, id, changes, state, opts)
           end) do
      upload_ops = Radix.operations(planned.tree)
      metadata_operations = decorate(operations, planned.replacements)

      {:ok,
       %{
         metadata_operations: metadata_operations,
         compare: planned.compare ++ upload_ops.compare,
         index_operations: planned.index_operations ++ upload_ops.success,
         root_id: upload_ops.root_id,
         count_delta: upload_ops.count_delta
       }}
    end
  end

  def parts_namespace(bucket, root, upload_id),
    do: ListIndex.namespace(bucket, root, :parts) <> Keys.encode_component(upload_id) <> ":"

  def parts_page(bucket, upload_id, opts \\ []) do
    do_parts_page(bucket, upload_id, opts, false)
  end

  @doc false
  def parts_inventory_page(bucket, upload_id, opts \\ []),
    do: do_parts_page(bucket, upload_id, opts, true)

  defp do_parts_page(bucket, upload_id, opts, terminal?) do
    marker = Keyword.get(opts, :part_number_marker, 0)
    maximum = Keyword.get(opts, :max_parts, 1000)
    backend = Keyword.get(opts, :backend, Backend)

    if is_integer(marker) and marker in 0..10_000 and is_integer(maximum) and maximum in 0..1000 do
      tree_fun = fn reader, root ->
        with {:ok, _key, %{value: upload}} <- read_upload(reader, bucket, upload_id),
             :ok <- readable_upload(upload, bucket, upload_id, root, terminal?),
             :ok <- parts_ready(upload, root) do
          {:ok, parts_namespace(bucket, root, upload_id), upload.parts_root}
        end
      end

      bound = if marker == 0, do: <<>>, else: Codec.part(marker)

      page_opts =
        Keyword.merge(opts,
          tree_fun: tree_fun,
          limit: maximum,
          exclusive: marker > 0,
          select: &select_part/2
        )

      with {:ok, page} <- ListIndex.page(bucket, :uploads, bound, page_opts),
           {:ok, _key, %{value: upload}} <-
             read_upload(&backend.get(&1, read_opts(opts)), bucket, upload_id),
           true <- terminal? or upload.status in @active do
        selected = Enum.take(page.entries, maximum)
        parts = Enum.map(selected, &elem(&1, 1))
        truncated = maximum > 0 and length(page.entries) > maximum

        {:ok,
         %{
           parts: parts,
           part_number_marker: marker,
           max_parts: maximum,
           next_part_number_marker: if(truncated, do: List.last(parts).part_number),
           is_truncated: truncated,
           checksum_algorithm: get_in(upload, [:attributes, :checksum_algorithm]),
           checksum_type: get_in(upload, [:attributes, :checksum_type]),
           stats: page.stats
         }}
      else
        false -> {:error, :not_found}
        error -> error
      end
    else
      {:error, :invalid_pagination}
    end
  end

  def uploads_page(bucket, opts \\ []) do
    maximum = Keyword.get(opts, :max_uploads, 1000)
    prefix = Keyword.get(opts, :prefix, "")
    delimiter = Keyword.get(opts, :delimiter)
    marker = Keyword.get(opts, :key_marker, "")
    upload_marker = Keyword.get(opts, :upload_id_marker, "")

    if is_integer(maximum) and maximum in 0..1000 and valid_text?(prefix) and
         valid_text?(marker) and valid_text?(upload_marker) and
         (is_nil(delimiter) or valid_text?(delimiter)) do
      {marker_bound, exclusive} = upload_bound(marker, upload_marker)
      prefix_bound = Codec.key_prefix(prefix)
      bound = max(marker_bound, prefix_bound)
      exclusive = exclusive and bound == marker_bound
      select = &select_upload(&1, &2, prefix, delimiter, marker)
      page_opts = Keyword.merge(opts, limit: maximum, exclusive: exclusive, select: select)

      with {:ok, page} <- ListIndex.page(bucket, :uploads, bound, page_opts) do
        selected = Enum.take(page.entries, maximum)
        truncated = maximum > 0 and length(page.entries) > maximum
        last = if truncated, do: List.last(selected)

        {:ok,
         %{
           uploads: for({_, upload} <- selected, upload != :common_prefix, do: upload),
           common_prefixes: for({common, :common_prefix} <- selected, do: common),
           prefix: prefix,
           delimiter: delimiter,
           key_marker: marker,
           upload_id_marker: upload_marker,
           next_key_marker: if(last, do: item_key(last)),
           next_upload_id_marker: if(last, do: item_upload_id(last)),
           max_uploads: maximum,
           is_truncated: truncated,
           encoding_type: Keyword.get(opts, :encoding_type),
           stats: page.stats
         }}
      end
    else
      {:error, :invalid_pagination}
    end
  end

  defp plan_upload(backend, bucket, root, id, changes, state, opts) do
    reader = &backend.get(&1, read_opts(opts))

    with {:ok, observed_key, observed} <- read_optional_upload(reader, bucket, id),
         {:ok, upload_key, next} <- next_upload(changes, bucket, id, observed_key, observed),
         :ok <- identity(value(observed), bucket, id, root),
         :ok <- identity(next, bucket, id, root),
         {:ok, upload, part_ops} <-
           plan_parts(reader, bucket, root, id, value(observed), next, changes),
         {:ok, tree} <- remove_upload(state.tree, value(observed)),
         {:ok, tree} <- insert_upload(tree, upload) do
      {:ok,
       %{
         state
         | tree: tree,
           replacements: Map.put(state.replacements, upload_key, upload),
           compare:
             Enum.uniq([
               compare(observed_key, observed),
               compare(upload_key, if(upload_key == observed_key, do: observed)) | state.compare
             ]) ++ part_ops.compare,
           index_operations: state.index_operations ++ part_ops.success
       }}
    end
  end

  defp plan_parts(_reader, _bucket, _root, _id, _old, nil, _changes),
    do: {:ok, nil, %{compare: [], success: []}}

  defp plan_parts(reader, bucket, root, id, old, upload, changes) do
    with {:ok, upload} <- initialize_parts(old, upload, root),
         {:ok, changes} <- part_changes(changes, bucket, id) do
      tree = Radix.new(reader, parts_namespace(bucket, root, id), upload.parts_root)

      with {:ok, tree} <-
             reduce(changes, tree, fn {number, part}, tree ->
               case part do
                 nil ->
                   Radix.delete(tree, Codec.part(number))

                 value ->
                   with {:ok, summary} <- part_summary(number, value),
                        do: Radix.put(tree, Codec.part(number), summary)
               end
             end) do
        operations = Radix.operations(tree)

        updated = %{
          upload
          | parts_root: operations.root_id,
            parts_count: upload.parts_count + operations.count_delta
        }

        if updated.parts_count >= 0,
          do: {:ok, updated, operations},
          else: {:error, :listing_index_corrupt}
      end
    end
  end

  defp initialize_parts(nil, upload, root) do
    {:ok,
     Map.merge(upload, %{
       parts_index_schema: 1,
       parts_root: nil,
       parts_count: 0,
       parts_index_epoch: root.epoch,
       parts_index_complete: true
     })}
  end

  defp initialize_parts(old, upload, root) do
    with :ok <- parts_ready(old, root) do
      {:ok,
       Map.merge(
         upload,
         Map.take(
           old,
           [
             :parts_index_schema,
             :parts_root,
             :parts_count,
             :parts_index_epoch,
             :parts_index_complete
           ]
         )
       )}
    end
  end

  defp parts_ready(upload, root) do
    cond do
      Map.get(upload, :parts_index_complete) != true ->
        {:error, :listing_index_migration_required}

      Map.get(upload, :parts_index_schema) != 1 or
          Map.get(upload, :parts_index_epoch) != root.epoch ->
        {:error, :listing_index_corrupt}

      not is_integer(Map.get(upload, :parts_count)) or upload.parts_count < 0 or
          upload.parts_count > 10_000 ->
        {:error, :listing_index_corrupt}

      not Map.has_key?(upload, :parts_root) or
          not (is_nil(upload.parts_root) or
                   (is_binary(upload.parts_root) and byte_size(upload.parts_root) == 16)) ->
        {:error, :listing_index_corrupt}

      true ->
        :ok
    end
  end

  defp next_upload(changes, bucket, id, observed_key, observed) do
    writes = Enum.filter(changes, fn {key, _} -> upload_record_id(key, bucket) == id end)

    case writes do
      [] when not is_nil(observed) ->
        {:ok, observed_key, observed.value}

      [{key, next}] ->
        if observed != nil and observed_key != key,
          do: {:error, :multipart_index_identity_conflict},
          else: {:ok, key, next}

      _ ->
        {:error, :multipart_index_identity_conflict}
    end
  end

  defp part_changes(changes, bucket, id) do
    Enum.reduce_while(changes, {:ok, %{}}, fn {key, value}, {:ok, parts} ->
      case part_identity(key, bucket) do
        {^id, number} ->
          if Map.has_key?(parts, number) and parts[number] != value do
            {:halt, {:error, :multipart_index_identity_conflict}}
          else
            {:cont, {:ok, Map.put(parts, number, value)}}
          end

        _ ->
          {:cont, {:ok, parts}}
      end
    end)
  end

  defp part_summary(number, part) when is_map(part) do
    if Map.get(part, :part_number) == number and is_binary(Map.get(part, :etag)) and
         is_integer(Map.get(part, :size)) and part.size >= 0 do
      {:ok, Map.take(part, @part_fields)}
    else
      {:error, :listing_index_corrupt}
    end
  end

  defp part_summary(_number, _part), do: {:error, :listing_index_corrupt}

  defp select_part(encoded, summary) do
    with {:ok, number} <- Codec.decode_part(encoded),
         {:ok, ^summary} <- part_summary(number, summary) do
      {:emit, encoded, summary}
    else
      _ -> {:error, :listing_index_corrupt}
    end
  end

  defp remove_upload(tree, %{status: status} = upload) when status in @active,
    do: Radix.delete(tree, Codec.upload(upload.key, upload.upload_id))

  defp remove_upload(tree, _), do: {:ok, tree}

  defp insert_upload(tree, %{status: status} = upload) when status in @active do
    summary = Map.take(upload, [:bucket, :key, :upload_id, :created_at])
    Radix.put(tree, Codec.upload(upload.key, upload.upload_id), summary)
  end

  defp insert_upload(tree, _), do: {:ok, tree}

  defp readable_upload(upload, bucket, id, root, terminal?) do
    with :ok <- identity(upload, bucket, id, root) do
      if terminal? or upload.status in @active, do: :ok, else: {:error, :not_found}
    end
  end

  defp identity(nil, _bucket, _id, _root), do: :ok

  defp identity(upload, bucket, id, root) when is_map(upload) do
    if Map.get(upload, :bucket) == bucket and Map.get(upload, :upload_id) == id and
         is_binary(Map.get(upload, :key)) and
         Map.get(upload, :bucket_generation) == root.bucket_generation and
         Map.get(upload, :status) in (@active ++ [:aborted, :completed]),
       do: :ok,
       else: {:error, :multipart_index_identity_conflict}
  end

  defp identity(_upload, _bucket, _id, _root), do: {:error, :multipart_index_identity_conflict}

  defp read_upload(reader, bucket, id) do
    case read_optional_upload(reader, bucket, id) do
      {:ok, _key, nil} -> {:error, :not_found}
      result -> result
    end
  end

  defp read_optional_upload(reader, bucket, id) do
    key = Keys.multipart_upload(id)

    with {:ok, record} <- reader.(key) do
      if record == nil do
        legacy = "mpu:#{bucket}:#{id}"
        with {:ok, record} <- reader.(legacy), do: {:ok, legacy, record}
      else
        {:ok, key, record}
      end
    end
  end

  defp decorate(operations, replacements) do
    seen =
      MapSet.new(
        Enum.map(operations, fn operation -> operation |> operation_entry() |> elem(0) end)
      )

    decorated =
      Enum.map(operations, fn operation ->
        {key, _} = operation_entry(operation)

        case Map.fetch(replacements, key) do
          {:ok, nil} -> {:delete, {:key, key}, %{}}
          {:ok, value} -> {:put, key, value, %{}}
          :error -> operation
        end
      end)

    extra =
      for {key, value} <- replacements, not MapSet.member?(seen, key), do: {:put, key, value, %{}}

    decorated ++ extra
  end

  defp upload_identity(key, bucket) do
    case upload_record_id(key, bucket) do
      nil ->
        case part_identity(key, bucket) do
          {id, _} -> id
          nil -> nil
        end

      id ->
        id
    end
  end

  defp upload_record_id(key, bucket) do
    legacy = "mpu:#{bucket}:"
    current = Keys.multipart_upload_prefix()

    cond do
      String.starts_with?(key, legacy) -> String.replace_prefix(key, legacy, "")
      String.starts_with?(key, current) -> decode(String.replace_prefix(key, current, ""))
      true -> nil
    end
  end

  defp part_identity(key, bucket) do
    current = "ess:v2:multipart_part:"
    legacy = "mpu_part:#{bucket}:"

    cond do
      String.starts_with?(key, current) ->
        case String.split(String.replace_prefix(key, current, ""), ":", parts: 2) do
          [id, number] -> parsed_part(decode(id), number)
          _ -> nil
        end

      String.starts_with?(key, legacy) ->
        case String.split(String.replace_prefix(key, legacy, ""), ":") |> Enum.reverse() do
          [number | reversed_id] when reversed_id != [] ->
            parsed_part(Enum.reverse(reversed_id) |> Enum.join(":"), number)

          _ ->
            nil
        end

      true ->
        nil
    end
  end

  defp parsed_part(nil, _), do: nil

  defp parsed_part(id, number) do
    case Integer.parse(number) do
      {number, ""} when number in 1..10_000 -> {id, number}
      _ -> nil
    end
  end

  defp upload_bound("", _upload_id), do: {<<>>, false}
  defp upload_bound(key, ""), do: {Codec.prefix_successor(Codec.upload_prefix(key)), false}
  defp upload_bound(key, upload_id), do: {Codec.upload(key, upload_id), true}

  defp select_upload(encoded, summary, prefix, delimiter, marker) do
    with {:ok, %{key: key, upload_id: id}} <- Codec.decode_upload(encoded),
         true <- Map.get(summary, :key) == key and Map.get(summary, :upload_id) == id do
      if String.starts_with?(key, prefix) do
        rest = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))

        match =
          if is_binary(delimiter) and delimiter != "",
            do: :binary.match(rest, delimiter),
            else: :nomatch

        case match do
          :nomatch ->
            {:emit, encoded, summary}

          {position, length} ->
            common = prefix <> binary_part(rest, 0, position + length)

            successor =
              case Codec.prefix_successor(common) do
                nil -> nil
                raw -> Codec.key_prefix(raw)
              end

            if common <= marker, do: {:skip, successor}, else: {:prefix, common, successor}
        end
      else
        :stop
      end
    else
      _ -> {:error, :listing_index_corrupt}
    end
  end

  defp item_key({key, :common_prefix}), do: key
  defp item_key({_, upload}), do: upload.key
  defp item_upload_id({_, :common_prefix}), do: ""
  defp item_upload_id({_, upload}), do: upload.upload_id

  defp valid_text?(text),
    do: is_binary(text) and String.valid?(text) and not String.contains?(text, <<0>>)

  defp decode(value) do
    case Keys.decode_component(value) do
      {:ok, decoded} -> decoded
      :error -> nil
    end
  end

  defp value(nil), do: nil
  defp value(record), do: record.value
  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, record), do: {:mod_revision, key, :==, record.mod_revision}
  defp operation_entry({:put, key, value, _}), do: {key, value}
  defp operation_entry({:delete, {:key, key}, _}), do: {key, nil}

  defp read_opts(opts),
    do: Keyword.take(opts, [:engine, :timeout, :barrier]) ++ [consistency: :linearizable]

  defp reduce(entries, initial, fun) do
    Enum.reduce_while(entries, {:ok, initial}, fn entry, {:ok, state} ->
      case fun.(entry, state) do
        {:ok, next} -> {:cont, {:ok, next}}
        error -> {:halt, error}
      end
    end)
  end
end
