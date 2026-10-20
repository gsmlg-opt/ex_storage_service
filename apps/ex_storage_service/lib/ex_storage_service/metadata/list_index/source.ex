defmodule ExStorageService.Metadata.ListIndex.Source do
  @moduledoc false
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.ListIndex.Manifest

  # Snapshot allocation is explicitly offline. Keep only one object's summary
  # while external-sort runs and the final action stream are written to disk.
  def capture(bucket, generation, snapshot, raw_path, path) when is_map(snapshot) do
    entries =
      Stream.flat_map(snapshot, fn {key, value} ->
        case classify(bucket, key, value, snapshot) do
          nil -> []
          identity -> [{identity, key, value}]
        end
      end)

    with {:ok, _raw} <- Manifest.create(raw_path, entries) do
      actions =
        Manifest.stream(raw_path)
        |> Stream.concat([:finish])
        |> Stream.transform(nil, fn
          :finish, state ->
            {finish(state), nil}

          {{domain, group, _, _}, _key, _value} = row, state ->
            identity = {domain, group}

            if state != nil and state.identity == identity do
              consume(row, state, bucket, generation, snapshot)
            else
              previous = finish(state)
              {initial, state} = initialize(identity, bucket, generation, snapshot)
              {actions, state} = consume(row, state, bucket, generation, snapshot)
              {previous ++ initial ++ actions, state}
            end
        end)

      Manifest.create(path, actions)
    end
  catch
    {:source_error, reason} -> {:error, reason}
    {:manifest_error, reason} -> {:error, reason}
  end

  defp classify(bucket, key, value, snapshot) do
    head_prefix = Keys.object_head_prefix() <> Keys.encode_component(bucket) <> ":"
    version_prefix = "ess:v2:object_version:" <> Keys.encode_component(bucket) <> ":"

    cond do
      String.starts_with?(key, head_prefix) ->
        object = decode!(String.replace_prefix(key, head_prefix, ""), key)
        identity!(value, bucket, object, nil, key)
        {0, object, 0, key}

      String.starts_with?(key, version_prefix) ->
        case String.split(String.replace_prefix(key, version_prefix, ""), ":", parts: 2) do
          [encoded, id] when id != "" ->
            object = decode!(encoded, key)
            identity!(value, bucket, object, id, key)
            {0, object, 1, key}

          _ ->
            fail(key, :invalid_version_key)
        end

      String.starts_with?(key, "obj:#{bucket}:") ->
        object = String.replace_prefix(key, "obj:#{bucket}:", "")
        identity!(value, bucket, object, nil, key, true)
        {0, object, 0, key}

      String.starts_with?(key, "obj_ver_list:#{bucket}:") ->
        unless is_list(value) and Enum.all?(value, &(is_binary(&1) and &1 != "")),
          do: fail(key, :invalid_legacy_version_list)

        {0, String.replace_prefix(key, "obj_ver_list:#{bucket}:", ""), 0, key}

      String.starts_with?(key, "obj_ver:#{bucket}:") ->
        id = field(value, :version_id) || key |> String.split(":") |> List.last()

        unless is_binary(id) and id != "" and String.ends_with?(key, ":" <> id),
          do: fail(key, :invalid_legacy_version_identity)

        suffix = String.replace_prefix(key, "obj_ver:#{bucket}:", "")
        object = binary_part(suffix, 0, byte_size(suffix) - byte_size(id) - 1)
        identity!(value, bucket, object, id, key, true)
        {0, object, 1, key}

      String.starts_with?(key, "mpu:#{bucket}:") ->
        id = String.replace_prefix(key, "mpu:#{bucket}:", "")
        upload_identity!(value, bucket, id, key)
        {1, id, 0, key}

      String.starts_with?(key, Keys.multipart_upload_prefix()) and field(value, :bucket) == bucket ->
        id = decode!(String.replace_prefix(key, Keys.multipart_upload_prefix(), ""), key)
        upload_identity!(value, bucket, id, key)
        {1, id, 0, key}

      String.starts_with?(key, "mpu_part:#{bucket}:") ->
        {id, number} = legacy_part!(key, bucket)
        ensure_upload!(snapshot, bucket, id, key)
        part_identity!(value, number, key)
        {1, id, 1, number}

      String.starts_with?(key, "ess:v2:multipart_part:") ->
        case String.split(String.replace_prefix(key, "ess:v2:multipart_part:", ""), ":", parts: 2) do
          [encoded, raw_number] ->
            id = decode!(encoded, key)
            upload = snapshot[Keys.multipart_upload(id)]
            legacy = snapshot["mpu:#{bucket}:#{id}"]

            if field(upload || legacy, :bucket) == bucket do
              number = number!(raw_number, key)
              part_identity!(value, number, key)
              {1, id, 1, number}
            end

          _ ->
            fail(key, :invalid_part_key)
        end

      true ->
        nil
    end
  end

  defp initialize({0, object} = identity, bucket, _generation, snapshot) do
    head_key = Keys.object_head(bucket, object)
    head = snapshot[head_key]
    current = snapshot["obj:#{bucket}:#{object}"]
    listed = snapshot["obj_ver_list:#{bucket}:#{object}"] || []

    preferred =
      if head do
        field(head, :version_id)
      else
        Enum.find(listed, fn id ->
          Map.has_key?(snapshot, Keys.object_version(bucket, object, id)) or
            Map.has_key?(snapshot, "obj_ver:#{bucket}:#{object}:#{id}")
        end) || if(current, do: field(current, :version_id) || "null")
      end

    state = %{
      identity: identity,
      bucket: bucket,
      key: object,
      head: head,
      preferred: preferred,
      candidate: nil,
      selected: nil,
      count: 0
    }

    initial =
      if head == nil and is_map(current) do
        id = field(current, :version_id) || "null"
        target = Keys.object_version(bucket, object, id)

        if Map.has_key?(snapshot, target) or
             Map.has_key?(snapshot, "obj_ver:#{bucket}:#{object}:#{id}") do
          []
        else
          version = normalize(current, bucket, object, id)

          [
            action(
              {0, object, 1, target},
              :version,
              [{"obj:#{bucket}:#{object}", current}],
              target,
              version,
              nil
            )
          ]
        end
      else
        []
      end

    state =
      Enum.reduce(initial, state, fn {_sort, action}, state -> select(state, action.value) end)

    {initial, state}
  end

  defp initialize({1, id} = identity, bucket, generation, snapshot) do
    current_key = Keys.multipart_upload(id)
    legacy_key = "mpu:#{bucket}:#{id}"
    current = snapshot[current_key]
    legacy = snapshot[legacy_key]
    upload = current || legacy || fail(id, :orphan_part)

    if current && legacy && field(current, :key) != field(legacy, :key),
      do: fail(id, :conflicting_upload_identity)

    upload_identity!(upload, bucket, id, id)

    if field(upload, :bucket_generation) not in [nil, generation],
      do: fail(id, :bucket_generation_changed)

    key = if current, do: current_key, else: legacy_key

    upload =
      upload
      |> Map.put(:bucket_generation, generation)
      |> Map.drop([
        :parts_root,
        :parts_count,
        :parts_index_schema,
        :parts_index_epoch,
        :parts_index_complete,
        :expected_parts_count
      ])

    sources =
      for {key, value} <- [{current_key, current}, {legacy_key, legacy}],
          value != nil,
          do: {key, value}

    init = action({1, id, 0, ""}, :upload_init, sources, key, upload, current || legacy)
    {[init], %{identity: identity, bucket: bucket, id: id, key: key, upload: upload, parts: 0}}
  end

  defp consume({{0, object, _rank, _}, source_key, value}, state, bucket, _generation, snapshot) do
    legacy = String.starts_with?(source_key, "obj_ver:")
    version? = legacy or String.starts_with?(source_key, "ess:v2:object_version:")

    if version? do
      id = field(value, :version_id) || source_key |> String.split(":") |> List.last()
      target = Keys.object_version(bucket, object, id)

      if legacy and (state.head != nil or Map.has_key?(snapshot, target)) do
        {[action({0, object, 0, source_key}, :assert, [{source_key, value}], nil, nil, nil)],
         state}
      else
        normalized = normalize(value, bucket, object, id)

        entry =
          action(
            {0, object, 1, target},
            :version,
            [{source_key, value}],
            target,
            normalized,
            snapshot[target]
          )

        {[entry], select(state, normalized)}
      end
    else
      {[action({0, object, 0, source_key}, :assert, [{source_key, value}], nil, nil, nil)], state}
    end
  end

  defp consume({{1, id, rank, _}, source_key, value}, state, bucket, _generation, snapshot) do
    if rank == 0 do
      {[], state}
    else
      number = field(value, :part_number)
      current_key = Keys.multipart_part(id, number)
      legacy_key = "mpu_part:#{bucket}:#{id}:#{number}"
      current = snapshot[current_key]
      legacy = snapshot[legacy_key]
      if current && legacy && current != legacy, do: fail(source_key, :conflicting_part)
      chosen = if current, do: current_key, else: legacy_key

      if source_key == chosen do
        sources =
          for {key, value} <- [{current_key, current}, {legacy_key, legacy}],
              value != nil,
              do: {key, value}

        entry = action({1, id, 1, number}, :part, sources, chosen, value, value)
        {[entry], %{state | parts: state.parts + 1}}
      else
        {[], state}
      end
    end
  end

  defp finish(nil), do: []

  defp finish(%{identity: {0, object}} = state) do
    selected = state.selected || state.candidate

    head =
      cond do
        state.head != nil ->
          id = field(state.head, :version_id)

          if id != nil and (selected == nil or selected.version_id != id),
            do: fail(Keys.object_head(state.bucket, object), :missing_head_version)

          state.head

        selected != nil ->
          %{
            schema: 2,
            bucket: state.bucket,
            key: object,
            version_id: selected.version_id,
            delete_marker: selected.delete_marker,
            etag: Map.get(selected, :etag),
            updated_at: selected.created_at
          }

        true ->
          nil
      end

    if head do
      key = Keys.object_head(state.bucket, object)
      [action({0, object, 2, key}, :head, [], key, head, state.head)]
    else
      []
    end
  end

  defp finish(%{identity: {1, id}} = state) do
    [
      action(
        {1, id, 2, ""},
        :upload_finish,
        [],
        state.key,
        Map.put(state.upload, :expected_parts_count, state.parts),
        nil
      )
    ]
  end

  defp select(state, version) do
    candidate =
      case state.candidate do
        nil ->
          version

        previous ->
          if {version.created_at, version.version_id} >
               {previous.created_at, previous.version_id},
             do: version,
             else: previous
      end

    selected = if version.version_id == state.preferred, do: version, else: state.selected
    %{state | candidate: candidate, selected: selected, count: state.count + 1}
  end

  defp normalize(value, bucket, key, id) when is_map(value) do
    marker = field(value, :delete_marker) || field(value, :is_delete_marker) || false
    time = field(value, :created_at) || field(value, :updated_at) || ""
    unless is_binary(time), do: fail(key, :invalid_timestamp)
    unless is_boolean(marker), do: fail(key, :invalid_delete_marker)

    value
    |> Map.merge(%{
      schema: 2,
      bucket: bucket,
      key: key,
      version_id: id,
      created_at: time,
      delete_marker: marker,
      is_delete_marker: marker
    })
    |> Map.put_new(
      :operation_id,
      "listing-backfill:" <> Keys.encode_component(bucket <> <<0>> <> key <> <<0>> <> id)
    )
    |> Map.put_new(:object_type, :blob)
    |> normalize_hash()
  end

  defp normalize_hash(%{content_hash: "sha256:" <> hash} = value),
    do: %{value | content_hash: hash}

  defp normalize_hash(value), do: value

  defp action(sort, type, sources, key, value, previous),
    do: {sort, %{type: type, sources: sources, key: key, value: value, previous: previous}}

  defp identity!(value, bucket, key, id, source, legacy? \\ false) do
    valid =
      is_map(value) and
        (field(value, :bucket) == bucket or (legacy? and field(value, :bucket) == nil)) and
        (field(value, :key) == key or (legacy? and field(value, :key) == nil)) and
        (id == nil or field(value, :version_id) == id or
           (legacy? and field(value, :version_id) == nil))

    unless valid, do: fail(source, :conflicting_object_identity)
  end

  defp upload_identity!(value, bucket, id, key) do
    unless is_map(value) and field(value, :bucket) == bucket and field(value, :upload_id) == id and
             is_binary(field(value, :key)) and
             field(value, :status) in [:initiated, :completing, :completed, :aborted],
           do: fail(key, :invalid_upload_identity)
  end

  defp part_identity!(value, number, key) do
    unless is_map(value) and field(value, :part_number) == number and
             is_binary(field(value, :etag)) and is_integer(field(value, :size)) and
             field(value, :size) >= 0,
           do: fail(key, :invalid_part_identity)
  end

  defp ensure_upload!(snapshot, bucket, id, key) do
    unless snapshot[Keys.multipart_upload(id)] || snapshot["mpu:#{bucket}:#{id}"],
      do: fail(key, :orphan_part)
  end

  defp legacy_part!(key, bucket) do
    suffix = String.replace_prefix(key, "mpu_part:#{bucket}:", "")

    case String.split(suffix, ":") |> Enum.reverse() do
      [number | id] when id != [] ->
        {id |> Enum.reverse() |> Enum.join(":"), number!(number, key)}

      _ ->
        fail(key, :invalid_part_key)
    end
  end

  defp number!(value, key) do
    case Integer.parse(value) do
      {number, ""} when number in 1..10_000 -> number
      _ -> fail(key, :invalid_part_number)
    end
  end

  defp decode!(value, key) do
    case Keys.decode_component(value) do
      {:ok, decoded} -> decoded
      _ -> fail(key, :invalid_encoded_key)
    end
  end

  defp field(value, key) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key)))

  defp field(_, _), do: nil
  defp fail(key, reason), do: throw({:source_error, {key, reason}})
end
