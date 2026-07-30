defmodule ExStorageService.Metadata.Schema do
  @moduledoc """
  Read-only inventory of legacy and v2 metadata records.

  Counts are collected through bounded Concord pages. Pass a Concord
  `revision:` option to pin every page to one metadata snapshot; without one,
  the result is a live inventory.
  """

  alias ExStorageService.InstanceConfig
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.BlobLocation

  @default_page_size 250
  @max_invalid_samples 100

  @spec status(keyword()) :: {:ok, map()} | {:error, term()}
  def status(opts \\ []) do
    with {:ok, target_replication_factor} <- target_replication_factor(opts),
         {:ok, configured_schema} <- configured_schema(opts),
         {:ok, v1_objects} <-
           inspect_prefix("obj:", &validate_legacy_current_record/3, opts),
         {:ok, v1_versions} <-
           inspect_prefix("obj_ver:", &validate_legacy_version_record/3, opts),
         {:ok, v1_version_lists} <-
           inspect_prefix("obj_ver_list:", &validate_version_list/3, opts),
         {:ok, v2_heads} <-
           inspect_prefix(Keys.object_head_prefix(), &validate_v2_head/3, opts),
         {:ok, v2_versions} <-
           inspect_prefix("ess:v2:object_version:", &validate_v2_version/3, opts),
         {:ok, v2_blobs} <-
           inspect_prefix(
             Keys.blob_prefix(),
             &validate_v2_blob(&1, &2, &3, target_replication_factor),
             opts
           ),
         {:ok, v2_blob_locations} <-
           inspect_prefix("ess:v2:blob_location:", &validate_v2_location/3, opts),
         {:ok, v1_only_objects} <- count_v1_only_objects(opts) do
      v1 = %{
        objects: v1_objects.count,
        versions: v1_versions.count,
        version_lists: v1_version_lists.count
      }

      v2 = %{
        heads: v2_heads.count,
        versions: v2_versions.count,
        blobs: v2_blobs.count,
        blob_locations: v2_blob_locations.count,
        under_target_descriptors: v2_blobs.under_target_count
      }

      inspections = [
        v1_objects,
        v1_versions,
        v1_version_lists,
        v2_heads,
        v2_versions,
        v2_blobs,
        v2_blob_locations
      ]

      invalid_record_count = Enum.sum(Enum.map(inspections, & &1.invalid_count))

      invalid_records =
        inspections
        |> Enum.flat_map(& &1.invalid_records)
        |> Enum.take(@max_invalid_samples)

      migration_ready = invalid_record_count == 0
      replication_ready = v2_blobs.under_target_count == 0
      migration_required = v1_only_objects > 0 or not replication_ready

      {:ok,
       %{
         configured_schema: configured_schema,
         target_replication_factor: target_replication_factor,
         v1: v1,
         v2: v2,
         v1_only_objects: v1_only_objects,
         migration_required: migration_required,
         migration_ready: migration_ready,
         replication_ready: replication_ready,
         migration_complete: migration_ready and not migration_required,
         v2_writes_present: v2.heads > 0 or v2.versions > 0,
         validation: %{
           invalid_record_count: invalid_record_count,
           invalid_records: invalid_records,
           invalid_records_truncated: invalid_record_count > length(invalid_records)
         }
       }}
    end
  end

  defp inspect_prefix(prefix, validator, opts) do
    do_inspect_prefix(prefix, nil, page_size(opts), validator, empty_inspection(), opts)
  end

  defp do_inspect_prefix(prefix, cursor, page_size, validator, inspection, opts) do
    with {:ok, %{entries: entries, next_cursor: next_cursor}} <-
           backend(opts).list_page(prefix, cursor, page_size, read_opts(opts)),
         :ok <- validate_cursor(cursor, next_cursor),
         {:ok, inspection} <- inspect_entries(entries, validator, inspection, opts) do
      inspection = %{inspection | count: inspection.count + length(entries)}

      if next_cursor,
        do:
          do_inspect_prefix(
            prefix,
            next_cursor,
            page_size,
            validator,
            inspection,
            opts
          ),
        else: {:ok, inspection}
    end
  end

  defp inspect_entries(entries, validator, inspection, opts) do
    Enum.reduce_while(entries, {:ok, inspection}, fn entry, {:ok, current} ->
      case validator.(entry.key, entry.value, opts) do
        :ok ->
          {:cont, {:ok, current}}

        {:invalid, reason} ->
          invalid_records =
            if length(current.invalid_records) < @max_invalid_samples,
              do: [%{key: entry.key, reason: reason} | current.invalid_records],
              else: current.invalid_records

          {:cont,
           {:ok,
            %{
              current
              | invalid_count: current.invalid_count + 1,
                invalid_records: invalid_records
            }}}

        {:under_target, _actual_replication_factor} ->
          {:cont, {:ok, %{current | under_target_count: current.under_target_count + 1}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_legacy_object(_key, value, _opts) when is_map(value) do
    delete_marker =
      field(value, :delete_marker, field(value, :is_delete_marker, false)) == true

    case {delete_marker, field(value, :content_hash), field(value, :size)} do
      {true, _hash, _size} ->
        :ok

      {false, hash, size}
      when is_binary(hash) and hash != "" and is_integer(size) and size >= 0 ->
        :ok

      _other ->
        {:invalid, :invalid_legacy_object}
    end
  end

  defp validate_legacy_object(_key, _value, _opts), do: {:invalid, :expected_map}

  defp validate_legacy_current_record(key, value, opts) do
    with {:ok, _bucket, _object_key} <- decode_legacy_key(key, "obj:"),
         :ok <- validate_legacy_object(key, value, opts) do
      :ok
    else
      {:error, reason} -> {:invalid, reason}
      {:invalid, reason} -> {:invalid, reason}
    end
  end

  defp validate_legacy_version_record(key, value, opts) when is_map(value) do
    with :ok <- validate_legacy_object(key, value, opts),
         version_id when is_binary(version_id) and version_id != "" <- field(value, :version_id),
         {:ok, bucket, object_key} <- decode_legacy_version_key(key, version_id),
         {:ok, %{value: version_ids}} <-
           backend(opts).get("obj_ver_list:#{bucket}:#{object_key}", read_opts(opts)),
         true <- is_list(version_ids) and version_id in version_ids do
      :ok
    else
      nil -> {:invalid, :missing_version_id}
      {:ok, nil} -> {:invalid, :missing_version_list}
      {:ok, _record} -> {:invalid, :invalid_version_list}
      {:error, reason} -> {:error, reason}
      false -> {:invalid, :unindexed_legacy_version}
      _other -> {:invalid, :invalid_legacy_version_key}
    end
  end

  defp validate_legacy_version_record(_key, _value, _opts),
    do: {:invalid, :expected_map}

  defp validate_version_list(key, version_ids, opts) when is_list(version_ids) do
    with true <-
           Enum.all?(version_ids, &(is_binary(&1) and &1 != "")) and
             length(version_ids) == length(Enum.uniq(version_ids)),
         {:ok, bucket, object_key} <- decode_legacy_key(key, "obj_ver_list:"),
         :ok <- validate_legacy_versions_exist(bucket, object_key, version_ids, opts) do
      :ok
    else
      false -> {:invalid, :invalid_version_ids}
      {:error, reason} -> {:invalid, reason}
      {:backend_error, reason} -> {:error, reason}
    end
  end

  defp validate_version_list(_key, _value, _opts),
    do: {:invalid, :expected_version_id_list}

  defp validate_legacy_versions_exist(bucket, key, version_ids, opts) do
    Enum.reduce_while(version_ids, :ok, fn version_id, :ok ->
      case backend(opts).get("obj_ver:#{bucket}:#{key}:#{version_id}", read_opts(opts)) do
        {:ok, %{value: value}} when is_map(value) ->
          case {validate_legacy_object("", value, opts), field(value, :version_id)} do
            {:ok, ^version_id} ->
              {:cont, :ok}

            {{:invalid, reason}, _stored_version_id} ->
              {:halt, {:error, {:invalid_legacy_version, version_id, reason}}}

            {:ok, stored_version_id} ->
              {:halt, {:error, {:legacy_version_id_mismatch, version_id, stored_version_id}}}
          end

        {:ok, nil} ->
          {:halt, {:error, {:missing_legacy_version, version_id}}}

        {:ok, _record} ->
          {:halt, {:error, {:invalid_legacy_version, version_id}}}

        {:error, reason} ->
          {:halt, {:backend_error, reason}}
      end
    end)
  end

  defp validate_v2_head(key, value, opts) when is_map(value) do
    bucket = field(value, :bucket)
    object_key = field(value, :key)
    version_id = field(value, :version_id)

    with 2 <- field(value, :schema),
         true <- is_binary(bucket) and is_binary(object_key) and is_binary(version_id),
         true <- key == Keys.object_head(bucket, object_key),
         version_key = Keys.object_version(bucket, object_key, version_id),
         {:ok, %{value: version}} <- backend(opts).get(version_key, read_opts(opts)),
         :ok <- validate_v2_version(version_key, version, opts) do
      :ok
    else
      {:ok, nil} -> {:invalid, :dangling_head}
      {:error, reason} -> {:error, reason}
      {:invalid, reason} -> {:invalid, {:invalid_head_version, reason}}
      _other -> {:invalid, :invalid_object_head}
    end
  end

  defp validate_v2_head(_key, _value, _opts), do: {:invalid, :expected_map}

  defp validate_v2_version(key, value, opts) when is_map(value) do
    bucket = field(value, :bucket)
    object_key = field(value, :key)
    version_id = field(value, :version_id)
    parent_version_id = field(value, :parent_version_id)

    with true <-
           field(value, :schema) == 2 and is_binary(bucket) and is_binary(object_key) and
             is_binary(version_id) and
             (is_nil(parent_version_id) or is_binary(parent_version_id)) and
             key == Keys.object_version(bucket, object_key, version_id),
         :ok <- validate_parent_version(bucket, object_key, parent_version_id, opts),
         :ok <- validate_version_blob(value, opts) do
      :ok
    else
      false -> {:invalid, :invalid_object_version}
      {:invalid, reason} -> {:invalid, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_v2_version(_key, _value, _opts), do: {:invalid, :expected_map}

  defp validate_parent_version(_bucket, _key, nil, _opts), do: :ok

  defp validate_parent_version(bucket, key, parent_version_id, opts) do
    case backend(opts).get(
           Keys.object_version(bucket, key, parent_version_id),
           read_opts(opts)
         ) do
      {:ok, %{value: value}} when is_map(value) -> :ok
      {:ok, nil} -> {:invalid, {:dangling_parent_version, parent_version_id}}
      {:ok, _record} -> {:invalid, {:invalid_parent_version, parent_version_id}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_version_blob(value, opts) do
    delete_marker =
      field(value, :delete_marker, field(value, :is_delete_marker, false)) == true

    if delete_marker do
      :ok
    else
      hash = field(value, :content_hash)
      size = field(value, :size)

      if is_binary(hash) and hash != "" and is_integer(size) and size >= 0,
        do: validate_version_blob_descriptor(hash, size, opts),
        else: {:invalid, :invalid_version_blob_identity}
    end
  end

  defp validate_version_blob_descriptor(hash, size, opts) do
    case backend(opts).get(Keys.blob(hash), read_opts(opts)) do
      {:ok, %{value: descriptor}} when is_map(descriptor) ->
        if field(descriptor, :schema) == 2 and field(descriptor, :hash) == hash and
             field(descriptor, :algorithm) == :sha256 and field(descriptor, :size) == size,
           do: :ok,
           else: {:invalid, {:blob_descriptor_identity_mismatch, hash}}

      {:ok, nil} ->
        {:invalid, {:missing_blob_descriptor, hash}}

      {:ok, _record} ->
        {:invalid, {:invalid_blob_descriptor, hash}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_v2_blob(key, value, _opts, target_replication_factor) when is_map(value) do
    hash = field(value, :hash)
    size = field(value, :size)
    replication_factor = field(value, :desired_replication_factor)

    if field(value, :schema) == 2 and is_binary(hash) and hash != "" and
         field(value, :algorithm) == :sha256 and is_integer(size) and size >= 0 and
         is_integer(replication_factor) and replication_factor > 0 and key == Keys.blob(hash) do
      if replication_factor < target_replication_factor,
        do: {:under_target, replication_factor},
        else: :ok
    else
      {:invalid, :invalid_blob_descriptor}
    end
  end

  defp validate_v2_blob(_key, _value, _opts, _target_replication_factor),
    do: {:invalid, :expected_map}

  defp validate_v2_location(key, value, _opts) do
    case BlobLocation.cast(value) do
      {:ok, location} ->
        if key == Keys.blob_location(location.hash, location.node_id),
          do: :ok,
          else: {:invalid, :blob_location_key_mismatch}

      {:error, reason} ->
        {:invalid, reason}
    end
  end

  defp decode_legacy_key(key, prefix) do
    with true <- String.starts_with?(key, prefix),
         [bucket, object_key] <-
           key |> String.replace_prefix(prefix, "") |> String.split(":", parts: 2),
         true <- bucket != "" do
      {:ok, bucket, object_key}
    else
      _other -> {:error, :invalid_legacy_key}
    end
  end

  defp decode_legacy_version_key("obj_ver:" <> rest, version_id) do
    suffix = ":" <> version_id

    if String.ends_with?(rest, suffix) do
      rest
      |> String.replace_suffix(suffix, "")
      |> then(fn bucket_and_key ->
        case String.split(bucket_and_key, ":", parts: 2) do
          [bucket, object_key] when bucket != "" -> {:ok, bucket, object_key}
          _other -> {:error, :invalid_legacy_version_key}
        end
      end)
    else
      {:error, :invalid_legacy_version_key}
    end
  end

  defp decode_legacy_version_key(_key, _version_id),
    do: {:error, :invalid_legacy_version_key}

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp empty_inspection,
    do: %{count: 0, invalid_count: 0, invalid_records: [], under_target_count: 0}

  defp count_v1_only_objects(opts) do
    with {:ok, versioned} <-
           count_v1_only_prefix("obj_ver_list:", :version_list, nil, 0, opts),
         {:ok, current_only} <- count_v1_only_prefix("obj:", :current, nil, 0, opts) do
      {:ok, versioned + current_only}
    end
  end

  defp count_v1_only_prefix(prefix, kind, cursor, count, opts) do
    with {:ok, %{entries: entries, next_cursor: next_cursor}} <-
           backend(opts).list_page(prefix, cursor, page_size(opts), read_opts(opts)),
         :ok <- validate_cursor(cursor, next_cursor),
         {:ok, count} <- count_v1_only_entries(entries, kind, count, opts) do
      if next_cursor,
        do: count_v1_only_prefix(prefix, kind, next_cursor, count, opts),
        else: {:ok, count}
    end
  end

  defp count_v1_only_entries(entries, kind, count, opts) do
    Enum.reduce_while(entries, {:ok, count}, fn entry, {:ok, current_count} ->
      case v1_only_object?(entry.key, kind, opts) do
        {:ok, true} -> {:cont, {:ok, current_count + 1}}
        {:ok, false} -> {:cont, {:ok, current_count}}
        :invalid -> {:cont, {:ok, current_count}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp v1_only_object?(key, :version_list, opts) do
    case decode_legacy_key(key, "obj_ver_list:") do
      {:ok, bucket, object_key} -> missing_v2_head?(bucket, object_key, opts)
      {:error, _reason} -> :invalid
    end
  end

  defp v1_only_object?(key, :current, opts) do
    with {:ok, bucket, object_key} <- decode_legacy_key(key, "obj:"),
         {:ok, version_list} <-
           backend(opts).get("obj_ver_list:#{bucket}:#{object_key}", read_opts(opts)) do
      case version_list do
        nil -> missing_v2_head?(bucket, object_key, opts)
        %{value: version_ids} when is_list(version_ids) -> {:ok, false}
        _invalid -> :invalid
      end
    else
      {:error, :invalid_legacy_key} -> :invalid
      {:error, reason} -> {:error, reason}
    end
  end

  defp missing_v2_head?(bucket, object_key, opts) do
    case backend(opts).get(Keys.object_head(bucket, object_key), read_opts(opts)) do
      {:ok, nil} -> {:ok, true}
      {:ok, %{value: _head}} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_cursor(cursor, cursor) when not is_nil(cursor),
    do: {:error, :metadata_scan_cursor_did_not_advance}

  defp validate_cursor(_cursor, _next_cursor), do: :ok

  defp configured_schema(opts) do
    case Keyword.fetch(opts, :metadata_schema) do
      {:ok, schema} when schema in [:v1, :v2] ->
        {:ok, schema}

      {:ok, _schema} ->
        {:error, :invalid_metadata_schema}

      :error ->
        case InstanceConfig.from_application_env() do
          {:ok, config} -> {:ok, config.metadata_schema}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp target_replication_factor(opts) do
    case Keyword.fetch(opts, :replication_factor) do
      {:ok, replication_factor}
      when is_integer(replication_factor) and replication_factor > 0 ->
        {:ok, replication_factor}

      {:ok, _replication_factor} ->
        {:error, :invalid_replication_factor}

      :error ->
        case InstanceConfig.from_application_env() do
          {:ok, config} -> {:ok, config.replication_factor}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp page_size(opts) do
    case Keyword.get(opts, :page_size, @default_page_size) do
      size when is_integer(size) and size > 0 -> size
      _other -> @default_page_size
    end
  end

  defp read_opts(opts) do
    opts
    |> Keyword.take([:consistency, :timeout, :engine, :barrier, :revision])
    |> Keyword.put_new(:consistency, :strong)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
end
