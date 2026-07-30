defmodule ExStorageService.Metadata.Migration do
  @moduledoc """
  Non-destructive, resumable migration from legacy object metadata to v2.

  Legacy mutable version lists are used only as migration indexes. Each object
  is rebuilt as immutable versions plus one head and committed atomically with
  compatible blob descriptors and checksum-verified local location evidence.
  Legacy records are retained for rollback and read compatibility.

  Run with object writes disabled. Source revisions are included in each
  transaction compare, so a concurrent legacy mutation fails closed instead of
  publishing a stale v2 chain.
  """

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Context
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.BlobLocation

  @default_page_size 100
  @default_max_attempts 8
  @ambiguous_errors [:timeout, :unknown, :cluster_not_ready, :no_leader]

  @type report :: %{
          objects_scanned: non_neg_integer(),
          objects_migrated: non_neg_integer(),
          objects_already_v2: non_neg_integer(),
          versions_migrated: non_neg_integer(),
          delete_markers_migrated: non_neg_integer(),
          blob_descriptors_created: non_neg_integer(),
          blob_descriptors_promoted: non_neg_integer(),
          blob_locations_created: non_neg_integer(),
          target_replication_factor: pos_integer(),
          missing_local_blobs: [binary()]
        }

  @spec migrate_v2(keyword()) :: {:ok, report()} | {:error, term()}
  def migrate_v2(opts \\ []) do
    with {:ok, target_replication_factor} <- target_replication_factor(opts),
         opts = Keyword.put(opts, :replication_factor, target_replication_factor),
         :ok <- validate_legacy_version_index(nil, opts),
         report = empty_report(target_replication_factor),
         {:ok, report} <-
           migrate_prefix("obj_ver_list:", :version_list, nil, report, opts),
         {:ok, report} <- migrate_prefix("obj:", :current, nil, report, opts),
         {:ok, promoted} <- promote_descriptors(nil, 0, opts) do
      {:ok,
       %{
         report
         | blob_descriptors_promoted: promoted,
           missing_local_blobs: report.missing_local_blobs |> Enum.uniq() |> Enum.sort()
       }}
    end
  end

  defp empty_report(target_replication_factor) do
    %{
      objects_scanned: 0,
      objects_migrated: 0,
      objects_already_v2: 0,
      versions_migrated: 0,
      delete_markers_migrated: 0,
      blob_descriptors_created: 0,
      blob_descriptors_promoted: 0,
      blob_locations_created: 0,
      target_replication_factor: target_replication_factor,
      missing_local_blobs: []
    }
  end

  defp validate_legacy_version_index(cursor, opts) do
    with {:ok, %{entries: entries, next_cursor: next_cursor}} <-
           backend(opts).list_page("obj_ver:", cursor, page_size(opts), read_opts(opts)),
         :ok <- validate_cursor(cursor, next_cursor),
         :ok <- validate_legacy_version_entries(entries, opts) do
      if next_cursor,
        do: validate_legacy_version_index(next_cursor, opts),
        else: :ok
    end
  end

  defp validate_legacy_version_entries(entries, opts) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case validate_legacy_version_entry(entry, opts) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:legacy_version_index_invalid, entry.key, reason}}}
      end
    end)
  end

  defp validate_legacy_version_entry(%{key: key, value: value}, opts)
       when is_map(value) do
    with version_id when is_binary(version_id) and version_id != "" <- field(value, :version_id),
         {:ok, bucket, object_key} <- decode_legacy_version_key(key, version_id),
         {:ok, %{value: version_ids}} <-
           backend(opts).get("obj_ver_list:#{bucket}:#{object_key}", read_opts(opts)),
         true <- is_list(version_ids),
         true <- version_id in version_ids do
      :ok
    else
      nil -> {:error, :missing_version_id}
      {:ok, nil} -> {:error, :missing_version_list}
      {:ok, _record} -> {:error, :invalid_version_list}
      {:error, reason} -> {:error, reason}
      false -> {:error, :unindexed_legacy_version}
      _other -> {:error, :invalid_legacy_version_key}
    end
  end

  defp validate_legacy_version_entry(_entry, _opts),
    do: {:error, :invalid_legacy_version}

  defp migrate_prefix(prefix, kind, cursor, report, opts) do
    with {:ok, %{entries: entries, next_cursor: next_cursor}} <-
           backend(opts).list_page(prefix, cursor, page_size(opts), read_opts(opts)),
         :ok <- validate_cursor(cursor, next_cursor),
         {:ok, report} <- migrate_entries(entries, kind, report, opts) do
      if next_cursor,
        do: migrate_prefix(prefix, kind, next_cursor, report, opts),
        else: {:ok, report}
    end
  end

  defp promote_descriptors(cursor, promoted, opts) do
    with {:ok, %{entries: entries, next_cursor: next_cursor}} <-
           backend(opts).list_page(Keys.blob_prefix(), cursor, page_size(opts), read_opts(opts)),
         :ok <- validate_cursor(cursor, next_cursor),
         {:ok, promoted} <- promote_descriptor_entries(entries, promoted, opts) do
      if next_cursor,
        do: promote_descriptors(next_cursor, promoted, opts),
        else: {:ok, promoted}
    end
  end

  defp promote_descriptor_entries(entries, promoted, opts) do
    Enum.reduce_while(entries, {:ok, promoted}, fn entry, {:ok, count} ->
      case promote_descriptor(entry.key, page_record(entry), opts, max_attempts(opts)) do
        {:ok, :promoted} -> {:cont, {:ok, count + 1}}
        {:ok, :already_at_target} -> {:cont, {:ok, count}}
        {:error, reason} -> {:halt, {:error, {:descriptor_promotion_failed, entry.key, reason}}}
      end
    end)
  end

  defp promote_descriptor(_key, _record, _opts, 0),
    do: {:error, :compare_retry_exhausted}

  defp promote_descriptor(key, %{value: value} = record, opts, attempts_left)
       when is_map(value) do
    target = Keyword.fetch!(opts, :replication_factor)

    with :ok <- validate_descriptor_key(key, value),
         replication_factor = field(value, :desired_replication_factor),
         true <- is_integer(replication_factor) and replication_factor > 0 do
      if replication_factor >= target do
        {:ok, :already_at_target}
      else
        updated = put_descriptor_replication_factor(value, target)

        spec = %{
          compare: [{:mod_revision, key, :==, record.mod_revision}],
          success: [{:put, key, updated, %{}}],
          failure: []
        }

        idempotency_key =
          "metadata-promote-rf:" <> fingerprint({key, record.mod_revision, target})

        case backend(opts).transaction(spec, transaction_opts(opts, idempotency_key)) do
          {:ok, %{succeeded: true}} ->
            {:ok, :promoted}

          {:ok, %{succeeded: false}} ->
            reread_descriptor_for_promotion(key, opts, attempts_left - 1)

          {:error, reason} when reason in @ambiguous_errors ->
            resolve_descriptor_promotion(
              key,
              idempotency_key,
              opts,
              attempts_left - 1
            )

          {:error, reason} ->
            {:error, reason}
        end
      end
    else
      false -> {:error, :invalid_blob_descriptor}
      {:error, reason} -> {:error, reason}
    end
  end

  defp promote_descriptor(_key, _record, _opts, _attempts_left),
    do: {:error, :invalid_blob_descriptor}

  defp resolve_descriptor_promotion(key, idempotency_key, opts, attempts_left) do
    case backend(opts).resolve_transaction(idempotency_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        {:ok, :promoted}

      _unresolved ->
        reread_descriptor_for_promotion(key, opts, attempts_left)
    end
  end

  defp reread_descriptor_for_promotion(_key, _opts, attempts_left)
       when attempts_left <= 0,
       do: {:error, :compare_retry_exhausted}

  defp reread_descriptor_for_promotion(key, opts, attempts_left) do
    case backend(opts).get(key, read_opts(opts)) do
      {:ok, %{value: _value} = record} ->
        promote_descriptor(key, record, opts, attempts_left)

      {:ok, nil} ->
        {:error, :blob_descriptor_disappeared}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_descriptor_key(key, value) do
    hash = field(value, :hash)
    size = field(value, :size)

    if field(value, :schema) == 2 and is_binary(hash) and hash != "" and
         field(value, :algorithm) == :sha256 and is_integer(size) and size >= 0 and
         key == Keys.blob(hash) do
      :ok
    else
      {:error, :invalid_blob_descriptor}
    end
  end

  defp put_descriptor_replication_factor(value, replication_factor) do
    cond do
      Map.has_key?(value, :desired_replication_factor) ->
        Map.put(value, :desired_replication_factor, replication_factor)

      Map.has_key?(value, "desired_replication_factor") ->
        Map.put(value, "desired_replication_factor", replication_factor)

      true ->
        Map.put(value, :desired_replication_factor, replication_factor)
    end
  end

  defp migrate_entries(entries, kind, report, opts) do
    Enum.reduce_while(entries, {:ok, report}, fn entry, {:ok, current_report} ->
      with {:ok, bucket, key} <- decode_legacy_object_key(entry.key, kind),
           {:ok, plan} <- build_plan(bucket, key, kind, entry, opts),
           {:ok, outcome} <- migrate_plan(plan, opts) do
        next_report =
          if outcome == :skip,
            do: current_report,
            else: merge_outcome(current_report, outcome)

        {:cont, {:ok, next_report}}
      else
        {:error, reason} -> {:halt, {:error, {:metadata_migration_failed, entry.key, reason}}}
      end
    end)
  end

  defp build_plan(bucket, key, :version_list, %{value: version_ids} = source, opts)
       when is_list(version_ids) do
    current_key = "obj:#{bucket}:#{key}"

    with :ok <- validate_version_ids(version_ids),
         {:ok, versions} <- read_legacy_versions(bucket, key, version_ids, opts),
         {:ok, current_record} <- read_optional_record(current_key, opts) do
      versions
      |> prepend_distinct_current(bucket, key, record_value(current_record))
      |> plan(
        bucket,
        key,
        [{source.key, page_record(source)}] ++
          Enum.map(versions, & &1.source) ++ [{current_key, current_record}]
      )
    end
  end

  defp build_plan(_bucket, _key, :version_list, _source, _opts),
    do: {:error, :invalid_legacy_version_list}

  defp build_plan(bucket, key, :current, %{value: current} = source, opts)
       when is_map(current) do
    version_list_key = "obj_ver_list:#{bucket}:#{key}"

    case backend(opts).get(version_list_key, read_opts(opts)) do
      {:ok, nil} ->
        plan(
          [legacy_current(bucket, key, current)],
          bucket,
          key,
          [{source.key, page_record(source)}, {version_list_key, nil}]
        )

      {:ok, %{value: version_ids}} when is_list(version_ids) ->
        {:ok, :skip}

      {:ok, _record} ->
        {:error, :invalid_legacy_version_list}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_plan(_bucket, _key, :current, _source, _opts),
    do: {:error, :invalid_legacy_object}

  defp read_legacy_versions(bucket, key, version_ids, opts) do
    Enum.reduce_while(version_ids, {:ok, []}, fn version_id, {:ok, versions} ->
      legacy_key = "obj_ver:#{bucket}:#{key}:#{version_id}"

      case backend(opts).get(legacy_key, read_opts(opts)) do
        {:ok, %{value: value} = record} when is_map(value) ->
          if field(value, :version_id) == version_id do
            legacy = legacy_version(version_id, value, {legacy_key, record})
            {:cont, {:ok, [legacy | versions]}}
          else
            {:halt,
             {:error, {:legacy_version_id_mismatch, version_id, field(value, :version_id)}}}
          end

        {:ok, nil} ->
          {:halt, {:error, {:missing_legacy_version, version_id}}}

        {:ok, _record} ->
          {:halt, {:error, {:invalid_legacy_version, version_id}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, versions} -> {:ok, Enum.reverse(versions)}
      error -> error
    end
  end

  defp prepend_distinct_current([], _bucket, _key, nil), do: []

  defp prepend_distinct_current([], bucket, key, current),
    do: [legacy_current(bucket, key, current)]

  defp prepend_distinct_current(versions, _bucket, _key, nil), do: versions

  defp prepend_distinct_current([latest | _] = versions, bucket, key, current) do
    if equivalent_legacy?(latest.metadata, current),
      do: versions,
      else: [legacy_current(bucket, key, current) | versions]
  end

  defp plan([], _bucket, _key, _sources), do: {:error, :empty_legacy_object}

  defp plan(legacy_versions, bucket, key, sources) do
    versions =
      legacy_versions
      |> Enum.with_index()
      |> Enum.map(fn {legacy, index} ->
        parent =
          legacy_versions
          |> Enum.at(index + 1)
          |> then(&if(&1, do: &1.version_id, else: nil))

        normalize_version(legacy, bucket, key, parent)
      end)

    [head_version | _] = versions

    {:ok,
     %{
       bucket: bucket,
       key: key,
       sources: sources,
       versions: versions,
       head: %{
         schema: 2,
         bucket: bucket,
         key: key,
         version_id: head_version.version_id,
         delete_marker: head_version.delete_marker,
         etag: field(head_version, :etag),
         updated_at: field(head_version, :created_at)
       }
     }}
  end

  defp migrate_plan(:skip, _opts), do: {:ok, :skip}
  defp migrate_plan(plan, opts), do: commit_plan(plan, opts, max_attempts(opts))

  defp commit_plan(plan, opts, attempts_left) do
    head_key = Keys.object_head(plan.bucket, plan.key)

    case backend(opts).get(head_key, read_opts(opts)) do
      {:ok, %{value: head}} ->
        validate_existing_head(plan, head, opts)

      {:ok, nil} when attempts_left > 0 ->
        with {:ok, blob_state} <- prepare_blobs(plan.versions, opts),
             spec = transaction_spec(plan, blob_state),
             idempotency_key = migration_attempt_key(plan, spec),
             result <- backend(opts).transaction(spec, transaction_opts(opts, idempotency_key)) do
          handle_transaction_result(
            result,
            plan,
            blob_state,
            idempotency_key,
            opts,
            attempts_left
          )
        end

      {:ok, nil} ->
        {:error, :compare_retry_exhausted}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_transaction_result(
         {:ok, %{succeeded: true}},
         plan,
         blob_state,
         _idempotency_key,
         _opts,
         _attempts_left
       ) do
    {:ok, migrated_outcome(plan, blob_state)}
  end

  defp handle_transaction_result(
         {:ok, %{succeeded: false}},
         plan,
         _blob_state,
         _idempotency_key,
         opts,
         attempts_left
       ) do
    commit_plan(plan, opts, attempts_left - 1)
  end

  defp handle_transaction_result(
         {:error, reason},
         plan,
         blob_state,
         idempotency_key,
         opts,
         attempts_left
       )
       when reason in @ambiguous_errors do
    case backend(opts).resolve_transaction(idempotency_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        {:ok, migrated_outcome(plan, blob_state)}

      _unresolved ->
        commit_plan(plan, opts, attempts_left - 1)
    end
  end

  defp handle_transaction_result(
         {:error, reason},
         _plan,
         _blob_state,
         _idempotency_key,
         _opts,
         _attempts_left
       ),
       do: {:error, reason}

  defp validate_existing_head(plan, head, opts) do
    with :ok <- exact_record(head, plan.head, :conflicting_v2_head),
         :ok <- validate_existing_versions(plan, opts),
         {:ok, blobs} <- prepare_blobs(plan.versions, opts),
         :ok <- validate_existing_blobs(blobs) do
      {:ok, %{kind: :already_v2}}
    end
  end

  defp validate_existing_versions(plan, opts) do
    Enum.reduce_while(plan.versions, :ok, fn expected, :ok ->
      key = Keys.object_version(plan.bucket, plan.key, expected.version_id)

      case backend(opts).get(key, read_opts(opts)) do
        {:ok, %{value: actual}} ->
          case exact_record(actual, expected, {:conflicting_v2_version, expected.version_id}) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:ok, nil} ->
          {:halt, {:error, {:missing_v2_version, expected.version_id}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp exact_record(actual, expected, _reason) when actual == expected, do: :ok
  defp exact_record(_actual, _expected, reason), do: {:error, reason}

  defp validate_existing_blobs(blobs) do
    Enum.reduce_while(blobs, :ok, fn {hash, blob}, :ok ->
      cond do
        is_nil(blob.descriptor_record) ->
          {:halt, {:error, {:missing_v2_blob_descriptor, hash}}}

        not is_nil(blob.location) and is_nil(blob.location_record) ->
          {:halt, {:error, {:missing_v2_blob_location, hash, blob.location.node_id}}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp prepare_blobs(versions, opts) do
    versions
    |> Enum.reject(& &1.delete_marker)
    |> Enum.reduce_while({:ok, %{}}, fn version, {:ok, blobs} ->
      case blob_identity(version) do
        {:ok, {hash, size}} ->
          case Map.get(blobs, hash) do
            nil ->
              case prepare_blob(hash, size, field(version, :created_at), opts) do
                {:ok, blob} -> {:cont, {:ok, Map.put(blobs, hash, blob)}}
                {:error, reason} -> {:halt, {:error, reason}}
              end

            %{size: ^size} ->
              {:cont, {:ok, blobs}}

            %{size: other_size} ->
              {:halt, {:error, {:blob_size_conflict, hash, other_size, size}}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp prepare_blob(hash, size, created_at, opts) do
    descriptor_key = Keys.blob(hash)

    with {:ok, descriptor_record} <- backend(opts).get(descriptor_key, read_opts(opts)),
         :ok <- validate_descriptor(descriptor_record, hash, size),
         {:ok, location} <- local_location(hash, size, opts),
         {:ok, location_record} <- read_location(location, opts),
         :ok <- validate_location(location_record, location, hash, size) do
      {:ok,
       %{
         hash: hash,
         size: size,
         descriptor_key: descriptor_key,
         descriptor_record: descriptor_record,
         descriptor: descriptor(hash, size, created_at, desired_replication_factor(opts)),
         location: location,
         location_record: location_record
       }}
    end
  end

  defp transaction_spec(plan, blob_state) do
    head_key = Keys.object_head(plan.bucket, plan.key)

    version_compares =
      Enum.map(plan.versions, fn version ->
        {:exists, Keys.object_version(plan.bucket, plan.key, version.version_id), :==, false}
      end)

    version_operations =
      Enum.map(plan.versions, fn version ->
        {:put, Keys.object_version(plan.bucket, plan.key, version.version_id), version, %{}}
      end)

    {blob_compares, blob_operations} =
      blob_state
      |> Map.values()
      |> Enum.reduce({[], []}, fn blob, {compares, operations} ->
        descriptor_compare = revision_compare(blob.descriptor_key, blob.descriptor_record)

        descriptor_operations =
          if blob.descriptor_record,
            do: [],
            else: [{:put, blob.descriptor_key, blob.descriptor, %{}}]

        {location_compares, location_operations} =
          case blob.location do
            nil ->
              {[], []}

            location ->
              location_key = Keys.blob_location(blob.hash, location.node_id)

              operations =
                if blob.location_record,
                  do: [],
                  else: [{:put, location_key, Map.from_struct(location), %{}}]

              {[revision_compare(location_key, blob.location_record)], operations}
          end

        {
          [descriptor_compare | location_compares] ++ compares,
          descriptor_operations ++ location_operations ++ operations
        }
      end)

    %{
      compare:
        source_compares(plan.sources) ++
          [{:exists, head_key, :==, false}] ++
          version_compares ++ Enum.reverse(blob_compares),
      success:
        version_operations ++
          Enum.reverse(blob_operations) ++ [{:put, head_key, plan.head, %{}}],
      failure: []
    }
  end

  defp local_location(hash, size, opts) do
    case Keyword.get(opts, :local_blob_probe) do
      probe when is_function(probe, 2) ->
        normalize_probe_result(probe.(hash, size), hash, size)

      nil ->
        default_local_location(hash, size, opts)
    end
  end

  defp default_local_location(hash, size, opts) do
    with {:ok, context} <- context(opts),
         true <- context.config.node_role == :data,
         :ok <- LocalCAS.verify(hash, root: context.blob_root),
         path = LocalCAS.blob_path(hash, root: context.blob_root),
         {:ok, %File.Stat{type: :regular, size: ^size}} <- File.stat(path) do
      normalize_probe_result(
        {:ok,
         %{
           node_id: context.config.node_id,
           node_generation: context.config.node_generation,
           verified_at: timestamp()
         }},
        hash,
        size
      )
    else
      false -> {:ok, nil}
      {:error, :not_found} -> {:ok, nil}
      {:error, :enoent} -> {:ok, nil}
      {:error, :checksum_mismatch} -> {:error, {:local_blob_corrupt, hash}}
      {:ok, %File.Stat{}} -> {:error, {:local_blob_size_mismatch, hash}}
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, {:local_blob_probe_failed, hash, reason}}
    end
  end

  defp normalize_probe_result(:missing, _hash, _size), do: {:ok, nil}
  defp normalize_probe_result({:ok, nil}, _hash, _size), do: {:ok, nil}

  defp normalize_probe_result({:ok, evidence}, hash, size) when is_map(evidence) do
    location =
      struct(BlobLocation, %{
        schema: 2,
        hash: hash,
        node_id: field(evidence, :node_id),
        node_generation: field(evidence, :node_generation),
        state: :ready,
        size: size,
        verified_at: field(evidence, :verified_at, timestamp()),
        updated_at: field(evidence, :updated_at, timestamp())
      })

    case BlobLocation.cast(location) do
      {:ok, valid} -> {:ok, valid}
      {:error, _reason} -> {:error, {:invalid_local_blob_evidence, hash}}
    end
  end

  defp normalize_probe_result({:error, reason}, hash, _size),
    do: {:error, {:local_blob_probe_failed, hash, reason}}

  defp normalize_probe_result(_result, hash, _size),
    do: {:error, {:invalid_local_blob_evidence, hash}}

  defp read_location(nil, _opts), do: {:ok, nil}

  defp read_location(%BlobLocation{} = location, opts) do
    backend(opts).get(Keys.blob_location(location.hash, location.node_id), read_opts(opts))
  end

  defp validate_location(_record, nil, _hash, _size), do: :ok
  defp validate_location(nil, %BlobLocation{}, _hash, _size), do: :ok

  defp validate_location(%{value: value}, %BlobLocation{} = expected, hash, size) do
    case BlobLocation.cast(value) do
      {:ok, actual}
      when actual.hash == hash and actual.node_id == expected.node_id and actual.size == size and
             actual.node_generation == expected.node_generation ->
        :ok

      _other ->
        {:error, {:blob_location_conflict, hash, expected.node_id}}
    end
  end

  defp validate_descriptor(nil, _hash, _size), do: :ok

  defp validate_descriptor(%{value: value}, hash, size) do
    replication_factor = field(value, :desired_replication_factor)

    if field(value, :schema, 2) == 2 and field(value, :hash) == hash and
         field(value, :algorithm, :sha256) == :sha256 and field(value, :size) == size and
         is_integer(replication_factor) and replication_factor > 0 do
      :ok
    else
      {:error, {:blob_descriptor_conflict, hash}}
    end
  end

  defp descriptor(hash, size, created_at, replication_factor) do
    %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: size,
      desired_replication_factor: replication_factor,
      created_at: created_at || timestamp()
    }
  end

  defp normalize_version(legacy, bucket, key, parent_version_id) do
    delete_marker = delete_marker?(legacy.metadata)
    content_hash = normalize_hash(field(legacy.metadata, :content_hash))

    legacy.metadata
    |> Map.put(:schema, 2)
    |> Map.put(:bucket, bucket)
    |> Map.put(:key, key)
    |> Map.put(:version_id, legacy.version_id)
    |> Map.put(:operation_id, migration_operation_id(bucket, key, legacy.version_id))
    |> Map.put(:parent_version_id, parent_version_id)
    |> Map.put(:delete_marker, delete_marker)
    |> Map.put(:is_delete_marker, delete_marker)
    |> Map.put_new(:object_type, :blob)
    |> put_if_present(:content_hash, content_hash)
  end

  defp blob_identity(version) do
    case {field(version, :content_hash), field(version, :size)} do
      {hash, size} when is_binary(hash) and hash != "" and is_integer(size) and size >= 0 ->
        {:ok, {hash, size}}

      {nil, size} ->
        {:error, {:invalid_blob_identity, nil, size}}

      {hash, size} ->
        {:error, {:invalid_blob_identity, hash, size}}
    end
  end

  defp migrated_outcome(plan, blob_state) do
    blobs = Map.values(blob_state)

    %{
      kind: :migrated,
      versions: length(plan.versions),
      delete_markers: Enum.count(plan.versions, & &1.delete_marker),
      blob_descriptors: Enum.count(blobs, &is_nil(&1.descriptor_record)),
      blob_locations:
        Enum.count(blobs, &(not is_nil(&1.location) and is_nil(&1.location_record))),
      missing_local_blobs: for(%{location: nil, hash: hash} <- blobs, do: hash)
    }
  end

  defp merge_outcome(report, %{kind: :already_v2}) do
    %{
      report
      | objects_scanned: report.objects_scanned + 1,
        objects_already_v2: report.objects_already_v2 + 1
    }
  end

  defp merge_outcome(report, outcome) do
    %{
      report
      | objects_scanned: report.objects_scanned + 1,
        objects_migrated: report.objects_migrated + 1,
        versions_migrated: report.versions_migrated + outcome.versions,
        delete_markers_migrated: report.delete_markers_migrated + outcome.delete_markers,
        blob_descriptors_created: report.blob_descriptors_created + outcome.blob_descriptors,
        blob_locations_created: report.blob_locations_created + outcome.blob_locations,
        missing_local_blobs: outcome.missing_local_blobs ++ report.missing_local_blobs
    }
  end

  defp read_optional_record(key, opts) do
    case backend(opts).get(key, read_opts(opts)) do
      {:ok, nil} -> {:ok, nil}
      {:ok, %{value: value} = record} when is_map(value) -> {:ok, record}
      {:ok, _record} -> {:error, :invalid_legacy_object}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_legacy_object_key(key, :version_list),
    do: decode_after_prefix(key, "obj_ver_list:")

  defp decode_legacy_object_key(key, :current), do: decode_after_prefix(key, "obj:")

  defp decode_after_prefix(key, prefix) do
    case key do
      ^prefix <> rest ->
        case String.split(rest, ":", parts: 2) do
          [bucket, object_key] when bucket != "" -> {:ok, bucket, object_key}
          _other -> {:error, :invalid_legacy_key}
        end

      _other ->
        {:error, :invalid_legacy_key}
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

  defp validate_version_ids(version_ids) do
    if Enum.all?(version_ids, &(is_binary(&1) and &1 != "")) and
         length(version_ids) == length(Enum.uniq(version_ids)),
       do: :ok,
       else: {:error, :invalid_legacy_version_list}
  end

  defp legacy_version(version_id, metadata, source \\ nil),
    do: %{version_id: version_id, metadata: metadata, source: source}

  defp legacy_current(bucket, key, metadata) do
    version_id = "legacy-current-" <> fingerprint({bucket, key, metadata})
    legacy_version(version_id, metadata)
  end

  defp equivalent_legacy?(left, right) do
    normalize_legacy_comparison(left) == normalize_legacy_comparison(right)
  end

  defp normalize_legacy_comparison(metadata) do
    Map.drop(metadata, [
      :version_id,
      "version_id",
      :parent_version_id,
      "parent_version_id",
      :operation_id,
      "operation_id"
    ])
  end

  defp delete_marker?(metadata),
    do: field(metadata, :delete_marker, field(metadata, :is_delete_marker, false)) == true

  defp normalize_hash("sha256:" <> hash), do: hash
  defp normalize_hash(hash), do: hash

  defp migration_operation_id(bucket, key, version_id),
    do: "metadata-migrate-v2-" <> fingerprint({bucket, key, version_id})

  defp migration_attempt_key(plan, spec),
    do: "metadata-migrate-v2:" <> fingerprint({plan.bucket, plan.key, spec})

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp revision_compare(key, nil), do: {:exists, key, :==, false}

  defp revision_compare(key, %{mod_revision: revision}),
    do: {:mod_revision, key, :==, revision}

  defp source_compares(sources),
    do: Enum.map(sources, fn {key, record} -> revision_compare(key, record) end)

  defp page_record(entry),
    do: %{value: entry.value, mod_revision: entry.mod_revision}

  defp record_value(nil), do: nil
  defp record_value(%{value: value}), do: value

  defp field(map, key, default \\ nil) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp desired_replication_factor(opts) do
    Keyword.get_lazy(opts, :replication_factor, fn ->
      case Context.default() do
        {:ok, context} -> context.config.replication_factor
        {:error, _reason} -> 1
      end
    end)
  end

  defp context(opts) do
    case Keyword.fetch(opts, :context) do
      {:ok, %Context{} = context} -> {:ok, context}
      {:ok, _other} -> {:error, :invalid_context}
      :error -> Context.default()
    end
  end

  defp validate_cursor(cursor, cursor) when not is_nil(cursor),
    do: {:error, :metadata_scan_cursor_did_not_advance}

  defp validate_cursor(_cursor, _next_cursor), do: :ok

  defp page_size(opts) do
    case Keyword.get(opts, :page_size, @default_page_size) do
      size when is_integer(size) and size > 0 -> size
      _other -> @default_page_size
    end
  end

  defp max_attempts(opts) do
    case Keyword.get(opts, :max_attempts, @default_max_attempts) do
      attempts when is_integer(attempts) and attempts > 0 -> attempts
      _other -> @default_max_attempts
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
        {:error, :replication_factor_required}
    end
  end

  defp read_opts(opts) do
    opts
    |> Keyword.take([:consistency, :timeout, :engine, :barrier])
    |> Keyword.put_new(:consistency, :strong)
  end

  defp transaction_opts(opts, idempotency_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, idempotency_key)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp timestamp, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
