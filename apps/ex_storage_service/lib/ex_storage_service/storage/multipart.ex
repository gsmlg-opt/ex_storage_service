defmodule ExStorageService.Storage.Multipart do
  @moduledoc """
  Multipart upload storage operations.

  Manages the lifecycle of S3 multipart uploads: initiating uploads,
  storing individual parts, completing (concatenating) uploads, and
  aborting/cleaning up.

  Cluster metadata uses encoded v2 upload and part keys. Standalone reads and
  writes retain the legacy `"mpu:"` / `"mpu_part:"` records for compatibility.

  Part data is stored as global CAS blobs; cluster parts require their full
  desired replica count before the part record becomes visible.
  """

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Cluster.WriteCoordinator
  alias ExStorageService.Context
  alias ExStorageService.ObjectService
  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  alias ExStorageService.Metadata.{
    BucketLifecycle,
    Keys,
    ListIndex,
    MultipartCommit,
    OperationIntents
  }

  alias ExStorageService.Metadata.ListIndex.Multipart, as: MultipartIndex
  alias ExStorageService.Storage.{CAS, Manifest}

  @doc """
  Initiate a new multipart upload. Returns `{:ok, upload_id}`.
  """
  def init_upload(bucket, key, attributes \\ %{}) do
    upload_id = generate_upload_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    meta = %{
      bucket: bucket,
      key: key,
      upload_id: upload_id,
      status: :initiated,
      parts_revision: 0,
      attributes: attributes,
      created_at: now,
      updated_at: now
    }

    with {:ok, %{value: bucket_meta}} <-
           Backend.get("bucket:#{bucket}", consistency: :linearizable) do
      meta = Map.put(meta, :bucket_generation, BucketLifecycle.generation(bucket_meta))
      init_upload_attempt(bucket, meta, 32)
    else
      {:ok, nil} -> {:error, :bucket_not_found}
      error -> error
    end
  end

  defp init_upload_attempt(_bucket, _meta, 0), do: {:error, :conflict}

  defp init_upload_attempt(bucket, meta, attempts) do
    with {:ok, guard} <-
           BucketLifecycle.guard(Backend, bucket, bucket_generation: meta.bucket_generation) do
      key = upload_key(bucket, meta.upload_id)

      spec = %{
        compare: guard.compare ++ [{:exists, key, :==, false}],
        success: guard.success ++ [{:put, key, meta, %{}}],
        failure: []
      }

      case indexed_transaction(bucket, spec) do
        {:ok, %{succeeded: true}} -> {:ok, meta.upload_id}
        {:ok, %{succeeded: false}} -> init_upload_attempt(bucket, meta, attempts - 1)
        error -> error
      end
    end
  end

  @doc """
  Store a single part for a multipart upload.

  Writes the part data to the global CAS and records metadata in Concord.
  Returns `{:ok, etag}` on success.
  """
  def store_part(bucket, upload_id, part_number, data),
    do: store_part(bucket, upload_id, part_number, data, [])

  @doc false
  def store_part(bucket, upload_id, part_number, data, opts)
      when is_integer(part_number) and part_number in 1..10_000 do
    result =
      with :ok <- writable_index(bucket) do
        if cluster_data_plane?() do
          store_cluster_part(bucket, upload_id, part_number, data, opts)
        else
          store_local_part(bucket, upload_id, part_number, data, opts)
        end
      end

    normalize_stream_error(result)
  end

  def store_part(_bucket, _upload_id, _part_number, _data, _opts),
    do: {:error, :invalid_part}

  @doc false
  def store_part_from_reader(bucket, upload_id, part_number, reader, initial_state, opts \\ []) do
    with :ok <- writable_index(bucket),
         {:ok, context} <- Context.default(),
         stage_opts <-
           context
           |> Context.blob_store_options()
           |> Keyword.put(:max_size, Keyword.fetch!(opts, :max_size)),
         {:ok, staged, final_state} <-
           LocalCAS.stage_from_reader(reader, initial_state, stage_opts) do
      attributes = Keyword.get(opts, :part_attributes, fn _ -> %{} end).(final_state)
      opts = Keyword.put(opts, :attributes, attributes)

      result =
        if cluster_data_plane?() do
          store_cluster_staged_part(context, bucket, upload_id, part_number, staged, opts)
        else
          store_local_staged_part(context, bucket, upload_id, part_number, staged, opts)
        end

      attach_reader_state(normalize_stream_error(result), final_state)
    else
      {:error, reason, final_state} ->
        {:error, normalize_reader_error(reason), final_state}

      {:error, reason} ->
        {:error, normalize_reader_error(reason), initial_state}
    end
  end

  defp store_local_part(bucket, upload_id, part_number, data, opts) do
    with {:ok, context} <- Context.default(),
         blob_opts = Context.blob_store_options(context),
         {:ok, staged} <-
           LocalCAS.stage(data, Keyword.merge(blob_opts, Keyword.take(opts, [:max_size]))) do
      store_local_staged_part(context, bucket, upload_id, part_number, staged, opts)
    end
  end

  defp store_local_staged_part(context, bucket, upload_id, part_number, staged, opts) do
    operation_id =
      Keyword.get(
        opts,
        :operation_id,
        "multipart-part:#{upload_id}:#{part_number}:#{staged.hash}"
      )

    with {:ok, staged} <-
           LocalCAS.protect(staged, {:hosted, operation_id}, Context.blob_store_options(context)) do
      do_store_staged_part(context, bucket, upload_id, part_number, staged, operation_id, opts)
    end
  end

  defp do_store_staged_part(context, bucket, upload_id, part_number, staged, operation_id, opts) do
    blob_opts = Context.blob_store_options(context)

    case operation_intents(opts).open(
           operation_id,
           staged.hash,
           staged.size,
           context.config.node_id,
           context.config.node_generation,
           protection_ms: context.config.orphan_grace_seconds * 1_000
         ) do
      {:ok, _intent} ->
        result = commit_local_part(bucket, upload_id, part_number, staged, blob_opts, opts)

        if match?({:error, _reason}, result) do
          _ = LocalCAS.discard_unprotected(staged, blob_opts)
        end

        finish_part_intent(result, operation_id, opts)

      {:error, _reason} = error ->
        _ = LocalCAS.discard_unprotected(staged, blob_opts)
        error
    end
  end

  defp commit_local_part(bucket, upload_id, part_number, staged, blob_opts, opts) do
    with {:ok, ready} <- LocalCAS.commit(staged, blob_opts),
         :ok <- Metadata.ensure_blob_meta(ready.hash, ready.size),
         {:ok, etag} <-
           MultipartCommit.put_local_part(
             bucket,
             upload_id,
             part_number,
             Map.merge(Keyword.get(opts, :attributes, %{}), %{
               part_number: part_number,
               etag: ready.etag,
               size: ready.size,
               hash: ready.hash
             }),
             opts
           ) do
      {:ok, etag}
    end
  end

  defp store_cluster_part(bucket, upload_id, part_number, data, opts) do
    with {:ok, context} <- Context.default(),
         {:ok, staged} <-
           LocalCAS.stage(
             data,
             Keyword.merge(Context.blob_store_options(context), Keyword.take(opts, [:max_size]))
           ) do
      store_cluster_staged_part(context, bucket, upload_id, part_number, staged, opts)
    end
  end

  defp store_cluster_staged_part(context, bucket, upload_id, part_number, staged, opts) do
    timestamp =
      Keyword.get_lazy(opts, :timestamp, fn ->
        DateTime.utc_now() |> DateTime.to_iso8601()
      end)

    operation_id =
      Keyword.get(
        opts,
        :operation_id,
        "multipart-part:#{upload_id}:#{part_number}:#{staged.hash}"
      )

    with {:ok, staged} <-
           LocalCAS.protect(staged, {:hosted, operation_id}, Context.blob_store_options(context)) do
      do_store_cluster_staged_part(
        context,
        bucket,
        upload_id,
        part_number,
        staged,
        operation_id,
        timestamp,
        opts
      )
    end
  end

  defp do_store_cluster_staged_part(
         context,
         bucket,
         upload_id,
         part_number,
         staged,
         operation_id,
         timestamp,
         opts
       ) do
    case operation_intents(opts).open(
           operation_id,
           staged.hash,
           staged.size,
           context.config.node_id,
           context.config.node_generation,
           protection_ms: context.config.orphan_grace_seconds * 1_000
         ) do
      {:ok, _intent} ->
        result =
          case WriteCoordinator.ensure_blob(
                 context,
                 staged,
                 operation_id: operation_id,
                 timestamp: timestamp,
                 write_quorum: context.config.replication_factor,
                 allow_degraded_writes: false
               ) do
            {:ok, durability} ->
              part =
                Map.merge(Keyword.get(opts, :attributes, %{}), %{
                  part_number: part_number,
                  etag: staged.etag,
                  size: staged.size,
                  hash: staged.hash
                })

              case MultipartCommit.put_part(
                     bucket,
                     upload_id,
                     part_number,
                     part,
                     durability,
                     operation_id: operation_id,
                     operation_intent: true,
                     timestamp: timestamp
                   ) do
                {:ok, _result} -> {:ok, staged.etag}
                {:error, _reason} = error -> error
              end

            {:error, _reason} = error ->
              _ = LocalCAS.discard_unprotected(staged, Context.blob_store_options(context))
              error
          end

        finish_part_intent(result, operation_id, opts)

      {:error, _reason} = error ->
        _ = LocalCAS.discard_unprotected(staged, Context.blob_store_options(context))
        error
    end
  end

  defp finish_part_intent({:ok, _etag} = result, operation_id, opts) do
    _ = operation_intents(opts).transition(operation_id, :committed)
    result
  end

  defp finish_part_intent({:error, reason} = error, operation_id, opts) do
    state =
      if reason in [
           :metadata_quorum_unavailable,
           :unknown_transaction_outcome,
           :timeout,
           :unknown,
           :cluster_not_ready,
           :no_leader
         ],
         do: :unknown,
         else: :aborted

    _ = operation_intents(opts).transition(operation_id, state)
    error
  end

  defp operation_intents(opts),
    do: Keyword.get(opts, :operation_intents, OperationIntents)

  defp normalize_stream_error({:error, {:stage, reason}})
       when reason in [:entity_too_large, :malformed_chunked],
       do: {:error, reason}

  defp normalize_stream_error(result), do: result

  defp normalize_reader_error({:stage, reason}), do: reason
  defp normalize_reader_error(reason), do: reason

  defp attach_reader_state({:ok, etag}, state), do: {:ok, etag, state}
  defp attach_reader_state({:error, reason}, state), do: {:error, reason, state}

  @doc """
  Complete a multipart upload.

  Concatenates all parts in order, computes final SHA-256 content hash
  and the S3 multipart etag (MD5-of-MD5s-partcount), moves the result
  to the content-addressed store, and cleans up part files.

  `parts` is a list of `{part_number, etag}` tuples from the client XML.

  Returns `{:ok, {content_hash, etag, size, manifest_hash}}`.
  """
  def complete_upload(bucket, upload_id, parts) do
    with {:ok, prepared} <- prepare_complete_upload(bucket, upload_id, parts),
         :ok <- finalize_complete_upload(bucket, upload_id, prepared) do
      {:ok, {prepared.content_hash, prepared.etag, prepared.size, prepared.manifest_hash}}
    end
  end

  @doc """
  Builds and durably commits the final multipart blob without publishing object
  metadata or cleaning the upload.

  The caller must publish the object through `ExStorageService.ObjectService`
  and call `finalize_complete_upload/3` only after that metadata commit
  succeeds. A failure therefore leaves a retryable upload and a recoverable CAS
  orphan rather than a falsely completed upload.

  `:backend` and `:engine` options apply only to the completion-state transaction
  boundary; part/blob I/O continues to use the configured storage instance.
  """
  def prepare_complete_upload(bucket, upload_id, parts, opts \\ []) do
    prepare_complete_upload(bucket, upload_id, parts, opts, 32)
  end

  defp prepare_complete_upload(_bucket, _upload_id, _parts, _opts, 0),
    do: {:error, :multipart_completion_conflict}

  defp prepare_complete_upload(bucket, upload_id, parts, opts, attempts) do
    with :ok <- writable_index(bucket),
         :ok <- validate_requested_parts(parts),
         {:ok, record_key, record} <- upload_record(bucket, upload_id),
         {:ok, _guard} <-
           BucketLifecycle.guard(Backend, bucket,
             bucket_generation: Map.get(record.value, :bucket_generation)
           ) do
      case record.value.status do
        :completed ->
          completed_prepared(record.value, parts)

        :completing ->
          resume_completion(bucket, upload_id, record_key, record, parts, attempts, opts)

        :initiated ->
          prepare_initiated_upload(bucket, upload_id, parts, record_key, record, attempts, opts)

        _ ->
          {:error, :not_found}
      end
    end
  end

  defp prepare_initiated_upload(bucket, upload_id, parts, record_key, record, attempts, opts) do
    min_part_size =
      Application.get_env(:ex_storage_service, :min_part_size, 5 * 1024 * 1024)

    with {:ok, part_records} <- resolve_part_records(bucket, upload_id, parts),
         :ok <- validate_parts(part_records, min_part_size, length(parts) - 1),
         true <- Enum.reduce(part_records, 0, &(&1.size + &2)) <= 5 * 1024 * 1024 * 1024 * 1024 do
      frozen =
        record.value
        |> Map.put(:status, :completing)
        |> Map.put(:completion, %{
          token: generate_upload_id(),
          parts: parts,
          part_checksums:
            Enum.map(part_records, fn part ->
              %{part_number: part.part_number, checksums: Map.get(part, :checksums, %{})}
            end)
        })

      case completion_state_transaction(bucket, record_key, record, frozen, opts) do
        {:ok, %{succeeded: true}} ->
          prepare_complete_upload(bucket, upload_id, parts, opts, attempts)

        {:ok, %{succeeded: false}} ->
          # Other uploads also fence the bucket. Re-read both identities after
          # a compare loss instead of treating unrelated bucket activity as a
          # conflicting completion of this upload.
          Process.sleep(1 + :erlang.phash2({self(), attempts}, 8))
          prepare_complete_upload(bucket, upload_id, parts, opts, attempts - 1)

        {:error, _} = error ->
          error
      end
    else
      false -> {:error, :entity_too_large}
      error -> error
    end
  end

  defp resume_completion(bucket, upload_id, record_key, record, parts, attempts, opts) do
    cond do
      record.value.completion.parts != parts ->
        {:error, :multipart_completion_conflict}

      Map.has_key?(record.value.completion, :hash) ->
        {:ok, prepared(record_key, record)}

      true ->
        with {:ok, part_records} <- resolve_part_records(bucket, upload_id, parts),
             {:ok, {hash, etag, size, manifest_hash}} <- concatenate_parts(bucket, part_records) do
          completion =
            Map.merge(record.value.completion, %{
              hash: hash,
              etag: etag,
              size: size,
              manifest_hash: manifest_hash
            })

          value = Map.put(record.value, :completion, completion)

          case completion_state_transaction(bucket, record_key, record, value, opts) do
            {:ok, %{succeeded: true}} ->
              prepare_complete_upload(bucket, upload_id, parts, opts, attempts)

            {:ok, %{succeeded: false}} ->
              prepare_complete_upload(bucket, upload_id, parts, opts, attempts - 1)

            {:error, _} = error ->
              error
          end
        end
    end
  end

  defp prepared(record_key, record) do
    completion = record.value.completion

    %{
      content_hash: completion.hash,
      etag: completion.etag,
      size: completion.size,
      manifest_hash: completion.manifest_hash,
      upload: record.value,
      multipart_completion:
        Map.merge(completion, %{
          upload_key: record_key,
          upload_id: record.value.upload_id,
          revision: record.mod_revision,
          bucket_generation: record.value.bucket_generation
        })
    }
  end

  defp completed_prepared(upload, parts) do
    if get_in(upload, [:completion, :parts]) in [nil, parts] do
      {:ok,
       %{
         content_hash: upload.content_hash,
         etag: upload.etag,
         size: upload.size,
         manifest_hash: upload.manifest_hash,
         upload: upload,
         completion_result: Map.get(upload, :completion_result)
       }}
    else
      {:error, :not_found}
    end
  end

  @doc "Releases only this frozen completion after a definitive precondition failure."
  def release_completion(bucket, upload_id, prepared) do
    with {:ok, record_key, record} <- upload_record(bucket, upload_id),
         true <-
           record.value.status == :completing and
             record.value.completion.token == prepared.multipart_completion.token do
      value = record.value |> Map.put(:status, :initiated) |> Map.delete(:completion)

      case completion_state_transaction(bucket, record_key, record, value) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> {:error, :multipart_completion_conflict}
        error -> error
      end
    else
      false -> {:error, :multipart_completion_conflict}
      error -> error
    end
  end

  # Freeze, composition and release retain active membership, listing summary and
  # part trees. Fence their identities without rewriting the shared listing tree
  # or bucket revision and invalidating unrelated object publication attempts.
  defp completion_state_transaction(bucket, record_key, record, value, opts \\ []) do
    generation = Map.get(record.value, :bucket_generation)
    backend = Keyword.get(opts, :backend, Backend)
    backend_opts = Keyword.take(opts, [:engine, :timeout])

    with {:ok, guard} <-
           BucketLifecycle.guard(
             backend,
             bucket,
             Keyword.put(backend_opts, :bucket_generation, generation)
           ),
         index_key = ListIndex.root_key(bucket, generation),
         {:ok, index} <-
           backend.get(index_key, Keyword.put(backend_opts, :consistency, :linearizable)),
         :ok <- ListIndex.ready(index, generation) do
      backend.transaction(
        %{
          compare:
            guard.compare ++
              [
                {:mod_revision, record_key, :==, record.mod_revision},
                {:mod_revision, index_key, :==, index.mod_revision}
              ],
          success: [{:put, record_key, value, %{}}],
          failure: []
        },
        backend_opts
      )
    end
  end

  defp validate_requested_parts([]), do: {:error, :invalid_part}

  defp validate_requested_parts(parts) when length(parts) > 10_000,
    do: {:error, :invalid_part}

  defp validate_requested_parts(parts) do
    numbers = Enum.map(parts, &elem(&1, 0))

    cond do
      Enum.any?(numbers, &(not is_integer(&1) or &1 not in 1..10_000)) -> {:error, :invalid_part}
      numbers != Enum.sort(Enum.uniq(numbers)) -> {:error, :invalid_part_order}
      true -> :ok
    end
  end

  defp upload_record(bucket, upload_id) do
    key = Keys.multipart_upload(upload_id)

    case Backend.get(key, consistency: :linearizable) do
      {:ok, nil} ->
        legacy = mpu_key(bucket, upload_id)

        case Backend.get(legacy, consistency: :linearizable) do
          {:ok, nil} -> {:error, :not_found}
          {:ok, %{value: %{bucket: ^bucket}} = record} -> {:ok, legacy, record}
          {:ok, _} -> {:error, :not_found}
          error -> error
        end

      {:ok, %{value: %{bucket: ^bucket}} = record} ->
        {:ok, key, record}

      {:ok, _} ->
        {:error, :not_found}

      error ->
        error
    end
  end

  @doc """
  Marks a prepared multipart upload complete.

  The completed upload record retains its stable result for retries. Standalone
  removes part metadata only after the object transaction has completed; cluster
  part records remain protected through the orphan grace boundary.
  """
  def finalize_complete_upload(bucket, upload_id, prepared, opts \\ []) do
    before_finalize = Keyword.get(opts, :before_finalize, fn -> :ok end)

    with :ok <- before_finalize.(),
         {:ok, record_key, record} <- upload_record(bucket, upload_id) do
      case record.value.status do
        :completed ->
          maybe_cleanup_completed_parts(bucket, upload_id)

        :completing ->
          # Compatibility core callers do not publish object metadata. S3 uses
          # completion_operations/5 in the same transaction as its object head.
          if record.value.completion.token == prepared.multipart_completion.token do
            completed =
              Map.merge(record.value, %{
                status: :completed,
                content_hash: prepared.content_hash,
                manifest_hash: prepared.manifest_hash,
                etag: prepared.etag,
                size: prepared.size
              })

            case indexed_transaction(bucket, %{
                   compare: [{:mod_revision, record_key, :==, record.mod_revision}],
                   success: [{:put, record_key, completed, %{}}],
                   failure: []
                 }) do
              {:ok, %{succeeded: true}} ->
                maybe_cleanup_completed_parts(bucket, upload_id)

              {:ok, %{succeeded: false}} ->
                {:error, :multipart_completion_conflict}

              error ->
                error
            end
          else
            {:error, :multipart_completion_conflict}
          end

        _ ->
          {:error, :not_found}
      end
    end
  end

  defp maybe_cleanup_completed_parts(bucket, upload_id) do
    if cluster_data_plane?(), do: :ok, else: cleanup_parts(bucket, upload_id)
  end

  @doc """
  Abort a multipart upload. Deletes upload and part metadata without deleting
  shared content-addressed blob bytes.
  """
  def abort_upload(bucket, upload_id) do
    with :ok <- MultipartCommit.abort_upload(bucket, upload_id) do
      cleanup_parts(bucket, upload_id)
    end
  end

  @doc """
  List all parts for a multipart upload with their etags and sizes.

  Returns `{:ok, parts}` where parts is a sorted list of part metadata maps.
  """
  def list_parts(bucket, upload_id) do
    case get_upload(bucket, upload_id) do
      {:ok, _upload_meta} ->
        list_part_records(bucket, upload_id)

      {:error, _} = error ->
        error
    end
  end

  @doc "Returns one protocol-bounded page of parts after a part-number marker."
  def list_parts(bucket, upload_id, opts) do
    MultipartIndex.parts_page(bucket, upload_id, opts)
  end

  @doc "Lists active uploads by key and upload ID, with delimiter-aware pages."
  def list_uploads(bucket, opts \\ []) do
    MultipartIndex.uploads_page(bucket, opts)
  end

  @doc """
  Get upload metadata from Concord.
  """
  def get_upload(bucket, upload_id) do
    case upload_record(bucket, upload_id) do
      {:ok, _key, %{value: %{status: :aborted}}} -> {:error, :not_found}
      {:ok, _key, %{value: value}} -> {:ok, value}
      error -> error
    end
  end

  @doc """
  List all active (non-completed) multipart uploads. Used by the GC.
  """
  def list_active_uploads do
    case Concord.get_all() do
      {:ok, all} ->
        uploads =
          all
          |> Enum.filter(fn {k, _v} ->
            String.starts_with?(k, "mpu:") or
              String.starts_with?(k, Keys.multipart_upload_prefix())
          end)
          |> Enum.map(fn {_k, v} -> v end)
          |> Enum.filter(fn v -> v.status in [:initiated, :completing] end)

        {:ok, uploads}

      error ->
        error
    end
  end

  # Private helpers

  defp generate_upload_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  defp mpu_key(bucket, upload_id), do: "mpu:#{bucket}:#{upload_id}"

  defp mpu_part_key(bucket, upload_id, part_number),
    do: "mpu_part:#{bucket}:#{upload_id}:#{part_number}"

  defp upload_key(bucket, upload_id) do
    if cluster_data_plane?(),
      do: Keys.multipart_upload(upload_id),
      else: mpu_key(bucket, upload_id)
  end

  defp list_part_records(bucket, upload_id), do: list_part_records(bucket, upload_id, 0, [])

  defp list_part_records(bucket, upload_id, marker, pages) do
    with {:ok, page} <-
           MultipartIndex.parts_inventory_page(bucket, upload_id,
             part_number_marker: marker,
             max_parts: 1000
           ) do
      pages = [page.parts | pages]

      if page.is_truncated do
        list_part_records(bucket, upload_id, page.next_part_number_marker, pages)
      else
        {:ok, pages |> Enum.reverse() |> List.flatten()}
      end
    end
  end

  # Look up the Concord part record for each client-requested part and
  # check the client-supplied etags, preserving the historical error shapes.
  defp resolve_part_records(bucket, upload_id, sorted_parts) do
    sorted_parts
    |> Enum.reduce_while({:ok, []}, fn {pn, client_etag}, {:ok, acc} ->
      case get_part_record(bucket, upload_id, pn) do
        {:ok, nil} ->
          {:halt, {:error, {:missing_part, pn, :not_found}}}

        {:ok, record} ->
          if client_etag != "" and record.etag != client_etag do
            {:halt, {:error, {:etag_mismatch, pn, client_etag, record.etag}}}
          else
            {:cont, {:ok, [record | acc]}}
          end

        {:error, reason} ->
          {:halt, {:error, {:missing_part, pn, reason}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp get_part_record(bucket, upload_id, part_number) do
    case Concord.get(Keys.multipart_part(upload_id, part_number)) do
      {:ok, nil} -> Concord.get(mpu_part_key(bucket, upload_id, part_number))
      {:error, :not_found} -> Concord.get(mpu_part_key(bucket, upload_id, part_number))
      result -> result
    end
  end

  defp validate_parts(part_records, min_part_size, last_index) do
    part_records
    |> Enum.with_index()
    |> Enum.find(fn {record, idx} -> idx < last_index and record.size < min_part_size end)
    |> case do
      nil ->
        :ok

      {record, _idx} ->
        {:error, {:entity_too_small, record.part_number, record.size, min_part_size}}
    end
  end

  # Streams each CAS part blob through LocalCAS, then records the manifest.
  defp concatenate_parts(bucket, part_records) do
    with {:ok, streams} <- part_streams(bucket, part_records),
         source =
           Source.stateful_stream(
             fn initial, reducer ->
               Enum.reduce_while(streams, {:ok, initial}, fn source, {:ok, state} ->
                 case Source.reduce(source, state, reducer) do
                   {:ok, next} -> {:cont, {:ok, next}}
                   {:error, _, _} = error -> {:halt, error}
                 end
               end)
             end,
             Enum.reduce(part_records, 0, &(&1.size + &2))
           ),
         {:ok, staged} <-
           LocalCAS.stage(
             Source.request_body(source),
             Keyword.put(blob_store_opts(), :max_size, Source.content_length(source))
           ) do
      case LocalCAS.commit(staged, blob_store_opts()) do
        {:ok, ready} ->
          finish_concatenation(ready, part_records)

        {:error, _reason} = error ->
          _ = LocalCAS.discard(staged, blob_store_opts())
          error
      end
    end
  end

  defp part_streams(bucket, part_records) do
    part_records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, streams} ->
      case ObjectService.open_source(%{content_hash: record.hash, size: record.size},
             bucket: bucket
           ) do
        {:ok, source} ->
          {:cont, {:ok, [source | streams]}}

        {:error, reason} ->
          {:halt, {:error, {:multipart_part_unavailable, record.part_number, reason}}}
      end
    end)
    |> case do
      {:ok, streams} -> {:ok, Enum.reverse(streams)}
      {:error, _reason} = error -> error
    end
  end

  defp finish_concatenation(ready, part_records) do
    with {:ok, etag} <- multipart_etag(part_records) do
      unless cluster_data_plane?() do
        ExStorageService.Metadata.ensure_blob_meta(ready.hash, ready.size)
      end

      manifest_parts =
        Enum.map(part_records, fn record ->
          %{
            number: record.part_number,
            hash: record.hash,
            size: record.size,
            etag: record.etag
          }
        end)

      case Manifest.create_manifest(manifest_parts, ready.size, etag) do
        {:ok, manifest_hash} ->
          {:ok, {ready.hash, etag, ready.size, manifest_hash}}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp multipart_etag(part_records) do
    part_records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, digests} ->
      case Base.decode16(record.etag, case: :mixed) do
        {:ok, digest} -> {:cont, {:ok, [digest | digests]}}
        :error -> {:halt, {:error, {:invalid_part_etag, record.part_number}}}
      end
    end)
    |> case do
      {:ok, digests} ->
        combined_md5 = digests |> Enum.reverse() |> IO.iodata_to_binary() |> md5()

        {:ok, "#{Base.encode16(combined_md5, case: :lower)}-#{length(part_records)}"}

      {:error, _reason} = error ->
        error
    end
  end

  defp md5(data), do: :crypto.hash(:md5, data)

  defp blob_store_opts do
    case Context.default() do
      {:ok, context} -> Context.blob_store_options(context)
      {:error, _} -> [root: CAS.blob_root(), tmp_dir: Path.join(CAS.tmp_root(), "uploads")]
    end
  end

  defp cleanup_parts(bucket, upload_id) do
    # Delete part files from disk
    dir = Path.join([ExStorageService.Storage.CAS.data_root(), bucket, "multipart", upload_id])

    if File.dir?(dir) do
      _ = File.rm_rf(dir)
    end

    cleanup_part_page(bucket, upload_id, 100)
  end

  defp cleanup_part_page(_bucket, _upload_id, 0), do: {:error, :multipart_cleanup_conflict}

  defp cleanup_part_page(bucket, upload_id, attempts) do
    with {:ok, page} <- MultipartIndex.parts_inventory_page(bucket, upload_id, max_parts: 100) do
      if page.parts == [] do
        :ok
      else
        deletes =
          Enum.flat_map(page.parts, fn part ->
            [
              {:delete, {:key, Keys.multipart_part(upload_id, part.part_number)}, %{}},
              {:delete, {:key, mpu_part_key(bucket, upload_id, part.part_number)}, %{}}
            ]
          end)

        case indexed_transaction(bucket, %{compare: [], success: deletes, failure: []}) do
          {:ok, %{succeeded: true}} -> cleanup_part_page(bucket, upload_id, 100)
          {:ok, %{succeeded: false}} -> cleanup_part_page(bucket, upload_id, attempts - 1)
          error -> error
        end
      end
    end
  end

  defp indexed_transaction(bucket, spec) do
    ListIndex.transaction(Backend, bucket, spec)
  end

  defp writable_index(bucket) do
    with {:ok, status} <- ListIndex.status(bucket) do
      case status do
        %{bucket_generation: generation} -> ListIndex.ready(%{value: status}, generation)
        _ -> {:error, :listing_index_unavailable}
      end
    end
  end

  defp cluster_data_plane? do
    match?(
      {:ok, %Context{config: %{mode: :cluster, cluster_data_plane_enabled: true}}},
      Context.default()
    )
  end
end
