defmodule ExStorageService.ObjectService do
  @moduledoc """
  Coordinates object metadata with durable blob storage.

  This is the object-domain boundary used by protocol adapters. It deliberately
  contains no Plug or S3 response logic. Blob and metadata implementations are
  injectable per call so fault and concurrency tests do not require mutable
  global configuration.
  """

  alias ExStorageService.BlobStore.{LocalCAS, StagedBlob}
  alias ExStorageService.Cluster.{ReadCoordinator, WriteCoordinator}
  alias ExStorageService.Context
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.OperationIntents
  alias ExStorageService.Storage.{StagingRecovery, Versioning}

  @type result :: %{
          required(:version_id) => String.t() | nil,
          required(:metadata) => map(),
          optional(:ready_blob) => term(),
          optional(:source) => term(),
          optional(:delete_marker) => boolean()
        }

  @doc """
  Streams data into the blob store and makes the object visible atomically.

  A metadata failure intentionally leaves the committed blob as a recoverable
  orphan. The object head is not made visible unless the versioning commit
  succeeds.
  """
  @spec put(String.t(), String.t(), Enumerable.t() | binary(), String.t(), map(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def put(bucket, key, data, content_type, user_metadata, opts \\ []) do
    opts = ensure_operation_id(opts)

    with {:ok, opts} <- pin_bucket_generation(bucket, opts),
         :ok <- ensure_bucket(bucket, opts),
         :ok <- ensure_cluster_write_enabled(opts) do
      attributes =
        %{
          content_type: content_type,
          metadata: user_metadata,
          user_metadata: user_metadata
        }
        |> Map.merge(attributes_option(opts))

      if cluster_write?(opts) do
        put_cluster(bucket, key, data, attributes, opts)
      else
        put_standalone(bucket, key, data, attributes, opts)
      end
    end
  end

  @doc """
  Stages a request body from a state-threaded reader and returns its final state.

  Protocol adapters use this form when their body reader carries connection
  state that must also be used to send the response.
  """
  @spec put_from_reader(
          String.t(),
          String.t(),
          (state -> LocalCAS.reader_result(state)),
          state,
          String.t(),
          map(),
          keyword()
        ) ::
          {:ok, result(), state} | {:error, term(), state}
        when state: term()
  def put_from_reader(
        bucket,
        key,
        reader,
        initial_state,
        content_type,
        user_metadata,
        opts \\ []
      ) do
    opts = ensure_operation_id(opts)

    with {:ok, opts} <- pin_bucket_generation(bucket, opts),
         :ok <- ensure_bucket(bucket, opts),
         :ok <- ensure_cluster_write_enabled(opts),
         {:ok, staged, final_state} <-
           stage_blob_from_reader(reader, initial_state, opts) do
      attributes =
        %{
          content_type: content_type,
          metadata: user_metadata,
          user_metadata: user_metadata
        }
        |> Map.merge(attributes_option(opts))
        |> Map.merge(reader_attributes(opts, final_state))

      result =
        if cluster_write?(opts) do
          put_cluster_staged(bucket, key, staged, attributes, opts)
        else
          put_standalone_staged(bucket, key, staged, attributes, opts)
        end

      attach_reader_state(result, final_state)
    else
      {:error, reason, final_state} -> {:error, reason, final_state}
      {:error, reason} -> {:error, reason, initial_state}
    end
  end

  @doc """
  Returns object metadata and an efficient blob source.

  Delete markers are returned with `source: nil`; the protocol adapter decides
  how to express that marker. Missing latest and explicit versions are
  distinguished for adapters that support both error models.
  """
  @spec get(String.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, result()} | {:error, term()}
  def get(bucket, key, version_id, opts) do
    with {:ok, result} <- head(bucket, key, version_id, opts) do
      if result.delete_marker do
        {:ok, Map.put(result, :source, nil)}
      else
        case open_source(result.metadata, Keyword.put(opts, :bucket, bucket)) do
          {:ok, source} -> {:ok, Map.put(result, :source, source)}
          {:error, reason} -> {:error, reason}
        end
      end
    end
  end

  @doc """
  Opens a servable source for metadata already pinned by `head/4`.

  Protocol adapters use this after evaluating conditional and Range headers so
  the immutable version is not looked up a second time.
  """
  @spec open_source(map(), keyword()) :: {:ok, term()} | {:error, term()}
  def open_source(metadata, opts \\ []) when is_map(metadata) do
    with hash when is_binary(hash) <- Map.get(metadata, :content_hash),
         size when is_integer(size) and size >= 0 <- Map.get(metadata, :size),
         {:ok, context} <- context(opts) do
      case read_coordinator(opts).open(
             context,
             hash,
             size,
             Keyword.get(opts, :range),
             read_coordinator_opts(opts)
           ) do
        {:ok, source} ->
          {:ok, source}

        {:error, :not_found} ->
          if context.config.mode == :cluster,
            do: {:error, :all_blob_replicas_unavailable},
            else: {:error, :blob_not_found}

        {:error, reason} ->
          {:error, reason}
      end
    else
      _missing_or_invalid_identity -> {:error, :invalid_object_metadata}
    end
  end

  @doc "Returns latest object metadata without opening its blob."
  @spec head(String.t(), String.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def head(bucket, key, opts \\ []) when is_list(opts) do
    head(bucket, key, nil, opts)
  end

  @doc "Returns latest or explicit-version metadata without opening its blob."
  @spec head(String.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, result()} | {:error, term()}
  def head(bucket, key, version_id, opts) do
    with :ok <- ensure_bucket(bucket, opts),
         {:ok, metadata} <- get_version(bucket, key, version_id, strong_read_opts(opts)) do
      delete_marker = Map.get(metadata, :is_delete_marker, false)

      {:ok,
       %{
         version_id: Map.get(metadata, :version_id, version_id),
         metadata: metadata,
         delete_marker: delete_marker
       }}
    else
      {:error, :not_found} when is_nil(version_id) -> {:error, :object_not_found}
      {:error, :not_found} -> {:error, :version_not_found}
      error -> error
    end
  end

  @doc """
  Creates a delete marker or permanently deletes an explicit metadata version.

  Blob bytes are never removed by this operation.
  """
  @spec delete(String.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, %{version_id: String.t(), kind: :delete_marker | :deleted}} | {:error, term()}
  def delete(bucket, key, version_id, opts \\ []) do
    opts = ensure_operation_id(opts)

    with {:ok, opts} <- pin_bucket_generation(bucket, opts),
         :ok <- ensure_bucket(bucket, opts),
         :ok <- ensure_cluster_write_enabled(opts),
         {:ok, opts} <- attach_cross_cluster_events(:delete, bucket, key, nil, opts),
         {:ok, deleted_version_id, kind} <-
           delete_version(bucket, key, version_id, opts) do
      run_side_effects(:delete, bucket, key, opts)
      {:ok, %{version_id: deleted_version_id, kind: kind}}
    end
  end

  @doc """
  Reuses a source object's immutable blob for a destination object.

  The local implementation verifies and stats the source blob without loading
  it into an object-sized binary.
  """
  @spec copy(String.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def copy(source_bucket, source_key, destination_bucket, destination_key, opts \\ []) do
    opts = ensure_operation_id(opts)
    source_version_id = Keyword.get(opts, :source_version_id)

    with {:ok, opts} <- pin_bucket_generation(destination_bucket, opts),
         :ok <- ensure_bucket(source_bucket, opts),
         :ok <- ensure_bucket(destination_bucket, opts),
         :ok <- ensure_cluster_write_enabled(opts),
         {:ok, source} <-
           copy_source_snapshot(source_bucket, source_key, source_version_id, opts),
         false <- source.delete_marker,
         hash when is_binary(hash) <- Map.get(source.metadata, :content_hash),
         :ok <- ensure_copy_ready(hash, source_bucket, opts),
         :ok <- verify_copy_blob(hash, source_bucket, opts),
         {:ok, blob_info} <- blob_store(opts).stat(hash, blob_opts(opts, bucket: source_bucket)) do
      attributes =
        source.metadata
        |> Map.drop([
          :bucket,
          :key,
          :version_id,
          :parent_version_id,
          :operation_id,
          :schema,
          :created_at,
          :updated_at,
          :is_delete_marker,
          :delete_marker
        ])
        |> Map.merge(attributes_option(opts))

      ready =
        blob_info
        |> to_plain_map()
        |> Map.put_new(:hash, hash)
        |> Map.put_new(:content_hash, hash)

      commit_existing_ready(
        destination_bucket,
        destination_key,
        ready,
        attributes,
        Keyword.put(opts, :blob_bucket, source_bucket)
      )
    else
      true -> {:error, :object_not_found}
      nil -> {:error, :blob_not_found}
      {:error, :not_found} -> {:error, :blob_not_found}
      error -> error
    end
  end

  @doc """
  Commits metadata for a blob that is already durable in the local blob store.

  Multipart completion uses this path after composing and committing the final
  blob. The caller retains the ready blob when metadata fails.
  """
  @spec commit_existing_blob(String.t(), String.t(), term(), map(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def commit_existing_blob(bucket, key, ready, attributes, opts \\ []) do
    blob_bucket = Keyword.get(opts, :blob_bucket, bucket)

    with {:ok, opts} <- pin_bucket_generation(bucket, opts),
         :ok <- ensure_bucket(bucket, opts),
         :ok <- ensure_cluster_write_enabled(opts),
         {:ok, %{content_hash: hash}} <- blob_identity(ready),
         :ok <- ensure_copy_ready(hash, blob_bucket, opts),
         :ok <- verify_copy_blob(hash, blob_bucket, opts),
         {:ok, blob_info} <-
           blob_store(opts).stat(hash, blob_opts(opts, bucket: blob_bucket)) do
      coordinated_ready =
        blob_info
        |> to_plain_map()
        |> Map.merge(to_plain_map(ready))
        |> Map.put_new(:hash, hash)

      commit_existing_ready(bucket, key, coordinated_ready, attributes, opts)
    end
  end

  defp put_cluster(bucket, key, data, attributes, opts) do
    opts = ensure_operation_id(opts)

    with {:ok, staged} <- stage_blob(data, opts) do
      put_cluster_staged(bucket, key, staged, attributes, opts)
    end
  end

  defp put_cluster_staged(bucket, key, staged, attributes, opts) do
    with {:ok, context} <- context(opts),
         {:ok, staged} <- protect_staged(staged, context, opts) do
      case open_operation_intent(staged, context, opts) do
        {:ok, opts} ->
          result =
            case write_coordinator(opts).ensure_blob(context, staged, coordinator_opts(opts)) do
              {:ok, evidence} ->
                commit_quorum_blob(bucket, key, staged, evidence, attributes, opts)

              {:error, _reason} = error ->
                _ = discard_staged(blob_store(opts), staged, blob_opts(opts))
                error
            end

          finish_operation_intent(result, opts)

        {:error, _reason} = error ->
          _ = discard_staged(blob_store(opts), staged, blob_opts(opts))
          error
      end
    end
  end

  defp put_standalone(bucket, key, data, attributes, opts) do
    with {:ok, staged} <- stage_blob(data, opts) do
      put_standalone_staged(bucket, key, staged, attributes, opts)
    end
  end

  defp put_standalone_staged(bucket, key, staged, attributes, opts) do
    with {:ok, context} <- context(opts),
         {:ok, staged} <- protect_staged(staged, context, opts) do
      case open_operation_intent(staged, context, opts) do
        {:ok, opts} ->
          case commit_staged_blob(blob_store(opts), staged, blob_opts(opts), opts) do
            {:ok, ready} ->
              finish_operation_intent(
                commit_ready_blob(bucket, key, ready, attributes, opts),
                opts
              )

            {:error, reason, :blob_commit_failed} ->
              finish_operation_intent({:error, reason}, opts, staged)

            {:error, _reason} = error ->
              finish_operation_intent(error, opts)
          end

        {:error, _reason} = error ->
          _ = discard_staged(blob_store(opts), staged, blob_opts(opts))
          error
      end
    end
  end

  defp protect_staged(staged, context, opts) do
    if blob_store(opts) == LocalCAS do
      operation_id = Keyword.fetch!(metadata_opts(opts), :operation_id)

      LocalCAS.protect(
        staged,
        {:hosted, operation_id},
        Keyword.merge(Context.blob_store_options(context), blob_opts(opts))
      )
    else
      {:ok, staged}
    end
  end

  defp open_operation_intent(blob, context, opts) do
    operation_id = Keyword.fetch!(metadata_opts(opts), :operation_id)

    with {:ok, %{content_hash: hash, size: size}} <- blob_identity(blob),
         {:ok, _intent} <-
           operation_intents(opts).open(
             operation_id,
             hash,
             size,
             context.config.node_id,
             context.config.node_generation,
             operation_intent_opts(context, opts)
           ) do
      metadata_opts =
        opts
        |> metadata_opts()
        |> Keyword.put(:operation_intent, true)
        |> Keyword.put(:operation_intents, operation_intents(opts))

      {:ok, Keyword.put(opts, :metadata_opts, metadata_opts)}
    end
  end

  defp finish_operation_intent(result, opts, owned_staged \\ nil)

  defp finish_operation_intent({:ok, _result} = result, _opts, _owned_staged), do: result

  defp finish_operation_intent({:error, reason} = error, opts, owned_staged) do
    state = if uncertain_operation_reason?(reason), do: :unknown, else: :aborted

    if state == :unknown, do: ExStorageService.Telemetry.uncertain_operation(:hosted, :unknown)
    operation_id = Keyword.fetch!(metadata_opts(opts), :operation_id)

    case operation_intents(opts).transition(operation_id, state, operation_intent_opts(nil, opts)) do
      {:ok, %{operation_id: ^operation_id, state: :aborted}} when state == :aborted ->
        discard_owned_aborted_stage(owned_staged, opts)

      _other ->
        :ok
    end

    error
  end

  defp uncertain_operation_reason?({phase, reason})
       when phase in [:sync, :rename, :directory_sync, :stat, :verify, :discard, :commit],
       do: uncertain_operation_reason?(reason)

  defp uncertain_operation_reason?(reason),
    do:
      reason in [
        :metadata_quorum_unavailable,
        :unknown_transaction_outcome,
        :timeout,
        :unknown,
        :cluster_not_ready,
        :no_leader
      ]

  defp discard_owned_aborted_stage(%StagedBlob{} = staged, opts) do
    # Only the active standalone writer's pre-metadata failure passes this path.
    # An aborted intent is mutable and never authorizes sweeping other stages.
    if blob_store(opts) == LocalCAS and StagingRecovery.protected?(staged.path),
      do: LocalCAS.discard(staged, blob_opts(opts)),
      else: :ok
  end

  defp discard_owned_aborted_stage(nil, _opts), do: :ok

  defp operation_intent_opts(context, opts) do
    protection_ms =
      case context do
        %Context{} -> context.config.orphan_grace_seconds * 1_000
        nil -> Keyword.get(opts, :operation_intent_protection_ms, 86_400_000)
      end

    opts
    |> metadata_opts()
    |> Keyword.take([:backend, :consistency, :timeout, :engine, :barrier])
    |> Keyword.put(:protection_ms, protection_ms)
    |> then(fn intent_opts ->
      case Keyword.fetch(opts, :now_ms) do
        {:ok, now_ms} -> Keyword.put(intent_opts, :now_ms, now_ms)
        :error -> intent_opts
      end
    end)
  end

  defp operation_intents(opts),
    do: Keyword.get(opts, :operation_intents, OperationIntents)

  defp commit_existing_ready(bucket, key, ready, attributes, opts) do
    opts = ensure_operation_id(opts)

    with {:ok, context} <- context(opts),
         {:ok, opts} <- open_operation_intent(ready, context, opts) do
      result =
        with {:ok, ready} <- revalidate_existing_ready(ready, bucket, opts) do
          if cluster_write?(opts) do
            with {:ok, evidence} <-
                   write_coordinator(opts).ensure_blob(
                     context,
                     ready,
                     coordinator_opts(opts)
                   ) do
              commit_quorum_blob(bucket, key, ready, evidence, attributes, opts)
            end
          else
            commit_ready_blob(bucket, key, ready, attributes, opts)
          end
        end

      finish_operation_intent(result, opts)
    end
  end

  defp revalidate_existing_ready(ready, bucket, opts) do
    blob_bucket = Keyword.get(opts, :blob_bucket, bucket)

    with {:ok, %{content_hash: hash, size: expected_size}} <- blob_identity(ready),
         :ok <- verify_copy_blob(hash, blob_bucket, opts),
         {:ok, %{size: actual_size} = blob_info} <-
           blob_store(opts).stat(hash, blob_opts(opts, bucket: blob_bucket)),
         true <- actual_size == expected_size do
      {:ok,
       ready
       |> to_plain_map()
       |> Map.merge(to_plain_map(blob_info))
       |> Map.put_new(:hash, hash)}
    else
      false -> {:error, :blob_size_mismatch}
      {:error, :not_found} -> {:error, :blob_not_found}
      {:error, _reason} = error -> error
    end
  end

  defp stage_blob(data, opts) do
    store = blob_store(opts)
    blob_opts = blob_opts(opts)

    case store.stage(data, blob_opts) do
      {:ok, staged} ->
        case run_fault(opts, :after_stage, %{staged_blob: staged}) do
          :ok ->
            {:ok, staged}

          {:error, _reason} = error ->
            discard_staged(store, staged, blob_opts)
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stage_blob_from_reader(reader, initial_state, opts) do
    store = blob_store(opts)
    blob_opts = blob_opts(opts)

    case store.stage_from_reader(reader, initial_state, blob_opts) do
      {:ok, staged, final_state} ->
        case run_fault(opts, :after_stage, %{staged_blob: staged}) do
          :ok ->
            {:ok, staged, final_state}

          {:error, reason} ->
            discard_staged(store, staged, blob_opts)
            {:error, reason, final_state}
        end

      {:error, reason, final_state} ->
        {:error, normalize_reader_stage_error(reason), final_state}

      {:error, reason} ->
        {:error, normalize_reader_stage_error(reason), initial_state}
    end
  end

  defp normalize_reader_stage_error({:stage, _reason} = error), do: error
  defp normalize_reader_stage_error(reason), do: {:stage, reason}

  defp attach_reader_state({:ok, result}, state), do: {:ok, result, state}
  defp attach_reader_state({:error, reason}, state), do: {:error, reason, state}

  defp commit_staged_blob(store, staged, blob_opts, opts) do
    case store.commit(staged, blob_opts) do
      {:ok, ready} ->
        case run_fault(opts, :after_blob_commit, %{ready_blob: ready}) do
          :ok -> {:ok, ready}
          {:error, _reason} = error -> error
        end

      {:error, reason} ->
        discard_staged(store, staged, blob_opts)
        {:error, reason, :blob_commit_failed}
    end
  end

  defp commit_ready_blob(bucket, key, ready, attributes, opts) do
    now =
      Keyword.get_lazy(opts, :timestamp, fn -> DateTime.utc_now() |> DateTime.to_iso8601() end)

    with {:ok, identity} <- blob_identity(ready),
         metadata <-
           attributes
           |> Map.new()
           |> Map.merge(identity)
           |> Map.put_new(:object_type, :blob)
           |> Map.put_new(:created_at, now)
           |> Map.put(:updated_at, now),
         :ok <-
           run_fault(opts, :metadata_commit, %{
             bucket: bucket,
             key: key,
             metadata: metadata,
             ready_blob: ready,
             operation_id: Keyword.get(metadata_opts(opts), :operation_id)
           }),
         {:ok, opts} <- attach_cross_cluster_events(:put, bucket, key, metadata, opts),
         {:ok, %{version_id: version_id} = publication} <-
           put_version(bucket, key, metadata, opts) do
      run_side_effects(:put, bucket, key, opts)

      {:ok,
       Map.merge(
         %{
           version_id: version_id,
           metadata: public_metadata(metadata, version_id),
           ready_blob: ready
         },
         Map.take(publication, [:versioning_state])
       )}
    end
  end

  defp commit_quorum_blob(bucket, key, source, evidence, attributes, opts) do
    now =
      Keyword.get_lazy(opts, :timestamp, fn -> DateTime.utc_now() |> DateTime.to_iso8601() end)

    with {:ok, identity} <- blob_identity(source),
         metadata <-
           attributes
           |> Map.new()
           |> Map.merge(identity)
           |> Map.put_new(:object_type, :blob)
           |> Map.put_new(:created_at, now)
           |> Map.put(:updated_at, now),
         :ok <-
           run_fault(opts, :metadata_commit, %{
             bucket: bucket,
             key: key,
             metadata: metadata,
             ready_blob: evidence.ready_blob,
             operation_id: Keyword.get(metadata_opts(opts), :operation_id),
             durability: evidence
           }),
         opts <- put_durability(opts, evidence),
         {:ok, opts} <- attach_cross_cluster_events(:put, bucket, key, metadata, opts),
         {:ok, %{version_id: version_id} = publication} <-
           put_version(bucket, key, metadata, opts) do
      run_side_effects(:put, bucket, key, opts)

      result =
        Map.merge(
          %{
            version_id: version_id,
            metadata: public_metadata(metadata, version_id)
          },
          Map.take(publication, [:versioning_state])
        )

      {:ok,
       if(evidence.ready_blob,
         do: Map.put(result, :ready_blob, evidence.ready_blob),
         else: result
       )}
    else
      {:error, reason} when reason in [:cluster_not_ready, :no_leader, :timeout, :unknown] ->
        {:error, :metadata_quorum_unavailable}

      error ->
        error
    end
  end

  defp blob_identity(%_{} = ready), do: ready |> Map.from_struct() |> blob_identity()

  defp blob_identity(ready) when is_map(ready) do
    hash = Map.get(ready, :hash, Map.get(ready, :content_hash))
    size = Map.get(ready, :size)
    etag = Map.get(ready, :etag)

    cond do
      not is_binary(hash) ->
        {:error, :invalid_ready_blob}

      not is_integer(size) or size < 0 ->
        {:error, :invalid_ready_blob}

      true ->
        identity = %{content_hash: hash, size: size}
        {:ok, if(is_binary(etag), do: Map.put(identity, :etag, etag), else: identity)}
    end
  end

  defp blob_identity(_ready), do: {:error, :invalid_ready_blob}

  defp public_metadata(metadata, "null"), do: Map.delete(metadata, :version_id)
  defp public_metadata(metadata, version_id), do: Map.put(metadata, :version_id, version_id)

  defp copy_source_snapshot(bucket, key, version_id, opts) do
    case Keyword.fetch(opts, :source_metadata) do
      {:ok, metadata} when is_map(metadata) ->
        {:ok, %{metadata: metadata, delete_marker: Map.get(metadata, :is_delete_marker, false)}}

      :error ->
        # `copy/5` pins the destination generation for publication. Source
        # selection pins its own bucket; it must not inherit that other fence.
        source_opts =
          Keyword.update(opts, :metadata_opts, [], &Keyword.delete(&1, :bucket_generation))

        head(bucket, key, version_id, source_opts)
    end
  end

  defp verify_copy_blob(hash, bucket, opts) do
    if Keyword.get(opts, :verify_copy, true) do
      case blob_store(opts).verify(hash, blob_opts(opts, bucket: bucket)) do
        :ok -> :ok
        {:error, :checksum_mismatch} -> {:error, :checksum_mismatch}
        {:error, :corrupt} -> {:error, :checksum_mismatch}
        {:error, :missing} -> {:error, :blob_not_found}
        {:error, :not_found} -> {:error, :blob_not_found}
        {:error, reason} -> {:error, reason}
      end
    else
      :ok
    end
  end

  defp ensure_copy_ready(hash, bucket, opts) do
    store = blob_store(opts)

    if Code.ensure_loaded?(store) and function_exported?(store, :ensure_ready, 2) do
      case store.ensure_ready(hash, blob_opts(opts, bucket: bucket)) do
        :ok -> :ok
        {:ok, _ready} -> :ok
        {:error, :not_found} -> {:error, :blob_not_found}
        {:error, reason} -> {:error, reason}
      end
    else
      :ok
    end
  end

  defp get_version(bucket, key, version_id, opts) do
    versioning = versioning(opts)
    metadata_opts = metadata_opts(opts)

    if Code.ensure_loaded?(versioning) and function_exported?(versioning, :get_version, 4) do
      versioning.get_version(bucket, key, version_id, metadata_opts)
    else
      versioning.get_version(bucket, key, version_id)
    end
  end

  defp put_version(bucket, key, metadata, opts) do
    versioning = versioning(opts)
    metadata_opts = metadata_opts(opts)

    cond do
      Code.ensure_loaded?(versioning) and function_exported?(versioning, :put_version_result, 4) ->
        versioning.put_version_result(bucket, key, metadata, metadata_opts)

      Code.ensure_loaded?(versioning) and function_exported?(versioning, :put_version, 4) ->
        normalize_publication(versioning.put_version(bucket, key, metadata, metadata_opts))

      true ->
        normalize_publication(versioning.put_version(bucket, key, metadata))
    end
  end

  defp normalize_publication({:ok, version_id}), do: {:ok, %{version_id: version_id}}
  defp normalize_publication(error), do: error

  defp delete_version(bucket, key, version_id, opts) do
    versioning = versioning(opts)
    metadata_opts = metadata_opts(opts)

    if Code.ensure_loaded?(versioning) and function_exported?(versioning, :delete_version, 4) do
      versioning.delete_version(bucket, key, version_id, metadata_opts)
    else
      versioning.delete_version(bucket, key, version_id)
    end
  end

  defp pin_bucket_generation(bucket, opts) do
    module = metadata(opts)

    if Code.ensure_loaded?(module) and function_exported?(module, :get_bucket, 1) do
      case module.get_bucket(bucket) do
        {:ok, value} ->
          generation = ExStorageService.Metadata.BucketLifecycle.generation(value)
          metadata_opts = Keyword.put_new(metadata_opts(opts), :bucket_generation, generation)

          ready =
            if function_exported?(module, :check_object_mutation, 2),
              do: module.check_object_mutation(bucket, metadata_opts),
              else: :ok

          with :ok <- ready, do: {:ok, Keyword.put(opts, :metadata_opts, metadata_opts)}

        {:error, :not_found} ->
          {:error, :bucket_not_found}

        error ->
          error
      end
    else
      # Injected metadata adapters may define their own lifecycle contract.
      {:ok, opts}
    end
  end

  defp ensure_bucket(bucket, opts) do
    case metadata(opts).head_bucket(bucket) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :bucket_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp run_fault(opts, boundary, context) do
    case Keyword.get(opts, :faults, []) do
      false ->
        :ok

      faults when is_list(faults) ->
        invoke_fault(Keyword.get(faults, boundary), context)

      faults when is_map(faults) ->
        invoke_fault(Map.get(faults, boundary), context)

      faults when is_atom(faults) ->
        if function_exported?(faults, :run, 2),
          do: normalize_fault_result(faults.run(boundary, context)),
          else: :ok
    end
  end

  defp invoke_fault(nil, _context), do: :ok
  defp invoke_fault(:ok, _context), do: :ok
  defp invoke_fault({:error, _reason} = error, _context), do: error

  defp invoke_fault(callback, context) when is_function(callback, 1),
    do: callback.(context) |> normalize_fault_result()

  defp invoke_fault(callback, _context) when is_function(callback, 0),
    do: callback.() |> normalize_fault_result()

  defp invoke_fault(other, _context), do: {:error, {:invalid_fault, other}}

  defp normalize_fault_result(:ok), do: :ok
  defp normalize_fault_result({:error, _reason} = error), do: error
  defp normalize_fault_result(other), do: {:error, {:invalid_fault_result, other}}

  defp run_side_effects(action, bucket, key, opts) do
    case Keyword.get(opts, :side_effects, DefaultSideEffects) do
      false ->
        :ok

      true ->
        run_side_effect_module(DefaultSideEffects, action, bucket, key)

      effects when is_atom(effects) ->
        run_side_effect_module(effects, action, bucket, key)

      effects when is_list(effects) ->
        invoke_effect(Keyword.get(effects, effect_name(action)), [bucket, key])
        invoke_effect(Keyword.get(effects, :broadcast), [bucket, action, key])
        :ok

      effects when is_map(effects) ->
        invoke_effect(Map.get(effects, effect_name(action)), [bucket, key])
        invoke_effect(Map.get(effects, :broadcast), [bucket, action, key])
        :ok
    end
  end

  defp run_side_effect_module(effects, action, bucket, key) do
    effect = effect_name(action)
    if function_exported?(effects, effect, 2), do: apply(effects, effect, [bucket, key])
    if function_exported?(effects, :broadcast, 3), do: effects.broadcast(bucket, action, key)
    :ok
  end

  defp invoke_effect(nil, _args), do: :ok

  defp invoke_effect(callback, args) when is_function(callback),
    do: apply(callback, Enum.take(args, Function.info(callback, :arity) |> elem(1)))

  defp effect_name(:put), do: :after_put
  defp effect_name(:delete), do: :after_delete

  defp discard_staged(store, staged, opts) do
    cond do
      store == LocalCAS -> LocalCAS.discard_unprotected(staged, opts)
      function_exported?(store, :discard, 2) -> store.discard(staged, opts)
      function_exported?(store, :discard, 1) -> store.discard(staged)
      true -> :ok
    end
  end

  defp to_plain_map(%_{} = struct), do: Map.from_struct(struct)
  defp to_plain_map(map) when is_map(map), do: map

  defp reader_attributes(opts, final_state) do
    case Keyword.get(opts, :reader_attributes) do
      nil -> %{}
      finalize when is_function(finalize, 1) -> finalize.(final_state)
    end
  end

  defp attributes_option(opts), do: opts |> Keyword.get(:attributes, %{}) |> Map.new()
  defp metadata_opts(opts), do: Keyword.get(opts, :metadata_opts, [])

  defp put_durability(opts, evidence) do
    Keyword.update(opts, :metadata_opts, [durability: evidence], fn metadata_opts ->
      Keyword.put(metadata_opts, :durability, evidence)
    end)
  end

  defp attach_cross_cluster_events(:put, bucket, key, object, opts) do
    if Keyword.get(opts, :side_effects) == false do
      {:ok, opts}
    else
      event_opts = [operation_id: Keyword.fetch!(metadata_opts(opts), :operation_id)]

      with {:ok, events} <-
             cross_cluster_hooks(opts).events_for_put(bucket, key, object, event_opts) do
        {:ok, put_metadata_events(opts, events)}
      end
    end
  end

  defp attach_cross_cluster_events(:delete, bucket, key, _object, opts) do
    if Keyword.get(opts, :side_effects) == false do
      {:ok, opts}
    else
      event_opts = [operation_id: Keyword.fetch!(metadata_opts(opts), :operation_id)]

      with {:ok, events} <-
             cross_cluster_hooks(opts).events_for_delete(bucket, key, event_opts) do
        {:ok, put_metadata_events(opts, events)}
      end
    end
  end

  defp put_metadata_events(opts, events) do
    Keyword.update(opts, :metadata_opts, [events: events], fn metadata_opts ->
      Keyword.put(metadata_opts, :events, events)
    end)
  end

  defp cross_cluster_hooks(opts) do
    Keyword.get(
      opts,
      :cross_cluster_hooks,
      ExStorageService.CrossClusterReplication.Hooks
    )
  end

  defp ensure_operation_id(opts) do
    metadata_opts =
      opts
      |> metadata_opts()
      |> Keyword.put_new_lazy(:operation_id, &generated_operation_id/0)

    Keyword.put(opts, :metadata_opts, metadata_opts)
  end

  defp generated_operation_id do
    "object-service-" <>
      (:crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false))
  end

  defp coordinator_opts(opts) do
    operation_id = Keyword.get(metadata_opts(opts), :operation_id)

    opts
    |> Keyword.take([
      :placement_records,
      :membership,
      :placement,
      :transport,
      :transport_opts,
      :task_supervisor,
      :replica_concurrency,
      :transfer_timeout,
      :replication_factor,
      :write_quorum,
      :allow_degraded_writes,
      :blob_store,
      :blob_store_opts,
      :timestamp
    ])
    |> Keyword.put(:operation_id, operation_id)
    |> Keyword.merge(
      Keyword.take(metadata_opts(opts), [:backend, :consistency, :timeout, :engine, :barrier])
    )
  end

  defp read_coordinator_opts(opts) do
    opts
    |> Keyword.take([
      :backend,
      :consistency,
      :timeout,
      :engine,
      :barrier,
      :timestamp,
      :bucket,
      :placement_records,
      :membership,
      :placement,
      :transport,
      :transport_opts,
      :source_order,
      :max_remote_attempts,
      :prefetch_bytes,
      :request_id,
      :replication_factor,
      :blob_store,
      :blob_store_opts,
      :verify_local,
      :locations,
      :read_repair,
      :read_repair_module,
      :capacity_policy,
      :repair_task_supervisor,
      :repair_finalizer
    ])
    |> Keyword.merge(
      Keyword.take(metadata_opts(opts), [:backend, :consistency, :timeout, :engine, :barrier])
    )
  end

  defp strong_read_opts(opts) do
    case context(opts) do
      {:ok, %Context{config: %{mode: :cluster}}} ->
        metadata_opts =
          opts
          |> metadata_opts()
          |> Keyword.put_new(:consistency, :strong)

        Keyword.put(opts, :metadata_opts, metadata_opts)

      _ ->
        opts
    end
  end

  defp ensure_cluster_write_enabled(opts) do
    case context(opts) do
      {:ok, %Context{config: %{mode: :cluster, cluster_data_plane_enabled: false}}} ->
        {:error, :cluster_data_plane_disabled}

      {:ok, _context} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cluster_write?(opts) do
    match?(
      {:ok, %Context{config: %{mode: :cluster, cluster_data_plane_enabled: true}}},
      context(opts)
    )
  end

  defp context(opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context -> {:ok, context}
      nil -> Context.default()
    end
  end

  defp blob_opts(opts, extra \\ []) do
    context_opts =
      case Keyword.get(opts, :context) do
        %Context{} = context ->
          Context.blob_store_options(context)

        _ ->
          case Context.default() do
            {:ok, context} -> Context.blob_store_options(context)
            {:error, _} -> []
          end
      end

    context_opts
    |> Keyword.merge(Keyword.get(opts, :blob_store_opts, []))
    |> Keyword.merge(extra)
  end

  defp metadata(opts), do: Keyword.get(opts, :metadata, Metadata)
  defp blob_store(opts), do: Keyword.get(opts, :blob_store, LocalCAS)
  defp versioning(opts), do: Keyword.get(opts, :versioning, Versioning)
  defp write_coordinator(opts), do: Keyword.get(opts, :write_coordinator, WriteCoordinator)
  defp read_coordinator(opts), do: Keyword.get(opts, :read_coordinator, ReadCoordinator)

  defmodule DefaultSideEffects do
    @moduledoc false

    def after_put(_bucket, _key), do: :ok
    def after_delete(_bucket, _key), do: :ok

    def broadcast(bucket, action, key) do
      Phoenix.PubSub.broadcast(
        ExStorageService.PubSub,
        "bucket:#{bucket}",
        {:bucket_changed, %{action: action, key: key, bucket: bucket}}
      )
    end
  end
end
