defmodule ExStorageService.ObjectService do
  @moduledoc """
  Coordinates object metadata with durable blob storage.

  This is the object-domain boundary used by protocol adapters. It deliberately
  contains no Plug or S3 response logic. Blob and metadata implementations are
  injectable per call so fault and concurrency tests do not require mutable
  global configuration.
  """

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Context
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning

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
    with :ok <- ensure_bucket(bucket, opts),
         {:ok, ready} <- store_blob(data, opts) do
      attributes = %{
        content_type: content_type,
        metadata: user_metadata,
        user_metadata: user_metadata
      }

      commit_ready_blob(bucket, key, ready, attributes, opts)
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
        case Map.fetch(result.metadata, :content_hash) do
          {:ok, hash} when is_binary(hash) ->
            range = Keyword.get(opts, :range)

            case blob_store(opts).open(hash, range, blob_opts(opts, bucket: bucket)) do
              {:ok, source} -> {:ok, Map.put(result, :source, source)}
              {:error, :not_found} -> {:error, :blob_not_found}
              {:error, reason} -> {:error, reason}
            end

          _missing_or_invalid_hash ->
            {:error, :invalid_object_metadata}
        end
      end
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
         {:ok, metadata} <- get_version(bucket, key, version_id, opts) do
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
    with :ok <- ensure_bucket(bucket, opts),
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
    source_version_id = Keyword.get(opts, :source_version_id)

    with :ok <- ensure_bucket(source_bucket, opts),
         :ok <- ensure_bucket(destination_bucket, opts),
         {:ok, source} <- head(source_bucket, source_key, source_version_id, opts),
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

      commit_ready_blob(destination_bucket, destination_key, ready, attributes, opts)
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

    with :ok <- ensure_bucket(bucket, opts),
         {:ok, %{content_hash: hash}} <- blob_identity(ready),
         :ok <- ensure_copy_ready(hash, blob_bucket, opts),
         :ok <- verify_copy_blob(hash, blob_bucket, opts) do
      commit_ready_blob(bucket, key, ready, attributes, opts)
    end
  end

  defp store_blob(data, opts) do
    store = blob_store(opts)
    blob_opts = blob_opts(opts)

    case store.stage(data, blob_opts) do
      {:ok, staged} ->
        case run_fault(opts, :after_stage, %{staged_blob: staged}) do
          :ok ->
            commit_staged_blob(store, staged, blob_opts, opts)

          {:error, _reason} = error ->
            discard_staged(store, staged, blob_opts)
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp commit_staged_blob(store, staged, blob_opts, opts) do
    case store.commit(staged, blob_opts) do
      {:ok, ready} ->
        case run_fault(opts, :after_blob_commit, %{ready_blob: ready}) do
          :ok -> {:ok, ready}
          {:error, _reason} = error -> error
        end

      {:error, reason} ->
        discard_staged(store, staged, blob_opts)
        {:error, reason}
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
         {:ok, version_id} <- put_version(bucket, key, metadata, opts) do
      run_side_effects(:put, bucket, key, opts)

      {:ok,
       %{
         version_id: version_id,
         metadata: public_metadata(metadata, version_id),
         ready_blob: ready
       }}
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

    if function_exported?(versioning, :get_version, 4) do
      versioning.get_version(bucket, key, version_id, metadata_opts)
    else
      versioning.get_version(bucket, key, version_id)
    end
  end

  defp put_version(bucket, key, metadata, opts) do
    versioning = versioning(opts)
    metadata_opts = metadata_opts(opts)

    if function_exported?(versioning, :put_version, 4) do
      versioning.put_version(bucket, key, metadata, metadata_opts)
    else
      versioning.put_version(bucket, key, metadata)
    end
  end

  defp delete_version(bucket, key, version_id, opts) do
    versioning = versioning(opts)
    metadata_opts = metadata_opts(opts)

    if function_exported?(versioning, :delete_version, 4) do
      versioning.delete_version(bucket, key, version_id, metadata_opts)
    else
      versioning.delete_version(bucket, key, version_id)
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
      function_exported?(store, :discard, 2) -> store.discard(staged, opts)
      function_exported?(store, :discard, 1) -> store.discard(staged)
      true -> :ok
    end
  end

  defp to_plain_map(%_{} = struct), do: Map.from_struct(struct)
  defp to_plain_map(map) when is_map(map), do: map

  defp attributes_option(opts), do: opts |> Keyword.get(:attributes, %{}) |> Map.new()
  defp metadata_opts(opts), do: Keyword.get(opts, :metadata_opts, [])

  defp blob_opts(opts, extra \\ []) do
    context_opts =
      case Keyword.get(opts, :context) do
        %Context{} = context -> Context.blob_store_options(context)
        _ -> []
      end

    context_opts
    |> Keyword.merge(Keyword.get(opts, :blob_store_opts, []))
    |> Keyword.merge(extra)
  end

  defp metadata(opts), do: Keyword.get(opts, :metadata, Metadata)
  defp blob_store(opts), do: Keyword.get(opts, :blob_store, LocalCAS)
  defp versioning(opts), do: Keyword.get(opts, :versioning, Versioning)

  defmodule DefaultSideEffects do
    @moduledoc false

    alias ExStorageService.Replication.Hooks

    def after_put(bucket, key), do: Hooks.after_put(bucket, key)
    def after_delete(bucket, key), do: Hooks.after_delete(bucket, key)

    def broadcast(bucket, action, key) do
      Phoenix.PubSub.broadcast(
        ExStorageService.PubSub,
        "bucket:#{bucket}",
        {:bucket_changed, %{action: action, key: key, bucket: bucket}}
      )
    end
  end
end
