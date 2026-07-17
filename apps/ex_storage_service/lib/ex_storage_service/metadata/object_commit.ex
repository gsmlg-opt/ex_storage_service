defmodule ExStorageService.Metadata.ObjectCommit do
  @moduledoc """
  Atomic v2 object metadata commits.

  Each logical operation writes an operation record in the same Concord
  transaction as the immutable version and mutable head. That record is the
  source of truth when the transaction result is ambiguous.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys

  @default_max_attempts 16

  @type commit_result :: %{
          operation_id: String.t(),
          version_id: String.t(),
          kind: :put | :delete_marker | :deleted
        }

  @spec put(String.t(), String.t(), map(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def put(bucket, key, metadata, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      version_id = Keyword.get_lazy(opts, :version_id, &generate_version_id/0)

      commit_new_version(
        bucket,
        key,
        metadata,
        operation_id,
        version_id,
        :put,
        opts
      )
    end
  end

  @spec delete_marker(String.t(), String.t(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def delete_marker(bucket, key, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      version_id = Keyword.get_lazy(opts, :version_id, &generate_version_id/0)
      now = Keyword.get_lazy(opts, :timestamp, &timestamp/0)

      metadata = %{
        is_delete_marker: true,
        delete_marker: true,
        object_type: :blob,
        created_at: now,
        updated_at: now
      }

      commit_new_version(
        bucket,
        key,
        metadata,
        operation_id,
        version_id,
        :delete_marker,
        opts
      )
    end
  end

  @doc """
  Permanently removes one metadata version and atomically repairs the head.

  Blob bytes are intentionally untouched.
  """
  @spec delete_version(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def delete_version(bucket, key, version_id, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      backend = backend(opts)
      operation_key = Keys.outbox(operation_id)

      case resolve(backend, operation_key, opts) do
        {:ok, result} ->
          {:ok, result}

        :not_found ->
          do_delete_version(
            backend,
            bucket,
            key,
            version_id,
            operation_id,
            operation_key,
            opts,
            max_attempts(opts)
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec get_head(String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def get_head(bucket, key, opts \\ []) do
    backend = backend(opts)

    with {:ok, %{value: head}} <- backend.get(Keys.object_head(bucket, key), read_opts(opts)),
         {:ok, %{value: version}} <-
           backend.get(Keys.object_version(bucket, key, head.version_id), read_opts(opts)) do
      {:ok, version_to_public(version)}
    else
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  @spec get_version(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def get_version(bucket, key, version_id, opts \\ []) do
    case backend(opts).get(Keys.object_version(bucket, key, version_id), read_opts(opts)) do
      {:ok, %{value: version}} -> {:ok, version_to_public(version)}
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  @spec list_versions(String.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def list_versions(bucket, key, opts \\ []) do
    prefix = Keys.object_version_prefix(bucket, key)

    with {:ok, records} <- backend(opts).scan(prefix, read_opts(opts)) do
      versions =
        records
        |> Enum.map(fn {_record_key, version} -> version_to_public(version) end)
        |> Enum.sort_by(
          fn version -> {Map.get(version, :created_at, ""), version.version_id} end,
          :desc
        )

      {:ok, versions}
    end
  end

  defp commit_new_version(
         bucket,
         key,
         metadata,
         operation_id,
         version_id,
         kind,
         opts
       ) do
    backend = backend(opts)
    operation_key = Keys.outbox(operation_id)

    case resolve(backend, operation_key, opts) do
      {:ok, result} ->
        {:ok, result}

      :not_found ->
        do_commit_new_version(
          backend,
          bucket,
          key,
          metadata,
          operation_id,
          operation_key,
          version_id,
          kind,
          opts,
          max_attempts(opts)
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_commit_new_version(_backend, _bucket, _key, _meta, _op, _op_key, _vid, _kind, _opts, 0),
    do: {:error, :compare_retry_exhausted}

  defp do_commit_new_version(
         backend,
         bucket,
         key,
         metadata,
         operation_id,
         operation_key,
         version_id,
         kind,
         opts,
         attempts_left
       ) do
    head_key = Keys.object_head(bucket, key)

    with {:ok, observed_head} <- backend.get(head_key, read_opts(opts)) do
      parent_version_id = value_field(observed_head, :version_id)
      now = Map.get(metadata, :created_at, timestamp())
      delete_marker? = kind == :delete_marker

      version =
        metadata
        |> Map.put(:schema, 2)
        |> Map.put(:bucket, bucket)
        |> Map.put(:key, key)
        |> Map.put(:version_id, version_id)
        |> Map.put(:parent_version_id, parent_version_id)
        |> Map.put(:delete_marker, delete_marker?)
        |> Map.put(:is_delete_marker, delete_marker?)
        |> Map.put_new(:object_type, :blob)
        |> Map.put_new(:created_at, now)

      head = %{
        schema: 2,
        bucket: bucket,
        key: key,
        version_id: version_id,
        delete_marker: delete_marker?,
        etag: Map.get(version, :etag),
        updated_at: Map.get(metadata, :updated_at, now)
      }

      result = %{
        operation_id: operation_id,
        version_id: version_id,
        kind: kind
      }

      spec = %{
        compare: [
          head_compare(head_key, observed_head),
          {:exists, operation_key, :==, false},
          {:exists, Keys.object_version(bucket, key, version_id), :==, false}
        ],
        success:
          [
            {:put, Keys.object_version(bucket, key, version_id), version, %{}},
            {:put, head_key, head, %{}}
          ] ++ blob_operation(version, now) ++ [{:put, operation_key, result, %{}}],
        failure: []
      }

      case backend.transaction(spec, transaction_opts(opts, operation_id)) do
        {:ok, %{succeeded: true}} ->
          {:ok, result}

        {:ok, %{succeeded: false}} ->
          retry_or_resolve(
            backend,
            operation_key,
            opts,
            fn ->
              do_commit_new_version(
                backend,
                bucket,
                key,
                metadata,
                operation_id,
                operation_key,
                version_id,
                kind,
                opts,
                attempts_left - 1
              )
            end
          )

        {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready] ->
          retry_or_resolve(
            backend,
            operation_key,
            opts,
            fn ->
              do_commit_new_version(
                backend,
                bucket,
                key,
                metadata,
                operation_id,
                operation_key,
                version_id,
                kind,
                opts,
                attempts_left - 1
              )
            end
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp do_delete_version(
         _backend,
         _bucket,
         _key,
         _version_id,
         _operation_id,
         _operation_key,
         _opts,
         0
       ),
       do: {:error, :compare_retry_exhausted}

  defp do_delete_version(
         backend,
         bucket,
         key,
         version_id,
         operation_id,
         operation_key,
         opts,
         attempts_left
       ) do
    head_key = Keys.object_head(bucket, key)
    version_key = Keys.object_version(bucket, key, version_id)

    with {:ok, observed_head} <- backend.get(head_key, read_opts(opts)),
         {:ok, observed_version} <- backend.get(version_key, read_opts(opts)),
         {:ok, versions} <- list_versions(bucket, key, Keyword.put(opts, :backend, backend)) do
      if observed_version == nil do
        {:ok, %{operation_id: operation_id, version_id: version_id, kind: :deleted}}
      else
        remaining = Enum.reject(versions, &(&1.version_id == version_id))
        replacement = List.first(remaining)
        result = %{operation_id: operation_id, version_id: version_id, kind: :deleted}

        head_ops =
          if value_field(observed_head, :version_id) == version_id do
            replacement_head_operation(head_key, bucket, key, replacement)
          else
            []
          end

        spec = %{
          compare: [
            head_compare(head_key, observed_head),
            {:mod_revision, version_key, :==, observed_version.mod_revision},
            {:exists, operation_key, :==, false}
          ],
          success:
            [{:delete, {:key, version_key}, %{}}] ++
              head_ops ++ [{:put, operation_key, result, %{}}],
          failure: []
        }

        case backend.transaction(spec, transaction_opts(opts, operation_id)) do
          {:ok, %{succeeded: true}} ->
            {:ok, result}

          {:ok, %{succeeded: false}} ->
            retry_or_resolve(backend, operation_key, opts, fn ->
              do_delete_version(
                backend,
                bucket,
                key,
                version_id,
                operation_id,
                operation_key,
                opts,
                attempts_left - 1
              )
            end)

          {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready] ->
            retry_or_resolve(backend, operation_key, opts, fn ->
              do_delete_version(
                backend,
                bucket,
                key,
                version_id,
                operation_id,
                operation_key,
                opts,
                attempts_left - 1
              )
            end)

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  defp replacement_head_operation(head_key, _bucket, _key, nil),
    do: [{:delete, {:key, head_key}, %{}}]

  defp replacement_head_operation(head_key, bucket, key, version) do
    head = %{
      schema: 2,
      bucket: bucket,
      key: key,
      version_id: version.version_id,
      delete_marker: Map.get(version, :delete_marker, false),
      etag: Map.get(version, :etag),
      updated_at: Map.get(version, :created_at)
    }

    [{:put, head_key, head, %{}}]
  end

  defp blob_operation(%{content_hash: hash, size: size}, now)
       when is_binary(hash) and is_integer(size) do
    blob = %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: size,
      desired_replication_factor: 1,
      created_at: now
    }

    [{:put, Keys.blob(hash), blob, %{}}]
  end

  defp blob_operation(_version, _now), do: []

  defp head_compare(head_key, nil), do: {:mod_revision, head_key, :==, 0}

  defp head_compare(head_key, %{mod_revision: revision}),
    do: {:mod_revision, head_key, :==, revision}

  defp value_field(nil, _field), do: nil
  defp value_field(%{value: value}, field), do: Map.get(value, field)

  defp retry_or_resolve(backend, operation_key, opts, retry) do
    case resolve(backend, operation_key, opts) do
      {:ok, result} -> {:ok, result}
      :not_found -> retry.()
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve(backend, operation_key, opts) do
    case backend.resolve_operation(operation_key, read_opts(opts)) do
      {:ok, %{value: result}} -> {:ok, result}
      {:ok, nil} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp version_to_public(version) do
    version
    |> Map.put(:is_delete_marker, Map.get(version, :delete_marker, false))
    |> Map.put(:metadata, Map.get(version, :user_metadata, Map.get(version, :metadata, %{})))
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp max_attempts(opts), do: Keyword.get(opts, :max_attempts, @default_max_attempts)

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, operation_id) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, operation_id)
  end

  defp ensure_v2_writes(opts) do
    schema =
      Keyword.get_lazy(opts, :metadata_schema, fn ->
        :ex_storage_service
        |> Application.get_env(:instance_config, [])
        |> Keyword.get(:metadata_schema, :v2)
      end)

    if schema == :v2, do: :ok, else: {:error, :v2_metadata_writes_disabled}
  end

  defp generate_operation_id, do: "object-commit-" <> random_id()

  defp generate_version_id do
    "#{System.system_time(:microsecond)}-#{random_id()}"
  end

  defp random_id, do: :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
  defp timestamp, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
