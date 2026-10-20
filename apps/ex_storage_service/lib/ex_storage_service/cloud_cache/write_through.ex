defmodule ExStorageService.CloudCache.WriteThrough do
  @moduledoc """
  Validated, disk-staged gateway writes with an explicit uncertain-outcome journal.

  Reconciliation never repeats an upstream mutation. It requires the independent
  upstream HEAD identity markers before repairing local metadata/cache state.
  Prepared or unknown operations remain visible for operator resolution when the
  marker is missing or has been replaced by another writer.
  """
  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.CloudCache.{Client, Config, LocalStore}
  alias ExStorageService.Metadata.{BucketLifecycle, Keys}
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  @prefix "ess:gateway-write:"

  def put(bucket, key, staged, %Config{} = config, attributes, operation_id, opts \\ []) do
    blob_opts =
      Keyword.get(opts, :blob_store_opts, Keyword.take(opts, [:tmp_dir, :managed_staging]))

    with {:ok, staged} <- LocalCAS.protect(staged, {:gateway, operation_id}, blob_opts) do
      put_protected(bucket, key, staged, config, attributes, operation_id, opts)
    end
  end

  defp put_protected(bucket, key, staged, config, attributes, operation_id, opts) do
    backend = Keyword.get(opts, :backend, ConcordBackend)

    record = %{
      operation_id: operation_id,
      bucket: bucket,
      key: key,
      state: :prepared,
      path: staged.path,
      hash: staged.hash,
      etag: staged.etag,
      size: staged.size,
      endpoint: Config.endpoint_url(config),
      remote_bucket: config.bucket,
      attributes: attributes
    }

    with {:ok, snapshot} <- snapshot(backend, bucket, key, opts),
         record = Map.merge(record, snapshot),
         :ok <- sync_stage(staged.path),
         :ok <- normalize(backend.put(journal_key(operation_id), record, [])) do
      client = Keyword.get(opts, :client, Client)

      metadata =
        Map.merge(attributes.metadata, %{
          "ess-operation-id" => operation_id,
          "ess-content-sha256" => staged.hash
        })

      case client.put_file(config, key, staged.path, attributes.content_type, metadata,
             size: staged.size,
             sha256: staged.hash,
             headers: Map.get(attributes, :headers, [])
           ) do
        :ok ->
          finish(record, config, opts)

        {:error, {:http_error, status}} = error when status in 400..499 ->
          case normalize(backend.delete(journal_key(operation_id), [])) do
            :ok -> LocalCAS.discard(staged)
            {:error, _} -> :ok
          end

          error

        {:error, reason} ->
          ExStorageService.Telemetry.uncertain_operation(:gateway_write, :unknown)
          _ = mark_unknown(backend, operation_id)
          {:error, {:upstream_unknown, reason}}
      end
    else
      {:error, reason} ->
        _ = LocalCAS.discard_unprotected(staged)
        {:error, reason}
    end
  end

  defp mark_unknown(backend, operation_id) do
    key = journal_key(operation_id)

    with {:ok, %{value: %{state: :prepared} = current, mod_revision: revision}} <-
           backend.get(key, consistency: :linearizable) do
      backend.transaction(
        %{
          compare: [{:mod_revision, key, :==, revision}],
          success: [{:put, key, %{current | state: :unknown}, %{}}],
          failure: []
        },
        []
      )
    end
  end

  @doc "Reconciles one bounded page; no upstream mutation is ever retried."
  def reconcile_page(cursor \\ nil, limit \\ 100, opts \\ []) when limit in 1..1000 do
    backend = Keyword.get(opts, :backend, ConcordBackend)

    with {:ok, page} <- backend.list_page(@prefix, cursor, limit, []) do
      results =
        Enum.map(page.entries, fn %{value: record} ->
          {record.operation_id, reconcile(record, opts)}
        end)

      {:ok, %{results: results, next_cursor: page.next_cursor}}
    end
  end

  defp reconcile(record, opts) do
    get_config = Keyword.get(opts, :get_config, &Config.get_active_config/1)
    client = Keyword.get(opts, :client, Client)

    with {:ok, config} <- get_config.(record.bucket),
         true <-
           Config.endpoint_url(config) == record.endpoint and
             config.bucket == record.remote_bucket,
         {:ok, head} <- client.head_object(config, record.key),
         %{"ess-operation-id" => operation, "ess-content-sha256" => hash} <-
           Map.get(head, :metadata, %{}),
         true <-
           operation == record.operation_id and hash == record.hash and
             head.content_length == record.size do
      finish(record, config, opts)
    else
      false -> {:error, :upstream_identity_changed}
      :disabled -> {:error, :gateway_disabled}
      {:error, _} = error -> error
      _ -> {:error, :upstream_outcome_unresolved}
    end
  end

  defp finish(record, config, opts) do
    backend = Keyword.get(opts, :backend, ConcordBackend)
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    meta = %{
      content_hash: record.hash,
      etag: record.etag,
      size: record.size,
      content_type: record.attributes.content_type,
      metadata: record.attributes.metadata,
      system_headers: Map.get(record.attributes, :headers, []),
      checksums: Map.get(record.attributes, :checksums, %{}),
      created_at: now,
      updated_at: now,
      cloud_backed: true,
      gateway_operation_id: record.operation_id
    }

    before_commit = Keyword.get(opts, :before_local_commit, fn -> :ok end)

    # Each concurrent writer can advance the bucket for publication and cache
    # admission. Keep retries bounded while accommodating both revision changes.
    with :ok <- before_commit.(),
         :ok <- commit_metadata(backend, record, meta, opts, 8),
         :ok <- cache(record, config, opts),
         :ok <- normalize(backend.delete(journal_key(record.operation_id), [])) do
      _ = File.rm(record.path)
      {:ok, meta}
    else
      {:error, reason} ->
        ExStorageService.Telemetry.uncertain_operation(:gateway_write, :reconciliation_required)
        {:error, {:local_reconciliation_required, reason}}
    end
  end

  defp snapshot(backend, bucket, key, opts) do
    with {:ok, %{value: bucket_meta}} <-
           backend.get("bucket:#{bucket}", consistency: :linearizable),
         true <-
           not Keyword.has_key?(opts, :bucket_generation) or
             opts[:bucket_generation] == BucketLifecycle.generation(bucket_meta),
         :ok <-
           ExStorageService.Metadata.ListIndex.mutation_ready(
             backend,
             bucket,
             BucketLifecycle.generation(bucket_meta),
             opts
           ),
         {:ok, local} <- backend.get("obj:#{bucket}:#{key}", consistency: :linearizable),
         {:ok, head} <- backend.get(Keys.object_head(bucket, key), consistency: :linearizable) do
      {:ok,
       %{
         bucket_generation: BucketLifecycle.generation(bucket_meta),
         local_revision: revision(local),
         head_revision: revision(head)
       }}
    else
      {:ok, nil} -> {:error, :bucket_not_found}
      false -> {:error, :bucket_generation_changed}
      {:error, _} = error -> error
    end
  end

  defp commit_metadata(_backend, _record, _meta, _opts, 0), do: {:error, :local_metadata_conflict}

  defp commit_metadata(backend, record, meta, opts, attempts) do
    local_key = "obj:#{record.bucket}:#{record.key}"
    journal_key = journal_key(record.operation_id)

    with {:ok, guard} <-
           BucketLifecycle.guard(backend, record.bucket,
             bucket_generation: record.bucket_generation
           ),
         {:ok, local} <- backend.get(local_key, consistency: :linearizable),
         {:ok, head} <-
           backend.get(Keys.object_head(record.bucket, record.key), consistency: :linearizable),
         {:ok, journal} <- backend.get(journal_key, consistency: :linearizable) do
      cond do
        local != nil and Map.get(local.value, :gateway_operation_id) == record.operation_id ->
          :ok

        revision(local) != record.local_revision or revision(head) != record.head_revision ->
          {:error, :local_metadata_conflict}

        journal == nil ->
          {:error, :journal_missing}

        true ->
          spec = %{
            compare:
              guard.compare ++
                [
                  compare(local_key, record.local_revision),
                  compare(Keys.object_head(record.bucket, record.key), record.head_revision),
                  {:mod_revision, journal_key, :==, journal.mod_revision}
                ],
            success:
              guard.success ++
                [
                  {:put, local_key, meta, %{}},
                  {:put, journal_key, %{record | state: :local_committed}, %{}}
                ],
            failure: []
          }

          case backend.transaction(spec, []) do
            {:ok, %{succeeded: true}} ->
              :ok

            {:ok, %{succeeded: false}} ->
              commit_metadata(backend, record, meta, opts, attempts - 1)

            {:error, _} = error ->
              error
          end
      end
    end
  end

  defp revision(nil), do: nil
  defp revision(record), do: record.mod_revision
  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, revision), do: {:mod_revision, key, :==, revision}

  defp cache(record, %{cache_enabled: true} = config, opts) do
    cache = Keyword.get(opts, :cache, LocalStore)

    result =
      if Code.ensure_loaded?(cache) and function_exported?(cache, :put_file, 7) do
        cache.put_file(record.bucket, record.key, record.path, record.hash, record.size, config,
          bucket_generation: record.bucket_generation
        )
      else
        cache.put_file(record.bucket, record.key, record.path, record.hash, record.size, config)
      end

    case result do
      {:ok, _path} -> :ok
      # Cache admission is optional after upstream and local identity commit.
      # A competing cache fill must not turn an acknowledged write into a
      # retryable S3 failure that could create another upstream version.
      {:error, :cache_busy} -> :ok
      {:error, _} = error -> error
    end
  end

  defp cache(_record, _config, _opts), do: :ok

  defp sync_stage(path) do
    with {:ok, io} <- File.open(path, [:read, :write, :raw, :binary]) do
      try do
        with :ok <- :file.sync(io),
             {:ok, directory} <-
               :file.open(String.to_charlist(Path.dirname(path)), [:read, :raw, :directory]) do
          try do
            :file.sync(directory)
          after
            :file.close(directory)
          end
        end
      after
        File.close(io)
      end
    end
  end

  defp journal_key(id), do: @prefix <> Base.url_encode64(id, padding: false)
  defp normalize(:ok), do: :ok
  defp normalize({:ok, _}), do: :ok
  defp normalize({:error, _} = error), do: error
end
