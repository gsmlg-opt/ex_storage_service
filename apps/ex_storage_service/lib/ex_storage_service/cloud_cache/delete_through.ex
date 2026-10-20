defmodule ExStorageService.CloudCache.DeleteThrough do
  @moduledoc """
  Upstream-authoritative gateway deletion with durable, fenced reconciliation.

  An uncertain DELETE is never repeated. Recovery requires a HEAD confirming
  absence and compares the original local object and bucket identities before
  atomically removing metadata and its cache entry. Shared cache files are left
  for safe garbage collection rather than unlinked underneath concurrent readers.
  """
  alias ExStorageService.CloudCache.{Client, Config}
  alias ExStorageService.Metadata.{BucketLifecycle, Keys}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  @prefix "ess:gateway-delete:"

  def delete(bucket, key, %Config{} = config, operation_id, opts \\ []) do
    backend = Keyword.get(opts, :backend, Backend)
    client = Keyword.get(opts, :client, Client)

    with {:ok, snapshot} <- snapshot(backend, bucket, key, opts),
         {:ok, remote} <- remote_identity(client.head_object(config, key)),
         record =
           Map.merge(snapshot, %{
             bucket: bucket,
             key: key,
             operation_id: operation_id,
             state: :prepared,
             endpoint: Config.endpoint_url(config),
             remote_bucket: config.bucket,
             remote_etag: remote
           }),
         :ok <- prepare(backend, record) do
      if remote == :absent do
        finish(record, opts)
      else
        case client.delete_object(config, key, if_match: "\"#{remote}\"") do
          :ok ->
            confirm_absence(record, config, opts)

          {:error, {:http_error, status}} = error when status in 400..499 ->
            _ = backend.delete(journal_key(operation_id), [])
            error

          {:error, reason} ->
            ExStorageService.Telemetry.uncertain_operation(:gateway_delete, :unknown)
            # The prepared intent already records uncertainty. Never rewrite it
            # here: a concurrent reconciler may already have completed it.
            {:error, {:upstream_unknown, reason}}
        end
      end
    end
  end

  @doc "Reconciles one bounded page without sending another upstream mutation."
  def reconcile_page(cursor \\ nil, limit \\ 100, opts \\ []) when limit in 1..1000 do
    backend = Keyword.get(opts, :backend, Backend)

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

    with {:ok, config} <- get_config.(record.bucket),
         true <-
           Config.endpoint_url(config) == record.endpoint and
             config.bucket == record.remote_bucket do
      confirm_absence(record, config, opts)
    else
      false -> {:error, :upstream_identity_changed}
      :disabled -> {:error, :gateway_disabled}
      {:error, _} = error -> error
    end
  end

  defp confirm_absence(record, config, opts) do
    client = Keyword.get(opts, :client, Client)

    case client.head_object(config, record.key) do
      {:error, :not_found} -> finish(record, opts)
      {:ok, _} -> {:error, :upstream_outcome_unresolved}
      {:error, _} = error -> error
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
         {:ok, nil} <- backend.get(Keys.object_head(bucket, key), consistency: :linearizable),
         {:ok, cache} <- backend.get("cloud_cache_index:#{bucket}", consistency: :linearizable) do
      {:ok,
       %{
         bucket_generation: BucketLifecycle.generation(bucket_meta),
         local_revision: revision(local),
         cache_entry: cache_entry(cache, key)
       }}
    else
      false -> {:error, :bucket_generation_changed}
      {:ok, nil} -> {:error, :bucket_not_found}
      {:ok, %{}} -> {:error, :gateway_hosted_history_unsupported}
      {:error, _} = error -> error
    end
  end

  defp prepare(backend, record) do
    case backend.transaction(
           %{
             compare: [{:exists, journal_key(record.operation_id), :==, false}],
             success: [{:put, journal_key(record.operation_id), record, %{}}],
             failure: []
           },
           []
         ) do
      {:ok, %{succeeded: true}} -> :ok
      {:ok, %{succeeded: false}} -> {:error, :operation_id_conflict}
      {:error, _} = error -> error
    end
  end

  defp finish(record, opts) do
    before_commit = Keyword.get(opts, :before_local_commit, fn -> :ok end)

    with :ok <- before_commit.(), :ok <- commit(record, opts, 8) do
      :ok
    else
      {:error, reason} ->
        ExStorageService.Telemetry.uncertain_operation(:gateway_delete, :reconciliation_required)
        {:error, {:local_reconciliation_required, reason}}
    end
  end

  defp commit(_record, _opts, 0), do: {:error, :local_metadata_conflict}

  defp commit(record, opts, attempts) do
    backend = Keyword.get(opts, :backend, Backend)
    local_key = "obj:#{record.bucket}:#{record.key}"
    cache_key = "cloud_cache_index:#{record.bucket}"
    journal_key = journal_key(record.operation_id)

    with {:ok, guard} <-
           BucketLifecycle.guard(backend, record.bucket,
             bucket_generation: record.bucket_generation
           ),
         {:ok, local} <- backend.get(local_key, consistency: :linearizable),
         {:ok, head} <-
           backend.get(Keys.object_head(record.bucket, record.key), consistency: :linearizable),
         {:ok, cache} <- backend.get(cache_key, consistency: :linearizable),
         {:ok, journal} <- backend.get(journal_key, consistency: :linearizable) do
      cond do
        revision(local) != record.local_revision or head != nil or
            cache_entry(cache, record.key) != record.cache_entry ->
          {:error, :local_metadata_conflict}

        journal == nil ->
          {:error, :journal_missing}

        true ->
          spec = %{
            compare:
              guard.compare ++
                [
                  compare(local_key, revision(local)),
                  compare(Keys.object_head(record.bucket, record.key), nil),
                  compare(cache_key, revision(cache)),
                  compare(journal_key, revision(journal))
                ],
            success:
              guard.success ++
                [{:delete, {:key, local_key}, %{}}, {:delete, {:key, journal_key}, %{}}] ++
                cache_delete(cache_key, cache, record.key),
            failure: []
          }

          case backend.transaction(spec, []) do
            {:ok, %{succeeded: true}} -> :ok
            {:ok, %{succeeded: false}} -> commit(record, opts, attempts - 1)
            {:error, _} = error -> error
          end
      end
    end
  end

  defp remote_identity({:error, :not_found}), do: {:ok, :absent}

  defp remote_identity({:ok, %{etag: etag}}) when is_binary(etag) and byte_size(etag) > 0,
    do: {:ok, String.trim(etag, "\"")}

  defp remote_identity({:ok, _}), do: {:error, :upstream_identity_missing}
  defp remote_identity({:error, _} = error), do: error
  defp cache_entry(nil, _key), do: nil
  defp cache_entry(%{value: index}, key), do: Map.get(index, key)
  defp cache_delete(_key, nil, _object_key), do: []

  defp cache_delete(key, %{value: index}, object_key),
    do: [{:put, key, Map.delete(index, object_key), %{}}]

  defp revision(nil), do: nil
  defp revision(record), do: record.mod_revision
  defp compare(key, nil), do: {:exists, key, :==, false}
  defp compare(key, revision), do: {:mod_revision, key, :==, revision}
  defp journal_key(id), do: @prefix <> Base.url_encode64(id, padding: false)
end
