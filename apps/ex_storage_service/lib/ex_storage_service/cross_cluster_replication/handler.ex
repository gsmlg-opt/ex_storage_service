defmodule ExStorageService.CrossClusterReplication.Handler do
  @moduledoc """
  Idempotent handlers for eventual external-replication jobs.

  Every attempt reconciles an active destination to the current local head,
  including recovered and superseded jobs. Removed destinations are retired
  without receiving later source content. After the transfer, the head is read again.
  A changed or unavailable head retains durable reconciliation work without
  exhausting the ordinary endpoint-failure retry budget.

  Directly enqueued jobs without a Hooks operation identity retain their
  explicit one-time authorization for an exactly matching pinned PUT or an
  already-absent key DELETE. This preserves resource and restore tooling while
  requiring an active rule before an old job can forward later source content.

  Work is bounded to one transfer and two head reads per attempt. The existing
  leased job remains the recovery record if the process dies before its final
  check. Replica requests have no receiver-side fencing. An unacknowledged
  request that times out or outlives its worker can commit remotely after
  recovery acknowledged a newer transfer. Such a late commit can still require
  independent operator reconciliation.
  """

  alias ExStorageService.CrossClusterReplication.Worker
  alias ExStorageService.{Context, ObjectService}

  @spec perform(map(), Context.t(), keyword()) :: :ok | {:error, term()}
  def perform(job, context, opts \\ [])

  def perform(%{kind: kind, payload: payload}, context, opts)
      when kind in [:cross_cluster_put, :cross_cluster_delete] do
    bucket = field(payload, :bucket)
    key = field(payload, :key)
    target = field(payload, :replica)

    with {:ok, before} <- current_state(bucket, key, context, opts),
         {:ok, replica} <- authorized_replica(kind, payload, before, bucket, target, opts) do
      result = transfer(before, bucket, key, replica, context, opts)

      case current_state(bucket, key, context, opts) do
        {:ok, after_transfer} ->
          if identity(before) == identity(after_transfer),
            do: result,
            else: reconciliation_required(:head_changed)

        {:error, reason} ->
          reconciliation_required({:head_unavailable, reason})
      end
    else
      :removed ->
        :ok

      {:error, reason}
      when reason in [
             :timeout,
             :unknown,
             :cluster_not_ready,
             :no_leader,
             :compare_retry_exhausted
           ] ->
        reconciliation_required({:head_unavailable, reason})

      error ->
        error
    end
  end

  def perform(%{kind: kind}, _context, _opts), do: {:error, {:unsupported_job_kind, kind}}

  defp current_head(bucket, key, context, opts) do
    case Keyword.get(opts, :head) do
      callback when is_function(callback, 3) -> callback.(bucket, key, context)
      nil -> ObjectService.head(bucket, key, nil, context: context)
    end
  end

  defp current_state(bucket, key, context, opts) do
    case current_head(bucket, key, context, opts) do
      {:ok, %{delete_marker: false, metadata: current}} -> {:ok, {:present, current}}
      {:ok, %{delete_marker: true}} -> {:ok, :absent}
      {:error, :object_not_found} -> {:ok, :absent}
      error -> error
    end
  end

  defp active_replica(bucket, target, opts) do
    config = Keyword.get(opts, :config, ExStorageService.CrossClusterReplication.Config)

    case config.get_bucket_replicas(bucket) do
      {:ok, replicas} ->
        case Enum.find(replicas, &(destination(&1, bucket) == destination(target, bucket))) do
          nil -> :removed
          replica -> {:ok, replica}
        end

      {:error, reason} ->
        reconciliation_required({:configuration_unavailable, reason})
    end
  end

  defp authorized_replica(kind, payload, current, bucket, target, opts) do
    if field(payload, :operation_id) == nil and explicit_snapshot?(kind, payload, current),
      do: {:ok, target},
      else: active_replica(bucket, target, opts)
  end

  defp explicit_snapshot?(:cross_cluster_put, payload, {:present, current}) do
    case field(payload, :object) do
      pinned when is_map(pinned) -> identity({:present, pinned}) == identity({:present, current})
      _missing -> false
    end
  end

  defp explicit_snapshot?(:cross_cluster_delete, _payload, :absent), do: true
  defp explicit_snapshot?(_kind, _payload, _current), do: false

  defp destination(replica, source_bucket) do
    {String.trim_trailing(field(replica, :endpoint), "/"),
     field(replica, :bucket) || source_bucket}
  end

  defp transfer({:present, current}, bucket, key, replica, context, opts),
    do: replicate_put(bucket, key, replica, current, context, opts)

  defp transfer(:absent, bucket, key, replica, _context, opts),
    do: replicate_delete(bucket, key, replica, opts)

  defp replicate_put(bucket, key, replica, object, context, opts) do
    worker_opts =
      Keyword.merge([source_opts: [context: context]], Keyword.get(opts, :worker_opts, []))

    case Keyword.get(opts, :put) do
      callback when is_function(callback, 5) ->
        callback.(bucket, key, replica, object, worker_opts)

      nil ->
        Worker.replicate_put(bucket, key, replica, object, worker_opts)
    end
  end

  defp replicate_delete(bucket, key, replica, opts) do
    case Keyword.get(opts, :delete) do
      callback when is_function(callback, 3) -> callback.(bucket, key, replica)
      nil -> Worker.replicate_delete(bucket, key, replica)
    end
  end

  defp identity(:absent), do: :absent

  defp identity({:present, object}),
    do: Enum.map([:version_id, :content_hash, :etag, :size, :content_type], &field(object, &1))

  defp reconciliation_required(reason),
    do: {:error, {:replication_reconciliation_required, reason}}

  defp field(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, to_string(key))
end
