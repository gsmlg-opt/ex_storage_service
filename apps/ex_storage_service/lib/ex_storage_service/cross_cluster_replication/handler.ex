defmodule ExStorageService.CrossClusterReplication.Handler do
  @moduledoc """
  Idempotent handlers for eventual external-replication jobs.

  Before applying a job, the handler compares its pinned object with the
  current local head. Superseded PUTs are acknowledged without changing the
  destination. A DELETE job reconciles the destination to the current head,
  which prevents delayed work from deleting newer data and also handles an
  explicit version deletion that reveals an older current version.
  """

  alias ExStorageService.CrossClusterReplication.Worker
  alias ExStorageService.{Context, ObjectService}

  @spec perform(map(), Context.t(), keyword()) :: :ok | {:error, term()}
  def perform(job, context, opts \\ [])

  def perform(%{kind: :cross_cluster_put, payload: payload}, context, opts) do
    bucket = field(payload, :bucket)
    key = field(payload, :key)
    object = field(payload, :object)
    replica = field(payload, :replica)

    case current_head(bucket, key, context, opts) do
      {:ok, %{delete_marker: false, metadata: current}} ->
        if same_object?(current, object),
          do: replicate_put(bucket, key, replica, object, context, opts),
          else: :ok

      {:ok, %{delete_marker: true}} ->
        :ok

      {:error, :object_not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  def perform(%{kind: :cross_cluster_delete, payload: payload}, context, opts) do
    bucket = field(payload, :bucket)
    key = field(payload, :key)
    replica = field(payload, :replica)

    case current_head(bucket, key, context, opts) do
      {:ok, %{delete_marker: false, metadata: current}} ->
        replicate_put(bucket, key, replica, current, context, opts)

      {:ok, %{delete_marker: true}} ->
        replicate_delete(bucket, key, replica, opts)

      {:error, :object_not_found} ->
        replicate_delete(bucket, key, replica, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def perform(%{kind: kind}, _context, _opts), do: {:error, {:unsupported_job_kind, kind}}

  defp current_head(bucket, key, context, opts) do
    case Keyword.get(opts, :head) do
      callback when is_function(callback, 3) -> callback.(bucket, key, context)
      nil -> ObjectService.head(bucket, key, nil, context: context)
    end
  end

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

  defp same_object?(current, pinned) do
    field(current, :content_hash) == field(pinned, :content_hash) and
      same_version?(field(current, :version_id), field(pinned, :version_id))
  end

  defp same_version?(_current, nil), do: true
  defp same_version?(current, pinned), do: current == pinned

  defp field(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, to_string(key))
end
