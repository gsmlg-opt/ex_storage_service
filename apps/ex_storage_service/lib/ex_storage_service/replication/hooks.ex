defmodule ExStorageService.Replication.Hooks do
  @moduledoc """
  Integration hooks for S3 handlers.

  These functions should be called from the S3 PutObject and DeleteObject handlers
  after a successful operation. They enqueue replication jobs for all configured
  replicas of the bucket.
  """

  require Logger

  alias ExStorageService.Replication.Config
  alias ExStorageService.Replication.JobQueue

  @doc """
  Called after a successful PutObject operation.

  Looks up replica configurations for the bucket and enqueues a replication PUT
  job for each replica.
  """
  @spec after_put(String.t(), String.t()) :: :ok
  def after_put(bucket, key) do
    case Config.get_bucket_replicas(bucket) do
      {:ok, []} ->
        :ok

      {:ok, replicas} ->
        Enum.each(replicas, fn replica ->
          JobQueue.enqueue(
            queue: :replication,
            payload: %{
              action: :put,
              bucket: bucket,
              key: key,
              replica: %{
                endpoint: replica.endpoint,
                access_key: replica.access_key,
                secret_key_enc: replica.secret_key_enc,
                bucket: replica.bucket
              }
            }
          )
        end)

        Logger.debug("Enqueued #{length(replicas)} replication PUT jobs for #{bucket}/#{key}")
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to get replicas for after_put hook on #{bucket}/#{key}: #{inspect(reason)}"
        )

        :ok
    end
  end

  @doc """
  Called after a successful DeleteObject operation.

  Looks up replica configurations for the bucket and enqueues a replication DELETE
  job for each replica.
  """
  @spec after_delete(String.t(), String.t()) :: :ok
  def after_delete(bucket, key) do
    case Config.get_bucket_replicas(bucket) do
      {:ok, []} ->
        :ok

      {:ok, replicas} ->
        Enum.each(replicas, fn replica ->
          JobQueue.enqueue(
            queue: :replication,
            payload: %{
              action: :delete,
              bucket: bucket,
              key: key,
              replica: %{
                endpoint: replica.endpoint,
                access_key: replica.access_key,
                secret_key_enc: replica.secret_key_enc,
                bucket: replica.bucket
              }
            }
          )
        end)

        Logger.debug("Enqueued #{length(replicas)} replication DELETE jobs for #{bucket}/#{key}")
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to get replicas for after_delete hook on #{bucket}/#{key}: #{inspect(reason)}"
        )

        :ok
    end
  end
end
