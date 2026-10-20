defmodule ExStorageService.Metadata.BucketConfig do
  @moduledoc """
  Bucket configuration mutations fenced against bucket deletion and recreation.

  The original generation is retained across transaction retries. Configuration
  changes touch the bucket revision in the same transaction, serializing them
  with deletion without recreating orphan configuration after deletion.
  """
  alias ExStorageService.Metadata.BucketLifecycle
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  def put(bucket, prefix, value, opts \\ []),
    do: mutate(bucket, {:put, "#{prefix}:#{bucket}", value, %{}}, opts)

  def delete(bucket, prefix, opts \\ []),
    do: mutate(bucket, {:delete, {:key, "#{prefix}:#{bucket}"}, %{}}, opts)

  defp mutate(bucket, operation, opts) do
    backend = Keyword.get(opts, :backend, Backend)

    with {:ok, record} <- backend.get("bucket:#{bucket}", consistency: :linearizable) do
      case record do
        nil ->
          {:error, :bucket_not_found}

        %{value: meta} ->
          opts = Keyword.put_new(opts, :bucket_generation, BucketLifecycle.generation(meta))
          commit(backend, bucket, operation, opts, 8)
      end
    end
  end

  defp commit(_backend, _bucket, _operation, _opts, 0), do: {:error, :compare_retry_exhausted}

  defp commit(backend, bucket, operation, opts, attempts) do
    with {:ok, guard} <- BucketLifecycle.guard(backend, bucket, opts) do
      spec = %{compare: guard.compare, success: guard.success ++ [operation], failure: []}

      case backend.transaction(spec, []) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> commit(backend, bucket, operation, opts, attempts - 1)
        {:error, _} = error -> error
      end
    end
  end
end
