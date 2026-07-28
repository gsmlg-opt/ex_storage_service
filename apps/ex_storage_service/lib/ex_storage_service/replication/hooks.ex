defmodule ExStorageService.Replication.Hooks do
  @moduledoc """
  Compatibility facade for eventual cross-cluster replication hooks.

  New object-service writes create events inside the object metadata
  transaction through `ExStorageService.CrossClusterReplication.Hooks`.
  These post-commit functions remain for legacy callers such as the cloud
  backend and create a standalone durable outbox operation.
  """

  alias ExStorageService.CrossClusterReplication.Hooks
  alias ExStorageService.Metadata.Outbox

  @doc """
  Called after a successful PutObject operation.

  Legacy compatibility entry point. Prefer transactionally attaching
  `events_for_put/4` to the object commit.
  """
  @spec after_put(String.t(), String.t()) :: :ok
  def after_put(bucket, key) do
    with {:ok, object} <- ExStorageService.Metadata.get_object_meta(bucket, key),
         {:ok, events} <- Hooks.events_for_put(bucket, key, object),
         :ok <- Outbox.enqueue_legacy(events) do
      :ok
    else
      _error -> :ok
    end
  end

  @doc """
  Called after a successful DeleteObject operation.

  Legacy compatibility entry point. Prefer transactionally attaching
  `events_for_delete/3` to the object commit.
  """
  @spec after_delete(String.t(), String.t()) :: :ok
  def after_delete(bucket, key) do
    with {:ok, events} <- Hooks.events_for_delete(bucket, key),
         :ok <- Outbox.enqueue_legacy(events) do
      :ok
    else
      _error -> :ok
    end
  end
end
