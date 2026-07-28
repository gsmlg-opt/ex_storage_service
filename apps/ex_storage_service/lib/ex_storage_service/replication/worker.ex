defmodule ExStorageService.Replication.Worker do
  @moduledoc """
  Compatibility facade for external S3 replication.

  New code should use `ExStorageService.CrossClusterReplication.Worker`.
  """

  alias ExStorageService.CrossClusterReplication.Worker

  defdelegate replicate_put(bucket, key, replica), to: Worker

  defdelegate replicate_put(bucket, key, replica, object_info), to: Worker

  @doc false
  defdelegate replicate_put(bucket, key, replica, object_info, opts), to: Worker

  defdelegate replicate_delete(bucket, key, replica), to: Worker

  @doc false
  defdelegate replicate_delete(bucket, key, replica, opts), to: Worker
end
