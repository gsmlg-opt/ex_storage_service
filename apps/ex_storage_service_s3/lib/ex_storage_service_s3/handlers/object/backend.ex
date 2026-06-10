defmodule ExStorageServiceS3.Handlers.Object.Backend do
  @moduledoc """
  Behaviour for object storage backends, selected per bucket by `for_bucket/1`.

  A bucket with an active cloud-cache config dispatches to `CloudBackend`;
  otherwise to `LocalBackend`. This replaces the explicit cloud/local
  `case` branching that previously lived in each object handler.
  """
  alias ExStorageService.CloudCache.Config, as: CloudConfig

  @callback list_objects(Plug.Conn.t(), String.t(), keyword(), String.t()) :: Plug.Conn.t()
  @callback get_object(Plug.Conn.t(), String.t(), String.t(), String.t()) :: Plug.Conn.t()
  @callback put_object(Plug.Conn.t(), String.t(), String.t(), String.t()) :: Plug.Conn.t()

  @doc "Returns the storage backend module for a bucket based on its cloud-cache config."
  def for_bucket(bucket) do
    case CloudConfig.get_active_config(bucket) do
      {:ok, _cloud_config} -> ExStorageServiceS3.Handlers.Object.CloudBackend
      :disabled -> ExStorageServiceS3.Handlers.Object.LocalBackend
    end
  end
end
