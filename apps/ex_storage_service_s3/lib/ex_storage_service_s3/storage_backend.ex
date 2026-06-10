defmodule ExStorageServiceS3.StorageBackend do
  @moduledoc """
  Defines the StorageBackend behaviour and dispatcher for S3 object operations.
  """

  alias ExStorageService.CloudCache.Config, as: CloudConfig

  @callback list_objects(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              opts :: keyword(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback get_object(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              key :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback head_object(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              key :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback put_object(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              key :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback delete_object(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              key :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback copy_object(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              key :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  @callback delete_objects(
              conn :: Plug.Conn.t(),
              bucket :: String.t(),
              request_id :: String.t(),
              config :: any()
            ) :: Plug.Conn.t()

  def list_objects(conn, bucket, opts, request_id) do
    dispatch(bucket, :list_objects, [conn, bucket, opts, request_id])
  end

  def get_object(conn, bucket, key, request_id) do
    dispatch(bucket, :get_object, [conn, bucket, key, request_id])
  end

  def head_object(conn, bucket, key, request_id) do
    dispatch(bucket, :head_object, [conn, bucket, key, request_id])
  end

  def put_object(conn, bucket, key, request_id) do
    dispatch(bucket, :put_object, [conn, bucket, key, request_id])
  end

  def delete_object(conn, bucket, key, request_id) do
    dispatch(bucket, :delete_object, [conn, bucket, key, request_id])
  end

  def copy_object(conn, bucket, key, request_id) do
    dispatch(bucket, :copy_object, [conn, bucket, key, request_id])
  end

  def delete_objects(conn, bucket, request_id) do
    dispatch(bucket, :delete_objects, [conn, bucket, request_id])
  end

  defp dispatch(bucket, function, args) do
    case CloudConfig.get_active_config(bucket) do
      {:ok, cloud_config} ->
        apply(ExStorageServiceS3.StorageBackend.CloudCache, function, args ++ [cloud_config])

      :disabled ->
        apply(ExStorageServiceS3.StorageBackend.Local, function, args ++ [nil])
    end
  end
end
