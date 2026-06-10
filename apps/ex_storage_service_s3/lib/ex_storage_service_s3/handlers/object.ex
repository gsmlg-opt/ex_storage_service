defmodule ExStorageServiceS3.Handlers.Object do
  @moduledoc """
  Handlers for S3 object operations, delegating to the dynamic StorageBackend.
  """

  import Plug.Conn
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceS3.StorageBackend

  import ExStorageServiceS3.Handlers.Helpers

  def list_objects(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}",
          request_id
        )

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)

      :ok ->
        params = conn.query_params

        opts = [
          prefix: Map.get(params, "prefix", ""),
          delimiter: Map.get(params, "delimiter"),
          max_keys: parse_max_keys(Map.get(params, "max-keys", "1000")),
          continuation_token: Map.get(params, "continuation-token")
        ]

        StorageBackend.list_objects(conn, bucket, opts, request_id)
    end
  end

  def get_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:get_object, %{bucket: bucket, key: key}, fn ->
      StorageBackend.get_object(conn, bucket, key, request_id)
    end)
  end

  def head_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:head_object, %{bucket: bucket, key: key}, fn ->
      StorageBackend.head_object(conn, bucket, key, request_id)
    end)
  end

  def put_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:put_object, %{bucket: bucket, key: key}, fn ->
      case Metadata.head_bucket(bucket) do
        {:error, :not_found} ->
          error_response(
            conn,
            "NoSuchBucket",
            "The specified bucket does not exist.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)

        :ok ->
          StorageBackend.put_object(conn, bucket, key, request_id)
      end
    end)
  end

  def delete_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:delete_object, %{bucket: bucket, key: key}, fn ->
      StorageBackend.delete_object(conn, bucket, key, request_id)
    end)
  end

  def copy_object(conn, bucket, key) do
    request_id = request_id(conn)
    StorageBackend.copy_object(conn, bucket, key, request_id)
  end

  def delete_objects(conn, bucket) do
    request_id = request_id(conn)
    StorageBackend.delete_objects(conn, bucket, request_id)
  end

  ## Versioning handlers

  def get_object_version(conn, bucket, key, version_id) do
    request_id = request_id(conn)

    case Versioning.get_version(bucket, key, version_id) do
      {:ok, meta} ->
        if Map.get(meta, :is_delete_marker) do
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> put_resp_header("x-amz-version-id", version_id)
          |> send_resp(404, "")
        else
          content_hash = meta.content_hash

          case Engine.get_object(bucket, content_hash) do
            {:ok, file_path} ->
              content_type = Map.get(meta, :content_type, "application/octet-stream")
              etag = Map.get(meta, :etag, "")
              size = Map.get(meta, :size, 0)

              last_modified =
                format_http_date(Map.get(meta, :updated_at, Map.get(meta, :created_at)))

              conn
              |> put_s3_headers(request_id)
              |> put_resp_header("content-type", content_type)
              |> put_resp_header("etag", "\"#{etag}\"")
              |> put_resp_header("last-modified", last_modified)
              |> put_resp_header("content-length", to_string(size))
              |> put_resp_header("x-amz-version-id", version_id)
              |> put_custom_metadata_headers(meta)
              |> send_file(200, file_path)

            {:error, _} ->
              error_response(
                conn,
                "InternalError",
                "Content file missing",
                "/#{bucket}/#{key}",
                request_id
              )
          end
        end

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchKey",
          "The specified key does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )
    end
  end
end
