defmodule ExStorageServiceS3.Handlers.Bucket do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.BucketValidator
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine

  def list_buckets(conn) do
    request_id = request_id(conn)

    case Metadata.list_buckets() do
      {:ok, buckets} ->
        body = XML.list_buckets_response(buckets)
        xml_response(conn, 200, body, request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/", request_id)
    end
  end

  def create_bucket(conn, bucket) do
    request_id = request_id(conn)

    with :ok <- BucketValidator.validate(bucket) do
      case Metadata.head_bucket(bucket) do
        :ok ->
          error_response(
            conn,
            "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded.",
            "/#{bucket}",
            request_id
          )

        {:error, :not_found} ->
          Engine.ensure_bucket_dirs(bucket)

          case Metadata.create_bucket(bucket) do
            :ok ->
              conn
              |> put_s3_headers(request_id)
              |> put_resp_header("location", "/#{bucket}")
              |> send_resp(201, "")

            {:error, reason} ->
              error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
          end

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
      end
    else
      {:error, msg} ->
        error_response(conn, "InvalidBucketName", msg, "/#{bucket}", request_id)
    end
  end

  def delete_bucket(conn, bucket) do
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
        case Metadata.list_objects(bucket, max_keys: 1) do
          {:ok, %{keys: []}} ->
            Metadata.delete_bucket(bucket)

            conn
            |> put_s3_headers(request_id)
            |> send_resp(204, "")

          {:ok, _} ->
            error_response(
              conn,
              "BucketNotEmpty",
              "The bucket you tried to delete is not empty.",
              "/#{bucket}",
              request_id
            )

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
        end
    end
  end

  def head_bucket(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      :ok ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(200, "")

      {:error, :not_found} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(404, "")

      {:error, _reason} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(500, "")
    end
  end
end
