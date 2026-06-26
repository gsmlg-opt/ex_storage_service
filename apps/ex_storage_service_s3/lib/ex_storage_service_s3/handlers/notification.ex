defmodule ExStorageServiceS3.Handlers.Notification do
  import Plug.Conn
  require Logger
  alias ExStorageServiceS3.XML
  alias ExStorageService.BucketValidator
  alias ExStorageService.CloudCache.Client, as: CloudClient
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.LocalStore
  alias ExStorageService.Metadata
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Storage.Versioning

  import ExStorageServiceS3.Handlers.Helpers

  def put_bucket_notification(conn, bucket) do
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

      :ok ->
        case read_full_body(conn) do
          {:ok, body, conn} ->
            case parse_notification_xml(body) do
              {:ok, configs} ->
                Notifications.put_config(bucket, configs)

                conn
                |> put_s3_headers(request_id)
                |> send_resp(200, "")

              {:error, _} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}?notification",
                  request_id
                )
            end

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}?notification",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def get_bucket_notification(conn, bucket) do
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

      :ok ->
        case Notifications.get_config(bucket) do
          {:ok, configs} ->
            body = build_notification_xml(configs)
            xml_response(conn, 200, body, request_id)

          {:error, :not_found} ->
            body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></NotificationConfiguration>
            """

            xml_response(conn, 200, String.trim(body), request_id)
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def delete_bucket_notification(conn, bucket) do
    request_id = request_id(conn)
    Notifications.delete_config(bucket)

    conn
    |> put_s3_headers(request_id)
    |> send_resp(204, "")
  end

  # Private helpers

  # Returns {:ok, cloud_config} if cloud cache is active for bucket, :disabled otherwise.
end
