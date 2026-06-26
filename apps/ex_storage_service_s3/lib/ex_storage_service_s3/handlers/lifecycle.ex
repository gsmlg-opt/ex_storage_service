defmodule ExStorageServiceS3.Handlers.Lifecycle do
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

  def put_bucket_lifecycle(conn, bucket) do
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
            case parse_lifecycle_xml(body) do
              {:ok, rules} ->
                Lifecycle.put_rules(bucket, rules)

                conn
                |> put_s3_headers(request_id)
                |> send_resp(200, "")

              {:error, _} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}?lifecycle",
                  request_id
                )
            end

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}?lifecycle",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def get_bucket_lifecycle(conn, bucket) do
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
        case Lifecycle.get_rules(bucket) do
          {:ok, rules} ->
            body = build_lifecycle_xml(rules)
            xml_response(conn, 200, body, request_id)

          {:error, :not_found} ->
            error_response(
              conn,
              "NoSuchLifecycleConfiguration",
              "The lifecycle configuration does not exist.",
              "/#{bucket}?lifecycle",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def delete_bucket_lifecycle(conn, bucket) do
    request_id = request_id(conn)
    Lifecycle.delete_rules(bucket)

    conn
    |> put_s3_headers(request_id)
    |> send_resp(204, "")
  end

  ## Notification handlers
end
