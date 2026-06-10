defmodule ExStorageServiceS3.Handlers.Versioning do
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

  def put_bucket_versioning(conn, bucket) do
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
          {:ok, body, _conn} ->
            case parse_versioning_xml(body) do
              {:ok, status} ->
                state = if status == "Enabled", do: :enabled, else: :suspended
                Versioning.set_versioning(bucket, state)

                conn
                |> put_s3_headers(request_id)
                |> send_resp(200, "")

              {:error, _} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}?versioning",
                  request_id
                )
            end

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}?versioning",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def get_bucket_versioning(conn, bucket) do
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
        state = Versioning.get_versioning(bucket)

        status_element =
          case state do
            :disabled -> ""
            :enabled -> "<Status>Enabled</Status>"
            :suspended -> "<Status>Suspended</Status>"
          end

        body = """
        <?xml version="1.0" encoding="UTF-8"?>
        <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{status_element}</VersioningConfiguration>
        """

        xml_response(conn, 200, String.trim(body), request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  ## Lifecycle handlers
end
