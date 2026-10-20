defmodule ExStorageServiceS3.Handlers.Versioning do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.BucketLifecycle
  alias ExStorageService.Storage.Versioning

  @max_configuration_bytes 1_048_576

  def put_bucket_versioning(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.get_bucket(bucket) do
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}",
          request_id
        )

      {:ok, bucket_meta} ->
        case read_full_body(conn, @max_configuration_bytes) do
          {:ok, body, conn} ->
            case parse_versioning_xml(body) do
              {:ok, status} ->
                state = if status == "Enabled", do: :enabled, else: :suspended

                case Versioning.set_versioning(bucket, state,
                       bucket_generation: BucketLifecycle.generation(bucket_meta)
                     ) do
                  :ok ->
                    conn |> put_s3_headers(request_id) |> send_resp(200, "")

                  {:error, reason} ->
                    storage_error_response(conn, reason, "/#{bucket}?versioning", request_id)
                end

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
            storage_error_response(conn, reason, "/#{bucket}?versioning", request_id)
        end

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id)
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
        case Versioning.fetch_versioning(bucket) do
          {:ok, state} ->
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
            storage_error_response(conn, reason, "/#{bucket}?versioning", request_id)
        end

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id)
    end
  end

  ## Lifecycle handlers

  defp parse_versioning_xml(xml_body) do
    if xml_has_doctype?(xml_body) do
      {:error, :malformed_xml}
    else
      do_parse_versioning_xml(xml_body)
    end
  end

  defp do_parse_versioning_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      case :xmerl_xpath.string(~c"//Status/text()", doc) do
        [{:xmlText, _, _, _, value, _} | _] ->
          status = to_string(value)

          if status in ["Enabled", "Suspended"] do
            {:ok, status}
          else
            {:error, :invalid_status}
          end

        _ ->
          {:error, :missing_status}
      end
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end
end
