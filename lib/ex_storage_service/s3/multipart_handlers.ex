defmodule ExStorageService.S3.MultipartHandlers do
  @moduledoc """
  Request handlers for S3 multipart upload API operations.
  """

  import Plug.Conn

  alias ExStorageService.S3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Multipart

  @doc """
  POST /{bucket}/{key}?uploads — CreateMultipartUpload
  """
  def create_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}/#{key}", request_id)

      :ok ->
        case Multipart.init_upload(bucket, key) do
          {:ok, upload_id} ->
            body = XML.initiate_multipart_upload_response(bucket, key, upload_id)
            xml_response(conn, 200, body, request_id)

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  PUT /{bucket}/{key}?partNumber=N&uploadId=X — UploadPart
  """
  def upload_part(conn, bucket, key) do
    request_id = request_id(conn)
    params = conn.query_params
    upload_id = Map.get(params, "uploadId")
    part_number_str = Map.get(params, "partNumber")

    with {part_number, _} <- Integer.parse(part_number_str || ""),
         true <- part_number >= 1 and part_number <= 10_000,
         {:ok, _upload} <- Multipart.get_upload(bucket, upload_id),
         {:ok, body, conn} <- read_full_body(conn) do
      case Multipart.store_part(bucket, upload_id, part_number, body) do
        {:ok, etag} ->
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("etag", "\"#{etag}\"")
          |> send_resp(200, "")

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
      end
    else
      :error ->
        error_response(conn, "InvalidArgument", "Invalid part number.", "/#{bucket}/#{key}", request_id)

      false ->
        error_response(conn, "InvalidArgument", "Part number must be between 1 and 10000.", "/#{bucket}/#{key}", request_id)

      {:error, :not_found} ->
        error_response(conn, "NoSuchUpload", "The specified multipart upload does not exist.", "/#{bucket}/#{key}", request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  POST /{bucket}/{key}?uploadId=X — CompleteMultipartUpload
  """
  def complete_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    case Multipart.get_upload(bucket, upload_id) do
      {:ok, _upload} ->
        case read_full_body(conn) do
          {:ok, body, conn} ->
            case parse_complete_multipart_xml(body) do
              {:ok, parts} ->
                case Multipart.complete_upload(bucket, upload_id, parts) do
                  {:ok, {content_hash, etag, size}} ->
                    # Store object metadata
                    now = DateTime.utc_now() |> DateTime.to_iso8601()

                    content_type =
                      case get_req_header(conn, "content-type") do
                        [ct | _] when ct != "application/xml" -> ct
                        _ -> "application/octet-stream"
                      end

                    meta = %{
                      content_hash: content_hash,
                      size: size,
                      etag: etag,
                      content_type: content_type,
                      metadata: %{},
                      created_at: now,
                      updated_at: now
                    }

                    Metadata.put_object_meta(bucket, key, meta)
                    Hooks.after_put(bucket, key)

                    location = "/#{bucket}/#{key}"
                    response_body = XML.complete_multipart_upload_response(bucket, key, etag, location)
                    xml_response(conn, 200, response_body, request_id)

                  {:error, reason} ->
                    error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
                end

              {:error, _reason} ->
                error_response(conn, "MalformedXML", "The XML you provided was not well-formed.", "/#{bucket}/#{key}", request_id)
            end

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
        end

      {:error, :not_found} ->
        error_response(conn, "NoSuchUpload", "The specified multipart upload does not exist.", "/#{bucket}/#{key}", request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  DELETE /{bucket}/{key}?uploadId=X — AbortMultipartUpload
  """
  def abort_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    case Multipart.get_upload(bucket, upload_id) do
      {:ok, _upload} ->
        Multipart.abort_upload(bucket, upload_id)

        conn
        |> put_s3_headers(request_id)
        |> send_resp(204, "")

      {:error, :not_found} ->
        error_response(conn, "NoSuchUpload", "The specified multipart upload does not exist.", "/#{bucket}/#{key}", request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  GET /{bucket}/{key}?uploadId=X — ListParts
  """
  def list_parts(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    case Multipart.list_parts(bucket, upload_id) do
      {:ok, parts} ->
        body = XML.list_parts_response(bucket, key, upload_id, parts)
        xml_response(conn, 200, body, request_id)

      {:error, :not_found} ->
        error_response(conn, "NoSuchUpload", "The specified multipart upload does not exist.", "/#{bucket}/#{key}", request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  # Private helpers

  defp request_id(conn) do
    conn.assigns[:request_id] || :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  defp put_s3_headers(conn, request_id) do
    conn
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("server", "ExStorageService")
  end

  defp xml_response(conn, status, body, request_id) do
    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
  end

  defp error_response(conn, code, message, resource, request_id) do
    status = XML.error_status_code(code)
    body = XML.error_response(code, message, resource, request_id)

    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
  end

  defp read_full_body(conn, acc \\ <<>>) do
    case Plug.Conn.read_body(conn) do
      {:ok, body, conn} -> {:ok, acc <> body, conn}
      {:more, partial, conn} -> read_full_body(conn, acc <> partial)
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_complete_multipart_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      parts =
        :xmerl_xpath.string(~c"//Part", doc)
        |> Enum.map(fn part_element ->
          [part_number_text] = :xmerl_xpath.string(~c"PartNumber/text()", part_element)
          [etag_text] = :xmerl_xpath.string(~c"ETag/text()", part_element)

          part_number =
            case part_number_text do
              {:xmlText, _, _, _, value, _} -> value |> to_string() |> String.trim() |> String.to_integer()
            end

          etag =
            case etag_text do
              {:xmlText, _, _, _, value, _} -> value |> to_string() |> String.trim() |> String.replace("\"", "")
            end

          {part_number, etag}
        end)

      {:ok, parts}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end
end
