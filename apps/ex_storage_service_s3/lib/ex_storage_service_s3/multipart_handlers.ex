defmodule ExStorageServiceS3.MultipartHandlers do
  @moduledoc """
  Request handlers for S3 multipart upload API operations.
  """

  import Plug.Conn

  alias ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Multipart
  alias ExStorageService.Storage.Versioning

  # Cap for buffered XML request bodies (CompleteMultipartUpload). Generous
  # enough for the 10,000-part maximum while bounding memory use.
  @max_xml_body 16 * 1024 * 1024

  @doc """
  POST /{bucket}/{key}?uploads — CreateMultipartUpload
  """
  def create_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      :ok ->
        case Multipart.init_upload(bucket, key) do
          {:ok, upload_id} ->
            body = XML.initiate_multipart_upload_response(bucket, key, upload_id)
            xml_response(conn, 200, body, request_id)

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}/#{key}",
              request_id
            )
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

    max_part_size =
      Application.get_env(:ex_storage_service, :max_part_size, 5 * 1024 * 1024 * 1024)

    with {part_number, _} <- Integer.parse(part_number_str || ""),
         true <- part_number >= 1 and part_number <= 10_000,
         {:ok, _upload} <- Multipart.get_upload(bucket, upload_id) do
      case Multipart.store_part(
             bucket,
             upload_id,
             part_number,
             Shared.body_stream(conn, max_part_size)
           ) do
        {:ok, etag} ->
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("etag", "\"#{etag}\"")
          |> send_resp(200, "")

        {:error, :entity_too_large} ->
          error_response(
            conn,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed part size.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
      end
    else
      :error ->
        error_response(
          conn,
          "InvalidArgument",
          "Invalid part number.",
          "/#{bucket}/#{key}",
          request_id
        )

      false ->
        error_response(
          conn,
          "InvalidArgument",
          "Part number must be between 1 and 10000.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :entity_too_large} ->
        error_response(
          conn,
          "EntityTooLarge",
          "Your proposed upload exceeds the maximum allowed part size.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

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
        case Shared.read_full_body(conn, @max_xml_body) do
          {:ok, body, conn} ->
            case parse_complete_multipart_xml(body) do
              {:ok, parts} ->
                case Multipart.complete_upload(bucket, upload_id, parts) do
                  {:ok, {content_hash, etag, size, manifest_hash}} ->
                    # Store object metadata
                    now = DateTime.utc_now() |> DateTime.to_iso8601()

                    content_type =
                      case get_req_header(conn, "content-type") do
                        [ct | _] when ct != "application/xml" -> ct
                        _ -> "application/octet-stream"
                      end

                    meta = %{
                      content_hash: content_hash,
                      manifest_hash: manifest_hash,
                      object_type: :blob,
                      size: size,
                      etag: etag,
                      content_type: content_type,
                      metadata: %{},
                      created_at: now,
                      updated_at: now
                    }

                    {:ok, version_id} = Versioning.put_version(bucket, key, meta)
                    Hooks.after_put(bucket, key)
                    broadcast_bucket_change(bucket, :put, key)

                    location = "/#{bucket}/#{key}"

                    response_body =
                      XML.complete_multipart_upload_response(bucket, key, etag, location)

                    conn
                    |> maybe_put_version_header(version_id)
                    |> xml_response(200, response_body, request_id)

                  {:error, {:etag_mismatch, pn, _expected, _actual}} ->
                    error_response(
                      conn,
                      "InvalidPart",
                      "Part #{pn} has an invalid ETag.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, {:missing_part, pn, _reason}} ->
                    error_response(
                      conn,
                      "InvalidPart",
                      "Part #{pn} was not uploaded.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, {:entity_too_small, pn, size, min}} ->
                    error_response(
                      conn,
                      "EntityTooSmall",
                      "Part #{pn} is #{size} bytes; all parts except the last must be " <>
                        "at least #{min} bytes.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, reason} ->
                    error_response(
                      conn,
                      "InternalError",
                      inspect(reason),
                      "/#{bucket}/#{key}",
                      request_id
                    )
                end

              {:error, _reason} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}/#{key}",
                  request_id
                )
            end

          {:error, :entity_too_large} ->
            error_response(
              conn,
              "EntityTooLarge",
              "The CompleteMultipartUpload request body is too large.",
              "/#{bucket}/#{key}",
              request_id
            )

          {:error, reason} ->
            error_response(
              conn,
              "InternalError",
              inspect(reason),
              "/#{bucket}/#{key}",
              request_id
            )
        end

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

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
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

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
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  # Private helpers

  defp broadcast_bucket_change(bucket, action, key) do
    Phoenix.PubSub.broadcast(
      ExStorageService.PubSub,
      "bucket:#{bucket}",
      {:bucket_changed, %{action: action, key: key, bucket: bucket}}
    )
  end

  defp request_id(conn) do
    conn.assigns[:request_id] || :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  defp put_s3_headers(conn, request_id) do
    conn
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("server", "ExStorageService")
  end

  defp maybe_put_version_header(conn, "null"), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

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

  defp parse_complete_multipart_xml(xml_body) do
    if Shared.xml_has_doctype?(xml_body) do
      {:error, :malformed_xml}
    else
      do_parse_complete_multipart_xml(xml_body)
    end
  end

  defp do_parse_complete_multipart_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      parts =
        :xmerl_xpath.string(~c"//Part", doc)
        |> Enum.map(fn part_element ->
          [part_number_text] = :xmerl_xpath.string(~c"PartNumber/text()", part_element)
          [etag_text] = :xmerl_xpath.string(~c"ETag/text()", part_element)

          part_number =
            case part_number_text do
              {:xmlText, _, _, _, value, _} ->
                value |> to_string() |> String.trim() |> String.to_integer()
            end

          etag =
            case etag_text do
              {:xmlText, _, _, _, value, _} ->
                value |> to_string() |> String.trim() |> String.replace("\"", "")
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
