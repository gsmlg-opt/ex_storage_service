defmodule ExStorageServiceS3.Handlers.Object.LocalBackend do
  @moduledoc false
  @behaviour ExStorageServiceS3.Handlers.Object.Backend

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.ObjectService

  @impl true
  def list_objects(conn, bucket, opts, request_id) do
    with {:ok, local_opts} <- ExStorageServiceS3.Listing.local_options(bucket, opts),
         {:ok, result} <- Metadata.list_objects(bucket, local_opts) do
      objects =
        Enum.map(result.keys, fn {key, meta} ->
          %{
            key: key,
            last_modified: Map.get(meta, :updated_at, Map.get(meta, :created_at, "")),
            etag: "\"#{Map.get(meta, :etag, "")}\"",
            size: Map.get(meta, :size, 0),
            storage_class: "STANDARD"
          }
        end)

      token = ExStorageServiceS3.Listing.next_token(bucket, result.next_continuation_token, opts)
      response_opts = ExStorageServiceS3.Listing.response_options(opts, result, token)

      body = XML.list_objects_response(bucket, objects, response_opts)
      xml_response(conn, 200, body, request_id)
    else
      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id)
    end
  end

  @impl true
  def get_object(conn, bucket, key, request_id) do
    case ObjectService.head(bucket, key) do
      {:ok, %{delete_marker: true, version_id: version_id}} ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("x-amz-delete-marker", "true")
        |> maybe_put_version_header(version_id)
        |> send_resp(404, "")

      {:ok, %{metadata: meta}} ->
        conn = put_object_version_header(conn, meta)
        content_type = Map.get(meta, :content_type, "application/octet-stream")
        etag = Map.get(meta, :etag, "")
        quoted_etag = "\"#{etag}\""
        size = Map.get(meta, :size, 0)
        last_modified_raw = Map.get(meta, :updated_at, Map.get(meta, :created_at))
        last_modified = format_http_date(last_modified_raw)

        case ExStorageServiceS3.Plugs.ReadResponse.condition(conn, quoted_etag, last_modified_raw) do
          :failed ->
            storage_error_response(conn, :precondition_failed, "/#{bucket}/#{key}", request_id)

          :not_modified ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("etag", quoted_etag)
            |> put_resp_header("last-modified", last_modified)
            |> send_resp(304, "")

          :ok ->
            case get_req_header(conn, "range") do
              [range_header | _] ->
                case parse_range(range_header, size) do
                  {:ok, offset, length} ->
                    content_range = "bytes #{offset}-#{offset + length - 1}/#{size}"

                    case open_source(meta, bucket, {offset, length}, request_id) do
                      {:ok, source} ->
                        conn
                        |> put_s3_headers(request_id)
                        |> put_resp_header("content-type", content_type)
                        |> put_resp_header("etag", quoted_etag)
                        |> put_resp_header("last-modified", last_modified)
                        |> put_resp_header("content-length", to_string(length))
                        |> put_resp_header("content-range", content_range)
                        |> put_resp_header("accept-ranges", "bytes")
                        |> put_custom_metadata_headers(meta)
                        |> omit_object_checksum_headers()
                        |> send_blob_source(206, source, request_id: request_id)

                      {:error, reason} ->
                        read_error_response(conn, reason, bucket, key, request_id)
                    end

                  {:error, :invalid_range} ->
                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-range", "bytes */#{size}")
                    |> send_resp(416, "")
                end

              [] ->
                case open_source(meta, bucket, nil, request_id) do
                  {:ok, source} ->
                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-type", content_type)
                    |> put_resp_header("etag", quoted_etag)
                    |> put_resp_header("last-modified", last_modified)
                    |> put_resp_header("content-length", to_string(size))
                    |> put_resp_header("accept-ranges", "bytes")
                    |> put_custom_metadata_headers(meta)
                    |> send_blob_source(200, source, request_id: request_id)

                  {:error, reason} ->
                    read_error_response(conn, reason, bucket, key, request_id)
                end
            end
        end

      {:error, :bucket_not_found} ->
        error_response(
          conn,
          "NoSuchBucket",
          "The specified bucket does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :object_not_found} ->
        error_response(
          conn,
          "NoSuchKey",
          "The specified key does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @impl true
  def put_object(conn, bucket, key, request_id) do
    with {:ok, attributes} <- object_attributes(conn) do
      {reader, reader_state} = decoded_body_reader(conn)
      put_object_from_reader(conn, bucket, key, reader, reader_state, attributes, request_id)
    else
      {:error, reason} -> storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  defp put_object_from_reader(
         conn,
         bucket,
         key,
         reader,
         reader_state,
         attributes,
         request_id
       ) do
    case ObjectService.put_from_reader(
           bucket,
           key,
           reader,
           reader_state,
           attributes.content_type,
           attributes.metadata,
           attributes: attributes,
           reader_attributes: fn final -> %{checksums: final.checksums} end,
           metadata_opts: [
             operation_id: request_id,
             if_match: List.first(get_req_header(conn, "if-match")),
             if_none_match: List.first(get_req_header(conn, "if-none-match"))
           ]
         ) do
      {:ok, %{metadata: %{etag: etag}} = publication, final_state} ->
        final_state
        |> decoded_body_reader_conn()
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> ExStorageServiceS3.MultipartChecksums.response_headers(final_state.checksums)
        |> put_write_version_header(publication)
        |> send_resp(200, "")

      {:error, {:stage, :entity_too_large}, final_state} ->
        final_state
        |> decoded_body_reader_conn()
        |> entity_too_large_response(bucket, key, request_id)

      {:error, {:stage, :malformed_chunked}, final_state} ->
        conn = decoded_body_reader_conn(final_state)

        error_response(
          conn,
          "InvalidRequest",
          "The aws-chunked request body is malformed.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason, final_state} ->
        conn = decoded_body_reader_conn(final_state)
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  defp entity_too_large_response(conn, bucket, key, request_id) do
    error_response(
      conn,
      "EntityTooLarge",
      "Your proposed upload exceeds the maximum allowed object size.",
      "/#{bucket}/#{key}",
      request_id
    )
  end

  defp maybe_put_version_header(conn, nil), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

  defp open_source(meta, bucket, range, request_id) do
    ObjectService.open_source(meta,
      bucket: bucket,
      range: range,
      request_id: request_id
    )
  end

  defp read_error_response(conn, :blob_not_found, bucket, key, request_id) do
    error_response(
      conn,
      "InternalError",
      "Content file missing",
      "/#{bucket}/#{key}",
      request_id
    )
  end

  defp read_error_response(conn, reason, bucket, key, request_id),
    do: storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
end
