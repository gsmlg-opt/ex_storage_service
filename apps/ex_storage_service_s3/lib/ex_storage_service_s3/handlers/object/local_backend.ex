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
    case Metadata.list_objects(bucket, opts) do
      {:ok, result} ->
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

        response_opts = %{
          prefix: Keyword.get(opts, :prefix, ""),
          delimiter: Keyword.get(opts, :delimiter) || "",
          max_keys: Keyword.get(opts, :max_keys, 1000),
          is_truncated: result.is_truncated,
          key_count: length(objects),
          continuation_token: Keyword.get(opts, :continuation_token),
          next_continuation_token: result.next_continuation_token,
          common_prefixes: result.common_prefixes
        }

        body = XML.list_objects_response(bucket, objects, response_opts)
        xml_response(conn, 200, body, request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  @impl true
  def get_object(conn, bucket, key, request_id) do
    case ObjectService.get(bucket, key, nil, []) do
      {:ok, %{delete_marker: true, version_id: version_id}} ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("x-amz-delete-marker", "true")
        |> maybe_put_version_header(version_id)
        |> send_resp(404, "")

      {:ok, %{metadata: meta, source: source}} ->
        content_type = Map.get(meta, :content_type, "application/octet-stream")
        etag = Map.get(meta, :etag, "")
        quoted_etag = "\"#{etag}\""
        size = Map.get(meta, :size, 0)
        last_modified_raw = Map.get(meta, :updated_at, Map.get(meta, :created_at))
        last_modified = format_http_date(last_modified_raw)

        cond do
          not_modified_etag?(conn, quoted_etag) ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("etag", quoted_etag)
            |> put_resp_header("last-modified", last_modified)
            |> send_resp(304, "")

          not_modified_since?(conn, last_modified_raw) ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("etag", quoted_etag)
            |> put_resp_header("last-modified", last_modified)
            |> send_resp(304, "")

          true ->
            case get_req_header(conn, "range") do
              [range_header | _] ->
                case parse_range(range_header, size) do
                  {:ok, offset, length} ->
                    content_range = "bytes #{offset}-#{offset + length - 1}/#{size}"
                    {:file, send_path, base_offset, _source_length} = source

                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-type", content_type)
                    |> put_resp_header("etag", quoted_etag)
                    |> put_resp_header("last-modified", last_modified)
                    |> put_resp_header("content-length", to_string(length))
                    |> put_resp_header("content-range", content_range)
                    |> put_resp_header("accept-ranges", "bytes")
                    |> put_custom_metadata_headers(meta)
                    |> send_file(206, send_path, base_offset + offset, length)

                  {:error, :invalid_range} ->
                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-range", "bytes */#{size}")
                    |> send_resp(416, "")
                end

              [] ->
                {:file, send_path, base_offset, source_length} = source

                conn
                |> put_s3_headers(request_id)
                |> put_resp_header("content-type", content_type)
                |> put_resp_header("etag", quoted_etag)
                |> put_resp_header("last-modified", last_modified)
                |> put_resp_header("content-length", to_string(size))
                |> put_resp_header("accept-ranges", "bytes")
                |> put_custom_metadata_headers(meta)
                |> send_source(send_path, base_offset, source_length)
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

      {:error, :blob_not_found} ->
        error_response(
          conn,
          "InternalError",
          "Content file missing",
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
  end

  @impl true
  def put_object(conn, bucket, key, request_id) do
    content_type =
      case get_req_header(conn, "content-type") do
        [ct | _] -> ct
        [] -> "application/octet-stream"
      end

    custom_metadata = extract_custom_metadata(conn)

    # If the client uses aws-chunked (STREAMING-AWS4-HMAC-SHA256-PAYLOAD), we
    # must buffer and decode the body before writing. Otherwise, stream directly
    # to the storage engine for better memory efficiency.
    if aws_chunked?(conn) do
      put_object_local_buffered(conn, bucket, key, content_type, custom_metadata, request_id)
    else
      put_object_local_streamed(conn, bucket, key, content_type, custom_metadata, request_id)
    end
  end

  defp put_object_local_buffered(conn, bucket, key, content_type, custom_metadata, request_id) do
    try do
      case read_full_body(conn) do
        {:ok, raw_body, _conn} ->
          case decode_aws_chunked(raw_body) do
            {:error, :malformed_chunked} ->
              throw(:malformed_chunked)

            body ->
              put_decoded_object(
                conn,
                bucket,
                key,
                body,
                content_type,
                custom_metadata,
                request_id
              )
          end

        {:error, :entity_too_large} ->
          error_response(
            conn,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed object size.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
      end
    catch
      :malformed_chunked ->
        error_response(
          conn,
          "InvalidRequest",
          "The aws-chunked request body is malformed.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :entity_too_large} ->
        error_response(
          conn,
          "EntityTooLarge",
          "Your proposed upload exceeds the maximum allowed object size.",
          "/#{bucket}/#{key}",
          request_id
        )
    end
  end

  defp put_decoded_object(conn, bucket, key, body, content_type, custom_metadata, request_id) do
    put_object_data(conn, bucket, key, body, content_type, custom_metadata, request_id)
  end

  defp put_object_local_streamed(conn, bucket, key, content_type, custom_metadata, request_id) do
    stream = body_stream(conn)

    try do
      put_object_data(conn, bucket, key, stream, content_type, custom_metadata, request_id)
    catch
      {:error, :entity_too_large} ->
        entity_too_large_response(conn, bucket, key, request_id)
    end
  end

  defp put_object_data(conn, bucket, key, data, content_type, custom_metadata, request_id) do
    case ObjectService.put(bucket, key, data, content_type, custom_metadata) do
      {:ok, %{version_id: version_id, metadata: %{etag: etag}}} ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> maybe_put_version_header(version_id)
        |> send_resp(200, "")

      {:error, {:stage, :entity_too_large}} ->
        entity_too_large_response(conn, bucket, key, request_id)

      {:error, reason} ->
        error_response(
          conn,
          "InternalError",
          inspect(reason),
          "/#{bucket}/#{key}",
          request_id
        )
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

  defp maybe_put_version_header(conn, "null"), do: conn
  defp maybe_put_version_header(conn, nil), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

  defp send_source(conn, _path, _offset, 0), do: send_resp(conn, 200, "")
  defp send_source(conn, path, offset, length), do: send_file(conn, 200, path, offset, length)
end
