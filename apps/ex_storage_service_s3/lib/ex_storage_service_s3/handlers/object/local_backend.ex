defmodule ExStorageServiceS3.Handlers.Object.LocalBackend do
  @moduledoc false
  @behaviour ExStorageServiceS3.Handlers.Object.Backend

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Versioning

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
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        if Map.get(meta, :is_delete_marker) do
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> send_resp(404, "")
        else
          content_hash = meta.content_hash

          case Engine.get_object_location(bucket, content_hash) do
            {:ok, location} ->
              content_type = Map.get(meta, :content_type, "application/octet-stream")
              etag = Map.get(meta, :etag, "")
              quoted_etag = "\"#{etag}\""
              size = Map.get(meta, :size, 0)
              last_modified_raw = Map.get(meta, :updated_at, Map.get(meta, :created_at))
              last_modified = format_http_date(last_modified_raw)

              # Conditional request checks
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
                  # Check for Range header
                  case get_req_header(conn, "range") do
                    [range_header | _] ->
                      case parse_range(range_header, size) do
                        {:ok, offset, length} ->
                          content_range = "bytes #{offset}-#{offset + length - 1}/#{size}"
                          {send_path, base_offset} = location_file(location)

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
                      conn
                      |> put_s3_headers(request_id)
                      |> put_resp_header("content-type", content_type)
                      |> put_resp_header("etag", quoted_etag)
                      |> put_resp_header("last-modified", last_modified)
                      |> put_resp_header("content-length", to_string(size))
                      |> put_resp_header("accept-ranges", "bytes")
                      |> put_custom_metadata_headers(meta)
                      |> send_object(location)
                  end
              end

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
        case latest_delete_marker(bucket, key) do
          {:ok, version_id} ->
            delete_marker_response(conn, version_id, request_id)

          :no_such_bucket ->
            error_response(
              conn,
              "NoSuchBucket",
              "The specified bucket does not exist.",
              "/#{bucket}/#{key}",
              request_id
            )

          :not_found ->
            error_response(
              conn,
              "NoSuchKey",
              "The specified key does not exist.",
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
    content_hash = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    md5 = :crypto.hash(:md5, body)
    etag = Base.encode16(md5, case: :lower)
    size = byte_size(body)

    Engine.ensure_bucket_dirs(bucket)

    case Engine.put_object(bucket, key, body, content_type, custom_metadata) do
      {:ok, _} ->
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        meta = %{
          content_hash: content_hash,
          size: size,
          etag: etag,
          content_type: content_type,
          metadata: custom_metadata,
          created_at: now,
          updated_at: now
        }

        {:ok, version_id} = Versioning.put_version(bucket, key, meta)
        Hooks.after_put(bucket, key)
        broadcast_bucket_change(bucket, :put, key)

        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> maybe_put_version_header(version_id)
        |> send_resp(200, "")

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

  defp put_object_local_streamed(conn, bucket, key, content_type, custom_metadata, request_id) do
    # Stream the body directly to the storage engine rather than
    # accumulating the full object in memory. The stream enforces
    # max_object_size incrementally and halts early on oversize uploads.
    # We use put_object_stream/5 (not put_object/5) because Plug.Conn.read_body
    # must be called from the request process, not inside the Engine GenServer.
    stream = body_stream(conn)

    try do
      case Engine.put_object_stream(bucket, key, stream, content_type, custom_metadata) do
        {:ok, {content_hash, etag, size}} ->
          now = DateTime.utc_now() |> DateTime.to_iso8601()

          meta = %{
            content_hash: content_hash,
            size: size,
            etag: etag,
            content_type: content_type,
            metadata: custom_metadata,
            created_at: now,
            updated_at: now
          }

          {:ok, version_id} = Versioning.put_version(bucket, key, meta)
          Hooks.after_put(bucket, key)
          broadcast_bucket_change(bucket, :put, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("etag", "\"#{etag}\"")
          |> maybe_put_version_header(version_id)
          |> send_resp(200, "")

        {:error, reason} ->
          error_response(
            conn,
            "InternalError",
            inspect(reason),
            "/#{bucket}/#{key}",
            request_id
          )
      end
    catch
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

  defp maybe_put_version_header(conn, "null"), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

  defp location_file({:file, path}), do: {path, 0}
  defp location_file({:pack, path, offset, _size}), do: {path, offset}

  defp send_object(conn, {:file, path}), do: send_file(conn, 200, path)

  defp send_object(conn, {:pack, path, offset, size}),
    do: send_file(conn, 200, path, offset, size)
end
