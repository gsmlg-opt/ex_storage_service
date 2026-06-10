defmodule ExStorageServiceS3.StorageBackend.Local do
  @behaviour ExStorageServiceS3.StorageBackend

  import Plug.Conn
  alias ExStorageServiceS3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Replication.Hooks

  import ExStorageServiceS3.Handlers.Helpers

  @impl true
  def list_objects(conn, bucket, opts, request_id, _config) do
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
  def get_object(conn, bucket, key, request_id, _config) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        content_hash = meta.content_hash

        case Engine.get_object(bucket, content_hash) do
          {:ok, file_path} ->
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

                        conn
                        |> put_s3_headers(request_id)
                        |> put_resp_header("content-type", content_type)
                        |> put_resp_header("etag", quoted_etag)
                        |> put_resp_header("last-modified", last_modified)
                        |> put_resp_header("content-length", to_string(length))
                        |> put_resp_header("content-range", content_range)
                        |> put_resp_header("accept-ranges", "bytes")
                        |> put_custom_metadata_headers(meta)
                        |> send_file(206, file_path, offset, length)

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
                    |> send_file(200, file_path)
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

      {:error, :not_found} ->
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
            error_response(
              conn,
              "NoSuchKey",
              "The specified key does not exist.",
              "/#{bucket}/#{key}",
              request_id
            )
        end
    end
  end

  @impl true
  def head_object(conn, bucket, key, request_id, _config) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
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
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("content-type", content_type)
            |> put_resp_header("etag", quoted_etag)
            |> put_resp_header("last-modified", last_modified)
            |> put_resp_header("content-length", to_string(size))
            |> put_resp_header("accept-ranges", "bytes")
            |> put_custom_metadata_headers(meta)
            |> send_resp(200, "")
        end

      {:error, :not_found} ->
        conn |> put_s3_headers(request_id) |> send_resp(404, "")
    end
  end

  @impl true
  def put_object(conn, bucket, key, request_id, _config) do
    content_type =
      case get_req_header(conn, "content-type") do
        [ct | _] -> ct
        [] -> "application/octet-stream"
      end

    custom_metadata = extract_custom_metadata(conn)

    if aws_chunked?(conn) do
      put_object_buffered(conn, bucket, key, content_type, custom_metadata, request_id)
    else
      put_object_streamed(conn, bucket, key, content_type, custom_metadata, request_id)
    end
  end

  defp put_object_buffered(conn, bucket, key, content_type, custom_metadata, request_id) do
    try do
      case read_full_body(conn) do
        {:ok, raw_body, _conn} ->
          body = decode_aws_chunked(raw_body)
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

              Metadata.put_object_meta(bucket, key, meta)
              Hooks.after_put(bucket, key)
              broadcast_bucket_change(bucket, :put, key)

              conn
              |> put_s3_headers(request_id)
              |> put_resp_header("etag", "\"#{etag}\"")
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

  defp put_object_streamed(conn, bucket, key, content_type, custom_metadata, request_id) do
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

          Metadata.put_object_meta(bucket, key, meta)
          Hooks.after_put(bucket, key)
          broadcast_bucket_change(bucket, :put, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("etag", "\"#{etag}\"")
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

  @impl true
  def delete_object(conn, bucket, key, request_id, _config) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, _meta} ->
        Metadata.delete_object_meta(bucket, key)
        Hooks.after_delete(bucket, key)
        broadcast_bucket_change(bucket, :delete, key)

        conn
        |> put_s3_headers(request_id)
        |> send_resp(204, "")

      {:error, :not_found} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(204, "")
    end
  end

  @impl true
  def copy_object(conn, bucket, key, request_id, _config) do
    case get_req_header(conn, "x-amz-copy-source") do
      [copy_source | _] ->
        {source_bucket, source_key} = parse_copy_source(copy_source)

        case Metadata.get_object_meta(source_bucket, source_key) do
          {:ok, source_meta} ->
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
                now = DateTime.utc_now() |> DateTime.to_iso8601()

                new_meta = Map.merge(source_meta, %{created_at: now, updated_at: now})

                # Copy content file if different buckets (local storage)
                if source_bucket != bucket do
                  case Engine.get_object(source_bucket, source_meta.content_hash) do
                    {:ok, source_path} ->
                      data_root = Application.get_env(:ex_storage_service, :data_root)
                      dest_path = Engine.content_path(data_root, bucket, source_meta.content_hash)
                      File.mkdir_p!(Path.dirname(dest_path))
                      File.cp!(source_path, dest_path)

                    {:error, _} ->
                      :ok
                  end
                end

                Metadata.put_object_meta(bucket, key, new_meta)
                Hooks.after_put(bucket, key)
                broadcast_bucket_change(bucket, :put, key)
                # CopyObjectResult requires ISO 8601 (not HTTP date format)
                body = XML.copy_object_response("\"#{source_meta.etag}\"", now)
                xml_response(conn, 200, body, request_id)
            end

          {:error, :not_found} ->
            error_response(
              conn,
              "NoSuchKey",
              "The specified source key does not exist.",
              copy_source,
              request_id
            )
        end

      [] ->
        error_response(
          conn,
          "InvalidArgument",
          "Missing x-amz-copy-source header.",
          "/#{bucket}/#{key}",
          request_id
        )
    end
  end

  @impl true
  def delete_objects(conn, bucket, request_id, _config) do
    case read_full_body(conn) do
      {:ok, body, _conn} ->
        case parse_delete_objects_xml(body) do
          {:ok, keys} ->
            results =
              Enum.map(keys, fn key ->
                case Metadata.get_object_meta(bucket, key) do
                  {:ok, _meta} ->
                    Metadata.delete_object_meta(bucket, key)
                    {:deleted, key}

                  {:error, :not_found} ->
                    {:deleted, key}
                end
              end)

            broadcast_bucket_change(bucket, :delete_objects, nil)

            body = XML.delete_objects_response(results)
            xml_response(conn, 200, body, request_id)

          {:error, _reason} ->
            error_response(
              conn,
              "MalformedXML",
              "The XML you provided was not well-formed.",
              "/#{bucket}?delete",
              request_id
            )
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}?delete", request_id)
    end
  end
end
