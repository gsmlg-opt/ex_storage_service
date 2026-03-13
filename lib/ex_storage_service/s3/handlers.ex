defmodule ExStorageService.S3.Handlers do
  @moduledoc """
  Request handlers for S3-compatible API operations.

  Coordinates between Storage.Engine (disk I/O) and Metadata (Concord KV).
  """

  import Plug.Conn
  alias ExStorageService.S3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Storage.Versioning

  def list_buckets(conn) do
    request_id = request_id(conn)

    case Metadata.list_buckets() do
      {:ok, buckets} ->
        body = XML.list_buckets_response(buckets)
        xml_response(conn, 200, body, request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/", request_id)
    end
  end

  def create_bucket(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      :ok ->
        error_response(
          conn,
          "BucketAlreadyOwnedByYou",
          "Your previous request to create the named bucket succeeded.",
          "/#{bucket}",
          request_id
        )

      {:error, :not_found} ->
        Engine.ensure_bucket_dirs(bucket)

        case Metadata.create_bucket(bucket) do
          :ok ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("location", "/#{bucket}")
            |> send_resp(201, "")

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  def delete_bucket(conn, bucket) do
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

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)

      :ok ->
        case Metadata.list_objects(bucket, max_keys: 1) do
          {:ok, %{keys: []}} ->
            Metadata.delete_bucket(bucket)

            conn
            |> put_s3_headers(request_id)
            |> send_resp(204, "")

          {:ok, _} ->
            error_response(
              conn,
              "BucketNotEmpty",
              "The bucket you tried to delete is not empty.",
              "/#{bucket}",
              request_id
            )

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
        end
    end
  end

  def head_bucket(conn, bucket) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      :ok ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(200, "")

      {:error, :not_found} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(404, "")

      {:error, _reason} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(500, "")
    end
  end

  def list_objects(conn, bucket) do
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

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)

      :ok ->
        params = conn.query_params

        opts = [
          prefix: Map.get(params, "prefix", ""),
          delimiter: Map.get(params, "delimiter"),
          max_keys: parse_max_keys(Map.get(params, "max-keys", "1000")),
          continuation_token: Map.get(params, "continuation-token")
        ]

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
  end

  def get_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:get_object, %{bucket: bucket, key: key}, fn ->
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
    end)
  end

  def head_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:head_object, %{bucket: bucket, key: key}, fn ->
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
          conn
          |> put_s3_headers(request_id)
          |> send_resp(404, "")
      end
    end)
  end

  def put_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:put_object, %{bucket: bucket, key: key}, fn ->
      case Metadata.head_bucket(bucket) do
        {:error, :not_found} ->
          error_response(
            conn,
            "NoSuchBucket",
            "The specified bucket does not exist.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)

        :ok ->
          case read_full_body(conn) do
            {:ok, body, conn} ->
              content_type =
                case get_req_header(conn, "content-type") do
                  [ct | _] -> ct
                  [] -> "application/octet-stream"
                end

              custom_metadata = extract_custom_metadata(conn)

              case Engine.put_object(bucket, key, body, content_type, custom_metadata) do
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
              error_response(
                conn,
                "InternalError",
                inspect(reason),
                "/#{bucket}/#{key}",
                request_id
              )
          end
      end
    end)
  end

  def delete_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:delete_object, %{bucket: bucket, key: key}, fn ->
      case Metadata.get_object_meta(bucket, key) do
        {:ok, meta} ->
          Metadata.delete_object_meta(bucket, key)
          Engine.delete_content(bucket, meta.content_hash)
          Hooks.after_delete(bucket, key)

          conn
          |> put_s3_headers(request_id)
          |> send_resp(204, "")

        {:error, :not_found} ->
          conn
          |> put_s3_headers(request_id)
          |> send_resp(204, "")
      end
    end)
  end

  def copy_object(conn, bucket, key) do
    request_id = request_id(conn)

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

                # Copy content file if different buckets
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
                last_modified = format_http_date(now)
                body = XML.copy_object_response("\"#{source_meta.etag}\"", last_modified)
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

  def delete_objects(conn, bucket) do
    request_id = request_id(conn)

    case read_full_body(conn) do
      {:ok, body, _conn} ->
        case parse_delete_objects_xml(body) do
          {:ok, keys} ->
            results =
              Enum.map(keys, fn key ->
                case Metadata.get_object_meta(bucket, key) do
                  {:ok, meta} ->
                    Metadata.delete_object_meta(bucket, key)
                    Engine.delete_content(bucket, meta.content_hash)
                    {:deleted, key}

                  {:error, :not_found} ->
                    {:deleted, key}
                end
              end)

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

  ## Versioning handlers

  def get_object_version(conn, bucket, key, version_id) do
    request_id = request_id(conn)

    case Versioning.get_version(bucket, key, version_id) do
      {:ok, meta} ->
        if Map.get(meta, :is_delete_marker) do
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> put_resp_header("x-amz-version-id", version_id)
          |> send_resp(404, "")
        else
          content_hash = meta.content_hash

          case Engine.get_object(bucket, content_hash) do
            {:ok, file_path} ->
              content_type = Map.get(meta, :content_type, "application/octet-stream")
              etag = Map.get(meta, :etag, "")
              size = Map.get(meta, :size, 0)

              last_modified =
                format_http_date(Map.get(meta, :updated_at, Map.get(meta, :created_at)))

              conn
              |> put_s3_headers(request_id)
              |> put_resp_header("content-type", content_type)
              |> put_resp_header("etag", "\"#{etag}\"")
              |> put_resp_header("last-modified", last_modified)
              |> put_resp_header("content-length", to_string(size))
              |> put_resp_header("x-amz-version-id", version_id)
              |> put_custom_metadata_headers(meta)
              |> send_file(200, file_path)

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
        error_response(
          conn,
          "NoSuchKey",
          "The specified key does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )
    end
  end

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
          {:ok, body, _conn} ->
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
          {:ok, body, _conn} ->
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
    max_size = Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)

    case Plug.Conn.read_body(conn) do
      {:ok, body, conn} ->
        result = acc <> body

        if byte_size(result) > max_size do
          {:error, :entity_too_large}
        else
          {:ok, result, conn}
        end

      {:more, partial, conn} ->
        result = acc <> partial

        if byte_size(result) > max_size do
          {:error, :entity_too_large}
        else
          read_full_body(conn, result)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_max_keys(value) do
    case Integer.parse(value) do
      {n, _} when n > 0 and n <= 1000 -> n
      {n, _} when n > 1000 -> 1000
      _ -> 1000
    end
  end

  defp parse_copy_source(source) do
    source = String.trim_leading(source, "/")

    case String.split(source, "/", parts: 2) do
      [bucket, key] -> {bucket, URI.decode(key)}
      [bucket] -> {bucket, ""}
    end
  end

  defp parse_delete_objects_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      keys =
        :xmerl_xpath.string(~c"//Object/Key/text()", doc)
        |> Enum.map(fn
          {:xmlText, _, _, _, value, _} -> to_string(value)
          _ -> nil
        end)
        |> Enum.reject(&is_nil/1)

      {:ok, keys}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp extract_custom_metadata(conn) do
    conn.req_headers
    |> Enum.filter(fn {key, _} -> String.starts_with?(key, "x-amz-meta-") end)
    |> Enum.map(fn {"x-amz-meta-" <> name, value} -> {name, value} end)
    |> Map.new()
  end

  defp put_custom_metadata_headers(conn, meta) do
    custom = Map.get(meta, :metadata, %{})

    Enum.reduce(custom, conn, fn {name, value}, acc ->
      put_resp_header(acc, "x-amz-meta-#{name}", value)
    end)
  end

  @doc false
  def parse_range(range_header, total_size) do
    case Regex.run(~r/^bytes=(\d*)-(\d*)$/, range_header) do
      [_, start_str, ""] when start_str != "" ->
        start = String.to_integer(start_str)

        if start < total_size do
          {:ok, start, total_size - start}
        else
          {:error, :invalid_range}
        end

      [_, "", end_str] when end_str != "" ->
        suffix_length = String.to_integer(end_str)

        if suffix_length > 0 and suffix_length <= total_size do
          offset = total_size - suffix_length
          {:ok, offset, suffix_length}
        else
          {:error, :invalid_range}
        end

      [_, start_str, end_str] when start_str != "" and end_str != "" ->
        range_start = String.to_integer(start_str)
        range_end = String.to_integer(end_str)

        if range_start <= range_end and range_start < total_size do
          actual_end = min(range_end, total_size - 1)
          {:ok, range_start, actual_end - range_start + 1}
        else
          {:error, :invalid_range}
        end

      _ ->
        {:error, :invalid_range}
    end
  end

  defp not_modified_etag?(conn, quoted_etag) do
    case get_req_header(conn, "if-none-match") do
      [client_etag | _] ->
        # Strip whitespace and compare
        String.trim(client_etag) == quoted_etag

      [] ->
        false
    end
  end

  defp not_modified_since?(conn, last_modified_raw) do
    case get_req_header(conn, "if-modified-since") do
      [ims_str | _] ->
        with {:ok, ims_dt} <- parse_http_date(ims_str),
             {:ok, obj_dt} <- parse_object_datetime(last_modified_raw) do
          DateTime.compare(obj_dt, ims_dt) != :gt
        else
          _ -> false
        end

      [] ->
        false
    end
  end

  defp parse_http_date(date_str) do
    # Parse RFC 7231 date format: "Thu, 01 Jan 2026 00:00:00 GMT"
    date_str = String.trim(date_str)

    months = %{
      "Jan" => 1,
      "Feb" => 2,
      "Mar" => 3,
      "Apr" => 4,
      "May" => 5,
      "Jun" => 6,
      "Jul" => 7,
      "Aug" => 8,
      "Sep" => 9,
      "Oct" => 10,
      "Nov" => 11,
      "Dec" => 12
    }

    case Regex.run(
           ~r/\w+,\s+(\d{2})\s+(\w{3})\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+GMT/,
           date_str
         ) do
      [_, day, month_str, year, hour, min, sec] ->
        with month when month != nil <- Map.get(months, month_str),
             {:ok, dt} <-
               DateTime.new(
                 Date.new!(String.to_integer(year), month, String.to_integer(day)),
                 Time.new!(
                   String.to_integer(hour),
                   String.to_integer(min),
                   String.to_integer(sec)
                 ),
                 "Etc/UTC"
               ) do
          {:ok, dt}
        else
          _ -> {:error, :invalid_date}
        end

      _ ->
        {:error, :invalid_date}
    end
  end

  defp parse_object_datetime(nil), do: {:error, :no_date}

  defp parse_object_datetime(datetime_string) when is_binary(datetime_string) do
    case DateTime.from_iso8601(datetime_string) do
      {:ok, dt, _} -> {:ok, dt}
      _ -> {:error, :invalid_date}
    end
  end

  defp parse_object_datetime(%DateTime{} = dt), do: {:ok, dt}

  defp format_http_date(nil), do: ""

  defp format_http_date(datetime_string) when is_binary(datetime_string) do
    case DateTime.from_iso8601(datetime_string) do
      {:ok, dt, _} -> Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")
      _ -> datetime_string
    end
  end

  defp format_http_date(%DateTime{} = dt) do
    Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")
  end

  defp parse_versioning_xml(xml_body) do
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

  defp parse_lifecycle_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      rules =
        :xmerl_xpath.string(~c"//Rule", doc)
        |> Enum.map(fn rule_elem ->
          id = xpath_text(rule_elem, ~c"ID")

          prefix =
            xpath_text(rule_elem, ~c"Filter/Prefix") || xpath_text(rule_elem, ~c"Prefix") || ""

          status = xpath_text(rule_elem, ~c"Status") || "Enabled"
          days_str = xpath_text(rule_elem, ~c"Expiration/Days") || "0"
          days = String.to_integer(days_str)

          %{
            id: id || "",
            prefix: prefix,
            status: status,
            expiration_days: days
          }
        end)

      {:ok, rules}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp xpath_text(elem, path) do
    case :xmerl_xpath.string(path ++ ~c"/text()", elem) do
      [{:xmlText, _, _, _, value, _} | _] -> to_string(value)
      _ -> nil
    end
  end

  defp build_lifecycle_xml(rules) do
    rule_elements =
      Enum.map(rules, fn rule ->
        id = Map.get(rule, :id, "")
        prefix = Map.get(rule, :prefix, "")
        status = Map.get(rule, :status, "Enabled")
        days = Map.get(rule, :expiration_days, 0)

        """
        <Rule>\
        <ID>#{XML.escape(id)}</ID>\
        <Filter><Prefix>#{XML.escape(prefix)}</Prefix></Filter>\
        <Status>#{XML.escape(status)}</Status>\
        <Expiration><Days>#{days}</Days></Expiration>\
        </Rule>\
        """
      end)
      |> Enum.join()

    """
    <?xml version="1.0" encoding="UTF-8"?>\
    <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{rule_elements}</LifecycleConfiguration>\
    """
  end

  defp parse_notification_xml(xml_body) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml_body))

      configs =
        :xmerl_xpath.string(~c"//TopicConfiguration", doc)
        |> Enum.map(fn config_elem ->
          id = xpath_text(config_elem, ~c"Id") || ""
          endpoint = xpath_text(config_elem, ~c"Topic") || ""

          events =
            :xmerl_xpath.string(~c"Event/text()", config_elem)
            |> Enum.map(fn {:xmlText, _, _, _, value, _} -> to_string(value) end)

          %{
            id: id,
            endpoint: endpoint,
            events: events,
            enabled: true
          }
        end)

      {:ok, configs}
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp build_notification_xml(configs) do
    config_elements =
      Enum.map(configs, fn config ->
        events =
          Enum.map(Map.get(config, :events, []), fn event ->
            "<Event>#{XML.escape(event)}</Event>"
          end)
          |> Enum.join()

        """
        <TopicConfiguration>\
        <Id>#{XML.escape(Map.get(config, :id, ""))}</Id>\
        <Topic>#{XML.escape(Map.get(config, :endpoint, ""))}</Topic>\
        #{events}\
        </TopicConfiguration>\
        """
      end)
      |> Enum.join()

    """
    <?xml version="1.0" encoding="UTF-8"?>\
    <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">#{config_elements}</NotificationConfiguration>\
    """
  end
end
