defmodule ExStorageService.S3.Handlers do
  @moduledoc """
  Request handlers for S3-compatible API operations.

  Coordinates between Storage.Engine (disk I/O) and Metadata (Concord KV).
  """

  import Plug.Conn
  alias ExStorageService.S3.XML
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine

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
        error_response(conn, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded.", "/#{bucket}", request_id)

      {:error, :not_found} ->
        Engine.ensure_bucket_dirs(bucket)

        case Metadata.create_bucket(bucket) do
          :ok ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("location", "/#{bucket}")
            |> send_resp(200, "")

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
        error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}", request_id)

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
            error_response(conn, "BucketNotEmpty", "The bucket you tried to delete is not empty.", "/#{bucket}", request_id)

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
        error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}", request_id)

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

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        content_hash = meta.content_hash

        case Engine.get_object(bucket, content_hash) do
          {:ok, file_path} ->
            content_type = Map.get(meta, :content_type, "application/octet-stream")
            etag = Map.get(meta, :etag, "")
            size = Map.get(meta, :size, 0)
            last_modified = format_http_date(Map.get(meta, :updated_at, Map.get(meta, :created_at)))

            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("content-type", content_type)
            |> put_resp_header("etag", "\"#{etag}\"")
            |> put_resp_header("last-modified", last_modified)
            |> put_resp_header("content-length", to_string(size))
            |> put_custom_metadata_headers(meta)
            |> send_file(200, file_path)

          {:error, _} ->
            error_response(conn, "InternalError", "Content file missing", "/#{bucket}/#{key}", request_id)
        end

      {:error, :not_found} ->
        case Metadata.head_bucket(bucket) do
          {:error, :not_found} ->
            error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}/#{key}", request_id)

          :ok ->
            error_response(conn, "NoSuchKey", "The specified key does not exist.", "/#{bucket}/#{key}", request_id)
        end
    end
  end

  def head_object(conn, bucket, key) do
    request_id = request_id(conn)

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        content_type = Map.get(meta, :content_type, "application/octet-stream")
        etag = Map.get(meta, :etag, "")
        size = Map.get(meta, :size, 0)
        last_modified = format_http_date(Map.get(meta, :updated_at, Map.get(meta, :created_at)))

        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("content-type", content_type)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> put_resp_header("last-modified", last_modified)
        |> put_resp_header("content-length", to_string(size))
        |> put_custom_metadata_headers(meta)
        |> send_resp(200, "")

      {:error, :not_found} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(404, "")
    end
  end

  def put_object(conn, bucket, key) do
    request_id = request_id(conn)

    case Metadata.head_bucket(bucket) do
      {:error, :not_found} ->
        error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}/#{key}", request_id)

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

                conn
                |> put_s3_headers(request_id)
                |> put_resp_header("etag", "\"#{etag}\"")
                |> send_resp(200, "")

              {:error, reason} ->
                error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
            end

          {:error, reason} ->
            error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
        end
    end
  end

  def delete_object(conn, bucket, key) do
    request_id = request_id(conn)

    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        Metadata.delete_object_meta(bucket, key)
        Engine.delete_content(bucket, meta.content_hash)

        conn
        |> put_s3_headers(request_id)
        |> send_resp(204, "")

      {:error, :not_found} ->
        conn
        |> put_s3_headers(request_id)
        |> send_resp(204, "")
    end
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
                error_response(conn, "NoSuchBucket", "The specified bucket does not exist.", "/#{bucket}/#{key}", request_id)

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
                last_modified = format_http_date(now)
                body = XML.copy_object_response("\"#{source_meta.etag}\"", last_modified)
                xml_response(conn, 200, body, request_id)
            end

          {:error, :not_found} ->
            error_response(conn, "NoSuchKey", "The specified source key does not exist.", copy_source, request_id)
        end

      [] ->
        error_response(conn, "InvalidArgument", "Missing x-amz-copy-source header.", "/#{bucket}/#{key}", request_id)
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
            error_response(conn, "MalformedXML", "The XML you provided was not well-formed.", "/#{bucket}?delete", request_id)
        end

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}?delete", request_id)
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
end
