defmodule ExStorageServiceS3.Handlers.Object do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.Handlers.Object.Backend
  alias ExStorageServiceS3.XML
  alias ExStorageService.CloudCache.Client, as: CloudClient
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.LocalStore
  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Versioning

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

        Backend.for_bucket(bucket).list_objects(conn, bucket, opts, request_id)
    end
  end

  def get_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:get_object, %{bucket: bucket, key: key}, fn ->
      Backend.for_bucket(bucket).get_object(conn, bucket, key, request_id)
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
          # For cloud-cached buckets, fall back to HEAD on the remote
          case cloud_cache_config(bucket) do
            {:ok, cloud_config} ->
              case CloudClient.head_object(cloud_config, key) do
                {:ok, cloud_meta} ->
                  last_mod =
                    cloud_meta.last_modified ||
                      format_http_date(DateTime.utc_now() |> DateTime.to_iso8601())

                  conn
                  |> put_s3_headers(request_id)
                  |> put_resp_header(
                    "content-type",
                    cloud_meta.content_type || "application/octet-stream"
                  )
                  |> put_resp_header("etag", "\"#{cloud_meta.etag}\"")
                  |> put_resp_header("content-length", to_string(cloud_meta.content_length))
                  |> put_resp_header("last-modified", last_mod)
                  |> put_resp_header("accept-ranges", "bytes")
                  |> send_resp(200, "")

                {:error, :not_found} ->
                  conn |> put_s3_headers(request_id) |> send_resp(404, "")

                {:error, _} ->
                  conn |> put_s3_headers(request_id) |> send_resp(502, "")
              end

            :disabled ->
              conn |> put_s3_headers(request_id) |> send_resp(404, "")
          end
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
          Backend.for_bucket(bucket).put_object(conn, bucket, key, request_id)
      end
    end)
  end

  def delete_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:delete_object, %{bucket: bucket, key: key}, fn ->
      # For cloud-cached buckets, delete from upstream cloud and clear local cache
      case cloud_cache_config(bucket) do
        {:ok, cloud_config} ->
          case CloudClient.delete_object(cloud_config, key) do
            :ok ->
              Logger.info("CloudCache DELETE upstream OK: #{cloud_config.bucket}/#{key}")

            {:error, reason} ->
              Logger.error(
                "CloudCache DELETE upstream FAILED: #{cloud_config.bucket}/#{key} — #{inspect(reason)}"
              )
          end

          LocalStore.delete(bucket, key)

        :disabled ->
          :ok
      end

      version_id = conn.query_params["versionId"]

      case Versioning.delete_version(bucket, key, version_id) do
        {:ok, marker_id, :delete_marker} ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> put_resp_header("x-amz-version-id", marker_id)
          |> send_resp(204, "")

        {:ok, "null", :deleted} when is_nil(version_id) ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> send_resp(204, "")

        {:ok, deleted_vid, :deleted} ->
          Hooks.after_delete(bucket, key)
          broadcast_bucket_change(bucket, :delete, key)

          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-version-id", deleted_vid)
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

                case copy_destination_content(
                       source_bucket,
                       source_key,
                       bucket,
                       key,
                       source_meta,
                       cloud_cache_config(bucket)
                     ) do
                  :ok ->
                    {:ok, version_id} = Versioning.put_version(bucket, key, new_meta)
                    Hooks.after_put(bucket, key)
                    broadcast_bucket_change(bucket, :put, key)
                    # CopyObjectResult requires ISO 8601 (not HTTP date format)
                    body = XML.copy_object_response("\"#{source_meta.etag}\"", now)

                    conn = maybe_put_version_header(conn, version_id)
                    xml_response(conn, 200, body, request_id)

                  {:error, :source_missing} ->
                    error_response(
                      conn,
                      "NoSuchKey",
                      "The source object's content is missing.",
                      copy_source,
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
            # Resolve cloud cache config once for the batch
            cloud_cfg = cloud_cache_config(bucket)

            results =
              Enum.map(keys, fn key ->
                # Delete from upstream cloud and clear local cache
                case cloud_cfg do
                  {:ok, cloud_config} ->
                    case CloudClient.delete_object(cloud_config, key) do
                      :ok ->
                        Logger.info(
                          "CloudCache DELETE upstream OK: #{cloud_config.bucket}/#{key}"
                        )

                      {:error, reason} ->
                        Logger.error(
                          "CloudCache DELETE upstream FAILED: #{cloud_config.bucket}/#{key} — #{inspect(reason)}"
                        )
                    end

                    LocalStore.delete(bucket, key)

                  :disabled ->
                    :ok
                end

                {:ok, _vid, _kind} = Versioning.delete_version(bucket, key)
                Hooks.after_delete(bucket, key)
                {:deleted, key}
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

          case Engine.get_object_location(bucket, content_hash) do
            {:ok, location} ->
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
              |> send_versioned_object(location)

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

  defp cloud_cache_config(bucket) do
    CloudConfig.get_active_config(bucket)
  end

  defp maybe_put_version_header(conn, "null"), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

  defp copy_destination_content(
         source_bucket,
         _source_key,
         _dest_bucket,
         _dest_key,
         source_meta,
         :disabled
       ) do
    # Content is globally addressed: ensure the blob is in the CAS
    # (promoting pre-migration legacy content), then the copy is
    # metadata-only — no physical file duplication.
    case Engine.promote_to_global(source_bucket, source_meta.content_hash) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :source_missing}
    end
  end

  defp copy_destination_content(
         source_bucket,
         source_key,
         _dest_bucket,
         dest_key,
         source_meta,
         {:ok, cloud_config}
       ) do
    with {:ok, data} <- read_source_object_data(source_bucket, source_key, source_meta),
         content_type = Map.get(source_meta, :content_type, "application/octet-stream"),
         custom_metadata = Map.get(source_meta, :metadata, %{}),
         :ok <-
           CloudClient.put_object(cloud_config, dest_key, data, content_type, custom_metadata) do
      :ok
    else
      {:error, :not_found} -> {:error, :source_missing}
      {:error, :no_source} -> {:error, :source_missing}
      {:error, reason} -> {:error, {:cloud_copy_failed, reason}}
    end
  end

  defp read_source_object_data(source_bucket, source_key, source_meta) do
    case LocalStore.get(source_bucket, source_key) do
      {:ok, path} ->
        File.read(path)

      :miss ->
        read_uncached_source_object_data(source_bucket, source_key, source_meta.content_hash)
    end
  end

  defp read_uncached_source_object_data(source_bucket, source_key, content_hash) do
    case Engine.read_object(source_bucket, content_hash) do
      {:ok, data} ->
        {:ok, data}

      {:error, _} ->
        case cloud_cache_config(source_bucket) do
          {:ok, src_config} -> CloudClient.get_object(src_config, source_key)
          :disabled -> {:error, :no_source}
        end
    end
  end

  defp send_versioned_object(conn, {:file, path}), do: send_file(conn, 200, path)

  defp send_versioned_object(conn, {:pack, path, offset, size}),
    do: send_file(conn, 200, path, offset, size)

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
    if xml_has_doctype?(xml_body) do
      {:error, :malformed_xml}
    else
      do_parse_delete_objects_xml(xml_body)
    end
  end

  defp do_parse_delete_objects_xml(xml_body) do
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
end
