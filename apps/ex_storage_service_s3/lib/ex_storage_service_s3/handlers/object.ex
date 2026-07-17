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
  alias ExStorageService.ObjectService
  alias ExStorageService.Storage.Engine

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
      case ObjectService.head(bucket, key) do
        {:ok, %{delete_marker: true, version_id: version_id}} ->
          delete_marker_response(conn, version_id, request_id)

        {:ok, %{metadata: metadata}} ->
          head_object_response(conn, metadata, request_id)

        {:error, :object_not_found} ->
          cloud_head_object_response(conn, bucket, key, request_id)

        {:error, :bucket_not_found} ->
          conn |> put_s3_headers(request_id) |> send_resp(404, "")

        {:error, _reason} ->
          conn |> put_s3_headers(request_id) |> send_resp(500, "")
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

      case ObjectService.delete(bucket, key, version_id) do
        {:ok, %{version_id: marker_id, kind: :delete_marker}} ->
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-delete-marker", "true")
          |> put_resp_header("x-amz-version-id", marker_id)
          |> send_resp(204, "")

        {:ok, %{version_id: "null", kind: :deleted}} when is_nil(version_id) ->
          conn
          |> put_s3_headers(request_id)
          |> send_resp(204, "")

        {:ok, %{version_id: deleted_version_id, kind: :deleted}} ->
          conn
          |> put_s3_headers(request_id)
          |> put_resp_header("x-amz-version-id", deleted_version_id)
          |> send_resp(204, "")

        {:error, reason} ->
          error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
      end
    end)
  end

  def copy_object(conn, bucket, key) do
    request_id = request_id(conn)

    case get_req_header(conn, "x-amz-copy-source") do
      [copy_source | _] ->
        {source_bucket, source_key} = parse_copy_source(copy_source)

        case ObjectService.head(source_bucket, source_key) do
          {:ok, %{metadata: source_metadata, delete_marker: false}} ->
            now = DateTime.utc_now() |> DateTime.to_iso8601()

            case perform_copy(
                   source_bucket,
                   source_key,
                   bucket,
                   key,
                   source_metadata,
                   cloud_cache_config(bucket)
                 ) do
              {:ok, %{version_id: version_id, metadata: destination_metadata}} ->
                body =
                  XML.copy_object_response(
                    "\"#{Map.get(destination_metadata, :etag, source_metadata.etag)}\"",
                    now
                  )

                conn
                |> maybe_put_version_header(version_id)
                |> xml_response(200, body, request_id)

              {:error, :bucket_not_found} ->
                error_response(
                  conn,
                  "NoSuchBucket",
                  "The specified bucket does not exist.",
                  "/#{bucket}/#{key}",
                  request_id
                )

              {:error, reason} when reason in [:source_missing, :blob_not_found] ->
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

          {:error, reason} when reason in [:object_not_found, :bucket_not_found] ->
            error_response(
              conn,
              "NoSuchKey",
              "The specified source key does not exist.",
              copy_source,
              request_id
            )

          {:ok, %{delete_marker: true}} ->
            error_response(
              conn,
              "NoSuchKey",
              "The specified source key does not exist.",
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

                {:ok, _result} = ObjectService.delete(bucket, key, nil)
                {:deleted, key}
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

    case ObjectService.get(bucket, key, version_id, []) do
      {:ok, %{delete_marker: true}} ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("x-amz-delete-marker", "true")
        |> put_resp_header("x-amz-version-id", version_id)
        |> send_resp(404, "")

      {:ok, %{metadata: metadata, source: {:file, path, offset, length}}} ->
        content_type = Map.get(metadata, :content_type, "application/octet-stream")
        etag = Map.get(metadata, :etag, "")
        size = Map.get(metadata, :size, 0)

        last_modified =
          format_http_date(Map.get(metadata, :updated_at, Map.get(metadata, :created_at)))

        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("content-type", content_type)
        |> put_resp_header("etag", "\"#{etag}\"")
        |> put_resp_header("last-modified", last_modified)
        |> put_resp_header("content-length", to_string(size))
        |> put_resp_header("x-amz-version-id", version_id)
        |> put_custom_metadata_headers(metadata)
        |> send_version_source(path, offset, length)

      {:error, :version_not_found} ->
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
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}/#{key}", request_id)
    end
  end

  defp cloud_cache_config(bucket) do
    CloudConfig.get_active_config(bucket)
  end

  defp head_object_response(conn, metadata, request_id) do
    content_type = Map.get(metadata, :content_type, "application/octet-stream")
    quoted_etag = "\"#{Map.get(metadata, :etag, "")}\""
    size = Map.get(metadata, :size, 0)
    last_modified_raw = Map.get(metadata, :updated_at, Map.get(metadata, :created_at))
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
        |> put_custom_metadata_headers(metadata)
        |> send_resp(200, "")
    end
  end

  defp cloud_head_object_response(conn, bucket, key, request_id) do
    case cloud_cache_config(bucket) do
      {:ok, cloud_config} ->
        case CloudClient.head_object(cloud_config, key) do
          {:ok, cloud_meta} ->
            last_modified =
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
            |> put_resp_header("last-modified", last_modified)
            |> put_resp_header("accept-ranges", "bytes")
            |> send_resp(200, "")

          {:error, :not_found} ->
            conn |> put_s3_headers(request_id) |> send_resp(404, "")

          {:error, _reason} ->
            conn |> put_s3_headers(request_id) |> send_resp(502, "")
        end

      :disabled ->
        conn |> put_s3_headers(request_id) |> send_resp(404, "")
    end
  end

  defp maybe_put_version_header(conn, "null"), do: conn

  defp maybe_put_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-version-id", version_id)

  defp send_version_source(conn, _path, _offset, 0), do: send_resp(conn, 200, "")

  defp send_version_source(conn, path, offset, length),
    do: send_file(conn, 200, path, offset, length)

  defp perform_copy(
         source_bucket,
         source_key,
         destination_bucket,
         destination_key,
         _source_metadata,
         :disabled
       ) do
    ObjectService.copy(source_bucket, source_key, destination_bucket, destination_key)
  end

  defp perform_copy(
         source_bucket,
         source_key,
         destination_bucket,
         destination_key,
         source_metadata,
         {:ok, cloud_config}
       ) do
    with :ok <-
           copy_destination_content(
             source_bucket,
             source_key,
             destination_key,
             source_metadata,
             cloud_config
           ) do
      attributes =
        source_metadata
        |> Map.drop([:version_id, :parent_version_id, :created_at, :updated_at])

      ready = %{
        hash: source_metadata.content_hash,
        size: source_metadata.size,
        etag: source_metadata.etag
      }

      ObjectService.commit_existing_blob(
        destination_bucket,
        destination_key,
        ready,
        attributes,
        blob_bucket: source_bucket
      )
    end
  end

  defp copy_destination_content(
         source_bucket,
         source_key,
         dest_key,
         source_meta,
         cloud_config
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
