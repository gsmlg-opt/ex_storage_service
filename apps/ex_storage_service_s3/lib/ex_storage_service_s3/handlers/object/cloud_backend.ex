defmodule ExStorageServiceS3.Handlers.Object.CloudBackend do
  @moduledoc false
  @behaviour ExStorageServiceS3.Handlers.Object.Backend

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageService.CloudCache.Client, as: CloudClient
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.LocalStore
  alias ExStorageService.Metadata
  alias ExStorageService.Replication.Hooks

  @impl true
  def list_objects(conn, bucket, opts, request_id) do
    {:ok, cloud_config} = CloudConfig.get_active_config(bucket)

    cloud_opts = [
      prefix: Keyword.get(opts, :prefix, ""),
      delimiter: Keyword.get(opts, :delimiter),
      max_keys: Keyword.get(opts, :max_keys, 1000),
      continuation_token: Keyword.get(opts, :continuation_token)
    ]

    case CloudClient.list_objects(cloud_config, cloud_opts) do
      {:ok, result} ->
        objects =
          Enum.map(result.keys, fn {key, meta} ->
            %{
              key: key,
              last_modified: Map.get(meta, :last_modified, Map.get(meta, :updated_at, "")),
              etag: "\"#{Map.get(meta, :etag, "")}\"",
              size: Map.get(meta, :size, 0),
              storage_class: "STANDARD"
            }
          end)

        response_opts = %{
          prefix: Keyword.get(cloud_opts, :prefix, ""),
          delimiter: Keyword.get(cloud_opts, :delimiter) || "",
          max_keys: Keyword.get(cloud_opts, :max_keys, 1000),
          is_truncated: result.truncated,
          # S3 spec: KeyCount includes both Contents and CommonPrefixes
          key_count: length(objects) + length(result.common_prefixes),
          continuation_token: Keyword.get(cloud_opts, :continuation_token),
          next_continuation_token: result.next_continuation_token,
          common_prefixes: result.common_prefixes
        }

        body = XML.list_objects_response(bucket, objects, response_opts)
        xml_response(conn, 200, body, request_id)

      {:error, reason} ->
        error_response(conn, "InternalError", inspect(reason), "/#{bucket}", request_id)
    end
  end

  # LIST from local Concord metadata — original implementation

  @impl true
  def get_object(conn, bucket, key, request_id) do
    {:ok, cloud_config} = CloudConfig.get_active_config(bucket)

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
            # Check local cache first
            file_path =
              case LocalStore.get(bucket, key) do
                {:ok, path} ->
                  path

                :miss ->
                  # Fetch from cloud and populate cache
                  case CloudClient.get_object(cloud_config, key) do
                    {:ok, body} ->
                      case LocalStore.put(bucket, key, body, cloud_config) do
                        {:ok, path} -> path
                        _ -> nil
                      end

                    {:error, :not_found} ->
                      nil

                    {:error, _reason} ->
                      nil
                  end
              end

            if file_path do
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
            else
              error_response(
                conn,
                "NoSuchKey",
                "The specified key does not exist.",
                "/#{bucket}/#{key}",
                request_id
              )
            end
        end

      {:error, :not_found} ->
        # Not in local metadata — check cloud directly
        case CloudClient.head_object(cloud_config, key) do
          {:ok, cloud_meta} ->
            # Object exists on cloud but not indexed locally — fetch and serve
            case CloudClient.get_object(cloud_config, key) do
              {:ok, body} ->
                now = DateTime.utc_now() |> DateTime.to_iso8601()
                content_hash = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
                size = byte_size(body)
                etag = cloud_meta.etag || content_hash
                content_type = cloud_meta.content_type || "application/octet-stream"
                last_mod = cloud_meta.last_modified || format_http_date(now)

                meta = %{
                  content_hash: content_hash,
                  size: size,
                  etag: etag,
                  content_type: content_type,
                  metadata: %{},
                  created_at: now,
                  updated_at: now,
                  cloud_backed: true
                }

                Metadata.put_object_meta(bucket, key, meta)

                case LocalStore.put(bucket, key, body, cloud_config) do
                  {:ok, file_path} ->
                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-type", content_type)
                    |> put_resp_header("etag", "\"#{etag}\"")
                    |> put_resp_header("last-modified", last_mod)
                    |> put_resp_header("content-length", to_string(size))
                    |> put_resp_header("accept-ranges", "bytes")
                    |> send_file(200, file_path)

                  _ ->
                    conn
                    |> put_s3_headers(request_id)
                    |> put_resp_header("content-type", content_type)
                    |> put_resp_header("last-modified", last_mod)
                    |> put_resp_header("content-length", to_string(size))
                    |> send_resp(200, body)
                end

              {:error, :not_found} ->
                error_response(
                  conn,
                  "NoSuchKey",
                  "The specified key does not exist.",
                  "/#{bucket}/#{key}",
                  request_id
                )

              {:error, _reason} ->
                error_response(
                  conn,
                  "InternalError",
                  "Failed to fetch object from cloud backend.",
                  "/#{bucket}/#{key}",
                  request_id
                )
            end

          {:error, :not_found} ->
            error_response(
              conn,
              "NoSuchKey",
              "The specified key does not exist.",
              "/#{bucket}/#{key}",
              request_id
            )

          {:error, _reason} ->
            error_response(
              conn,
              "InternalError",
              "Failed to reach cloud backend.",
              "/#{bucket}/#{key}",
              request_id
            )
        end
    end
  end

  # GET: local (non-cloud) path — original implementation

  @impl true
  def put_object(conn, bucket, key, request_id) do
    {:ok, cloud_config} = CloudConfig.get_active_config(bucket)

    content_type =
      case get_req_header(conn, "content-type") do
        [ct | _] -> ct
        [] -> "application/octet-stream"
      end

    custom_metadata = extract_custom_metadata(conn)

    # Buffer the body (needed to compute hash and send to cloud)
    try do
      case read_full_body(conn) do
        {:ok, raw_body, _conn} ->
          case maybe_decode_aws_chunked(conn, raw_body) do
            {:error, :malformed_chunked} ->
              error_response(
                conn,
                "InvalidRequest",
                "The aws-chunked request body is malformed.",
                "/#{bucket}/#{key}",
                request_id
              )

            body ->
              put_decoded_object(
                conn,
                bucket,
                key,
                body,
                cloud_config,
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

  defp put_decoded_object(
         conn,
         bucket,
         key,
         body,
         cloud_config,
         content_type,
         custom_metadata,
         request_id
       ) do
    content_hash = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
    md5 = :crypto.hash(:md5, body)
    etag = Base.encode16(md5, case: :lower)
    size = byte_size(body)

    case CloudClient.put_object(cloud_config, key, body, content_type, custom_metadata) do
      :ok ->
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        meta = %{
          content_hash: content_hash,
          size: size,
          etag: etag,
          content_type: content_type,
          metadata: custom_metadata,
          created_at: now,
          updated_at: now,
          cloud_backed: true
        }

        Metadata.put_object_meta(bucket, key, meta)

        # Write-through: also cache locally if caching is enabled
        if cloud_config.cache_enabled do
          LocalStore.put(bucket, key, body, cloud_config)
        end

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
  end

  # PUT: local (non-cloud) path — original implementation
end
