defmodule ExStorageServiceS3.Handlers.Object do
  @moduledoc false

  import Plug.Conn
  require Logger
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.Handlers.Object.Backend
  alias ExStorageServiceS3.XML
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.Metadata
  alias ExStorageService.ObjectService

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
        storage_error_response(conn, reason, "/#{bucket}", request_id)

      :ok ->
        params = conn.query_params

        case ExStorageServiceS3.Listing.options(params) do
          {:ok, opts} -> Backend.for_bucket(bucket).list_objects(conn, bucket, opts, request_id)
          {:error, reason} -> storage_error_response(conn, reason, "/#{bucket}", request_id)
        end
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
      case cloud_cache_config(bucket) do
        {:ok, _config} ->
          ExStorageServiceS3.Handlers.Object.CloudBackend.head_object(
            conn,
            bucket,
            key,
            request_id
          )

        :disabled ->
          hosted_head_object(conn, bucket, key, request_id)
      end
    end)
  end

  defp hosted_head_object(conn, bucket, key, request_id) do
    case ObjectService.head(bucket, key, conn.query_params["versionId"], []) do
      {:ok, %{delete_marker: true} = result} ->
        delete_marker_response(conn, result, request_id)

      {:ok, %{metadata: metadata}} ->
        head_object_response(conn, metadata, request_id)

      {:error, :version_not_found} ->
        conn |> put_s3_headers(request_id) |> send_resp(404, "")

      {:error, :object_not_found} ->
        conn |> put_s3_headers(request_id) |> send_resp(404, "")

      {:error, :bucket_not_found} ->
        conn |> put_s3_headers(request_id) |> send_resp(404, "")

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
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
          storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)

        :ok ->
          Backend.for_bucket(bucket).put_object(conn, bucket, key, request_id)
      end
    end)
  end

  def delete_object(conn, bucket, key) do
    request_id = request_id(conn)

    ExStorageService.Telemetry.span(:delete_object, %{bucket: bucket, key: key}, fn ->
      version_id = conn.query_params["versionId"]
      entry = %{key: key, version_id: version_id}

      result = delete_entry(cloud_cache_config(bucket), bucket, entry)

      case result do
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

        {:error, :unsupported_gateway_version} ->
          error_response(
            conn,
            "NotImplemented",
            "Explicit version deletion is unavailable for gateway buckets.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, {:upstream, reason}} ->
          {code, message} = gateway_delete_error(reason)
          error_response(conn, code, message, "/#{bucket}/#{key}", request_id)

        {:error, reason} ->
          storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
      end
    end)
  end

  def copy_object(conn, bucket, key) do
    request_id = request_id(conn)

    case get_req_header(conn, "x-amz-copy-source") do
      [copy_source | _] ->
        source = conn.assigns.s3_operation.copy_source
        source_bucket = source.bucket
        source_key = source.key

        case ExStorageServiceS3.Handlers.Object.CloudCopy.head_source(source) do
          {:ok, %{metadata: source_metadata, delete_marker: false}} ->
            now = DateTime.utc_now() |> DateTime.to_iso8601()

            result =
              with :ok <- copy_source_conditions(conn, source_metadata) do
                perform_copy(
                  source_bucket,
                  source_key,
                  bucket,
                  key,
                  source_metadata,
                  cloud_cache_config(bucket),
                  conn
                )
              end

            case result do
              {:ok, %{metadata: destination_metadata} = publication} ->
                body =
                  XML.copy_object_response(
                    "\"#{Map.get(destination_metadata, :etag, source_metadata.etag)}\"",
                    now,
                    destination_metadata
                  )

                conn
                |> put_write_version_header(publication)
                |> maybe_put_copy_source_version_header(Map.get(source_metadata, :version_id))
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
                storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
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
            storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
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

    with :ok <- require_delete_checksum(conn),
         {:ok, body, conn} <- read_full_body(conn, 8 * 1024 * 1024),
         {:ok, %{objects: objects, quiet: quiet}} <- parse_delete_objects_xml(body) do
      cloud_cfg = cloud_cache_config(bucket)
      results = Enum.map(objects, &delete_authorized_entry(conn, bucket, &1, cloud_cfg))
      body = XML.delete_objects_response(results, quiet: quiet)
      xml_response(conn, 200, body, request_id)
    else
      {:error, :missing_checksum} ->
        error_response(
          conn,
          "InvalidRequest",
          "DeleteObjects requires Content-MD5 or a supported checksum.",
          "/#{bucket}?delete",
          request_id
        )

      {:error, :malformed_xml} ->
        error_response(
          conn,
          "MalformedXML",
          "The XML you provided was not well-formed.",
          "/#{bucket}?delete",
          request_id
        )

      {:error, :invalid_delete} ->
        error_response(
          conn,
          "InvalidArgument",
          "DeleteObjects requires between 1 and 1000 valid object entries.",
          "/#{bucket}?delete",
          request_id
        )

      {:error, :entity_too_large} ->
        error_response(
          conn,
          "EntityTooLarge",
          "The request body exceeds the control document limit.",
          "/#{bucket}?delete",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}?delete", request_id)
    end
  end

  defp require_delete_checksum(conn) do
    names =
      ~w(content-md5 x-amz-checksum-crc32 x-amz-checksum-crc32c x-amz-checksum-crc64nvme x-amz-checksum-sha1 x-amz-checksum-sha256)

    if Enum.any?(conn.req_headers, fn {name, _value} -> name in names end),
      do: :ok,
      else: {:error, :missing_checksum}
  end

  ## Versioning handlers

  def get_object_version(conn, bucket, key, version_id) do
    request_id = request_id(conn)

    case ObjectService.head(bucket, key, version_id, []) do
      {:ok, %{delete_marker: true} = result} ->
        delete_marker_response(conn, result, request_id)

      {:ok, %{metadata: metadata}} ->
        etag = "\"#{Map.get(metadata, :etag, "")}\""
        last_modified_raw = Map.get(metadata, :updated_at, Map.get(metadata, :created_at))
        last_modified = format_http_date(last_modified_raw)

        case ExStorageServiceS3.Plugs.ReadResponse.condition(conn, etag, last_modified_raw) do
          :failed ->
            storage_error_response(conn, :precondition_failed, "/#{bucket}/#{key}", request_id)

          :not_modified ->
            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("etag", etag)
            |> put_resp_header("last-modified", last_modified)
            |> put_resp_header("x-amz-version-id", version_id)
            |> send_resp(304, "")

          :ok ->
            send_object_version(
              conn,
              bucket,
              key,
              version_id,
              metadata,
              etag,
              last_modified,
              request_id
            )
        end

      {:error, :version_not_found} ->
        error_response(
          conn,
          "NoSuchVersion",
          "The specified version does not exist.",
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
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  defp send_object_version(
         conn,
         bucket,
         key,
         version_id,
         metadata,
         etag,
         last_modified,
         request_id
       ) do
    size = Map.get(metadata, :size, 0)

    case version_range(conn, size) do
      {:ok, range, status, headers} ->
        case ObjectService.open_source(metadata,
               bucket: bucket,
               range: range,
               request_id: request_id
             ) do
          {:ok, source} ->
            content_type = Map.get(metadata, :content_type, "application/octet-stream")

            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("content-type", content_type)
            |> put_resp_header("etag", etag)
            |> put_resp_header("last-modified", last_modified)
            |> put_resp_header("content-length", to_string(source_length(source)))
            |> put_resp_header("accept-ranges", "bytes")
            |> put_resp_header("x-amz-version-id", version_id)
            |> put_custom_metadata_headers(metadata)
            |> put_version_range_headers(headers)
            |> then(fn current ->
              if status == 206, do: omit_object_checksum_headers(current), else: current
            end)
            |> send_blob_source(status, source, request_id: request_id)

          {:error, :blob_not_found} ->
            error_response(
              conn,
              "InternalError",
              "Content file missing",
              "/#{bucket}/#{key}",
              request_id
            )

          {:error, reason} ->
            storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
        end

      {:error, :invalid_range} ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("content-range", "bytes */#{size}")
        |> send_resp(416, "")
    end
  end

  defp cloud_cache_config(bucket) do
    CloudConfig.get_active_config(bucket)
  end

  defp delete_authorized_entry(conn, bucket, entry, cloud_config) do
    alias ExStorageServiceS3.Plugs.Authorize
    action = if entry.version_id, do: "s3:DeleteObjectVersion", else: "s3:DeleteObject"

    if Authorize.allowed?(
         conn,
         action,
         Authorize.resource_arn(bucket, entry.key),
         "s3:DeleteObject"
       ) do
      with {:ok, result} <- delete_entry(cloud_config, bucket, entry) do
        {:deleted,
         %{
           key: entry.key,
           version_id: entry.version_id,
           delete_marker: result.kind == :delete_marker,
           delete_marker_version_id: if(result.kind == :delete_marker, do: result.version_id)
         }}
      else
        {:error, :unsupported_gateway_version} ->
          delete_entry_error(
            entry,
            "NotImplemented",
            "Explicit version deletion is unavailable for gateway buckets."
          )

        {:error, {:upstream, reason}} ->
          {code, message} = gateway_delete_error(reason)
          delete_entry_error(entry, code, message)

        {:error, _reason} ->
          delete_entry_error(entry, "InternalError", "The object could not be deleted.")
      end
    else
      delete_entry_error(entry, "AccessDenied", "Access Denied")
    end
  end

  defp gateway_delete_error({:http_error, status}) when status in [401, 403],
    do: {"AccessDenied", "The upstream storage service denied the request."}

  defp gateway_delete_error({:http_error, 429}),
    do: {"SlowDown", "The upstream storage service is throttling requests."}

  defp gateway_delete_error({:http_error, 412}),
    do: {"PreconditionFailed", "The upstream object changed during deletion."}

  defp gateway_delete_error({:upstream_unknown, reason}), do: gateway_delete_error(reason)

  defp gateway_delete_error(_reason),
    do: {"ServiceUnavailable", "The upstream object could not be deleted."}

  defp delete_entry_error(entry, code, message),
    do: {:error, Map.merge(entry, %{code: code, message: message})}

  defp delete_entry({:ok, _config}, _bucket, %{version_id: version_id})
       when not is_nil(version_id),
       do: {:error, :unsupported_gateway_version}

  defp delete_entry({:ok, cloud_config}, bucket, entry) do
    operation_id = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

    case ExStorageService.CloudCache.DeleteThrough.delete(
           bucket,
           entry.key,
           cloud_config,
           operation_id
         ) do
      :ok -> {:ok, %{version_id: "null", kind: :deleted}}
      {:error, reason} -> {:error, {:upstream, reason}}
    end
  end

  defp delete_entry(:disabled, bucket, entry),
    do: ObjectService.delete(bucket, entry.key, entry.version_id)

  defp head_object_response(conn, metadata, request_id) do
    conn = put_object_version_header(conn, metadata)
    content_type = Map.get(metadata, :content_type, "application/octet-stream")
    quoted_etag = "\"#{Map.get(metadata, :etag, "")}\""
    size = Map.get(metadata, :size, 0)
    last_modified_raw = Map.get(metadata, :updated_at, Map.get(metadata, :created_at))
    last_modified = format_http_date(last_modified_raw)

    case ExStorageServiceS3.Plugs.ReadResponse.condition(conn, quoted_etag, last_modified_raw) do
      :failed ->
        storage_error_response(conn, :precondition_failed, conn.request_path, request_id)

      :not_modified ->
        conn
        |> put_s3_headers(request_id)
        |> put_resp_header("etag", quoted_etag)
        |> put_resp_header("last-modified", last_modified)
        |> send_resp(304, "")

      :ok ->
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

  defp maybe_put_copy_source_version_header(conn, nil), do: conn

  defp maybe_put_copy_source_version_header(conn, version_id),
    do: put_resp_header(conn, "x-amz-copy-source-version-id", version_id)

  defp version_range(conn, size) do
    case get_req_header(conn, "range") do
      [] ->
        {:ok, nil, 200, []}

      [range_header] ->
        case parse_range(range_header, size) do
          {:ok, offset, length} ->
            {:ok, {offset, length}, 206,
             [{"content-range", "bytes #{offset}-#{offset + length - 1}/#{size}"}]}

          {:error, :invalid_range} = error ->
            error
        end

      _duplicate ->
        {:error, :invalid_range}
    end
  end

  defp put_version_range_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, current ->
      put_resp_header(current, name, value)
    end)
  end

  defp source_length({:file, _path, _offset, length}), do: length
  defp source_length({:stream, _producer, length}), do: length

  @doc false
  def copy_source_conditions(conn, metadata) do
    etag = Map.get(metadata, :etag, "")
    modified = Map.get(metadata, :updated_at, Map.get(metadata, :created_at))
    match = get_req_header(conn, "x-amz-copy-source-if-match")
    none_match = get_req_header(conn, "x-amz-copy-source-if-none-match")

    cond do
      match != [] and not copy_etag_matches?(match, etag) ->
        {:error, :precondition_failed}

      none_match != [] and copy_etag_matches?(none_match, etag) ->
        {:error, :precondition_failed}

      match == [] and
          copy_date_fails?(conn, "x-amz-copy-source-if-unmodified-since", modified, :gt) ->
        {:error, :precondition_failed}

      none_match == [] and
          copy_date_fails?(conn, "x-amz-copy-source-if-modified-since", modified, :not_gt) ->
        {:error, :precondition_failed}

      true ->
        :ok
    end
  end

  defp copy_etag_matches?([value], etag) do
    value
    |> String.split(",")
    |> Enum.any?(fn candidate -> String.trim(candidate) in ["*", "\"#{etag}\""] end)
  end

  defp copy_etag_matches?(_, _), do: false

  defp copy_date_fails?(conn, header, modified, comparison) do
    with [value] <- get_req_header(conn, header),
         {:ok, condition_date} <- parse_http_date(value),
         {:ok, modified_date} <- parse_object_datetime(modified) do
      # HTTP dates have whole-second precision.
      result = DateTime.compare(DateTime.truncate(modified_date, :second), condition_date)
      if comparison == :gt, do: result == :gt, else: result != :gt
    else
      _ -> false
    end
  end

  defp copy_attributes(conn, source_metadata) do
    with {:ok, metadata_directive} <- copy_directive(conn, "x-amz-metadata-directive"),
         {:ok, tagging_directive} <- copy_directive(conn, "x-amz-tagging-directive"),
         {:ok, replacement} <- object_attributes(conn) do
      names = [
        :content_type,
        :metadata,
        :user_metadata,
        :system_headers,
        :content_encoding,
        :cache_control,
        :content_disposition,
        :content_language,
        :expires,
        :tags,
        :checksums,
        :checksum_type
      ]

      copied = Map.take(source_metadata, names)

      attributes =
        if metadata_directive == "REPLACE" do
          copied
          |> Map.merge(
            Map.new(
              [
                :content_encoding,
                :cache_control,
                :content_disposition,
                :content_language,
                :expires
              ],
              &{&1, nil}
            )
          )
          |> Map.merge(Map.delete(replacement, :tags))
        else
          copied
        end

      attributes =
        Map.put(
          attributes,
          :tags,
          if(tagging_directive == "REPLACE",
            do: replacement.tags,
            else: Map.get(source_metadata, :tags, %{})
          )
        )

      source = conn.assigns.s3_operation.copy_source

      with :ok <-
             ExStorageServiceS3.Plugs.Authorize.authorize_copy_tags(
               conn,
               conn.assigns.s3_operation.bucket,
               conn.assigns.s3_operation.key,
               attributes.tags
             ) do
        ExStorageServiceS3.Handlers.Object.CloudCopy.with_source(
          source,
          source_metadata,
          nil,
          fn pinned ->
            ExStorageServiceS3.Handlers.Object.CloudCopy.copy_attributes_from_source(
              pinned,
              attributes,
              source_metadata,
              checksum_algorithm: List.first(get_req_header(conn, "x-amz-checksum-algorithm"))
            )
          end
        )
      end
    end
  end

  defp copy_directive(conn, header) do
    case get_req_header(conn, header) do
      [] -> {:ok, "COPY"}
      [value] when value in ["COPY", "REPLACE"] -> {:ok, value}
      _ -> {:error, :invalid_request}
    end
  end

  defp perform_copy(
         source_bucket,
         source_key,
         destination_bucket,
         destination_key,
         source_metadata,
         destination_config,
         conn
       ) do
    with {:ok, attributes} <- copy_attributes(conn, source_metadata) do
      if destination_config == :disabled and not Map.has_key?(source_metadata, :gateway_identity) do
        ObjectService.copy(source_bucket, source_key, destination_bucket, destination_key,
          source_version_id: Map.get(source_metadata, :version_id),
          source_metadata: source_metadata,
          attributes: attributes
        )
      else
        source = %{bucket: source_bucket, key: source_key, version_id: nil}

        operation_id = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

        ExStorageServiceS3.Handlers.Object.CloudCopy.copy(
          source,
          destination_bucket,
          destination_key,
          source_metadata,
          attributes,
          operation_id
        )
      end
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
      {doc, remaining} = :xmerl_scan.string(String.to_charlist(xml_body), quiet: true)
      entries = :xmerl_xpath.string(~c"/Delete/Object", doc)
      root = :xmerl_xpath.string(~c"/Delete", doc)
      unknown = :xmerl_xpath.string(~c"/Delete/*[not(self::Object or self::Quiet)]", doc)
      quiet_values = xml_text_values(~c"/Delete/Quiet/text()", doc)

      with true <- root != [] and unknown == [] and String.trim(to_string(remaining)) == "",
           true <- length(entries) in 1..1000,
           {:ok, quiet} <- parse_delete_quiet(quiet_values),
           {:ok, objects} <- parse_delete_entries(entries) do
        {:ok, %{objects: objects, quiet: quiet}}
      else
        _ -> {:error, :invalid_delete}
      end
    rescue
      _ -> {:error, :malformed_xml}
    catch
      :exit, _ -> {:error, :malformed_xml}
    end
  end

  defp parse_delete_quiet([]), do: {:ok, false}
  defp parse_delete_quiet(["false"]), do: {:ok, false}
  defp parse_delete_quiet(["true"]), do: {:ok, true}
  defp parse_delete_quiet(_), do: {:error, :invalid_delete}

  defp parse_delete_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, objects} ->
      keys = xml_text_values(~c"Key/text()", entry)
      versions = xml_text_values(~c"VersionId/text()", entry)
      unknown = :xmerl_xpath.string(~c"*[not(self::Key or self::VersionId)]", entry)

      case {keys, versions, unknown} do
        {[key], versions, []} when byte_size(key) in 1..1024 ->
          case versions do
            [] ->
              {:cont, {:ok, [%{key: key, version_id: nil} | objects]}}

            [version] when byte_size(version) > 0 ->
              {:cont, {:ok, [%{key: key, version_id: version} | objects]}}

            _ ->
              {:halt, {:error, :invalid_delete}}
          end

        _ ->
          {:halt, {:error, :invalid_delete}}
      end
    end)
    |> case do
      {:ok, objects} -> {:ok, Enum.reverse(objects)}
      error -> error
    end
  end

  defp xml_text_values(path, doc) do
    :xmerl_xpath.string(path, doc)
    |> Enum.map(fn {:xmlText, _, _, _, value, _} -> to_string(value) end)
  end
end
