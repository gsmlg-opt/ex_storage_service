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
      continuation_token: Keyword.get(opts, :continuation_token),
      start_after: if(opts[:list_type] == 1, do: opts[:marker], else: opts[:start_after])
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

        page = Map.put(result, :is_truncated, result.truncated)

        next =
          if opts[:list_type] == 1 and result.truncated do
            Enum.max(Enum.map(result.keys, &elem(&1, 0)) ++ result.common_prefixes, fn -> nil end)
          else
            result.next_continuation_token
          end

        response_opts = ExStorageServiceS3.Listing.response_options(opts, page, next)

        body = XML.list_objects_response(bucket, objects, response_opts)
        xml_response(conn, 200, body, request_id)

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id)
    end
  end

  # Upstream is authoritative: every read revalidates, including warm cache hits.
  @impl true
  def get_object(conn, bucket, key, request_id),
    do: read_object(conn, bucket, key, request_id, false)

  def head_object(conn, bucket, key, request_id),
    do: read_object(conn, bucket, key, request_id, true)

  defp read_object(conn, bucket, key, request_id, head?) do
    with {:ok, metadata} <- Metadata.get_bucket(bucket),
         generation = ExStorageService.Metadata.BucketLifecycle.generation(metadata),
         :ok <- Metadata.check_object_mutation(bucket, bucket_generation: generation) do
      conn = put_private(conn, :ess_cache_bucket_generation, generation)
      read_ready_object(conn, bucket, key, request_id, head?)
    else
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
    end
  end

  defp read_ready_object(conn, bucket, key, request_id, head?) do
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

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)

      :not_found ->
        with {:ok, config} <- CloudConfig.get_active_config(bucket),
             {:ok, meta} <- CloudClient.head_object(config, key),
             :ok <- valid_remote_metadata(meta) do
          conn = remote_headers(conn, meta, request_id)

          case read_condition(conn, meta) do
            :not_modified ->
              conn |> delete_resp_header("content-length") |> send_resp(304, "")

            :failed ->
              storage_error_response(conn, :precondition_failed, "/#{bucket}/#{key}", request_id)

            :ok when head? ->
              send_resp(conn, 200, "")

            :ok ->
              serve_remote(conn, bucket, key, config, meta, request_id)
          end
        else
          {:error, :not_found} ->
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
  end

  defp valid_remote_metadata(%{etag: etag, content_length: size})
       when is_binary(etag) and etag != "" and is_integer(size) and size >= 0,
       do: :ok

  defp valid_remote_metadata(_), do: {:error, :invalid_upstream_metadata}

  defp serve_remote(conn, bucket, key, config, meta, request_id) do
    validator = remote_validator(config, meta)

    case LocalStore.get_validated(bucket, key, validator) do
      {:ok, path} ->
        case File.open(path, [:read, :raw, :binary]) do
          {:ok, io} ->
            ExStorageService.Telemetry.cache(:read, :hit)

            try do
              serve_open(conn, io, meta, request_id)
            after
              File.close(io)
            end

          {:error, :enoent} ->
            ExStorageService.Telemetry.cache(:read, :miss)
            fetch_remote(conn, bucket, key, config, meta, request_id)

          {:error, reason} ->
            ExStorageService.Telemetry.cache(:read, :error)
            storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
        end

      :miss ->
        ExStorageService.Telemetry.cache(:read, :miss)
        fetch_remote(conn, bucket, key, config, meta, request_id)
    end
  end

  defp fetch_remote(conn, bucket, key, config, observed, request_id) do
    case CloudClient.get_file(config, key, if_match: quote_etag(observed.etag)) do
      {:ok, staged, meta} ->
        try do
          with :ok <- valid_remote_metadata(meta),
               true <- same_remote_object?(observed, meta),
               {:ok, io} <- File.open(staged.path, [:read, :raw, :binary]) do
            # Pin the stage before cache publication/eviction. Cache failure must not
            # publish incomplete bytes or prevent serving this verified download.
            try do
              if config.cache_enabled do
                LocalStore.put_file(bucket, key, staged.path, staged.hash, staged.size, config,
                  validator: remote_validator(config, meta),
                  bucket_generation: conn.private.ess_cache_bucket_generation
                )
              end

              conn |> remote_headers(meta, request_id) |> serve_open(io, meta, request_id)
            after
              File.close(io)
            end
          else
            false ->
              storage_error_response(conn, :upstream_changed, "/#{bucket}/#{key}", request_id)

            {:error, reason} ->
              storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
          end
        after
          ExStorageService.BlobStore.LocalCAS.discard(staged)
        end

      {:error, {:http_error, 412}} ->
        storage_error_response(conn, :upstream_changed, "/#{bucket}/#{key}", request_id)

      {:error, :not_found} ->
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

  defp same_remote_object?(left, right),
    do:
      left.etag == right.etag and left.content_length == right.content_length and
        left.version_id == right.version_id

  defp remote_validator(config, meta) do
    {CloudConfig.endpoint_url(config), config.bucket, meta}
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp remote_headers(conn, meta, request_id) do
    conn =
      conn
      |> put_s3_headers(request_id)
      |> put_resp_header("content-type", meta.content_type)
      |> put_resp_header("etag", quote_etag(meta.etag))
      |> put_resp_header("content-length", Integer.to_string(meta.content_length))
      |> put_resp_header("accept-ranges", "bytes")

    conn =
      if meta.last_modified,
        do: put_resp_header(conn, "last-modified", meta.last_modified),
        else: conn

    conn =
      Enum.reduce(meta.headers, conn, fn {name, value}, acc ->
        put_resp_header(acc, name, value)
      end)

    Enum.reduce(meta.metadata, conn, fn {name, value}, acc ->
      put_resp_header(acc, "x-amz-meta-" <> name, value)
    end)
  end

  defp serve_open(conn, io, meta, request_id) do
    range =
      case get_req_header(conn, "range") do
        [] -> {:ok, 0, meta.content_length}
        [value | _] -> parse_range(value, meta.content_length)
      end

    case range do
      {:ok, offset, length} ->
        conn =
          if get_req_header(conn, "range") != [] do
            put_resp_header(
              conn,
              "content-range",
              "bytes #{offset}-#{offset + length - 1}/#{meta.content_length}"
            )
          else
            conn
          end

        status = if get_req_header(conn, "range") == [], do: 200, else: 206

        source =
          ExStorageService.BlobStore.Source.stateful_stream(
            fn acc, reducer ->
              read_open(io, offset, length, acc, reducer)
            end,
            length
          )

        conn = if status == 206, do: omit_object_checksum_headers(conn), else: conn

        conn
        |> put_resp_header("content-length", Integer.to_string(length))
        |> send_blob_source(status, source, request_id: request_id)

      {:error, :invalid_range} ->
        conn
        |> put_resp_header("content-range", "bytes */#{meta.content_length}")
        |> delete_resp_header("content-length")
        |> send_resp(416, "")
    end
  end

  defp read_open(_io, _offset, 0, acc, _reducer), do: {:ok, acc}

  defp read_open(io, offset, remaining, acc, reducer) do
    case :file.pread(io, offset, min(remaining, 65_536)) do
      {:ok, bytes} ->
        case reducer.(bytes, acc) do
          {:cont, next} ->
            read_open(io, offset + byte_size(bytes), remaining - byte_size(bytes), next, reducer)

          {:halt, reason, next} ->
            {:error, reason, next}
        end

      :eof ->
        {:error, :unexpected_eof, acc}

      {:error, reason} ->
        {:error, reason, acc}
    end
  end

  defp read_condition(conn, meta) do
    modified =
      case parse_http_date(meta.last_modified || "") do
        {:ok, date} -> DateTime.to_iso8601(date)
        _ -> nil
      end

    ExStorageServiceS3.Plugs.ReadResponse.condition(conn, quote_etag(meta.etag), modified)
  end

  defp quote_etag(etag), do: "\"" <> String.trim(etag, "\"") <> "\""

  @impl true
  def put_object(conn, bucket, key, request_id) do
    if cluster_mode?() do
      error_response(
        conn,
        "ServiceUnavailable",
        "Cloud-backed writes are unavailable in active cluster mode.",
        "/#{bucket}/#{key}",
        request_id
      )
    else
      case Metadata.check_object_mutation(bucket) do
        :ok -> put_standalone_object(conn, bucket, key, request_id)
        {:error, reason} -> storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
      end
    end
  end

  defp put_standalone_object(conn, bucket, key, request_id) do
    {:ok, cloud_config} = CloudConfig.get_active_config(bucket)
    content_type = List.first(get_req_header(conn, "content-type")) || "application/octet-stream"

    generation =
      case Metadata.get_bucket(bucket) do
        {:ok, meta} -> ExStorageService.Metadata.BucketLifecycle.generation(meta)
        _ -> :missing
      end

    {reader, initial} = decoded_body_reader(conn)

    context = Map.get(conn.assigns, :ess_context)

    stage_opts =
      case context do
        %ExStorageService.Context{} ->
          ExStorageService.Context.blob_store_options(context)

        nil ->
          {:ok, context} = ExStorageService.Context.default()
          ExStorageService.Context.blob_store_options(context)
      end

    case ExStorageService.BlobStore.LocalCAS.stage_from_reader(reader, initial, stage_opts) do
      {:ok, staged, final_state} ->
        conn = decoded_body_reader_conn(final_state)
        # Remove transfer framing only; preserve genuine object encodings.
        headers =
          for name <-
                ~w(content-encoding cache-control content-disposition content-language expires),
              value <- get_req_header(conn, name),
              normalized =
                if(name == "content-encoding",
                  do:
                    value
                    |> String.split(",")
                    |> Enum.map(&String.trim/1)
                    |> Enum.reject(&(String.downcase(&1) == "aws-chunked"))
                    |> Enum.join(","),
                  else: value
                ),
              normalized != "",
              do: {name, normalized}

        checksum_headers =
          Enum.map(final_state.checksums, fn
            {"md5", value} -> {"content-md5", value}
            {name, value} -> {"x-amz-checksum-" <> name, value}
          end)

        attributes = %{
          content_type: content_type,
          metadata: extract_custom_metadata(conn),
          headers: headers ++ checksum_headers,
          checksums: final_state.checksums
        }

        case ExStorageService.CloudCache.WriteThrough.put(
               bucket,
               key,
               staged,
               cloud_config,
               attributes,
               request_id,
               bucket_generation: generation,
               blob_store_opts: stage_opts
             ) do
          {:ok, %{etag: etag}} ->
            Hooks.after_put(bucket, key)
            broadcast_bucket_change(bucket, :put, key)

            conn
            |> put_s3_headers(request_id)
            |> put_resp_header("etag", "\"#{etag}\"")
            |> ExStorageServiceS3.MultipartChecksums.response_headers(final_state.checksums)
            |> send_resp(200, "")

          {:error, reason} ->
            storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
        end

      {:error, :entity_too_large, final_state} ->
        error_response(
          decoded_body_reader_conn(final_state),
          "EntityTooLarge",
          "Your proposed upload exceeds the maximum allowed object size.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason, final_state} ->
        storage_error_response(
          decoded_body_reader_conn(final_state),
          reason,
          "/#{bucket}/#{key}",
          request_id
        )
    end
  end

  defp cluster_mode? do
    case Application.get_env(:ex_storage_service, :instance_config, []) do
      %{mode: :cluster} -> true
      config when is_list(config) -> Keyword.get(config, :mode, :standalone) == :cluster
      _other -> false
    end
  end

  # PUT: local (non-cloud) path — original implementation
end
