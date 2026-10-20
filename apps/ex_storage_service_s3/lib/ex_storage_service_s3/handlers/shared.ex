defmodule ExStorageServiceS3.Handlers.Shared do
  @moduledoc false

  import Plug.Conn
  require Logger
  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Versioning
  alias ExStorageServiceS3.XML

  @storage_error_atoms ~w(
    stage staging_protection verify commit stat open read close sync rename directory_sync discard
    metadata file_open file_read file_position invalid_file_offset invalid_hash invalid_staged_blob
    existing_blob_mismatch checksum_mismatch source_checksum_mismatch unexpected_eof cross_device
    operation_intent_conflict operation_intent_not_found operation_intent_before_blob_retirement
    invalid_operation_intent stale_gc_lock
    eacces eagain ebadf ebusy edquot eexist efbig eintr einval eio eisdir eloop emfile enfile enodev
    enoent enomem enospc enosys enotdir enotempty enotsup enxio eperm erofs espipe estale exdev
  )a

  def request_id(conn) do
    conn.assigns[:request_id] || :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  def put_s3_headers(conn, request_id) do
    conn
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("x-amz-bucket-region", ExStorageServiceS3.Handlers.Location.region())
    |> put_resp_header("server", "ExStorageService")
  end

  def xml_response(conn, status, body, request_id) do
    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
  end

  # The caller may reject a request before reading any body (including before
  # an Expect/continue client sends it). Draining then can consume the next
  # request on a reused connection. Closing is bounded even for huge bodies.
  defp close_error_body_connection(conn) do
    body_declared? =
      get_req_header(conn, "transfer-encoding") != [] or
        Enum.any?(get_req_header(conn, "content-length"), &(&1 != "0"))

    if body_declared? and get_http_protocol(conn) in [:"HTTP/1.0", :"HTTP/1.1"] do
      put_resp_header(conn, "connection", "close")
    else
      conn
    end
  end

  def error_response(conn, code, message, resource, request_id) do
    ExStorageService.Telemetry.s3_error(code)
    status = XML.error_status_code(code)
    message = if code == "InternalError", do: "An internal server error occurred.", else: message

    body =
      if conn.method == "HEAD",
        do: "",
        else: XML.error_response(code, message, resource, request_id)

    conn
    |> omit_object_payload_headers()
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> close_error_body_connection()
    |> send_resp(status, body)
  end

  # A failed read may already carry headers from an object HEAD. They describe
  # object bytes, not the XML error document. Retain version/delete-marker and
  # request diagnostics, including Last-Modified on explicit marker errors.
  defp omit_object_payload_headers(conn) do
    Enum.reduce(conn.resp_headers, conn, fn {name, _value}, current ->
      if name in [
           "content-length",
           "content-encoding",
           "content-range",
           "content-disposition",
           "content-language",
           "cache-control",
           "expires",
           "etag"
         ] or String.starts_with?(name, ["x-amz-checksum-", "x-amz-meta-"]) do
        delete_resp_header(current, name)
      else
        current
      end
    end)
  end

  def storage_error_response(conn, reason, resource, request_id) do
    if availability_error?(reason) do
      error_response(
        conn,
        "ServiceUnavailable",
        "The storage service is temporarily unavailable.",
        resource,
        request_id
      )
    else
      case integrity_error(reason) do
        {code, message} ->
          error_response(conn, code, message, resource, request_id)

        nil ->
          Logger.error(fn ->
            "S3 storage operation failed: #{inspect(storage_error_shape(reason, 0))}"
          end)

          error_response(
            conn,
            "InternalError",
            "An internal storage error occurred.",
            resource,
            request_id
          )
      end
    end
  end

  defp storage_error_shape(reason, _depth) when reason in @storage_error_atoms, do: reason

  defp storage_error_shape(reason, depth)
       when is_tuple(reason) and tuple_size(reason) <= 3 and depth < 2 do
    reason
    |> Tuple.to_list()
    |> Enum.map(&storage_error_shape(&1, depth + 1))
    |> List.to_tuple()
  end

  defp storage_error_shape(_reason, _depth), do: :redacted

  @doc false
  def omit_object_checksum_headers(conn) do
    Enum.reduce(conn.resp_headers, conn, fn {name, _}, current ->
      if String.starts_with?(name, "x-amz-checksum-"),
        do: delete_resp_header(current, name),
        else: current
    end)
  end

  @doc false
  def send_blob_source(conn, status, {:file, _path, _offset, 0}, _opts),
    do: send_resp(conn, status, "")

  def send_blob_source(conn, status, {:file, path, offset, length}, _opts) do
    result = send_file(conn, status, path, offset, length)
    ExStorageService.Telemetry.response_delivery(:file, length, :ok)
    result
  end

  def send_blob_source(conn, status, {:stream, _producer, 0}, _opts),
    do: send_resp(conn, status, "")

  def send_blob_source(conn, status, {:stream, _producer, length} = source, opts) do
    conn =
      conn
      |> put_resp_header("content-length", Integer.to_string(length))
      |> send_chunked(status)

    case Source.reduce(source, conn, fn data, current_conn ->
           case chunk(current_conn, data) do
             {:ok, next_conn} ->
               ExStorageService.Telemetry.response_delivery(:stream, IO.iodata_length(data), :ok)
               {:cont, next_conn}

             {:error, reason} ->
               {:halt, reason, current_conn}
           end
         end) do
      {:ok, final_conn} ->
        final_conn

      {:error, :closed, final_conn} ->
        ExStorageService.Telemetry.response_delivery(:stream, 0, :closed)
        put_private(final_conn, :ess_transfer_result, :error)

      {:error, {:sink, :closed}, final_conn} ->
        ExStorageService.Telemetry.response_delivery(:stream, 0, :closed)
        put_private(final_conn, :ess_transfer_result, :error)

      {:error, reason, _final_conn} ->
        ExStorageService.Telemetry.response_delivery(:stream, 0, :error)

        Logger.warning("blob response stream terminated",
          request_id: Keyword.get(opts, :request_id),
          reason: inspect(reason)
        )

        raise "blob response stream terminated after response headers"
    end
  end

  defp availability_error?(reason)
       when reason in [
              :blob_write_quorum_unavailable,
              :invalid_replica_ack,
              :metadata_quorum_unavailable,
              :listing_index_migration_required,
              :listing_index_unavailable,
              :listing_index_changed,
              :listing_index_corrupt,
              :corrupt_index,
              :insufficient_eligible_nodes,
              :cluster_data_plane_disabled,
              :no_leader,
              :cluster_not_ready,
              :all_blob_replicas_unavailable,
              :timeout,
              :compare_retry_exhausted,
              :operation_intent_compare_failed,
              :unknown,
              :read_idle_timeout,
              :write_idle_timeout,
              :request_timeout,
              :connect_timeout,
              :econnrefused,
              :nxdomain,
              :closed,
              :enetunreach,
              :ehostunreach,
              :upstream_changed,
              :invalid_upstream_metadata,
              :upstream_length_mismatch
            ],
       do: true

  defp availability_error?({:local_reconciliation_required, _reason}), do: true

  defp availability_error?(reason) when is_tuple(reason) do
    reason
    |> Tuple.to_list()
    |> Enum.any?(&availability_error?/1)
  end

  defp availability_error?(_reason), do: false

  defp integrity_error({:http_error, status}) when status in [401, 403],
    do: {"AccessDenied", "The upstream storage service denied the request."}

  defp integrity_error({:http_error, 404}),
    do: {"NoSuchKey", "The specified upstream key does not exist."}

  defp integrity_error({:http_error, status}) when status >= 500,
    do: {"ServiceUnavailable", "The upstream storage service is unavailable."}

  defp integrity_error({:http_error, 429}),
    do: {"SlowDown", "The upstream storage service is throttling requests."}

  defp integrity_error({:stage, reason}), do: integrity_error(reason)
  defp integrity_error({:metadata, reason}), do: integrity_error(reason)
  defp integrity_error({:upstream_unknown, reason}), do: integrity_error(reason)

  defp integrity_error(:access_denied), do: {"AccessDenied", "Access Denied"}

  defp integrity_error(:legacy_object_migration_required),
    do:
      {"InvalidRequest",
       "This legacy object requires the verified operator migration before mutation."}

  defp integrity_error(:invalid_tag), do: {"InvalidTag", "The tag set is invalid."}
  defp integrity_error(:invalid_request), do: {"InvalidRequest", "The request is invalid."}

  defp integrity_error(:bucket_not_found),
    do: {"NoSuchBucket", "The specified bucket does not exist."}

  defp integrity_error(:bucket_generation_changed),
    do: {"ConditionalRequestConflict", "The bucket identity changed during this operation."}

  defp integrity_error(reason)
       when reason in [:unsupported_gateway_version, :gateway_hosted_history_unsupported],
       do: {"NotImplemented", "The requested operation is unavailable for this gateway bucket."}

  defp integrity_error(:precondition_failed),
    do: {"PreconditionFailed", "At least one precondition failed."}

  defp integrity_error(:invalid_precondition),
    do: {"InvalidArgument", "The request precondition is invalid."}

  defp integrity_error(:invalid_pagination),
    do: {"InvalidArgument", "The pagination parameters are invalid."}

  defp integrity_error(:invalid_part),
    do: {"InvalidPart", "A requested part is missing or does not match its checksum."}

  defp integrity_error(:invalid_part_order),
    do: {"InvalidPartOrder", "Parts must be in ascending order."}

  defp integrity_error(:multipart_completion_conflict),
    do: {"ConditionalRequestConflict", "The multipart upload changed during completion."}

  defp integrity_error(:payload_hash_mismatch),
    do:
      {"XAmzContentSHA256Mismatch", "The provided payload hash does not match the received body."}

  defp integrity_error(:bad_digest),
    do: {"BadDigest", "The provided checksum does not match the received body."}

  defp integrity_error(:invalid_digest),
    do: {"InvalidDigest", "The checksum is malformed or missing."}

  defp integrity_error(:entity_too_large),
    do: {"EntityTooLarge", "The request body exceeds the allowed size."}

  defp integrity_error(:chunk_signature_mismatch),
    do: {"SignatureDoesNotMatch", "The chunk signature does not match."}

  defp integrity_error(reason)
       when reason in [
              :unsupported_payload_mode,
              :unsupported_checksum,
              :unsupported_checksum_type,
              :invalid_content_length,
              :malformed_chunked
            ],
       do:
         {"InvalidRequest",
          "The upload integrity or framing declaration is invalid or unsupported."}

  defp integrity_error(_reason), do: nil

  def put_object_version_header(conn, metadata) do
    case Map.get(metadata, :version_id) do
      id when is_binary(id) -> put_resp_header(conn, "x-amz-version-id", id)
      _ -> conn
    end
  end

  # The versioning policy is part of the committed outcome, including replay.
  # A later bucket-policy read could describe a different write than this one.
  def put_write_version_header(conn, %{version_id: "null", versioning_state: :disabled}),
    do: conn

  def put_write_version_header(conn, publication),
    do: put_object_version_header(conn, publication)

  def delete_marker_response(conn, %{metadata: metadata, version_id: version_id}, request_id) do
    explicit? = is_binary(conn.query_params["versionId"])

    conn =
      conn
      |> put_s3_headers(request_id)
      |> put_resp_header("x-amz-delete-marker", "true")
      |> put_resp_header("x-amz-version-id", version_id)

    conn =
      if explicit? do
        put_resp_header(conn, "last-modified", format_http_date(Map.get(metadata, :created_at)))
      else
        conn
      end

    send_resp(conn, if(explicit?, do: 405, else: 404), "")
  end

  def delete_marker_response(conn, version_id, request_id) do
    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("x-amz-delete-marker", "true")
    |> put_resp_header("x-amz-version-id", version_id)
    |> send_resp(404, "")
  end

  def latest_delete_marker(bucket, key) do
    case Metadata.head_bucket(bucket) do
      :ok ->
        case Versioning.get_version(bucket, key, nil) do
          {:ok, %{is_delete_marker: true, version_id: version_id}} -> {:ok, version_id}
          {:ok, _meta} -> :not_found
          {:error, :not_found} -> :not_found
          {:error, reason} -> {:error, reason}
        end

      {:error, :not_found} ->
        :no_such_bucket

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Returns true if the XML declares a DOCTYPE or custom ENTITY. xmerl expands
  # internal entities and may resolve external ones by default, exposing
  # entity-expansion (billion laughs) and XXE risks. Callers reject such bodies
  # before handing them to :xmerl_scan.
  def xml_has_doctype?(xml_body) when is_binary(xml_body) do
    downcased = String.downcase(xml_body)
    String.contains?(downcased, "<!doctype") or String.contains?(downcased, "<!entity")
  end

  def xpath_text(elem, path) do
    case :xmerl_xpath.string(path ++ ~c"/text()", elem) do
      [{:xmlText, _, _, _, value, _} | _] -> to_string(value)
      _ -> nil
    end
  end

  # Control requests are bounded independently of the object upload limit.
  def read_full_body(conn, max_size \\ 1_048_576, opts \\ []) do
    {reader, state} = decoded_body_reader(conn, max_size || 1_048_576, opts)
    collect_body(reader, state, [])
  end

  defp collect_body(reader, state, chunks) do
    case reader.(state) do
      {:more, chunk, state} ->
        collect_body(reader, state, [chunk | chunks])

      {:ok, chunk, state} ->
        {:ok, IO.iodata_to_binary(Enum.reverse([chunk | chunks])),
         decoded_body_reader_conn(state)}

      {:error, reason, _state} ->
        {:error, reason}
    end
  end

  def broadcast_bucket_change(bucket, action, key) do
    Phoenix.PubSub.broadcast(
      ExStorageService.PubSub,
      "bucket:#{bucket}",
      {:bucket_changed, %{action: action, key: key, bucket: bucket}}
    )
  end

  # Streaming body reader — yields chunks from the request body without
  # accumulating the entire object in memory. Enforces max_object_size
  # inline by throwing {:error, :entity_too_large} when the limit is exceeded.
  # The caller must catch this throw.
  def body_stream(conn, max_size \\ nil) do
    max =
      max_size ||
        Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)

    # 1 MiB read chunks — large enough for throughput, small enough for memory
    read_opts = [length: 1_048_576, read_timeout: 60_000]

    Stream.resource(
      fn -> {conn, 0} end,
      fn
        :done ->
          {:halt, :done}

        {conn, acc_size} ->
          case Plug.Conn.read_body(conn, read_opts) do
            {:ok, chunk, _conn} ->
              new_size = acc_size + byte_size(chunk)

              if exceeds_limit?(new_size, max) do
                throw({:error, :entity_too_large})
              end

              {[chunk], :done}

            {:more, chunk, conn} ->
              new_size = acc_size + byte_size(chunk)

              if exceeds_limit?(new_size, max) do
                throw({:error, :entity_too_large})
              end

              {[chunk], {conn, new_size}}

            {:error, reason} ->
              throw({:error, reason})
          end
      end,
      fn _ -> :ok end
    )
  end

  @doc false
  def decoded_body_stream(conn, max_size \\ nil) do
    {reader, initial} = decoded_body_reader(conn, max_size)

    Stream.resource(
      fn -> initial end,
      fn
        :done ->
          {:halt, :done}

        state ->
          case reader.(state) do
            {:more, bytes, next} -> {[bytes], next}
            {:ok, bytes, _next} -> {[bytes], :done}
            {:error, reason, _next} -> throw({:error, reason})
          end
      end,
      fn _ -> :ok end
    )
  end

  @doc false
  def decoded_body_reader(conn, max_size \\ nil, opts \\ []) do
    max =
      max_size ||
        Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)

    with {:ok, integrity} <- ExStorageServiceS3.Integrity.new(conn, max, opts),
         {:ok, decoder} <- request_decoder(conn) do
      {&read_decoded_body/1,
       %{conn: conn, decoder: decoder, integrity: integrity, checksums: %{}}}
    else
      {:error, reason} -> {&read_decoded_body/1, %{conn: conn, error: reason}}
    end
  end

  @doc false
  def decoded_body_reader_conn(%{conn: %Plug.Conn{} = conn}), do: conn

  defp request_decoder(conn) do
    if aws_chunked?(conn), do: ExStorageServiceS3.AwsChunked.new(conn), else: {:ok, nil}
  end

  defp read_decoded_body(%{error: reason} = state), do: {:error, reason, state}

  defp read_decoded_body(state) do
    case Plug.Conn.read_body(state.conn, body_read_opts()) do
      {disposition, bytes, conn} when disposition in [:ok, :more] ->
        state = %{state | conn: conn}

        with {:ok, decoded, decoder} <- decode_request_input(bytes, state.decoder),
             {:ok, integrity} <- ExStorageServiceS3.Integrity.update(state.integrity, decoded) do
          state = %{state | decoder: decoder, integrity: integrity}
          finalize_read(disposition, decoded, state)
        else
          {:error, reason} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp decode_request_input(bytes, nil), do: {:ok, bytes, nil}

  defp decode_request_input(bytes, decoder),
    do: ExStorageServiceS3.AwsChunked.feed(bytes, decoder)

  defp finalize_read(:more, bytes, state), do: {:more, bytes, state}

  defp finalize_read(:ok, bytes, state) do
    if state.decoder != nil and not ExStorageServiceS3.AwsChunked.finished?(state.decoder) do
      {:error, :malformed_chunked, state}
    else
      trailers = if state.decoder, do: state.decoder.trailers, else: %{}

      case ExStorageServiceS3.Integrity.finish(state.integrity, trailers) do
        {:ok, checksums} -> {:ok, bytes, %{state | checksums: checksums}}
        {:error, reason} -> {:error, reason, state}
      end
    end
  end

  defp body_read_opts,
    do: [length: 1_048_576, read_timeout: 60_000]

  # Returns true if the request uses S3 aws-chunked content encoding.
  def aws_chunked?(conn) do
    payload_hash =
      case get_req_header(conn, "x-amz-content-sha256") do
        [h | _] -> h
        [] -> ""
      end

    content_encoding =
      case get_req_header(conn, "content-encoding") do
        [ce | _] -> ce
        [] -> ""
      end

    String.contains?(payload_hash, "STREAMING") or
      String.contains?(content_encoding, "aws-chunked")
  end

  # Buffered compatibility entry point using the same integrity checks as the reader.
  def maybe_decode_aws_chunked(conn, body) do
    max = Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)

    with {:ok, integrity} <- ExStorageServiceS3.Integrity.new(conn, max),
         {:ok, decoder} <- request_decoder(conn),
         {:ok, decoded, decoder} <- decode_request_input(body, decoder),
         true <- decoder == nil or ExStorageServiceS3.AwsChunked.finished?(decoder),
         {:ok, integrity} <- ExStorageServiceS3.Integrity.update(integrity, decoded),
         {:ok, _checksums} <-
           ExStorageServiceS3.Integrity.finish(
             integrity,
             if(decoder, do: decoder.trailers, else: %{})
           ) do
      decoded
    else
      false -> {:error, :malformed_chunked}
      {:error, reason} -> {:error, reason}
    end
  end

  # Decodes S3 aws-chunked body format:
  #   <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...
  #   0;chunk-signature=<sig>\r\n\r\n
  #
  # Returns the decoded binary, or {:error, :malformed_chunked} when the framing
  # cannot be parsed. Callers must treat the error tuple as a client error
  # rather than storing partially-decoded data.
  def decode_aws_chunked(body) do
    try do
      [body]
      |> decode_aws_chunked_stream(byte_size(body))
      |> Enum.to_list()
      |> IO.iodata_to_binary()
    catch
      {:error, :malformed_chunked} -> {:error, :malformed_chunked}
      {:error, :entity_too_large} -> {:error, :entity_too_large}
    end
  end

  @doc false
  def decode_aws_chunked_stream(chunks, max_size) do
    initial = %{mode: :header, buffer: <<>>, decoded_size: 0, max_size: max_size}

    Stream.transform(
      chunks,
      fn -> initial end,
      fn
        "", %{mode: :done} = state ->
          {[], state}

        _chunk, %{mode: :done} = state ->
          {:halt, %{state | mode: {:error, :malformed_chunked}}}

        chunk, state when is_binary(chunk) ->
          try do
            decode_aws_input(chunk, state, [])
          catch
            {:error, reason} -> {:halt, %{state | mode: {:error, reason}}}
          end

        _chunk, _state ->
          throw({:error, :malformed_chunked})
      end,
      fn
        %{mode: :done} -> []
        %{mode: {:error, reason}} -> throw({:error, reason})
        _incomplete -> throw({:error, :malformed_chunked})
      end
    )
  end

  defp decode_aws_input(data, %{mode: :header} = state, output) do
    buffered = state.buffer <> data

    case :binary.match(buffered, "\r\n") do
      {header_size, 2} when header_size <= 8_192 ->
        <<header::binary-size(header_size), "\r\n", rest::binary>> = buffered

        case aws_chunk_size(header) do
          {:ok, 0} ->
            decode_aws_input(rest, %{state | mode: :terminal_crlf, buffer: <<>>}, output)

          {:ok, size} ->
            decode_aws_input(rest, %{state | mode: {:data, size}, buffer: <<>>}, output)

          :error ->
            throw({:error, :malformed_chunked})
        end

      {header_size, 2} when header_size > 8_192 ->
        throw({:error, :malformed_chunked})

      :nomatch when byte_size(buffered) <= 8_192 ->
        {Enum.reverse(output), %{state | buffer: buffered}}

      :nomatch ->
        throw({:error, :malformed_chunked})
    end
  end

  defp decode_aws_input(<<>>, %{mode: {:data, _remaining}} = state, output),
    do: {Enum.reverse(output), state}

  defp decode_aws_input(data, %{mode: {:data, remaining}} = state, output) do
    take = min(byte_size(data), remaining)
    <<decoded::binary-size(take), rest::binary>> = data
    decoded_size = state.decoded_size + take

    if exceeds_limit?(decoded_size, state.max_size),
      do: throw({:error, :entity_too_large})

    next_mode = if take == remaining, do: :chunk_crlf, else: {:data, remaining - take}
    next_output = if take == 0, do: output, else: [decoded | output]
    decode_aws_input(rest, %{state | mode: next_mode, decoded_size: decoded_size}, next_output)
  end

  defp decode_aws_input(data, %{mode: mode} = state, output)
       when mode in [:chunk_crlf, :terminal_crlf] do
    buffered = state.buffer <> data

    case buffered do
      <<"\r\n", rest::binary>> ->
        next_mode = if mode == :terminal_crlf, do: :done, else: :header
        decode_aws_input(rest, %{state | mode: next_mode, buffer: <<>>}, output)

      _ when byte_size(buffered) < 2 ->
        {Enum.reverse(output), %{state | buffer: buffered}}

      _ ->
        throw({:error, :malformed_chunked})
    end
  end

  defp decode_aws_input("", %{mode: :done} = state, output),
    do: {Enum.reverse(output), state}

  defp decode_aws_input(_data, %{mode: :done}, _output),
    do: throw({:error, :malformed_chunked})

  defp aws_chunk_size(header) do
    size = header |> :binary.split(";") |> hd()

    case Integer.parse(size, 16) do
      {chunk_size, ""} when chunk_size >= 0 -> {:ok, chunk_size}
      _ -> :error
    end
  end

  defp exceeds_limit?(_size, :infinity), do: false
  defp exceeds_limit?(size, limit), do: size > limit

  def object_attributes(conn) do
    system = ~w(content-encoding cache-control content-disposition content-language expires)

    with {:ok, tags} <- request_tags(conn) do
      {:ok,
       %{
         content_type:
           List.first(get_req_header(conn, "content-type")) || "application/octet-stream",
         metadata: extract_custom_metadata(conn),
         user_metadata: extract_custom_metadata(conn),
         system_headers: Enum.filter(conn.req_headers, fn {name, _} -> name in system end),
         tags: tags
       }}
    end
  end

  defp request_tags(conn) do
    case get_req_header(conn, "x-amz-tagging") do
      [] -> {:ok, %{}}
      [value] -> ExStorageService.Storage.Tags.from_header(value)
      _ -> {:error, :invalid_tag}
    end
  end

  def extract_custom_metadata(conn) do
    conn.req_headers
    |> Enum.filter(fn {key, _} -> String.starts_with?(key, "x-amz-meta-") end)
    |> Enum.map(fn {"x-amz-meta-" <> name, value} -> {name, value} end)
    |> Map.new()
  end

  defp tag_count_allowed?(%{assigns: %{s3_operation: operation}} = conn) do
    action =
      if operation.version_id, do: "s3:GetObjectVersionTagging", else: "s3:GetObjectTagging"

    ExStorageServiceS3.Plugs.Authorize.allowed?(
      conn,
      action,
      ExStorageServiceS3.Plugs.Authorize.resource_arn(operation.bucket, operation.key),
      "s3:Unknown"
    )
  end

  defp tag_count_allowed?(_conn), do: false

  def put_custom_metadata_headers(conn, meta) do
    conn =
      case Map.get(meta, :tags, %{}) do
        tags when is_map(tags) and map_size(tags) > 0 ->
          if tag_count_allowed?(conn),
            do: put_resp_header(conn, "x-amz-tagging-count", to_string(map_size(tags))),
            else: conn

        _ ->
          conn
      end

    system_names = ~w(content-encoding cache-control content-disposition content-language expires)
    checksum_names = ~w(crc32 crc32c crc64nvme sha1 sha256)

    allowed =
      system_names ++
        Enum.map(checksum_names, &("x-amz-checksum-" <> &1)) ++ ["x-amz-checksum-type"]

    conn =
      Enum.reduce(Map.get(meta, :system_headers, []), conn, fn {name, value}, acc ->
        if name in allowed and is_binary(value), do: put_resp_header(acc, name, value), else: acc
      end)

    conn =
      Enum.reduce(
        [
          {:content_encoding, "content-encoding"},
          {:cache_control, "cache-control"},
          {:content_disposition, "content-disposition"},
          {:content_language, "content-language"},
          {:expires, "expires"},
          {:checksum_type, "x-amz-checksum-type"}
        ],
        conn,
        fn {field, header}, acc ->
          case Map.get(meta, field) do
            value when is_binary(value) -> put_resp_header(acc, header, value)
            _ -> acc
          end
        end
      )

    conn =
      Enum.reduce(Map.get(meta, :checksums, %{}), conn, fn {name, value}, acc ->
        name = to_string(name)

        if name in checksum_names and is_binary(value),
          do: put_resp_header(acc, "x-amz-checksum-" <> name, value),
          else: acc
      end)

    Enum.reduce(Map.get(meta, :metadata, %{}), conn, fn {name, value}, acc ->
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

      [_, "", end_str] when end_str != "" and total_size > 0 ->
        suffix_length = String.to_integer(end_str)

        if suffix_length > 0 do
          length = min(suffix_length, total_size)
          {:ok, total_size - length, length}
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

  def not_modified_etag?(conn, quoted_etag) do
    case get_req_header(conn, "if-none-match") do
      [client_etag | _] ->
        # Strip whitespace and compare
        String.trim(client_etag) == quoted_etag

      [] ->
        false
    end
  end

  def conditional_not_modified?(conn, quoted_etag, last_modified_raw) do
    case get_req_header(conn, "if-none-match") do
      [] -> not_modified_since?(conn, last_modified_raw)
      _present -> not_modified_etag?(conn, quoted_etag)
    end
  end

  def not_modified_since?(conn, last_modified_raw) do
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

  def parse_http_date(date_str) do
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
           ~r/\A(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), (\d{2}) ([A-Z][a-z]{2}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT\z/,
           date_str
         ) do
      [_, day, month_str, year, hour, min, sec] ->
        with month when month != nil <- Map.get(months, month_str),
             {:ok, date} <- Date.new(String.to_integer(year), month, String.to_integer(day)),
             {:ok, time} <-
               Time.new(String.to_integer(hour), String.to_integer(min), String.to_integer(sec)),
             {:ok, dt} <- DateTime.new(date, time, "Etc/UTC") do
          {:ok, dt}
        else
          _ -> {:error, :invalid_date}
        end

      _ ->
        {:error, :invalid_date}
    end
  end

  def parse_object_datetime(nil), do: {:error, :no_date}

  def parse_object_datetime(datetime_string) when is_binary(datetime_string) do
    case DateTime.from_iso8601(datetime_string) do
      {:ok, dt, _} -> {:ok, dt}
      _ -> {:error, :invalid_date}
    end
  end

  def parse_object_datetime(%DateTime{} = dt), do: {:ok, dt}

  def format_http_date(nil), do: ""

  def format_http_date(datetime_string) when is_binary(datetime_string) do
    case DateTime.from_iso8601(datetime_string) do
      {:ok, dt, _} -> Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")
      _ -> datetime_string
    end
  end

  def format_http_date(%DateTime{} = dt) do
    Calendar.strftime(dt, "%a, %d %b %Y %H:%M:%S GMT")
  end
end
