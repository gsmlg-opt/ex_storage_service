defmodule ExStorageServiceS3.MultipartHandlers do
  @moduledoc """
  Request handlers for S3 multipart upload API operations.
  """

  import Plug.Conn

  alias ExStorageServiceS3.Handlers.Shared
  alias ExStorageServiceS3.XML
  alias ExStorageServiceS3.MultipartChecksums
  alias ExStorageServiceS3.Handlers.ControlXML
  alias ExStorageService.Metadata
  alias ExStorageService.ObjectService
  alias ExStorageService.Storage.Multipart

  # Cap for buffered XML request bodies (CompleteMultipartUpload). Generous
  # enough for the 10,000-part maximum while bounding memory use.
  @max_xml_body 16 * 1024 * 1024

  @doc """
  POST /{bucket}/{key}?uploads — CreateMultipartUpload
  """
  def create_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)

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
        case initiation_attributes(conn) do
          {:ok, attributes} ->
            create_upload(conn, bucket, key, attributes, request_id)

          {:error, reason} ->
            storage_error_response(conn, reason, conn.request_path, request_id)
        end

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  defp create_upload(conn, bucket, key, attributes, request_id) do
    case Multipart.init_upload(bucket, key, attributes) do
      {:ok, upload_id} ->
        body = XML.initiate_multipart_upload_response(bucket, key, upload_id)

        conn
        |> maybe_put_checksum_type(Map.get(attributes, :checksum_type))
        |> xml_response(200, body, request_id)

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  PUT /{bucket}/{key}?partNumber=N&uploadId=X — UploadPart
  """
  def upload_part(conn, bucket, key) do
    request_id = request_id(conn)
    params = conn.query_params
    upload_id = Map.get(params, "uploadId")
    part_number_str = Map.get(params, "partNumber")

    max_part_size =
      Application.get_env(:ex_storage_service, :max_part_size, 5 * 1024 * 1024 * 1024)

    with {:ok, part_number} <- parse_part_number(part_number_str),
         true <- part_number >= 1 and part_number <= 10_000,
         {:ok, upload} <- bound_upload(bucket, key, upload_id) do
      {reader, reader_state} =
        Shared.decoded_body_reader(conn, max_part_size, MultipartChecksums.read_options(upload))

      case Multipart.store_part_from_reader(
             bucket,
             upload_id,
             part_number,
             reader,
             reader_state,
             operation_id: request_id,
             max_size: max_part_size,
             part_attributes: &MultipartChecksums.part_attributes/1
           ) do
        {:ok, etag, final_state} ->
          final_state
          |> Shared.decoded_body_reader_conn()
          |> put_s3_headers(request_id)
          |> put_resp_header("etag", "\"#{etag}\"")
          |> MultipartChecksums.response_headers(Map.get(final_state, :checksums, %{}))
          |> send_resp(200, "")

        {:error, :entity_too_large, final_state} ->
          conn = Shared.decoded_body_reader_conn(final_state)

          error_response(
            conn,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed part size.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, :malformed_chunked, final_state} ->
          conn = Shared.decoded_body_reader_conn(final_state)

          error_response(
            conn,
            "InvalidRequest",
            "The aws-chunked request body is malformed.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, :not_found, final_state} ->
          final_state
          |> Shared.decoded_body_reader_conn()
          |> error_response(
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            "/#{bucket}/#{key}",
            request_id
          )

        {:error, reason, final_state} ->
          conn = Shared.decoded_body_reader_conn(final_state)
          storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
      end
    else
      :error ->
        error_response(
          conn,
          "InvalidArgument",
          "Invalid part number.",
          "/#{bucket}/#{key}",
          request_id
        )

      false ->
        error_response(
          conn,
          "InvalidArgument",
          "Part number must be between 1 and 10000.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :entity_too_large} ->
        error_response(
          conn,
          "EntityTooLarge",
          "Your proposed upload exceeds the maximum allowed part size.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  PUT /{bucket}/{key}?partNumber=N&uploadId=X with a copy source — UploadPartCopy.
  """
  def upload_part_copy(conn, bucket, key) do
    request_id = request_id(conn)
    operation = conn.assigns.s3_operation
    source = operation.copy_source
    max_size = Application.get_env(:ex_storage_service, :max_part_size, 5 * 1024 * 1024 * 1024)

    with {:ok, upload} <- bound_upload(bucket, key, operation.upload_id),
         {:ok, _body, conn} <- Shared.read_full_body(conn, 0),
         {:ok, %{delete_marker: false, metadata: metadata}} <-
           ExStorageServiceS3.Handlers.Object.CloudCopy.head_source(source),
         :ok <- ExStorageServiceS3.Handlers.Object.copy_source_conditions(conn, metadata),
         {:ok, range} <- copy_part_range(conn, metadata.size),
         {:ok, etag, checksums} <-
           ExStorageServiceS3.Handlers.Object.CloudCopy.with_source(
             source,
             metadata,
             range,
             fn pinned ->
               store_copied_part(bucket, operation, pinned, upload, max_size, request_id)
             end
           ) do
      modified = DateTime.utc_now() |> DateTime.to_iso8601()

      body =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LastModified>#{XML.escape(modified)}</LastModified><ETag>#{XML.escape("\"#{etag}\"")}</ETag></CopyPartResult>"

      conn =
        case Map.get(metadata, :version_id) do
          nil -> conn
          version -> put_resp_header(conn, "x-amz-copy-source-version-id", version)
        end

      body =
        String.replace(body, "</CopyPartResult>", checksum_xml(checksums) <> "</CopyPartResult>")

      conn
      |> MultipartChecksums.response_headers(checksums)
      |> xml_response(200, body, request_id)
    else
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          conn.request_path,
          request_id
        )

      {:error, reason} when reason in [:object_not_found, :version_not_found] ->
        error_response(
          conn,
          "NoSuchKey",
          "The specified source key does not exist.",
          conn.request_path,
          request_id
        )

      {:ok, %{delete_marker: true}} ->
        error_response(
          conn,
          "NoSuchKey",
          "The source is a delete marker.",
          conn.request_path,
          request_id
        )

      {:error, :invalid_range} ->
        error_response(
          conn,
          "InvalidRange",
          "The copy source range is invalid.",
          conn.request_path,
          request_id
        )

      {:error, :entity_too_large} ->
        error_response(
          conn,
          "EntityTooLarge",
          "The proposed part exceeds the supported size.",
          conn.request_path,
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, conn.request_path, request_id)
    end
  end

  defp store_copied_part(bucket, operation, source, upload, maximum, request_id) do
    alias ExStorageService.BlobStore.{LocalCAS, Source}

    with {:ok, context} <- ExStorageService.Context.default(),
         stage_opts =
           Keyword.put(ExStorageService.Context.blob_store_options(context), :max_size, maximum),
         {:ok, staged} <- LocalCAS.stage(Source.request_body(source), stage_opts) do
      try do
        names = Keyword.get(MultipartChecksums.read_options(upload), :calculate_checksums, [])

        with {:ok, checksums} <-
               MultipartChecksums.checksum_path(staged.path, staged.size, names),
             {:ok, etag} <-
               Multipart.store_part(
                 bucket,
                 operation.upload_id,
                 String.to_integer(operation.part_number),
                 File.stream!(staged.path, [], 262_144),
                 operation_id: request_id,
                 max_size: maximum,
                 attributes: %{checksums: checksums}
               ) do
          {:ok, etag, checksums}
        end
      after
        LocalCAS.discard(staged)
      end
    end
  end

  defp copy_part_range(conn, size) do
    case get_req_header(conn, "x-amz-copy-source-range") do
      [] ->
        {:ok, nil}

      [value] ->
        case Regex.run(~r/\Abytes=(\d+)-(\d+)\z/, value) do
          [_, first, last] ->
            first = String.to_integer(first)
            last = String.to_integer(last)

            if first <= last and last < size,
              do: {:ok, {first, last - first + 1}},
              else: {:error, :invalid_range}

          _ ->
            {:error, :invalid_range}
        end

      _ ->
        {:error, :invalid_range}
    end
  end

  @doc """
  POST /{bucket}/{key}?uploadId=X — CompleteMultipartUpload
  """
  def complete_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    case bound_upload(bucket, key, upload_id, true) do
      {:ok, _upload} ->
        case Shared.read_full_body(conn, @max_xml_body, verify_wire_checksums: false) do
          {:ok, body, conn} ->
            case parse_complete_multipart_xml(body) do
              {:ok, parts, declarations} ->
                case Multipart.prepare_complete_upload(bucket, upload_id, parts) do
                  {:ok, prepared} ->
                    complete_prepared_upload(
                      conn,
                      bucket,
                      key,
                      upload_id,
                      prepared,
                      declarations,
                      request_id
                    )

                  {:error, {:etag_mismatch, pn, _expected, _actual}} ->
                    error_response(
                      conn,
                      "InvalidPart",
                      "Part #{pn} has an invalid ETag.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, {:missing_part, pn, _reason}} ->
                    error_response(
                      conn,
                      "InvalidPart",
                      "Part #{pn} was not uploaded.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, {:entity_too_small, pn, size, min}} ->
                    error_response(
                      conn,
                      "EntityTooSmall",
                      "Part #{pn} is #{size} bytes; all parts except the last must be " <>
                        "at least #{min} bytes.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, :not_found} ->
                    error_response(
                      conn,
                      "NoSuchUpload",
                      "The specified multipart upload does not exist.",
                      "/#{bucket}/#{key}",
                      request_id
                    )

                  {:error, reason} ->
                    storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
                end

              {:error, _reason} ->
                error_response(
                  conn,
                  "MalformedXML",
                  "The XML you provided was not well-formed.",
                  "/#{bucket}/#{key}",
                  request_id
                )
            end

          {:error, :entity_too_large} ->
            error_response(
              conn,
              "EntityTooLarge",
              "The CompleteMultipartUpload request body is too large.",
              "/#{bucket}/#{key}",
              request_id
            )

          {:error, reason} ->
            storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
        end

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  DELETE /{bucket}/{key}?uploadId=X — AbortMultipartUpload
  """
  def abort_multipart_upload(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    case bound_upload(bucket, key, upload_id) do
      {:ok, _upload} ->
        case Multipart.abort_upload(bucket, upload_id) do
          :ok -> conn |> put_s3_headers(request_id) |> send_resp(204, "")
          {:error, reason} -> storage_error_response(conn, reason, conn.request_path, request_id)
        end

      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @doc """
  GET /{bucket}/{key}?uploadId=X — ListParts
  """
  def list_parts(conn, bucket, key) do
    request_id = request_id(conn)
    upload_id = Map.get(conn.query_params, "uploadId")

    with {:ok, _upload} <- bound_upload(bucket, key, upload_id),
         {:ok, marker} <- pagination_integer(conn, "part-number-marker", 0, 10_000),
         {:ok, maximum} <- pagination_integer(conn, "max-parts", 1000, 1000),
         {:ok, page} <-
           Multipart.list_parts(bucket, upload_id, part_number_marker: marker, max_parts: maximum) do
      body = XML.list_parts_response(bucket, key, upload_id, page)
      xml_response(conn, 200, body, request_id)
    else
      {:error, :not_found} ->
        error_response(
          conn,
          "NoSuchUpload",
          "The specified multipart upload does not exist.",
          "/#{bucket}/#{key}",
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}/#{key}", request_id)
    end
  end

  @doc "GET /{bucket}?uploads — ListMultipartUploads."
  def list_multipart_uploads(conn, bucket) do
    request_id = request_id(conn)

    with {:ok, maximum} <- pagination_integer(conn, "max-uploads", 1000, 1000),
         true <- Map.get(conn.query_params, "encoding-type") in [nil, "url"],
         {:ok, page} <-
           Multipart.list_uploads(bucket,
             max_uploads: maximum,
             prefix: Map.get(conn.query_params, "prefix", ""),
             delimiter: Map.get(conn.query_params, "delimiter"),
             key_marker: Map.get(conn.query_params, "key-marker", ""),
             upload_id_marker: Map.get(conn.query_params, "upload-id-marker", ""),
             encoding_type: Map.get(conn.query_params, "encoding-type")
           ) do
      xml_response(conn, 200, XML.list_multipart_uploads_response(bucket, page), request_id)
    else
      false ->
        error_response(
          conn,
          "InvalidArgument",
          "Invalid encoding type.",
          conn.request_path,
          request_id
        )

      {:error, reason} ->
        storage_error_response(conn, reason, conn.request_path, request_id)
    end
  end

  # Private helpers

  defp complete_prepared_upload(conn, bucket, key, upload_id, prepared, declarations, request_id) do
    result =
      with :ok <- MultipartChecksums.validate_parts(prepared, declarations),
           {:ok, checksums} <- MultipartChecksums.complete(conn, prepared) do
        committed =
          case Map.get(prepared, :completion_result) do
            %{version_id: _} = completed ->
              {:ok, completed}

            nil when not is_map_key(prepared, :multipart_completion) ->
              {:error, :not_found}

            _ ->
              ready = %{hash: prepared.content_hash, size: prepared.size, etag: prepared.etag}

              attributes =
                Map.merge(Map.get(prepared.upload, :attributes, %{}), %{
                  manifest_hash: prepared.manifest_hash,
                  object_type: :blob,
                  created_at: prepared.upload.created_at,
                  checksums: checksums
                })

              commit_complete_blob(conn, bucket, key, prepared, ready, attributes)
          end

        with {:ok, %{version_id: _version} = publication} <- committed,
             :ok <- Multipart.finalize_complete_upload(bucket, upload_id, prepared) do
          {:ok, publication, checksums}
        end
      end

    case result do
      {:ok, publication, checksums} ->
        body =
          XML.complete_multipart_upload_response(bucket, key, prepared.etag, "/#{bucket}/#{key}")
          |> completion_checksum_xml(checksums)
          |> then(fn body ->
            case get_in(prepared.upload, [:attributes, :checksum_type]) do
              nil ->
                body

              type ->
                String.replace(
                  body,
                  "</CompleteMultipartUploadResult>",
                  "<ChecksumType>#{type}</ChecksumType></CompleteMultipartUploadResult>"
                )
            end
          end)

        conn
        |> Shared.put_write_version_header(publication)
        |> MultipartChecksums.response_headers(checksums)
        |> maybe_put_checksum_type(get_in(prepared.upload, [:attributes, :checksum_type]))
        |> xml_response(200, body, request_id)

      {:error, reason} ->
        if reason in [
             :precondition_failed,
             :invalid_precondition,
             :bad_digest,
             :invalid_digest,
             :unsupported_checksum,
             :unsupported_checksum_type
           ] and
             Map.has_key?(prepared, :multipart_completion) do
          _ = Multipart.release_completion(bucket, upload_id, prepared)
        end

        storage_error_response(conn, reason, conn.request_path, request_id)
    end
  end

  defp completion_checksum_xml(body, checksums) do
    String.replace(
      body,
      "</CompleteMultipartUploadResult>",
      checksum_xml(checksums) <> "</CompleteMultipartUploadResult>"
    )
  end

  defp checksum_xml(checksums) do
    Enum.map_join(checksums, "", fn {name, value} ->
      tag = "Checksum" <> String.upcase(name)
      "<#{tag}>#{XML.escape(value)}</#{tag}>"
    end)
  end

  defp pagination_integer(conn, key, default, maximum) do
    case Integer.parse(Map.get(conn.query_params, key, to_string(default))) do
      {value, ""} when value >= 0 and value <= maximum -> {:ok, value}
      _ -> {:error, :invalid_pagination}
    end
  end

  defp commit_complete_blob(conn, bucket, key, prepared, ready, attributes) do
    ObjectService.commit_existing_blob(
      bucket,
      key,
      ready,
      attributes,
      metadata_opts: [
        operation_id: "multipart-complete:#{prepared.upload.upload_id}:#{prepared.content_hash}",
        if_match: List.first(get_req_header(conn, "if-match")),
        if_none_match: List.first(get_req_header(conn, "if-none-match")),
        bucket_generation: Map.get(prepared.upload, :bucket_generation),
        multipart_completion: prepared.multipart_completion
      ]
    )
  end

  defp initiation_attributes(conn) do
    system = [
      {:content_type, "content-type"},
      {:content_encoding, "content-encoding"},
      {:content_language, "content-language"},
      {:content_disposition, "content-disposition"},
      {:cache_control, "cache-control"},
      {:expires, "expires"}
    ]

    attributes =
      Enum.reduce(
        system,
        %{
          content_type: "application/octet-stream",
          metadata: Shared.extract_custom_metadata(conn)
        },
        fn {key, header}, attributes ->
          case get_req_header(conn, header) do
            [value] -> Map.put(attributes, key, value)
            [] -> attributes
            _ -> attributes
          end
        end
      )

    with {:ok, checksums} <- MultipartChecksums.initiation(conn),
         {:ok, tags} <- initiation_tags(conn) do
      {:ok, attributes |> Map.merge(checksums) |> Map.put(:tags, tags)}
    end
  end

  defp initiation_tags(conn) do
    case get_req_header(conn, "x-amz-tagging") do
      [] -> {:ok, %{}}
      [header] -> ExStorageService.Storage.Tags.from_header(header)
      _ -> {:error, :invalid_tag}
    end
  end

  defp parse_part_number(value) do
    case if(is_binary(value) and Regex.match?(~r/\A[0-9]+\z/, value),
           do: Integer.parse(value),
           else: :error
         ) do
      {number, ""} -> {:ok, number}
      _ -> :error
    end
  end

  defp bound_upload(bucket, key, upload_id, allow_completed \\ false) do
    case Multipart.get_upload(bucket, upload_id) do
      {:ok, %{bucket: ^bucket, key: ^key, status: status}} = upload ->
        if status in [:initiated, :completing] or (allow_completed and status == :completed),
          do: upload,
          else: {:error, :not_found}

      {:ok, _} ->
        {:error, :not_found}

      error ->
        error
    end
  end

  defp request_id(conn) do
    conn.assigns[:request_id] || :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  defp put_s3_headers(conn, request_id) do
    conn
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("server", "ExStorageService")
  end

  defp maybe_put_checksum_type(conn, nil), do: conn
  defp maybe_put_checksum_type(conn, type), do: put_resp_header(conn, "x-amz-checksum-type", type)

  defp xml_response(conn, status, body, request_id) do
    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
  end

  defp error_response(conn, code, message, resource, request_id),
    do: Shared.error_response(conn, code, message, resource, request_id)

  defp storage_error_response(conn, reason, resource, request_id),
    do: Shared.storage_error_response(conn, reason, resource, request_id)

  defp parse_complete_multipart_xml(body) do
    names =
      ["CompleteMultipartUpload", "Part", "PartNumber", "ETag"] ++
        Enum.map(ExStorageServiceS3.Integrity.algorithms(), &("Checksum" <> String.upcase(&1)))

    # SHA names are capitalized by the S3 wire schema; CRC names already match.
    with {:ok, document} <- ControlXML.parse(body, "CompleteMultipartUpload", names),
         true <- String.trim(document.text) == "" and length(document.children) <= 10_000 do
      Enum.reduce_while(document.children, {:ok, [], []}, fn
        %{name: "Part", text: text} = part, {:ok, parts, declarations} ->
          with true <- String.trim(text) == "",
               {:ok, number} <- ControlXML.text(part, "PartNumber"),
               true <- Regex.match?(~r/\A[0-9]+\z/, String.trim(number)),
               {number, ""} when number in 1..10_000 <- Integer.parse(String.trim(number)),
               {:ok, etag} <- ControlXML.text(part, "ETag"),
               {:ok, etag} <- completion_etag(String.trim(etag)),
               {:ok, checksums} <- completion_part_checksums(part),
               true <- length(part.children) == 2 + map_size(checksums) do
            {:cont, {:ok, [{number, etag} | parts], [{number, checksums} | declarations]}}
          else
            _ -> {:halt, {:error, :malformed_xml}}
          end

        _, _ ->
          {:halt, {:error, :malformed_xml}}
      end)
      |> case do
        {:ok, parts, declarations} -> {:ok, Enum.reverse(parts), Enum.reverse(declarations)}
        error -> error
      end
    else
      _ -> {:error, :malformed_xml}
    end
  end

  defp completion_etag(value) do
    value =
      case value do
        <<"\"", rest::binary>> ->
          if String.ends_with?(rest, "\""),
            do: String.slice(rest, 0, byte_size(rest) - 1),
            else: ""

        other ->
          other
      end

    if Regex.match?(~r/\A[0-9a-fA-F]{32}\z/, value),
      do: {:ok, value},
      else: {:error, :invalid_part}
  end

  defp completion_part_checksums(part) do
    Enum.reduce_while(ExStorageServiceS3.Integrity.algorithms(), {:ok, %{}}, fn name,
                                                                                {:ok, acc} ->
      case ControlXML.children(part, "Checksum" <> String.upcase(name)) do
        [] -> {:cont, {:ok, acc}}
        [%{children: [], text: value}] -> {:cont, {:ok, Map.put(acc, name, String.trim(value))}}
        _ -> {:halt, {:error, :malformed_xml}}
      end
    end)
  end
end
