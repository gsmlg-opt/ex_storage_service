defmodule ExStorageServiceS3.Handlers.Shared do
  @moduledoc false

  import Plug.Conn
  alias ExStorageServiceS3.XML

  def request_id(conn) do
    conn.assigns[:request_id] || :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end

  def put_s3_headers(conn, request_id) do
    conn
    |> put_resp_header("x-amz-request-id", request_id)
    |> put_resp_header("x-amz-id-2", request_id)
    |> put_resp_header("server", "ExStorageService")
  end

  def xml_response(conn, status, body, request_id) do
    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
  end

  def error_response(conn, code, message, resource, request_id) do
    status = XML.error_status_code(code)
    body = XML.error_response(code, message, resource, request_id)

    conn
    |> put_s3_headers(request_id)
    |> put_resp_header("content-type", "application/xml")
    |> send_resp(status, body)
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

  # Reads the entire request body into memory, enforcing a maximum size.
  # `max_size` defaults to the configured max_object_size; callers that buffer
  # smaller payloads (e.g. XML request bodies, multipart parts) should pass a
  # tighter cap to bound memory use. Returns {:error, :entity_too_large} when
  # the body exceeds the cap.
  def read_full_body(conn, max_size \\ nil) do
    max =
      max_size ||
        Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)

    do_read_full_body(conn, max, <<>>)
  end

  defp do_read_full_body(conn, max_size, acc) do
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
          do_read_full_body(conn, max_size, result)
        end

      {:error, reason} ->
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
  def body_stream(conn) do
    max_size = Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)
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

              if new_size > max_size do
                throw({:error, :entity_too_large})
              end

              {[chunk], :done}

            {:more, chunk, conn} ->
              new_size = acc_size + byte_size(chunk)

              if new_size > max_size do
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

  # Detects whether the body uses S3 aws-chunked content encoding
  # (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) and decodes it if so.
  #
  # Returns the decoded payload, or {:error, :malformed_chunked} if the framing
  # is invalid. Note: the per-chunk signature chain is NOT verified here, so the
  # body of an aws-chunked upload is not authenticated end-to-end even when the
  # request's Authorization header passes SigV4. Verifying the rolling
  # chunk-signature chain is tracked as a follow-up; until then this only
  # guarantees the framing is well-formed so signature/length bytes are never
  # written into stored object content.
  def maybe_decode_aws_chunked(conn, body) do
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

    if String.contains?(payload_hash, "STREAMING") or
         String.contains?(content_encoding, "aws-chunked") do
      decode_aws_chunked(body)
    else
      body
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
    decode_aws_chunks(body, <<>>)
  end

  def decode_aws_chunks(<<>>, acc), do: acc

  def decode_aws_chunks(data, acc) do
    case :binary.split(data, "\r\n") do
      [header, rest] ->
        # The chunk header is "<hex-size>" optionally followed by
        # ";chunk-signature=<sig>". Parse only the size portion.
        size_str = header |> :binary.split(";") |> hd()

        case Integer.parse(size_str, 16) do
          {0, _} ->
            # Terminal chunk — ignore any trailing headers/signatures.
            acc

          {chunk_size, _} ->
            case rest do
              <<chunk::binary-size(chunk_size), "\r\n", remaining::binary>> ->
                decode_aws_chunks(remaining, acc <> chunk)

              _ ->
                # Declared size does not match available data — malformed framing.
                {:error, :malformed_chunked}
            end

          :error ->
            {:error, :malformed_chunked}
        end

      [_no_crlf] ->
        {:error, :malformed_chunked}
    end
  end

  def extract_custom_metadata(conn) do
    conn.req_headers
    |> Enum.filter(fn {key, _} -> String.starts_with?(key, "x-amz-meta-") end)
    |> Enum.map(fn {"x-amz-meta-" <> name, value} -> {name, value} end)
    |> Map.new()
  end

  def put_custom_metadata_headers(conn, meta) do
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

  def not_modified_etag?(conn, quoted_etag) do
    case get_req_header(conn, "if-none-match") do
      [client_etag | _] ->
        # Strip whitespace and compare
        String.trim(client_etag) == quoted_etag

      [] ->
        false
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
