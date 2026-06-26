defmodule ExStorageServiceS3.Handlers.Helpers do
  import Plug.Conn
  require Logger
  alias ExStorageServiceS3.XML
  alias ExStorageService.BucketValidator
  alias ExStorageService.CloudCache.Client, as: CloudClient
  alias ExStorageService.CloudCache.Config, as: CloudConfig
  alias ExStorageService.CloudCache.LocalStore
  alias ExStorageService.Metadata
  alias ExStorageService.Notifications
  alias ExStorageService.Replication.Hooks
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Storage.Lifecycle
  alias ExStorageService.Storage.Versioning

  def cloud_cache_config(bucket) do
    CloudConfig.get_active_config(bucket)
  end

  def broadcast_bucket_change(bucket, action, key) do
    Phoenix.PubSub.broadcast(
      ExStorageService.PubSub,
      "bucket:#{bucket}",
      {:bucket_changed, %{action: action, key: key, bucket: bucket}}
    )
  end

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

  # Streaming body reader — yields chunks from the request body without
  # accumulating the entire object in memory. Enforces max_object_size
  # inline by throwing {:error, :entity_too_large} when the limit is exceeded.
  # The caller must catch this throw.
  def body_stream(conn) do
    max_size = Application.get_env(:ex_storage_service, :max_object_size, 5 * 1024 * 1024 * 1024)
    # 1 MiB read chunks — large enough for throughput, small enough for memory
    read_opts = [length: 1_048_576, read_timeout: 60_000]

    # Pre-populate with initial connection in case stream consumption is immediate or empty
    Process.put(:body_stream_conn, conn)

    Stream.resource(
      fn -> {conn, 0} end,
      fn
        :done ->
          {:halt, :done}

        {conn, acc_size} ->
          case Plug.Conn.read_body(conn, read_opts) do
            {:ok, chunk, conn} ->
              Process.put(:body_stream_conn, conn)
              new_size = acc_size + byte_size(chunk)

              if new_size > max_size do
                throw({:error, :entity_too_large})
              end

              {[chunk], :done}

            {:more, chunk, conn} ->
              Process.put(:body_stream_conn, conn)
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

  # Kept for handlers that must fully buffer the body (e.g., XML parse operations).
  # NOT used for PutObject — use body_stream/1 there.
  def read_full_body(conn, acc \\ <<>>) do
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
  def decode_aws_chunked(body) do
    decode_aws_chunks(body, <<>>)
  end

  def decode_aws_chunks(<<>>, acc), do: acc

  def decode_aws_chunks(data, acc) do
    case :binary.split(data, "\r\n") do
      [header, rest] ->
        case Integer.parse(header, 16) do
          {0, _} ->
            # Terminal chunk
            acc

          {chunk_size, _} ->
            <<chunk::binary-size(^chunk_size), "\r\n", remaining::binary>> = rest
            decode_aws_chunks(remaining, acc <> chunk)

          :error ->
            # Malformed — return what we have
            acc <> data
        end

      [_no_crlf] ->
        # No more CRLF — malformed or done
        acc
    end
  end

  def parse_max_keys(value) do
    case Integer.parse(value) do
      {n, _} when n > 0 and n <= 1000 -> n
      {n, _} when n > 1000 -> 1000
      _ -> 1000
    end
  end

  def parse_copy_source(source) do
    source = String.trim_leading(source, "/")

    case String.split(source, "/", parts: 2) do
      [bucket, key] -> {bucket, URI.decode(key)}
      [bucket] -> {bucket, ""}
    end
  end

  def parse_delete_objects_xml(xml_body) do
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

  def parse_versioning_xml(xml_body) do
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

  def parse_lifecycle_xml(xml_body) do
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

  def xpath_text(elem, path) do
    case :xmerl_xpath.string(path ++ ~c"/text()", elem) do
      [{:xmlText, _, _, _, value, _} | _] -> to_string(value)
      _ -> nil
    end
  end

  def build_lifecycle_xml(rules) do
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

  def parse_notification_xml(xml_body) do
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

  def build_notification_xml(configs) do
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
