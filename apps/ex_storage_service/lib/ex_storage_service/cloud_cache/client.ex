defmodule ExStorageService.CloudCache.Client do
  @moduledoc """
  HTTP client for S3-compatible cloud storage (AWS S3, Cloudflare R2, MinIO, S3-compatible).

  Handles:
  - `put_object/5` — upload to remote bucket (streaming via binary)
  - `get_object/2` — download from remote bucket
  - `head_object/2` — check existence and metadata
  - `delete_object/2` — delete from remote bucket
  - `test_connection/1` — HEAD on bucket to verify credentials/reachability

  All requests are signed with AWS Signature Version 4.

  Region used for SigV4 signing:
  - `:aws`      — uses `config.region` (default `us-east-1`)
  - `:r2`       — always `"auto"` (Cloudflare requirement)
  - `:minio`    — uses `config.region` (default `us-east-1`; value is arbitrary for MinIO)
  - `:s3_compat` — uses `config.region`
  """

  require Logger

  alias ExStorageService.CloudCache.Config
  alias ExStorageService.HTTPClient

  @doc """
  Upload an object to the remote S3/R2 bucket.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec put_object(Config.t(), String.t(), iodata(), String.t(), map()) ::
          :ok | {:error, term()}
  def put_object(%Config{} = config, key, data, content_type, metadata \\ %{}) do
    ExStorageService.Telemetry.upstream(:put, fn ->
      do_put_object(config, key, data, content_type, metadata)
    end)
  end

  defp do_put_object(%Config{} = config, key, data, content_type, _metadata) do
    body = IO.iodata_to_binary(data)
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    headers = [{"content-type", content_type}]

    signed_headers =
      sign_request("PUT", url, headers, body, config)

    case mutation_request(url, method: :put, body: body, headers: signed_headers) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.info("CloudCache PUT #{config.bucket}/#{key} → #{endpoint} (#{status})")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "CloudCache PUT failed #{config.bucket}/#{key}: HTTP #{status} — #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error("CloudCache PUT failed #{config.bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc "Uploads a staged file with its verified SHA-256 and explicit wire length."
  def put_file(%Config{} = config, key, path, content_type, metadata \\ %{}, opts \\ []) do
    ExStorageService.Telemetry.upstream(:put, fn ->
      do_put_file(config, key, path, content_type, metadata, opts)
    end)
  end

  defp do_put_file(%Config{} = config, key, path, content_type, metadata, opts) do
    size = Keyword.fetch!(opts, :size)
    hash = Keyword.fetch!(opts, :sha256)

    with {:ok, %{type: :regular, size: ^size}} <- File.stat(path) do
      url = object_url(Config.endpoint_url(config), config.bucket, key)

      headers =
        [{"content-type", content_type}, {"content-length", Integer.to_string(size)}] ++
          Enum.map(metadata, fn {name, value} -> {"x-amz-meta-" <> name, value} end) ++
          Keyword.get(opts, :headers, [])

      signed = sign_request("PUT", url, headers, {:sha256, hash}, config)

      case mutation_request(url,
             method: :put,
             body: File.stream!(path, 65_536, []),
             headers: signed,
             timeout: :infinity,
             connect_timeout: 30_000,
             read_idle_timeout: 60_000,
             write_idle_timeout: 60_000
           ) do
        {:ok, %{status: status}} when status in 200..299 -> :ok
        {:ok, %{status: status}} -> {:error, {:http_error, status}}
        {:error, _} = error -> error
      end
    else
      {:ok, _stat} -> {:error, :staged_file_changed}
      {:error, _} = error -> error
    end
  end

  @doc """
  Download an object from the remote S3/R2 bucket.

  Returns `{:ok, body_binary}` on success,
  `{:error, :not_found}` for 404, or `{:error, reason}`.
  """
  @spec get_object(Config.t(), String.t()) ::
          {:ok, binary()} | {:error, :not_found | term()}
  def get_object(%Config{} = config, key) do
    ExStorageService.Telemetry.upstream(:get, fn -> do_get_object(config, key) end)
  end

  defp do_get_object(%Config{} = config, key) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    signed_headers = sign_request("GET", url, [], "", config)

    case HTTPClient.request(url, method: :get, headers: signed_headers) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, %{status: status, body: resp_body}} ->
        Logger.warning(
          "CloudCache GET failed #{config.bucket}/#{key}: HTTP #{status} — #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error("CloudCache GET failed #{config.bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc "Downloads raw bytes to a private, hashed staging file with acknowledged backpressure."
  def get_file(%Config{} = config, key, opts \\ []) do
    ExStorageService.Telemetry.upstream(:get, fn -> do_get_file(config, key, opts) end)
  end

  defp do_get_file(%Config{} = config, key, opts) do
    url = object_url(Config.endpoint_url(config), config.bucket, key)
    headers = if etag = Keyword.get(opts, :if_match), do: [{"if-match", etag}], else: []
    signed = sign_request("GET", url, [{"x-amz-checksum-mode", "ENABLED"} | headers], "", config)

    case HTTPClient.fetch(url, transfer_options(method: :get, headers: signed)) do
      {:ok, %{status: 200} = response} ->
        stage_response(response, opts)

      {:ok, response} ->
        stop_response(response)

        if response.status == 404,
          do: {:error, :not_found},
          else: {:error, {:http_error, response.status}}

      {:error, _} = error ->
        error
    end
  end

  defp stage_response(response, opts) do
    metadata = parse_headers(response.headers.headers)
    stream = HTTP.Response.stream_pid(response)
    monitor = if stream, do: Process.monitor(stream)

    initial =
      if stream, do: %{stream: stream, monitor: monitor, ack: nil}, else: %{body: response.body}

    if stream, do: send(stream, {:read_chunk, self(), :ack})

    try do
      case ExStorageService.BlobStore.LocalCAS.stage_from_reader(&read_response/1, initial, opts) do
        {:ok, staged, _state} ->
          if is_integer(metadata.content_length) and metadata.content_length != staged.size do
            ExStorageService.BlobStore.LocalCAS.discard(staged)
            {:error, :upstream_length_mismatch}
          else
            {:ok, staged, Map.put(metadata, :content_length, staged.size)}
          end

        {:error, reason, _state} ->
          {:error, reason}
      end
    after
      if monitor, do: Process.demonitor(monitor, [:flush])
      stop_response(response)
    end
  end

  defp read_response(%{body: body} = state) when is_binary(body),
    do: {:ok, body, state}

  defp read_response(%{stream: stream, ack: ack} = state) do
    if ack, do: send(stream, {:stream_chunk_ack, ack})
    receive_response(%{state | ack: nil})
  end

  defp receive_response(%{stream: stream, monitor: monitor} = state) do
    receive do
      {:stream_chunk, ^stream, bytes, ack} -> {:more, bytes, %{state | ack: ack}}
      {:stream_chunk, ^stream, bytes} -> {:more, bytes, state}
      {:stream_trailers, ^stream, _headers} -> receive_response(state)
      {:stream_end, ^stream} -> {:done, state}
      {:stream_error, ^stream, reason} -> {:error, reason, state}
      {:DOWN, ^monitor, :process, ^stream, reason} -> {:error, {:stream_down, reason}, state}
    after
      65_000 -> {:error, :timeout, state}
    end
  end

  defp stop_response(response) do
    if stream = HTTP.Response.stream_pid(response), do: HTTP.Stream.stop(stream, :normal)
  end

  # Storage mutations use only the response status. Never buffer an upstream
  # error/success document proportional to an untrusted response Content-Length.
  defp mutation_request(url, options) do
    case HTTPClient.fetch(url, transfer_options(options)) do
      {:ok, response} ->
        stop_response(response)
        {:ok, %{status: response.status, body: ""}}

      {:error, _} = error ->
        error
    end
  end

  defp transfer_options(options) do
    Keyword.merge(
      [
        timeout: :infinity,
        connect_timeout: 30_000,
        read_idle_timeout: 60_000,
        write_idle_timeout: 60_000,
        response_stream_idle_timeout: :infinity,
        response_stream_ack_timeout: :infinity
      ],
      options
    )
  end

  @doc """
  Get metadata (headers) for an object without downloading the body.

  Returns `{:ok, headers_map}` with at least `:content_length`, `:etag`,
  `:content_type`, `:last_modified`.
  """
  @spec head_object(Config.t(), String.t()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def head_object(%Config{} = config, key) do
    ExStorageService.Telemetry.upstream(:head, fn -> do_head_object(config, key) end)
  end

  defp do_head_object(%Config{} = config, key) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    signed_headers = sign_request("HEAD", url, [{"x-amz-checksum-mode", "ENABLED"}], "", config)

    case HTTPClient.request(url, transfer_options(method: :head, headers: signed_headers)) do
      {:ok, %{status: 200, headers: resp_headers}} ->
        {:ok, parse_headers(resp_headers)}

      {:ok, %{status: 404}} ->
        {:error, :not_found}

      {:ok, %{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete an object from the remote S3/R2 bucket.

  Returns `:ok` for both successful deletes and 404 (idempotent).
  """
  @spec delete_object(Config.t(), String.t()) :: :ok | {:error, term()}
  def delete_object(%Config{} = config, key, opts \\ []) do
    ExStorageService.Telemetry.upstream(:delete, fn -> do_delete_object(config, key, opts) end)
  end

  defp do_delete_object(%Config{} = config, key, opts) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    headers = if etag = Keyword.get(opts, :if_match), do: [{"if-match", etag}], else: []
    signed_headers = sign_request("DELETE", url, headers, "", config)

    case mutation_request(url, method: :delete, headers: signed_headers) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 ->
        Logger.debug("CloudCache DELETE #{config.bucket}/#{key} (#{status})")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "CloudCache DELETE failed #{config.bucket}/#{key}: HTTP #{status} — #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error("CloudCache DELETE failed #{config.bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Test connectivity by calling HEAD on the remote bucket.

  Returns `:ok` if reachable, `{:error, reason}` otherwise.
  """
  @spec test_connection(Config.t()) :: :ok | {:error, term()}
  def test_connection(%Config{} = config) do
    ExStorageService.Telemetry.upstream(:probe, fn -> do_test_connection(config) end)
  end

  defp do_test_connection(%Config{} = config) do
    endpoint = Config.endpoint_url(config)
    url = bucket_url(endpoint, config.bucket)

    signed_headers = sign_request("HEAD", url, [], "", config)

    case HTTPClient.request(url, transfer_options(method: :head, headers: signed_headers)) do
      {:ok, %{status: status}} when status in 200..299 ->
        :ok

      {:ok, %{status: 403}} ->
        # 403 means we reached S3 but credentials may be wrong — still "connected"
        {:error, :forbidden}

      {:ok, %{status: 404}} ->
        {:error, :bucket_not_found}

      {:ok, %{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  List objects in the remote bucket, using S3 ListObjectsV2.

  Options:
  - `:prefix` — key prefix to filter (default `""`)
  - `:delimiter` — delimiter for virtual folders (default `"/"`)
  - `:max_keys` — max results (default 1000)
  - `:continuation_token` — pagination token

  Returns `{:ok, %{keys: [{key, meta}], common_prefixes: [prefix], truncated: bool}}`
  where `meta` is `%{size: integer, last_modified: string, etag: string}`.
  """
  @spec list_objects(Config.t(), keyword()) ::
          {:ok,
           %{
             keys: [{String.t(), map()}],
             common_prefixes: [String.t()],
             truncated: boolean(),
             next_continuation_token: String.t() | nil
           }}
          | {:error, term()}
  def list_objects(%Config{} = config, opts \\ []) do
    ExStorageService.Telemetry.upstream(:list, fn -> do_list_objects(config, opts) end)
  end

  defp do_list_objects(%Config{} = config, opts) do
    endpoint = Config.endpoint_url(config)
    base = String.trim_trailing(endpoint, "/")

    prefix = Keyword.get(opts, :prefix, "")
    delimiter = Keyword.get(opts, :delimiter)
    max_keys = Keyword.get(opts, :max_keys, 1000)
    cont_token = Keyword.get(opts, :continuation_token)

    params =
      %{
        "list-type" => "2",
        "prefix" => prefix,
        "max-keys" => to_string(max_keys)
      }
      |> then(fn m ->
        if delimiter && delimiter != "", do: Map.put(m, "delimiter", delimiter), else: m
      end)
      |> then(fn m ->
        if opts[:start_after], do: Map.put(m, "start-after", opts[:start_after]), else: m
      end)
      |> then(fn m ->
        if cont_token, do: Map.put(m, "continuation-token", cont_token), else: m
      end)

    query = URI.encode_query(params)
    url = "#{base}/#{config.bucket}?#{query}"

    signed_headers = sign_request("GET", url, [], "", config)

    case HTTPClient.request(url, method: :get, headers: signed_headers) do
      {:ok, %{status: 200, body: xml_body}} ->
        result = parse_list_objects_xml(xml_body)

        Logger.info(
          "CloudCache LIST #{config.bucket}: #{length(result.keys)} keys, " <>
            "#{length(result.common_prefixes)} prefixes, xml_bytes=#{byte_size(xml_body)}"
        )

        {:ok, result}

      {:ok, %{status: status, body: body}} ->
        Logger.warning(
          "CloudCache LIST failed #{config.bucket}: HTTP #{status} — #{inspect(body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error("CloudCache LIST failed #{config.bucket}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  ## Private

  defp object_url(endpoint, bucket, key) do
    base = String.trim_trailing(endpoint, "/")
    encoded_key = URI.encode(key, &(&1 == ?/ or URI.char_unreserved?(&1)))
    "#{base}/#{bucket}/#{encoded_key}"
  end

  defp bucket_url(endpoint, bucket) do
    base = String.trim_trailing(endpoint, "/")
    "#{base}/#{bucket}"
  end

  # Build AWS SigV4 Authorization header and return merged headers list.
  defp sign_request(method, url_string, extra_headers, body, %Config{} = config) do
    %URI{host: host, port: port, scheme: scheme, path: path, query: query} =
      URI.parse(url_string)

    # SigV4 requires the host header to include the port when it's non-standard.
    # URI.parse strips the port from host, so we must reconstruct it.
    host_header =
      case {scheme, port} do
        {"https", 443} -> host
        {"https", nil} -> host
        {"http", 80} -> host
        {"http", nil} -> host
        {_, port} when is_integer(port) -> "#{host}:#{port}"
        _ -> host
      end

    region = signing_region(config)
    access_key_id = config.access_key_id
    secret_key = Config.plaintext_secret(config)

    now = DateTime.utc_now()
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    # SigV4 requires query params sorted alphabetically by name
    canonical_query =
      case query do
        nil ->
          ""

        "" ->
          ""

        q ->
          q
          |> URI.decode_query()
          |> Enum.sort_by(fn {k, _} -> k end)
          |> Enum.map_join("&", fn {k, v} ->
            URI.encode(k, &URI.char_unreserved?/1) <>
              "=" <> URI.encode(v, &URI.char_unreserved?/1)
          end)
      end

    canonical_uri = uri_encode_path(path || "/")

    # Compute payload hash
    payload_hash =
      case body do
        {:sha256, hash} -> hash
        bytes -> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
      end

    # Canonical headers (must be sorted)
    base_headers = [
      {"host", host_header},
      {"x-amz-content-sha256", payload_hash},
      {"x-amz-date", amz_date}
    ]

    all_headers = (base_headers ++ extra_headers) |> Enum.sort_by(fn {k, _} -> k end)

    signed_headers_string =
      all_headers |> Enum.map(fn {k, _} -> k end) |> Enum.join(";")

    canonical_headers_string =
      all_headers
      |> Enum.map(fn {k, v} -> "#{k}:#{String.trim(v)}\n" end)
      |> Enum.join()

    canonical_request =
      Enum.join(
        [
          method,
          canonical_uri,
          canonical_query,
          canonical_headers_string,
          signed_headers_string,
          payload_hash
        ],
        "\n"
      )

    # String to sign
    credential_scope = "#{date_stamp}/#{region}/s3/aws4_request"

    string_to_sign =
      Enum.join(
        [
          "AWS4-HMAC-SHA256",
          amz_date,
          credential_scope,
          Base.encode16(:crypto.hash(:sha256, canonical_request), case: :lower)
        ],
        "\n"
      )

    # Signing key derivation
    signing_key =
      hmac_sha256("AWS4#{secret_key}", date_stamp)
      |> hmac_sha256(region)
      |> hmac_sha256("s3")
      |> hmac_sha256("aws4_request")

    signature = Base.encode16(hmac_sha256(signing_key, string_to_sign), case: :lower)

    authorization =
      "AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{credential_scope}, " <>
        "SignedHeaders=#{signed_headers_string}, Signature=#{signature}"

    [
      {"authorization", authorization},
      {"x-amz-date", amz_date},
      {"x-amz-content-sha256", payload_hash}
    ] ++ extra_headers
  end

  # Encode each path segment per the AWS SigV4 canonical URI spec.
  # Matches the server-side implementation in ExStorageServiceS3.Auth.SigV4.
  defp uri_encode_path(path) do
    path
    |> String.split("/")
    |> Enum.map(&URI.decode/1)
    |> Enum.map(&URI.encode(&1, fn byte -> URI.char_unreserved?(byte) end))
    |> Enum.join("/")
  end

  # R2 always uses "auto" for signing; everything else uses the configured region.
  defp signing_region(%Config{provider: :r2}), do: "auto"
  defp signing_region(%Config{region: r}) when is_binary(r) and r != "", do: r
  defp signing_region(_), do: "us-east-1"

  defp hmac_sha256(key, data) when is_binary(key) and is_binary(data) do
    :crypto.mac(:hmac, :sha256, key, data)
  end

  # Compatibility with callers supplying a map of header lists
  defp parse_headers(headers) when is_map(headers) do
    headers
    |> Enum.map(fn {name, value} ->
      {name, if(is_list(value), do: List.first(value), else: value)}
    end)
    |> parse_headers()
  end

  # http_fetch response headers are a list of {key, value} tuples
  defp parse_headers(headers) when is_list(headers) do
    header_map = Map.new(headers, fn {k, v} -> {String.downcase(k), v} end)

    %{
      content_length: parse_int(Map.get(header_map, "content-length")),
      etag: Map.get(header_map, "etag", "") |> String.trim("\""),
      content_type: Map.get(header_map, "content-type", "application/octet-stream"),
      last_modified: Map.get(header_map, "last-modified"),
      metadata: Map.new(for {"x-amz-meta-" <> name, value} <- header_map, do: {name, value}),
      content_encoding: Map.get(header_map, "content-encoding"),
      version_id: Map.get(header_map, "x-amz-version-id"),
      headers:
        Enum.filter(header_map, fn {name, _} ->
          name in ~w(content-encoding cache-control content-disposition content-language expires x-amz-checksum-crc32 x-amz-checksum-crc32c x-amz-checksum-crc64nvme x-amz-checksum-sha1 x-amz-checksum-sha256 x-amz-checksum-type)
        end)
    }
  end

  defp parse_headers(_), do: %{}

  defp parse_int(nil), do: nil

  defp parse_int(s) when is_binary(s) do
    case Integer.parse(s) do
      {value, ""} when value >= 0 -> value
      _ -> nil
    end
  end

  defp parse_int(n) when is_integer(n), do: n

  # Parse S3 ListObjectsV2 XML response into our standard shape.
  # Uses OTP built-in :xmerl_scan — no extra dependency needed.
  defp parse_list_objects_xml(xml) when is_binary(xml) do
    xml_charlist = :erlang.binary_to_list(xml)

    {doc, _rest} =
      :xmerl_scan.string(xml_charlist, [{:quiet, true}])

    keys =
      :xmerl_xpath.string(~c"//Contents", doc)
      |> Enum.map(fn node ->
        key = xpath_text(node, "Key")
        size = xpath_text(node, "Size") |> parse_int()
        last_modified = xpath_text(node, "LastModified")
        etag = xpath_text(node, "ETag") |> String.trim("\"")

        {key, %{size: size, last_modified: last_modified, etag: etag, updated_at: last_modified}}
      end)

    common_prefixes =
      :xmerl_xpath.string(~c"//CommonPrefixes/Prefix/text()", doc)
      |> Enum.map(&xpath_node_text/1)
      |> Enum.reject(&(&1 == ""))

    truncated =
      :xmerl_xpath.string(~c"//IsTruncated/text()", doc)
      |> case do
        [node] -> xpath_node_text(node) == "true"
        _ -> false
      end

    next_continuation_token =
      case :xmerl_xpath.string(~c"//NextContinuationToken/text()", doc) do
        [node] ->
          case xpath_node_text(node) do
            "" -> nil
            val -> val
          end

        _ ->
          nil
      end

    %{
      keys: keys,
      common_prefixes: common_prefixes,
      truncated: truncated,
      next_continuation_token: next_continuation_token
    }
  end

  defp parse_list_objects_xml(_),
    do: %{keys: [], common_prefixes: [], truncated: false, next_continuation_token: nil}

  defp xpath_text(node, tag) do
    case :xmerl_xpath.string(~c"#{tag}/text()", node) do
      [text_node | _] -> xpath_node_text(text_node)
      _ -> ""
    end
  end

  defp xpath_node_text(node) do
    case node do
      {:xmlText, _, _, _, value, _} when is_list(value) ->
        value |> List.to_string() |> String.trim()

      {:xmlText, _, _, _, value, _} when is_binary(value) ->
        String.trim(value)

      _ ->
        ""
    end
  end
end
