defmodule ExStorageServiceCli.SigV4 do
  @moduledoc """
  AWS Signature V4 client-side request signing.

  Signs outgoing HTTP requests with the standard AWS SigV4 algorithm.
  This is the client-side counterpart to the server's SigV4 verification.
  """

  @algorithm "AWS4-HMAC-SHA256"
  alias ExStorageServiceCli.S3.Encoding
  @unsigned_headers ~w(authorization expect transfer-encoding user-agent x-amzn-trace-id)

  @doc """
  Signs an HTTP request and returns the headers to add.

  ## Parameters

    * `method` - HTTP method (e.g., "GET", "PUT")
    * `url` - Full URL string
    * `headers` - Existing headers as a keyword list or map
    * `body` - Request body (binary or "" for GET)
    * `opts` - Options:
      * `:access_key_id` - AWS access key ID (required)
      * `:secret_access_key` - AWS secret access key (required)
      * `:region` - AWS region (default: "us-east-1")
      * `:service` - AWS service name (default: "s3")
      * `:now` - Override current time (for testing)
      * `:payload_hash` - Precomputed SHA256 hex or `UNSIGNED-PAYLOAD` for streamed bodies
      * `:session_token` - Temporary credential token

  ## Returns

    A list of header tuples to merge into the request.
  """
  @spec sign_headers(String.t(), String.t(), [{String.t(), String.t()}], binary(), keyword()) ::
          [{String.t(), String.t()}]
  def sign_headers(method, url, headers, body, opts) do
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.get(opts, :region, "us-east-1")
    service = Keyword.get(opts, :service, "s3")
    now = Keyword.get(opts, :now, DateTime.utc_now())

    uri = URI.parse(url)
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    host = build_host(uri)
    payload_hash = payload_hash(body, opts)
    generated = [{"x-amz-date", amz_date}, {"host", host}]

    generated =
      if Keyword.get(opts, :include_content_sha256, true),
        do: List.insert_at(generated, 1, {"x-amz-content-sha256", payload_hash}),
        else: generated

    generated = add_session_token(generated, opts)
    replaced = Enum.map(generated, &elem(&1, 0))

    headers =
      Enum.reject(headers, fn {key, _} -> String.downcase(to_string(key)) in replaced end) ++
        generated

    # Build canonical request
    canonical_headers = build_canonical_headers(headers)
    signed_headers = build_signed_headers(headers)

    canonical_request =
      build_canonical_request(
        method,
        uri.path || "/",
        uri.query || "",
        canonical_headers,
        signed_headers,
        payload_hash
      )

    # Build string to sign
    scope = "#{date_stamp}/#{region}/#{service}/aws4_request"
    string_to_sign = build_string_to_sign(amz_date, scope, canonical_request)

    # Compute signature
    signing_key = derive_signing_key(secret_access_key, date_stamp, region, service)
    signature = hmac_hex(signing_key, string_to_sign)

    # Build authorization header
    credential = "#{access_key_id}/#{scope}"

    authorization =
      "#{@algorithm} Credential=#{credential}, SignedHeaders=#{signed_headers}, Signature=#{signature}"

    [{"authorization", authorization} | generated]
  end

  @doc """
  Generates a presigned URL for the given parameters.

  ## Parameters

    * `method` - HTTP method
    * `url` - Base URL, including any existing query pairs
    * `opts` - Options:
      * `:access_key_id` - required
      * `:secret_access_key` - required
      * `:region` - default "us-east-1"
      * `:expires` - seconds until expiry (default 3600)
      * `:now` - override time for testing
      * `:session_token` - Temporary credential token
  """
  @spec presign_url(String.t(), String.t(), keyword()) :: String.t()
  def presign_url(method, url, opts) do
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.get(opts, :region, "us-east-1")
    service = Keyword.get(opts, :service, "s3")
    expires = Keyword.get(opts, :expires, 3600)

    unless is_integer(expires) and expires in 1..604_800 do
      raise ArgumentError, "presign expiry must be an integer between 1 and 604800 seconds"
    end

    now = Keyword.get(opts, :now, DateTime.utc_now())

    uri = URI.parse(url)
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    host = build_host(uri)
    credential = "#{access_key_id}/#{date_stamp}/#{region}/#{service}/aws4_request"
    scope = "#{date_stamp}/#{region}/#{service}/aws4_request"

    query_params =
      Encoding.query_pairs(uri.query) ++
        [
          {"X-Amz-Algorithm", @algorithm},
          {"X-Amz-Credential", credential},
          {"X-Amz-Date", amz_date},
          {"X-Amz-Expires", to_string(expires)},
          {"X-Amz-SignedHeaders", "host"}
        ]

    query_params =
      case Keyword.get(opts, :session_token) do
        nil -> query_params
        token -> query_params ++ [{opts[:session_token_query] || "X-Amz-Security-Token", token}]
      end

    canonical_query = Encoding.query(query_params)

    canonical_headers = "host:#{host}\n"
    signed_headers = "host"
    payload_hash = "UNSIGNED-PAYLOAD"

    canonical_request =
      [
        method,
        Encoding.url_path(uri.path),
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
      ]
      |> Enum.join("\n")

    string_to_sign = build_string_to_sign(amz_date, scope, canonical_request)
    signing_key = derive_signing_key(secret_access_key, date_stamp, region, service)
    signature = hmac_hex(signing_key, string_to_sign)

    %{
      uri
      | path: Encoding.url_path(uri.path),
        query: canonical_query <> "&X-Amz-Signature=" <> signature
    }
    |> URI.to_string()
  end

  # Private helpers

  defp build_host(%URI{host: host, port: port, scheme: scheme}) do
    default_port = if scheme == "https", do: 443, else: 80
    host = String.downcase(host)
    host = if String.contains?(host, ":"), do: "[#{host}]", else: host

    if port && port != default_port do
      "#{host}:#{port}"
    else
      host
    end
  end

  defp add_session_token(headers, opts) do
    case Keyword.get(opts, :session_token) do
      nil ->
        headers

      token when is_binary(token) ->
        headers ++ [{opts[:session_token_header] || "x-amz-security-token", token}]

      _ ->
        raise ArgumentError, "session_token must be a binary"
    end
  end

  defp build_canonical_headers(headers) do
    headers
    |> canonical_header_pairs()
    |> Enum.map(fn {k, values} -> "#{k}:#{Enum.join(values, ",")}\n" end)
    |> Enum.join()
  end

  defp build_signed_headers(headers) do
    headers
    |> canonical_header_pairs()
    |> Enum.map(&elem(&1, 0))
    |> Enum.join(";")
  end

  defp build_canonical_request(
         method,
         path,
         query,
         canonical_headers,
         signed_headers,
         payload_hash
       ) do
    canonical_path = Encoding.url_path(path)
    canonical_query = query |> Encoding.query_pairs() |> Encoding.query()

    [method, canonical_path, canonical_query, canonical_headers, signed_headers, payload_hash]
    |> Enum.join("\n")
  end

  defp canonical_header_pairs(headers) do
    headers
    |> Enum.map(fn {key, value} ->
      {String.downcase(to_string(key)), value |> to_string() |> String.split() |> Enum.join(" ")}
    end)
    |> Enum.reject(fn {key, _} -> key in @unsigned_headers end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp payload_hash(body, opts) do
    case Keyword.fetch(opts, :payload_hash) do
      :error ->
        hash_payload(body)

      {:ok, "UNSIGNED-PAYLOAD" = hash} ->
        hash

      {:ok, marker}
      when marker in [
             "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
             "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
             "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
           ] ->
        marker

      {:ok, hash} when is_binary(hash) ->
        if Regex.match?(~r/\A[0-9a-fA-F]{64}\z/, hash),
          do: String.downcase(hash),
          else:
            raise(ArgumentError, "payload_hash must be a SHA256 hex digest or UNSIGNED-PAYLOAD")

      _ ->
        raise ArgumentError, "payload_hash must be a SHA256 hex digest or UNSIGNED-PAYLOAD"
    end
  end

  defp build_string_to_sign(amz_date, scope, canonical_request) do
    hashed = :crypto.hash(:sha256, canonical_request) |> Base.encode16(case: :lower)
    "#{@algorithm}\n#{amz_date}\n#{scope}\n#{hashed}"
  end

  defp derive_signing_key(secret, date, region, service) do
    ("AWS4" <> secret)
    |> hmac(date)
    |> hmac(region)
    |> hmac(service)
    |> hmac("aws4_request")
  end

  defp hmac(key, data) do
    :crypto.mac(:hmac, :sha256, key, data)
  end

  defp hmac_hex(key, data) do
    :crypto.mac(:hmac, :sha256, key, data) |> Base.encode16(case: :lower)
  end

  defp hash_payload(body) when is_binary(body) do
    :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
  end
end
