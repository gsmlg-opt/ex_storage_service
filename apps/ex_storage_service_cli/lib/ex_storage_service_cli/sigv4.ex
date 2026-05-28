defmodule ExStorageServiceCli.SigV4 do
  @moduledoc """
  AWS Signature V4 client-side request signing.

  Signs outgoing HTTP requests with the standard AWS SigV4 algorithm.
  This is the client-side counterpart to the server's SigV4 verification.
  """

  @algorithm "AWS4-HMAC-SHA256"

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

    # Build host header
    host = build_host(uri)

    # Merge host and x-amz-date into headers
    headers =
      headers
      |> ensure_header("host", host)
      |> ensure_header("x-amz-date", amz_date)

    # Compute payload hash
    payload_hash = hash_payload(body)
    headers = ensure_header(headers, "x-amz-content-sha256", payload_hash)

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

    [
      {"authorization", authorization},
      {"x-amz-date", amz_date},
      {"x-amz-content-sha256", payload_hash},
      {"host", host}
    ]
  end

  @doc """
  Generates a presigned URL for the given parameters.

  ## Parameters

    * `method` - HTTP method
    * `url` - Base URL (without query params)
    * `opts` - Options:
      * `:access_key_id` - required
      * `:secret_access_key` - required
      * `:region` - default "us-east-1"
      * `:expires` - seconds until expiry (default 3600)
      * `:now` - override time for testing
  """
  @spec presign_url(String.t(), String.t(), keyword()) :: String.t()
  def presign_url(method, url, opts) do
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.get(opts, :region, "us-east-1")
    service = Keyword.get(opts, :service, "s3")
    expires = Keyword.get(opts, :expires, 3600)
    now = Keyword.get(opts, :now, DateTime.utc_now())

    uri = URI.parse(url)
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    host = build_host(uri)
    credential = "#{access_key_id}/#{date_stamp}/#{region}/#{service}/aws4_request"
    scope = "#{date_stamp}/#{region}/#{service}/aws4_request"

    query_params = [
      {"X-Amz-Algorithm", @algorithm},
      {"X-Amz-Credential", credential},
      {"X-Amz-Date", amz_date},
      {"X-Amz-Expires", to_string(expires)},
      {"X-Amz-SignedHeaders", "host"}
    ]

    canonical_query =
      query_params
      |> Enum.sort()
      |> Enum.map(fn {k, v} -> "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}" end)
      |> Enum.join("&")

    canonical_headers = "host:#{host}\n"
    signed_headers = "host"
    payload_hash = "UNSIGNED-PAYLOAD"

    canonical_request =
      [method, uri.path || "/", canonical_query, canonical_headers, signed_headers, payload_hash]
      |> Enum.join("\n")

    string_to_sign = build_string_to_sign(amz_date, scope, canonical_request)
    signing_key = derive_signing_key(secret_access_key, date_stamp, region, service)
    signature = hmac_hex(signing_key, string_to_sign)

    "#{uri.scheme}://#{host}#{uri.path}?#{canonical_query}&X-Amz-Signature=#{signature}"
  end

  # Private helpers

  defp build_host(%URI{host: host, port: port, scheme: scheme}) do
    default_port = if scheme == "https", do: 443, else: 80

    if port && port != default_port do
      "#{host}:#{port}"
    else
      host
    end
  end

  defp ensure_header(headers, key, value) do
    key_down = String.downcase(key)

    if Enum.any?(headers, fn {k, _} -> String.downcase(k) == key_down end) do
      headers
    else
      [{key, value} | headers]
    end
  end

  defp build_canonical_headers(headers) do
    headers
    |> Enum.map(fn {k, v} -> {String.downcase(k), String.trim(v)} end)
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map(fn {k, v} -> "#{k}:#{v}\n" end)
    |> Enum.join()
  end

  defp build_signed_headers(headers) do
    headers
    |> Enum.map(fn {k, _} -> String.downcase(k) end)
    |> Enum.sort()
    |> Enum.uniq()
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
    # URI-encode each path segment individually
    canonical_path =
      path
      |> String.split("/")
      |> Enum.map(&URI.encode/1)
      |> Enum.join("/")

    canonical_query = canonicalize_query(query)

    [method, canonical_path, canonical_query, canonical_headers, signed_headers, payload_hash]
    |> Enum.join("\n")
  end

  defp canonicalize_query(""), do: ""

  defp canonicalize_query(query) do
    query
    |> URI.decode_query()
    |> Enum.sort()
    |> Enum.map(fn {k, v} -> "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}" end)
    |> Enum.join("&")
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
