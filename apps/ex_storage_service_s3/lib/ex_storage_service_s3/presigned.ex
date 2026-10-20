defmodule ExStorageServiceS3.Presigned do
  @moduledoc """
  Pre-signed URL generation and validation for S3-compatible API.

  Generates time-limited URLs that grant temporary access to objects
  using AWS Signature V4 query string authentication.
  """

  alias ExStorageServiceS3.Auth.SigV4

  @default_expires 3600
  @max_expires 604_800
  @algorithm "AWS4-HMAC-SHA256"

  @doc """
  Generates a pre-signed URL for the given bucket and key.

  Options:
    - `:method` - HTTP method (default: "GET")
    - `:expires` - Expiration in seconds (default: 3600, max: 604800)
    - `:access_key_id` - AWS access key ID (required)
    - `:secret_access_key` - AWS secret access key (required)
    - `:region` - AWS region (default: "us-east-1")
    - `:host` - Host for the URL (default: "localhost:9000")
    - `:scheme` - URL scheme (default: "http")
    - `:headers` - Additional headers to sign (default: %{})
  """
  def generate_url(bucket, key, opts) do
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    method = Keyword.get(opts, :method, "GET")
    expires = opts |> Keyword.get(:expires, @default_expires) |> clamp_expires()
    region = Keyword.get(opts, :region, "us-east-1")
    host = Keyword.get(opts, :host, "localhost:9000")
    scheme = Keyword.get(opts, :scheme, "http")

    now = Keyword.get(opts, :now, DateTime.utc_now())
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    credential = "#{access_key_id}/#{date_stamp}/#{region}/s3/aws4_request"
    scope = "#{date_stamp}/#{region}/s3/aws4_request"

    path =
      "/" <>
        SigV4.uri_encode(bucket) <>
        "/" <> (key |> String.split("/") |> Enum.map_join("/", &SigV4.uri_encode/1))

    headers = [
      {"host", host}
      | Enum.map(Keyword.get(opts, :headers, %{}), fn {name, value} ->
          {String.downcase(name), value}
        end)
    ]

    signed_headers = headers |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort()

    query_params = [
      {"X-Amz-Algorithm", @algorithm},
      {"X-Amz-Credential", credential},
      {"X-Amz-Date", amz_date},
      {"X-Amz-Expires", to_string(expires)},
      {"X-Amz-SignedHeaders", Enum.join(signed_headers, ";")}
    ]

    canonical_query =
      query_params
      |> Enum.map_join("&", fn {key, value} ->
        SigV4.uri_encode(key) <> "=" <> SigV4.uri_encode(value)
      end)
      |> SigV4.canonical_query_string()

    canonical_request =
      SigV4.canonical_request(
        method,
        path,
        canonical_query,
        headers,
        signed_headers,
        "UNSIGNED-PAYLOAD"
      )

    string_to_sign = SigV4.string_to_sign(amz_date, scope, canonical_request)
    signing_key = SigV4.signing_key(secret_access_key, date_stamp, region, "s3")
    signature = SigV4.compute_signature(signing_key, string_to_sign)

    final_query = "#{canonical_query}&X-Amz-Signature=#{signature}"

    "#{scheme}://#{host}#{path}?#{final_query}"
  end

  @doc """
  Validates a pre-signed URL from query parameters on a connection.

  Returns `{:ok, conn}` if valid, `{:error, reason}` if invalid.

  `get_secret_fn` takes an access key ID and returns the secret access key or nil.
  """
  def validate_presigned(conn, get_secret_fn) do
    with {:ok, pairs} <- query_pairs(conn.query_string),
         params = Map.new(pairs),
         {:ok, algorithm} <- fetch_param(params, "X-Amz-Algorithm"),
         true <- algorithm == @algorithm,
         {:ok, credential_str} <- fetch_param(params, "X-Amz-Credential"),
         {:ok, credential} <- parse_credential(credential_str),
         {:ok, amz_date} <- fetch_param(params, "X-Amz-Date"),
         :ok <- validate_scope(credential, amz_date),
         {:ok, expires_str} <- fetch_param(params, "X-Amz-Expires"),
         {:ok, expires} <- parse_expires(expires_str),
         :ok <- check_expiration(amz_date, expires),
         {:ok, headers_str} <- fetch_param(params, "X-Amz-SignedHeaders"),
         {:ok, names, headers} <- signed_request_headers(conn, headers_str),
         {:ok, signature} <- fetch_param(params, "X-Amz-Signature"),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, signature),
         secret when is_binary(secret) <- get_secret_fn.(credential.access_key_id) do
      scope = "#{credential.date}/#{credential.region}/s3/aws4_request"

      query =
        pairs
        |> Enum.reject(fn {key, _} -> key == "X-Amz-Signature" end)
        |> Enum.map_join("&", fn {key, value} ->
          SigV4.uri_encode(key) <> "=" <> SigV4.uri_encode(value)
        end)

      payload_hash =
        case Plug.Conn.get_req_header(conn, "x-amz-content-sha256") do
          [hash] -> hash
          _ -> "UNSIGNED-PAYLOAD"
        end

      canonical =
        SigV4.canonical_request(
          conn.method,
          conn.request_path,
          query,
          headers,
          names,
          payload_hash
        )

      expected =
        SigV4.compute_signature(
          SigV4.signing_key(secret, credential.date, credential.region, "s3"),
          SigV4.string_to_sign(amz_date, scope, canonical)
        )

      if secure_compare(expected, signature),
        do: {:ok, conn},
        else: {:error, "SignatureDoesNotMatch"}
    else
      false -> {:error, "Invalid presigned authentication parameters"}
      nil -> {:error, "InvalidAccessKeyId"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query_pairs(query) do
    pairs =
      query
      |> String.split("&", trim: true)
      |> Enum.map(fn pair ->
        case String.split(pair, "=", parts: 2) do
          [key, value] -> {URI.decode(key), URI.decode(value)}
          [key] -> {URI.decode(key), ""}
        end
      end)

    auth =
      pairs
      |> Enum.filter(fn {name, _} -> String.starts_with?(name, "X-Amz-") end)
      |> Enum.map(&elem(&1, 0))

    if length(auth) == length(Enum.uniq(auth)),
      do: {:ok, pairs},
      else: {:error, "Duplicate presigned authentication parameter"}
  end

  defp parse_expires(value) do
    case Integer.parse(value) do
      {expires, ""} when expires in 1..@max_expires -> {:ok, expires}
      _ -> {:error, "Invalid X-Amz-Expires"}
    end
  end

  defp validate_scope(credential, date) do
    if credential.service == "s3" and credential.region != "" and credential.access_key_id != "" and
         Regex.match?(~r/\A[0-9]{8}\z/, credential.date) and
         String.starts_with?(date, credential.date <> "T"),
       do: :ok,
       else: {:error, "Invalid credential scope"}
  end

  defp signed_request_headers(conn, value) do
    names = String.split(value, ";")

    headers =
      if Enum.any?(conn.req_headers, fn {name, _} -> name == "host" end),
        do: conn.req_headers,
        else: [{"host", get_host(conn)} | conn.req_headers]

    if "host" in names and names == Enum.sort(Enum.uniq(names)) and
         Enum.all?(names, fn name ->
           Regex.match?(~r/\A[a-z0-9-]+\z/, name) and
             Enum.any?(headers, fn {key, _} -> key == name end)
         end),
       do: {:ok, names, headers},
       else: {:error, "Invalid SignedHeaders"}
  end

  defp clamp_expires(expires) when expires > @max_expires, do: @max_expires
  defp clamp_expires(expires) when expires < 1, do: 1
  defp clamp_expires(expires), do: expires

  defp fetch_param(params, key) do
    case Map.fetch(params, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, "Missing #{key}"}
    end
  end

  defp parse_credential(credential_str) do
    case String.split(credential_str, "/") do
      [access_key_id, date, region, service, "aws4_request"] ->
        {:ok, %{access_key_id: access_key_id, date: date, region: region, service: service}}

      _ ->
        {:error, "Malformed credential"}
    end
  end

  defp check_expiration(amz_date, expires_seconds) do
    case parse_amz_date(amz_date) do
      {:ok, request_time} ->
        expiry_time = DateTime.add(request_time, expires_seconds, :second)
        now = DateTime.utc_now()

        cond do
          DateTime.diff(request_time, now, :second) > 900 -> {:error, "RequestTimeTooSkewed"}
          DateTime.compare(now, expiry_time) != :lt -> {:error, "Request has expired"}
          true -> :ok
        end

      {:error, _} = err ->
        err
    end
  end

  defp parse_amz_date(amz_date) do
    case Regex.run(~r/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/, amz_date) do
      [_, y, mo, d, h, mi, s] ->
        case NaiveDateTime.new(
               String.to_integer(y),
               String.to_integer(mo),
               String.to_integer(d),
               String.to_integer(h),
               String.to_integer(mi),
               String.to_integer(s)
             ) do
          {:ok, ndt} -> {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}
          _ -> {:error, "Invalid date"}
        end

      _ ->
        {:error, "Invalid X-Amz-Date format"}
    end
  end

  defp get_host(conn) do
    case Plug.Conn.get_req_header(conn, "host") do
      [host | _] ->
        host

      [] ->
        # Fall back to conn.host + port
        host = conn.host || "localhost"

        if conn.port && conn.port not in [80, 443] do
          "#{host}:#{conn.port}"
        else
          host
        end
    end
  end

  defp secure_compare(a, b) when byte_size(a) == byte_size(b) do
    a_bytes = :binary.bin_to_list(a)
    b_bytes = :binary.bin_to_list(b)

    Enum.zip(a_bytes, b_bytes)
    |> Enum.reduce(0, fn {x, y}, acc -> Bitwise.bor(acc, Bitwise.bxor(x, y)) end)
    |> Kernel.==(0)
  end

  defp secure_compare(_, _), do: false
end
