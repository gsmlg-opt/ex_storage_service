defmodule ExStorageService.S3.Presigned do
  @moduledoc """
  Pre-signed URL generation and validation for S3-compatible API.

  Generates time-limited URLs that grant temporary access to objects
  using AWS Signature V4 query string authentication.
  """

  alias ExStorageService.S3.Auth.SigV4

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

    path = "/#{bucket}/#{key}"

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
      [method, path, canonical_query, canonical_headers, signed_headers, payload_hash]
      |> Enum.join("\n")

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
    params = conn.query_params

    with {:ok, algorithm} <- fetch_param(params, "X-Amz-Algorithm"),
         true <- algorithm == @algorithm,
         {:ok, credential_str} <- fetch_param(params, "X-Amz-Credential"),
         {:ok, credential} <- parse_credential(credential_str),
         {:ok, amz_date} <- fetch_param(params, "X-Amz-Date"),
         {:ok, expires_str} <- fetch_param(params, "X-Amz-Expires"),
         {:ok, _signed_headers} <- fetch_param(params, "X-Amz-SignedHeaders"),
         {:ok, claimed_signature} <- fetch_param(params, "X-Amz-Signature"),
         secret when not is_nil(secret) <- get_secret_fn.(credential.access_key_id) do
      # Check expiration
      expires = String.to_integer(expires_str)

      case check_expiration(amz_date, expires) do
        :ok ->
          # Rebuild and verify signature
          scope = "#{credential.date}/#{credential.region}/#{credential.service}/aws4_request"

          # Rebuild canonical query without X-Amz-Signature
          query_params =
            params
            |> Map.delete("X-Amz-Signature")
            |> Enum.sort()
            |> Enum.map(fn {k, v} ->
              "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}"
            end)
            |> Enum.join("&")

          host = get_host(conn)
          canonical_headers = "host:#{host}\n"
          signed_headers = "host"
          payload_hash = "UNSIGNED-PAYLOAD"

          canonical_request =
            [
              conn.method,
              conn.request_path,
              query_params,
              canonical_headers,
              signed_headers,
              payload_hash
            ]
            |> Enum.join("\n")

          string_to_sign = SigV4.string_to_sign(amz_date, scope, canonical_request)

          signing_key =
            SigV4.signing_key(secret, credential.date, credential.region, credential.service)

          expected_signature = SigV4.compute_signature(signing_key, string_to_sign)

          if secure_compare(expected_signature, claimed_signature) do
            {:ok, conn}
          else
            {:error, "SignatureDoesNotMatch"}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      false -> {:error, "Invalid algorithm"}
      nil -> {:error, "InvalidAccessKeyId"}
      {:error, reason} -> {:error, reason}
    end
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

        if DateTime.compare(now, expiry_time) == :lt do
          :ok
        else
          {:error, "Request has expired"}
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
