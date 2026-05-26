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

  @doc """
  Upload an object to the remote S3/R2 bucket.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec put_object(Config.t(), String.t(), iodata(), String.t(), map()) ::
          :ok | {:error, term()}
  def put_object(%Config{} = config, key, data, content_type, _metadata \\ %{}) do
    body = IO.iodata_to_binary(data)
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    headers = [{"content-type", content_type}]

    signed_headers =
      sign_request("PUT", url, headers, body, config)

    case Req.put(url, body: body, headers: signed_headers) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.debug("CloudCache PUT #{config.bucket}/#{key} → #{endpoint} (#{status})")
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

  @doc """
  Download an object from the remote S3/R2 bucket.

  Returns `{:ok, body_binary}` on success,
  `{:error, :not_found}` for 404, or `{:error, reason}`.
  """
  @spec get_object(Config.t(), String.t()) ::
          {:ok, binary()} | {:error, :not_found | term()}
  def get_object(%Config{} = config, key) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    signed_headers = sign_request("GET", url, [], "", config)

    case Req.get(url, headers: signed_headers) do
      {:ok, %{status: 200, body: body}} ->
        body_bin = if is_binary(body), do: body, else: Jason.encode!(body)
        {:ok, body_bin}

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

  @doc """
  Get metadata (headers) for an object without downloading the body.

  Returns `{:ok, headers_map}` with at least `:content_length`, `:etag`,
  `:content_type`, `:last_modified`.
  """
  @spec head_object(Config.t(), String.t()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def head_object(%Config{} = config, key) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    signed_headers = sign_request("HEAD", url, [], "", config)

    case Req.head(url, headers: signed_headers) do
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
  def delete_object(%Config{} = config, key) do
    endpoint = Config.endpoint_url(config)
    url = object_url(endpoint, config.bucket, key)

    signed_headers = sign_request("DELETE", url, [], "", config)

    case Req.delete(url, headers: signed_headers) do
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
    endpoint = Config.endpoint_url(config)
    url = bucket_url(endpoint, config.bucket)

    signed_headers = sign_request("HEAD", url, [], "", config)

    case Req.head(url, headers: signed_headers) do
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

  ## Private

  defp object_url(endpoint, bucket, key) do
    base = String.trim_trailing(endpoint, "/")
    encoded_key = URI.encode(key, &URI.char_unreserved?/1)
    "#{base}/#{bucket}/#{encoded_key}"
  end

  defp bucket_url(endpoint, bucket) do
    base = String.trim_trailing(endpoint, "/")
    "#{base}/#{bucket}"
  end

  # Build AWS SigV4 Authorization header and return merged headers list.
  defp sign_request(method, url_string, extra_headers, body, %Config{} = config) do
    %URI{host: host, path: path, query: query} = URI.parse(url_string)

    region = signing_region(config)
    access_key_id = config.access_key_id
    secret_key = Config.plaintext_secret(config)

    now = DateTime.utc_now()
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")

    canonical_query = query || ""
    canonical_uri = path || "/"

    # Compute payload hash
    payload_hash = Base.encode16(:crypto.hash(:sha256, body), case: :lower)

    # Canonical headers (must be sorted)
    base_headers = [
      {"host", host},
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

  # R2 always uses "auto" for signing; everything else uses the configured region.
  defp signing_region(%Config{provider: :r2}), do: "auto"
  defp signing_region(%Config{region: r}) when is_binary(r) and r != "", do: r
  defp signing_region(_), do: "us-east-1"

  defp hmac_sha256(key, data) when is_binary(key) and is_binary(data) do
    :crypto.mac(:hmac, :sha256, key, data)
  end

  defp parse_headers(headers) when is_list(headers) do
    header_map = Map.new(headers, fn {k, v} -> {String.downcase(k), v} end)

    %{
      content_length: parse_int(Map.get(header_map, "content-length")),
      etag: Map.get(header_map, "etag", "") |> String.trim("\""),
      content_type: Map.get(header_map, "content-type", "application/octet-stream"),
      last_modified: Map.get(header_map, "last-modified")
    }
  end

  defp parse_headers(_), do: %{}

  defp parse_int(nil), do: 0
  defp parse_int(s) when is_binary(s), do: String.to_integer(s)
  defp parse_int(n) when is_integer(n), do: n
end
