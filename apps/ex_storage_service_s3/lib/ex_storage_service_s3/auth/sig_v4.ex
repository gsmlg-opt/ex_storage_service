defmodule ExStorageServiceS3.Auth.SigV4 do
  @moduledoc """
  AWS Signature Version 4 authentication for S3-compatible API.

  When auth is enabled (config :s3_auth_enabled), the plug:
  1. Parses the Authorization header
  2. Looks up the access key in IAM
  3. Verifies the signature using the decrypted secret
  4. Sets conn.assigns[:user_id] for downstream authorization

  When auth is disabled (default), all requests pass through (bypass mode).
  """

  import Plug.Conn

  alias ExStorageService.IAM.AccessKey
  alias ExStorageService.IAM.User

  @doc """
  Plug-compatible init/1 callback.
  """
  def init(opts), do: opts

  @doc """
  Plug-compatible call/2 callback.

  Checks if authentication is configured. If not, passes the request through (bypass mode).
  If configured, verifies the AWS Signature V4 on the request using IAM access keys.

  Options:
    - `:get_secret_fn` - optional override function `(access_key_id) -> secret_access_key | nil`.
      If not provided, defaults to looking up keys via IAM.AccessKey.
  """
  def call(conn, opts) do
    cond do
      # Health endpoint must be accessible without authentication
      conn.request_path == "/health" ->
        conn

      # Presigned requests already verified by router — skip SigV4
      conn.assigns[:presigned_auth] ->
        conn

      auth_configured?() ->
        get_secret_fn = Keyword.get(opts, :get_secret_fn) || (&iam_get_secret/1)

        case verify_request_with_iam(conn, get_secret_fn) do
          {:ok, conn} ->
            conn

          {:error, reason} ->
            request_id = generate_request_id()

            body =
              ExStorageServiceS3.XML.error_response(
                "AccessDenied",
                reason,
                conn.request_path,
                request_id
              )

            conn
            |> put_resp_header("content-type", "application/xml")
            |> put_resp_header("x-amz-request-id", request_id)
            |> send_resp(403, body)
            |> halt()
        end

      true ->
        # Bypass mode: no auth configured, allow all requests
        conn
    end
  end

  @doc """
  Verifies the AWS Signature V4 on a request.

  `get_secret_fn` is a function that takes an access key ID and returns
  the corresponding secret access key, or nil if not found.
  """
  def verify_request(conn, get_secret_fn) do
    with {:ok, auth_header} <- get_authorization_header(conn),
         {:ok, parsed} <- parse_authorization(auth_header),
         secret when not is_nil(secret) <- get_secret_fn.(parsed.credential.access_key_id) do
      # Extract components for verification
      %{
        credential: credential,
        signed_headers: signed_headers_list,
        signature: claimed_signature
      } = parsed

      scope = "#{credential.date}/#{credential.region}/#{credential.service}/aws4_request"

      # Get the payload hash
      payload_hash = get_payload_hash(conn)

      # Build canonical request
      canonical =
        canonical_request(
          conn.method,
          conn.request_path,
          conn.query_string || "",
          conn.req_headers,
          signed_headers_list,
          payload_hash
        )

      # Build string to sign
      datetime = get_amz_date(conn)
      sts = string_to_sign(datetime, scope, canonical)

      # Compute expected signature
      key = signing_key(secret, credential.date, credential.region, credential.service)
      expected_signature = compute_signature(key, sts)

      if secure_compare(expected_signature, claimed_signature) do
        {:ok, conn}
      else
        {:error, "SignatureDoesNotMatch"}
      end
    else
      nil -> {:error, "InvalidAccessKeyId"}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Parses the Authorization header into its components.

  Expected format:
    AWS4-HMAC-SHA256 Credential=AKID/20230101/us-east-1/s3/aws4_request,
    SignedHeaders=host;x-amz-date, Signature=abcdef1234

  Returns:
    {:ok, %{credential: %{...}, signed_headers: [...], signature: "..."}}
  """
  def parse_authorization(header) do
    case Regex.run(
           ~r/AWS4-HMAC-SHA256\s+Credential=([^,]+),\s*SignedHeaders=([^,]+),\s*Signature=(\S+)/,
           header
         ) do
      [_, credential_str, signed_headers_str, signature] ->
        case String.split(credential_str, "/") do
          [access_key_id, date, region, service, "aws4_request"] ->
            {:ok,
             %{
               credential: %{
                 access_key_id: access_key_id,
                 date: date,
                 region: region,
                 service: service
               },
               signed_headers: String.split(signed_headers_str, ";"),
               signature: signature
             }}

          _ ->
            {:error, "Malformed Credential in Authorization header"}
        end

      _ ->
        {:error, "Malformed Authorization header"}
    end
  end

  @doc """
  Builds the canonical request string for AWS Signature V4.
  """
  def canonical_request(method, path, query_string, headers, signed_headers, payload_hash) do
    canonical_uri = uri_encode_path(path)
    canonical_query = canonical_query_string(query_string)

    canonical_headers_str =
      signed_headers
      |> Enum.map(fn name ->
        value =
          headers
          |> Enum.find_value("", fn {k, v} ->
            if String.downcase(k) == name, do: String.trim(v)
          end)

        "#{name}:#{value}\n"
      end)
      |> Enum.join()

    signed_headers_str = Enum.join(signed_headers, ";")

    [
      method,
      canonical_uri,
      canonical_query,
      canonical_headers_str,
      signed_headers_str,
      payload_hash
    ]
    |> Enum.join("\n")
  end

  @doc """
  Builds the string to sign for AWS Signature V4.
  """
  def string_to_sign(datetime, scope, canonical_request) do
    canonical_hash = sha256_hex(canonical_request)

    [
      "AWS4-HMAC-SHA256",
      datetime,
      scope,
      canonical_hash
    ]
    |> Enum.join("\n")
  end

  @doc """
  Derives the signing key for AWS Signature V4.
  """
  def signing_key(secret, date, region, service) do
    ("AWS4" <> secret)
    |> hmac_sha256(date)
    |> hmac_sha256(region)
    |> hmac_sha256(service)
    |> hmac_sha256("aws4_request")
  end

  @doc """
  Computes the final signature using the signing key and string to sign.
  """
  def compute_signature(signing_key, string_to_sign) do
    signing_key
    |> hmac_sha256(string_to_sign)
    |> Base.encode16(case: :lower)
  end

  # IAM integration

  defp verify_request_with_iam(conn, get_secret_fn) do
    case verify_request(conn, get_secret_fn) do
      {:ok, conn} ->
        # Extract the access key ID from the Authorization header and set user_id
        case get_authorization_header(conn) do
          {:ok, auth_header} ->
            case parse_authorization(auth_header) do
              {:ok, parsed} ->
                access_key_id = parsed.credential.access_key_id

                case resolve_user_id(access_key_id) do
                  {:ok, user_id} ->
                    # Check if user is active
                    case User.get_user(user_id) do
                      {:ok, %{status: :active}} ->
                        {:ok, Plug.Conn.assign(conn, :user_id, user_id)}

                      {:ok, %{status: :suspended}} ->
                        {:error, "User account is suspended"}

                      _ ->
                        {:error, "User not found"}
                    end

                  {:error, reason} ->
                    {:error, reason}
                end

              _ ->
                {:ok, conn}
            end

          _ ->
            {:ok, conn}
        end

      error ->
        error
    end
  end

  defp iam_get_secret(access_key_id) do
    case AccessKey.lookup_by_access_key_id(access_key_id) do
      {:ok, %{secret_access_key: secret, status: :active}} -> secret
      {:ok, %{status: :inactive}} -> nil
      _ -> nil
    end
  end

  defp resolve_user_id(access_key_id) do
    case AccessKey.lookup_by_access_key_id(access_key_id) do
      {:ok, %{user_id: user_id}} -> {:ok, user_id}
      _ -> {:error, "InvalidAccessKeyId"}
    end
  end

  # Private helpers

  defp auth_configured? do
    Application.get_env(:ex_storage_service, :s3_auth_enabled, false)
  end

  defp get_authorization_header(conn) do
    case get_req_header(conn, "authorization") do
      [header | _] -> {:ok, header}
      [] -> {:error, "Missing Authorization header"}
    end
  end

  defp get_amz_date(conn) do
    case get_req_header(conn, "x-amz-date") do
      [date | _] -> date
      [] -> ""
    end
  end

  defp get_payload_hash(conn) do
    case get_req_header(conn, "x-amz-content-sha256") do
      [hash | _] -> hash
      [] -> "UNSIGNED-PAYLOAD"
    end
  end

  defp uri_encode_path(path) do
    path
    |> String.split("/")
    |> Enum.map(&URI.decode/1)
    |> Enum.map(&URI.encode_www_form/1)
    |> Enum.map(&String.replace(&1, "+", "%20"))
    |> Enum.join("/")
  end

  defp canonical_query_string(""), do: ""

  defp canonical_query_string(query_string) do
    query_string
    |> URI.decode_query()
    |> Enum.sort()
    |> Enum.map(fn {k, v} ->
      URI.encode_www_form(k) <> "=" <> URI.encode_www_form(v)
    end)
    |> Enum.join("&")
  end

  defp sha256_hex(data) do
    :crypto.hash(:sha256, data)
    |> Base.encode16(case: :lower)
  end

  defp hmac_sha256(key, data) do
    :crypto.mac(:hmac, :sha256, key, data)
  end

  defp secure_compare(a, b) when byte_size(a) == byte_size(b) do
    a_bytes = :binary.bin_to_list(a)
    b_bytes = :binary.bin_to_list(b)

    Enum.zip(a_bytes, b_bytes)
    |> Enum.reduce(0, fn {x, y}, acc -> Bitwise.bor(acc, Bitwise.bxor(x, y)) end)
    |> Kernel.==(0)
  end

  defp secure_compare(_, _), do: false

  defp generate_request_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)
  end
end
