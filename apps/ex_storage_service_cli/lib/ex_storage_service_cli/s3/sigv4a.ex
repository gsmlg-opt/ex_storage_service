defmodule ExStorageServiceCli.S3.SigV4a do
  @moduledoc "AWS SigV4a request signing with credential-derived P-256 keys and OTP crypto."
  alias ExStorageServiceCli.S3.Encoding

  @algorithm "AWS4-ECDSA-P256-SHA256"
  @order 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
  @unsigned ~w(authorization expect transfer-encoding user-agent x-amzn-trace-id)
  @generated ~w(host x-amz-date x-amz-region-set x-amz-content-sha256 x-amz-security-token)

  @doc "Returns authentication headers; options follow SigV4, with :signing_region_set."
  def sign_headers(method, url, headers, body, opts) do
    uri = URI.parse(url)
    {date, stamp, scope} = scope(opts)
    hash = payload_hash(body, opts)

    generated = [
      {"host", host(uri)},
      {"x-amz-date", stamp},
      {"x-amz-region-set", region_set(opts)}
    ]

    generated =
      if Keyword.get(opts, :include_content_sha256, true),
        do: generated ++ [{"x-amz-content-sha256", hash}],
        else: generated

    token = token(opts)
    token_header = if token, do: [{"x-amz-security-token", token}], else: []
    signed_token = if opts[:omit_session_token], do: [], else: token_header
    headers = clean(headers) ++ generated ++ signed_token
    {canonical_headers, signed_headers} = canonical_headers(headers)
    canonical = canonical_request(method, uri, canonical_headers, signed_headers, hash, opts)
    signature = sign_message(string_to_sign(stamp, scope, canonical), opts)

    credential =
      Keyword.fetch!(opts, :access_key_id) <>
        "/" <> date <> "/" <> service(opts) <> "/aws4_request"

    authorization =
      "#{@algorithm} Credential=#{credential}, SignedHeaders=#{signed_headers}, Signature=#{signature}"

    [{"authorization", authorization} | generated ++ token_header]
  end

  @doc "Presigns a URL, retaining query duplicates and optional :headers required by the caller."
  def presign_url(method, url, opts) do
    expires = Keyword.get(opts, :expires, 3600)

    unless is_integer(expires) and expires in 1..604_800,
      do: raise(ArgumentError, "presign expiry must be an integer between 1 and 604800 seconds")

    uri = URI.parse(url)
    {_date, stamp, scope} = scope(opts)
    headers = clean(Keyword.get(opts, :headers, [])) ++ [{"host", host(uri)}]
    {canonical_headers, signed_headers} = canonical_headers(headers)
    credential = Keyword.fetch!(opts, :access_key_id) <> "/" <> scope

    query =
      Encoding.query_pairs(uri.query) ++
        [
          {"X-Amz-Algorithm", @algorithm},
          {"X-Amz-Credential", credential},
          {"X-Amz-Date", stamp},
          {"X-Amz-Expires", to_string(expires)},
          {"X-Amz-Region-Set", region_set(opts)},
          {"X-Amz-SignedHeaders", signed_headers}
        ]

    token = token(opts)
    token_query = if token, do: [{"X-Amz-Security-Token", token}], else: []
    signed_query = if opts[:omit_session_token], do: query, else: query ++ token_query
    signing_uri = %{uri | query: Encoding.query(signed_query)}
    hash = payload_hash("", Keyword.put_new(opts, :payload_hash, "UNSIGNED-PAYLOAD"))

    canonical =
      canonical_request(method, signing_uri, canonical_headers, signed_headers, hash, opts)

    signature = sign_message(string_to_sign(stamp, scope, canonical), opts)

    %{
      uri
      | path: Encoding.url_path(uri.path),
        query: Encoding.query(query ++ token_query) <> "&X-Amz-Signature=" <> signature
    }
    |> URI.to_string()
  end

  @doc false
  def public_key(opts) do
    {public, _private} = :crypto.generate_key(:ecdh, :secp256r1, private_key(opts))
    public
  end

  @doc false
  def sign_message(message, opts) do
    :crypto.sign(:ecdsa, :sha256, message, [private_key(opts), :secp256r1])
    |> Base.encode16(case: :lower)
  end

  # AWS's SP800-108 counter-mode KDF: AWS4A secret, algorithm label, access key
  # context and an octet counter. Reject values above N-2, then add one.
  defp private_key(opts),
    do: derive(Keyword.fetch!(opts, :access_key_id), Keyword.fetch!(opts, :secret_access_key), 1)

  defp derive(_access, _secret, 255), do: raise(ArgumentError, "SigV4a key derivation failed")

  defp derive(access, secret, counter) do
    input = <<1::32>> <> @algorithm <> <<0>> <> access <> <<counter, 256::32>>
    value = :crypto.mac(:hmac, :sha256, "AWS4A" <> secret, input) |> :binary.decode_unsigned()
    if value <= @order - 2, do: <<value + 1::256>>, else: derive(access, secret, counter + 1)
  end

  defp canonical_request(method, uri, headers, signed_headers, hash, opts) do
    path = if opts[:normalize_uri_path], do: normalize(uri.path || "/"), else: uri.path || "/"
    path = if opts[:double_uri_encode], do: Encoding.path(path), else: Encoding.url_path(path)
    query = uri.query |> Encoding.query_pairs() |> Encoding.query()
    Enum.join([method, path, query, headers, signed_headers, hash], "\n")
  end

  defp normalize(path) do
    parts = String.split(path, "/")

    stack =
      Enum.reduce(parts, [], fn
        part, acc when part in ["", "."] -> acc
        "..", [_ | acc] -> acc
        "..", [] -> []
        part, acc -> [part | acc]
      end)

    normalized = "/" <> Enum.join(Enum.reverse(stack), "/")
    trailing = List.last(parts) in ["", ".", ".."]
    if trailing and normalized != "/", do: normalized <> "/", else: normalized
  end

  defp canonical_headers(headers) do
    pairs =
      headers
      |> Enum.map(fn {name, value} ->
        {String.downcase(to_string(name)),
         value |> to_string() |> String.split() |> Enum.join(" ")}
      end)
      |> Enum.reject(fn {name, _} -> name in @unsigned end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.sort_by(&elem(&1, 0))

    {Enum.map_join(pairs, fn {name, values} -> name <> ":" <> Enum.join(values, ",") <> "\n" end),
     Enum.map_join(pairs, ";", &elem(&1, 0))}
  end

  defp clean(headers),
    do:
      Enum.reject(headers, fn {name, _} ->
        String.downcase(to_string(name)) in @generated or
          String.downcase(to_string(name)) == "authorization"
      end)

  defp scope(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    date = Calendar.strftime(now, "%Y%m%d")

    {date, Calendar.strftime(now, "%Y%m%dT%H%M%SZ"),
     date <> "/" <> service(opts) <> "/aws4_request"}
  end

  defp service(opts), do: Keyword.get(opts, :service, "s3")

  defp region_set(opts) do
    value = opts[:signing_region_set] || opts[:region] || "*"
    value = if is_list(value), do: Enum.join(value, ","), else: value

    if is_binary(value) and
         Regex.match?(~r/\A(?:\*|[a-z0-9*-]+)(?:,(?:\*|[a-z0-9*-]+))*\z/, value),
       do: value,
       else: raise(ArgumentError, "invalid SigV4a signing region set")
  end

  defp token(opts) do
    case opts[:session_token] do
      nil -> nil
      token when is_binary(token) -> token
      _ -> raise ArgumentError, "session_token must be a binary"
    end
  end

  defp host(%URI{host: host, port: port, scheme: scheme}) do
    host = String.downcase(host)
    host = if String.contains?(host, ":"), do: "[#{host}]", else: host

    if port && port != if(scheme == "https", do: 443, else: 80),
      do: host <> ":" <> to_string(port),
      else: host
  end

  defp payload_hash(body, opts) do
    case Keyword.fetch(opts, :payload_hash) do
      :error when is_binary(body) ->
        :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

      {:ok, "UNSIGNED-PAYLOAD"} ->
        "UNSIGNED-PAYLOAD"

      {:ok, marker}
      when marker in [
             "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
             "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
             "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
           ] ->
        marker

      {:ok, hash} when is_binary(hash) ->
        if Regex.match?(~r/\A[0-9a-fA-F]{64}\z/, hash),
          do: String.downcase(hash),
          else: raise(ArgumentError, "invalid SigV4a payload hash")

      _ ->
        raise ArgumentError,
              "SigV4a streaming bodies require a precomputed SHA256 hash or UNSIGNED-PAYLOAD"
    end
  end

  defp string_to_sign(stamp, scope, canonical),
    do:
      @algorithm <>
        "\n" <>
        stamp <>
        "\n" <> scope <> "\n" <> (:crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower))
end
