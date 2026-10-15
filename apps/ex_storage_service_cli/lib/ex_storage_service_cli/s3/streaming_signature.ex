defmodule ExStorageServiceCli.S3.StreamingSignature do
  @moduledoc "Bounded SigV4/SigV4a aws-chunked payload chains and checksum trailers."
  alias ExStorageServiceCli.{Error, S3.Checksum, S3.SigV4a}
  @derive {Inspect, except: [:key, :previous]}
  defstruct [
    :key,
    :previous,
    :timestamp,
    :scope,
    :checksum,
    :algorithm,
    :expected_length,
    auth: :sigv4,
    mode: :signed,
    chunk_size: 65_536,
    bytes: 0,
    finished: false
  ]

  @empty Base.encode16(:crypto.hash(:sha256, ""), case: :lower)

  defmodule StreamError do
    @moduledoc false
    defexception [:error, message: "Streaming signing could not complete"]
  end

  @doc "Uses the already signed request's seed; secret key material is excluded from Inspect."
  def new(opts) do
    mode = opts[:mode] || :signed
    algorithm = opts[:checksum_algorithm]
    chunk_size = opts[:chunk_size] || 65_536
    length = opts[:decoded_length]
    auth = opts[:auth] || :sigv4

    cond do
      auth not in [:sigv4, :sigv4a] ->
        failure(:streaming_auth_unavailable, "Unknown streaming signing algorithm")

      mode not in [:signed, :unsigned] ->
        failure(:streaming_mode, "Unknown streaming signature mode")

      mode == :unsigned and algorithm == nil ->
        failure(:streaming_checksum, "Unsigned aws-chunked requires a checksum trailer")

      not is_integer(chunk_size) or chunk_size < 1 or chunk_size > 1_048_576 ->
        failure(:streaming_chunk_budget, "Chunk size must be between one byte and one MiB")

      length != nil and (not is_integer(length) or length < 0 or length > 0x7FFFFFFFFFFFFFFF) ->
        failure(:streaming_length, "Invalid decoded streaming length")

      true ->
        with {:ok, checksum} <- checksum(algorithm),
             {:ok, signing} <- signing(mode, opts) do
          {:ok,
           struct!(
             __MODULE__,
             signing ++
               [
                 mode: mode,
                 auth: auth,
                 checksum: checksum,
                 algorithm: algorithm,
                 chunk_size: chunk_size,
                 expected_length: length
               ]
           )}
        end
    end
  end

  @doc "Extracts only the seed signature from a previously generated Authorization header."
  def seed(authorization) when is_binary(authorization) do
    auth =
      if String.starts_with?(authorization, "AWS4-ECDSA-P256-SHA256 "), do: :sigv4a, else: :sigv4

    case Regex.run(~r/(?:^|[, ])Signature=([0-9a-fA-F*]{16,144})$/, authorization) do
      [_, signature] ->
        if valid_seed?(signature, auth),
          do: {:ok, signature |> String.trim_trailing("*") |> String.downcase()},
          else:
            failure(:streaming_seed, "Signed streaming requires a valid request seed signature")

      _ ->
        failure(:streaming_seed, "Signed streaming requires a valid request seed signature")
    end
  end

  def seed(_),
    do: failure(:streaming_seed, "Signed streaming requires a valid request seed signature")

  def payload_hash(opts) do
    cond do
      opts[:mode] == :unsigned ->
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

      opts[:auth] == :sigv4a and opts[:checksum_algorithm] ->
        "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"

      opts[:auth] == :sigv4a ->
        "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"

      opts[:checksum_algorithm] ->
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

      true ->
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    end
  end

  @doc "Content-Length is exact when encode/2 rechunks the captured source at the same configured size."
  def headers(decoded_length, opts \\ []) do
    with {:ok, state} <- layout(decoded_length, opts),
         {:ok, length} <- encoded_length(decoded_length, opts) do
      encoding =
        if opts[:content_encoding],
          do: opts[:content_encoding] <> ",aws-chunked",
          else: "aws-chunked"

      headers = [
        {"content-encoding", encoding},
        {"content-length", to_string(length)},
        {"x-amz-decoded-content-length", to_string(decoded_length)},
        {"x-amz-content-sha256", payload_hash(opts)}
      ]

      headers =
        if state.algorithm,
          do: headers ++ [{"x-amz-trailer", Checksum.header(state.algorithm)}],
          else: headers

      {:ok, headers}
    end
  end

  @doc "Encodes one raw-byte frame and advances the signature chain. Empty input is skipped."
  def chunk(%__MODULE__{finished: true}, _),
    do: failure(:streaming_finished, "Streaming signer is already finalized")

  def chunk(%__MODULE__{} = state, bytes) when is_binary(bytes) do
    cond do
      byte_size(bytes) > state.chunk_size ->
        failure(:streaming_chunk_budget, "Streaming input exceeds its chunk bound")

      state.expected_length != nil and state.bytes + byte_size(bytes) > state.expected_length ->
        failure(:streaming_length, "Streaming input exceeds decoded Content-Length")

      bytes == "" ->
        {:ok, "", state}

      true ->
        {header, state} = frame_header(state, bytes)
        checksum = if state.checksum, do: Checksum.update(state.checksum, bytes)

        {:ok, header <> "\r\n" <> bytes <> "\r\n",
         %{state | checksum: checksum, bytes: state.bytes + byte_size(bytes)}}
    end
  end

  def chunk(_, _), do: failure(:streaming_chunk_budget, "Streaming chunks must contain raw bytes")

  @doc "Signs the zero frame and optional checksum trailer, exactly once."
  def finish(%__MODULE__{finished: true}),
    do: failure(:streaming_finished, "Streaming signer is already finalized")

  def finish(%__MODULE__{} = state) do
    if state.expected_length != nil and state.bytes != state.expected_length do
      failure(:streaming_length, "Streaming input is shorter than decoded Content-Length")
    else
      {zero, state} = frame_header(state, "")

      trailer =
        if state.checksum,
          do:
            Checksum.header(state.algorithm) <> ":" <> Checksum.encoded(state.checksum) <> "\r\n",
          else: ""

      signature =
        if trailer != "" and state.mode == :signed do
          {:ok, signature} =
            sign_trailers(state, [
              {Checksum.header(state.algorithm), Checksum.encoded(state.checksum)}
            ])

          "x-amz-trailer-signature:" <> signature <> "\r\n"
        else
          ""
        end

      {:ok, zero <> "\r\n" <> trailer <> signature <> "\r\n", %{state | finished: true}}
    end
  end

  @doc "Signs bounded canonical trailing headers using the final zero-chunk signature."
  def sign_trailers(%__MODULE__{mode: :signed} = state, headers)
      when is_list(headers) and length(headers) in 1..128 do
    valid =
      Enum.all?(headers, fn
        {name, value} when is_binary(name) and is_binary(value) ->
          byte_size(name) <= 256 and byte_size(value) <= 8192 and
            Regex.match?(~r/^[a-zA-Z0-9-]+$/, name) and
            not String.contains?(value, ["\r", "\n", <<0>>])

        _ ->
          false
      end)

    normalized =
      if valid,
        do:
          Enum.map(headers, fn {name, value} ->
            {String.downcase(name),
             value |> String.split(~r/[ \t]+/, trim: true) |> Enum.join(" ")}
          end),
        else: []

    if valid and length(Enum.uniq_by(normalized, &elem(&1, 0))) == length(headers) do
      canonical =
        normalized
        |> Enum.sort()
        |> Enum.map_join("", fn {name, value} -> name <> ":" <> value <> "\n" end)

      text =
        Enum.join(
          [
            signing_algorithm(state) <> "-TRAILER",
            state.timestamp,
            state.scope,
            state.previous,
            hash(canonical)
          ],
          "\n"
        )

      {:ok, state |> sign(text) |> padded(state)}
    else
      failure(
        :streaming_trailers,
        "Trailing headers are invalid, duplicated, or exceed their bounds"
      )
    end
  end

  def sign_trailers(_, _),
    do: failure(:streaming_trailers, "Signed trailing headers require a signed payload chain")

  @doc "Lazy fixed-size framing. Upstream chunks must themselves fit the selected chunk bound."
  def encode(enumerable, opts) do
    with {:ok, initial} <- new(opts) do
      stream =
        Stream.transform(
          enumerable,
          fn -> {initial, ""} end,
          fn bytes, {state, carry} ->
            if not is_binary(bytes) or byte_size(bytes) > state.chunk_size,
              do:
                raise(StreamError,
                  error:
                    elem(
                      failure(:streaming_chunk_budget, "Streaming input exceeds its chunk bound"),
                      1
                    )
                )

            framed(carry <> bytes, state, [])
          end,
          fn {state, carry} ->
            {frames, state} =
              if carry == "" do
                {[], state}
              else
                case chunk(state, carry) do
                  {:ok, frame, state} -> {[frame], state}
                  {:error, error} -> raise StreamError, error: error
                end
              end

            case finish(state) do
              {:ok, final, state} -> {frames ++ [final], {state, ""}}
              {:error, error} -> raise StreamError, error: error
            end
          end,
          fn _ -> :ok end
        )

      {:ok, stream}
    end
  end

  def encoded_length(decoded_length, opts \\ []) do
    with {:ok, state} <- layout(decoded_length, opts) do
      full = div(decoded_length, state.chunk_size)
      remainder = rem(decoded_length, state.chunk_size)
      signature_length = if state.auth == :sigv4a, do: 144, else: 64
      overhead = if state.mode == :signed, do: signature_length + 21, else: 4
      frames = full * (state.chunk_size + byte_size(hex(state.chunk_size)) + overhead)
      tail = if remainder == 0, do: 0, else: remainder + byte_size(hex(remainder)) + overhead
      terminal = if state.mode == :signed, do: signature_length + 22, else: 5

      terminal =
        if state.algorithm do
          digest_size =
            %{"CRC32" => 8, "CRC32C" => 8, "CRC64NVME" => 12, "SHA1" => 28, "SHA256" => 44}[
              state.algorithm
            ]

          # With trailers the zero frame has one CRLF, followed by canonical headers and final CRLF.
          terminal + byte_size(Checksum.header(state.algorithm)) + 1 + digest_size + 2 +
            if(state.mode == :signed,
              do: byte_size("x-amz-trailer-signature:") + signature_length + 2,
              else: 0
            )
        else
          terminal
        end

      {:ok, frames + tail + terminal}
    end
  end

  defp layout(decoded_length, opts) when is_integer(decoded_length) and decoded_length >= 0 do
    opts =
      opts
      |> Keyword.put(:decoded_length, decoded_length)
      |> Keyword.put_new(
        :seed_signature,
        if(opts[:auth] == :sigv4a,
          do: "30" <> String.duplicate("0", 138),
          else: String.duplicate("0", 64)
        )
      )
      |> Keyword.put_new(:secret_access_key, "layout-only")
      |> Keyword.put_new(:access_key_id, "layout-only")

    new(opts)
  end

  defp layout(_, _), do: failure(:streaming_length, "An exact decoded Content-Length is required")

  defp framed(data, state, frames) when byte_size(data) < state.chunk_size,
    do: {Enum.reverse(frames), {state, data}}

  defp framed(data, state, frames) do
    <<bytes::binary-size(state.chunk_size), rest::binary>> = data

    case chunk(state, bytes) do
      {:ok, frame, state} -> framed(rest, state, [frame | frames])
      {:error, error} -> raise StreamError, error: error
    end
  end

  defp frame_header(%{mode: :unsigned} = state, bytes), do: {hex(byte_size(bytes)), state}

  defp frame_header(state, bytes) do
    text =
      Enum.join(
        [
          signing_algorithm(state) <> "-PAYLOAD",
          state.timestamp,
          state.scope,
          state.previous,
          @empty,
          hash(bytes)
        ],
        "\n"
      )

    signature = sign(state, text)

    {hex(byte_size(bytes)) <> ";chunk-signature=" <> padded(signature, state),
     %{state | previous: signature}}
  end

  defp checksum(nil), do: {:ok, nil}
  defp checksum(algorithm), do: Checksum.new(algorithm)
  defp signing(:unsigned, _), do: {:ok, []}

  defp signing(:signed, opts) do
    if opts[:auth] == :sigv4a, do: asymmetric(opts), else: symmetric(opts)
  end

  defp symmetric(opts) do
    signature = opts[:seed_signature]
    secret = opts[:secret_access_key]
    region = opts[:region] || "us-east-1"
    service = opts[:service] || "s3"
    now = opts[:now] || DateTime.utc_now()

    if is_binary(signature) and Regex.match?(~r/^[0-9a-fA-F]{64}$/, signature) and
         is_binary(secret) and secret != "" and is_struct(now, DateTime) and
         is_binary(region) and Regex.match?(~r/^[a-z0-9*-]+$/, region) and
         is_binary(service) and Regex.match?(~r/^[a-z0-9-]+$/, service) do
      date = Calendar.strftime(now, "%Y%m%d")
      key = :crypto.mac(:hmac, :sha256, "AWS4" <> secret, date)
      key = :crypto.mac(:hmac, :sha256, key, region)
      key = :crypto.mac(:hmac, :sha256, key, service)
      key = :crypto.mac(:hmac, :sha256, key, "aws4_request")

      {:ok,
       [
         key: key,
         previous: String.downcase(signature),
         timestamp: Calendar.strftime(now, "%Y%m%dT%H%M%SZ"),
         scope: date <> "/" <> region <> "/" <> service <> "/aws4_request"
       ]}
    else
      failure(
        :streaming_seed,
        "Signed streaming requires a valid seed, credentials, region and time"
      )
    end
  end

  defp asymmetric(opts) do
    signature = opts[:seed_signature]
    now = opts[:now] || DateTime.utc_now()
    service = opts[:service] || "s3"

    if valid_seed?(signature, :sigv4a) and is_binary(opts[:access_key_id]) and
         opts[:access_key_id] != "" and is_binary(opts[:secret_access_key]) and
         opts[:secret_access_key] != "" and is_struct(now, DateTime) and
         is_binary(service) and Regex.match?(~r/^[a-z0-9-]+$/, service) do
      {:ok,
       [
         key: Keyword.take(opts, [:access_key_id, :secret_access_key]),
         previous: signature |> String.trim_trailing("*") |> String.downcase(),
         timestamp: Calendar.strftime(now, "%Y%m%dT%H%M%SZ"),
         scope: Calendar.strftime(now, "%Y%m%d") <> "/" <> service <> "/aws4_request"
       ]}
    else
      failure(
        :streaming_seed,
        "Signed streaming requires a valid seed, credentials, service and time"
      )
    end
  end

  defp valid_seed?(signature, :sigv4a) when is_binary(signature) do
    raw = String.trim_trailing(signature, "*")

    byte_size(signature) <= 144 and rem(byte_size(raw), 2) == 0 and
      Regex.match?(~r/^30[0-9a-fA-F]{14,142}$/, raw)
  end

  defp valid_seed?(signature, :sigv4) when is_binary(signature),
    do: Regex.match?(~r/^[0-9a-fA-F]{64}$/, signature)

  defp valid_seed?(_, _), do: false

  defp signing_algorithm(%{auth: :sigv4a}), do: "AWS4-ECDSA-P256-SHA256"
  defp signing_algorithm(_), do: "AWS4-HMAC-SHA256"
  defp sign(%{auth: :sigv4a, key: key}, text), do: SigV4a.sign_message(text, key)
  defp sign(%{key: key}, text), do: hmac(key, text)
  defp padded(signature, %{auth: :sigv4a}), do: String.pad_trailing(signature, 144, "*")
  defp padded(signature, _), do: signature

  defp hex(value), do: value |> Integer.to_string(16) |> String.downcase()
  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp hmac(key, bytes),
    do: :crypto.mac(:hmac, :sha256, key, bytes) |> Base.encode16(case: :lower)

  defp failure(code, message), do: {:error, Error.new(:protocol, message, code: code)}
end
