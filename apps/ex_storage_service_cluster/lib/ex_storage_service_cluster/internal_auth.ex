defmodule ExStorageServiceCluster.InternalAuth do
  @moduledoc """
  Signs and verifies requests to the private blob transport.

  Authentication binds the method, request path, timestamp, request id, blob
  hash, declared size, and optional byte range to one HMAC-SHA256 signature.
  """

  alias ExStorageServiceCluster.ReplayCache

  @algorithm "ESS-HMAC-SHA256"
  @hash_pattern ~r/\A[0-9a-f]{64}\z/
  @request_id_pattern ~r/\A[A-Za-z0-9._~-]{16,128}\z/
  @authorization_pattern ~r/\AESS-HMAC-SHA256 ([0-9a-f]{64})\z/
  @range_pattern ~r/\Abytes\s*=\s*(\d*)\s*-\s*(\d*)\z/i

  @required_headers [
    "x-ess-timestamp",
    "x-ess-request-id",
    "x-ess-blob-sha256",
    "x-ess-blob-size",
    "authorization"
  ]

  @doc """
  Returns the request headers needed to authenticate an internal blob request.

  `:path` is required. `:timestamp`, `:request_id`, and `:range` may be
  supplied for deterministic callers and tests.
  """
  @spec sign(String.t() | atom(), String.t(), non_neg_integer() | String.t(), binary(), keyword()) ::
          [{String.t(), String.t()}]
  def sign(method, hash, size_or_dash, secret, opts \\ []) do
    path = Keyword.fetch!(opts, :path)
    timestamp = Keyword.get(opts, :timestamp, System.system_time(:second))
    request_id = Keyword.get_lazy(opts, :request_id, &generate_request_id/0)
    range = Keyword.get(opts, :range)

    with {:ok, method} <- normalize_method(method),
         {:ok, path} <- validate_path(path),
         {:ok, timestamp} <- normalize_timestamp(timestamp),
         {:ok, request_id} <- validate_request_id(request_id),
         {:ok, hash} <- normalize_hash(hash),
         {:ok, size} <- normalize_size(size_or_dash),
         {:ok, range} <- canonical_range(range),
         :ok <- validate_secret(secret) do
      authorization =
        canonical(method, path, timestamp, request_id, hash, size, range)
        |> signature(secret)
        |> then(&"#{@algorithm} #{&1}")

      [
        {"x-ess-timestamp", timestamp},
        {"x-ess-request-id", request_id},
        {"x-ess-blob-sha256", hash},
        {"x-ess-blob-size", size},
        {"authorization", authorization}
      ]
      |> maybe_add_range(range)
    else
      {:error, reason} -> raise ArgumentError, "invalid internal authentication input: #{reason}"
    end
  end

  @doc "Verifies request authentication and atomically claims its request id."
  @spec verify(
          Plug.Conn.t(),
          String.t() | atom(),
          String.t(),
          non_neg_integer() | String.t(),
          keyword()
        ) ::
          {:ok, %{request_id: String.t(), timestamp: integer()}} | {:error, term()}
  def verify(%Plug.Conn{} = conn, method, hash, size_or_dash, opts) do
    with {:ok, headers} <- required_headers(conn),
         {:ok, range_header} <- optional_header(conn, "range"),
         {:ok, method} <- normalize_method(method),
         :ok <- validate_conn_method(conn.method, method),
         {:ok, path} <- validate_path(conn.request_path),
         {:ok, timestamp} <- parse_timestamp(headers["x-ess-timestamp"]),
         {:ok, request_id} <- validate_request_id(headers["x-ess-request-id"]),
         {:ok, expected_hash} <- normalize_hash(hash),
         {:ok, header_hash} <- validate_hash(headers["x-ess-blob-sha256"]),
         :ok <- equal_field(header_hash, expected_hash, :hash_mismatch),
         {:ok, expected_size} <- normalize_size(size_or_dash),
         {:ok, header_size} <- normalize_size(headers["x-ess-blob-size"]),
         :ok <- equal_field(header_size, expected_size, :size_mismatch),
         {:ok, range} <- canonical_range(range_header),
         {:ok, claimed_signature} <- parse_authorization(headers["authorization"]),
         {:ok, secret} <- fetch_secret(opts),
         {:ok, now_seconds} <- fetch_now_seconds(opts),
         {:ok, skew_seconds} <- fetch_skew_seconds(opts),
         :ok <- validate_clock(timestamp, now_seconds, skew_seconds),
         expected_signature <-
           canonical(method, path, timestamp, request_id, header_hash, header_size, range)
           |> signature(secret),
         :ok <- compare_signatures(expected_signature, claimed_signature),
         {:ok, replay_table} <- fetch_replay_table(opts),
         :ok <-
           claim_request(
             replay_table,
             request_id,
             String.to_integer(timestamp),
             now_seconds,
             skew_seconds,
             opts
           ) do
      {:ok, %{request_id: request_id, timestamp: String.to_integer(timestamp)}}
    end
  end

  @doc false
  @spec canonical(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t()
        ) ::
          String.t()
  def canonical(method, path, timestamp, request_id, hash, size, range) do
    Enum.join([@algorithm, method, path, timestamp, request_id, hash, size, range], "\n")
  end

  defp required_headers(conn) do
    Enum.reduce_while(@required_headers, {:ok, %{}}, fn name, {:ok, headers} ->
      case Plug.Conn.get_req_header(conn, name) do
        [value] -> {:cont, {:ok, Map.put(headers, name, value)}}
        [] -> {:halt, {:error, {:missing_header, name}}}
        _ -> {:halt, {:error, {:duplicate_header, name}}}
      end
    end)
  end

  defp optional_header(conn, name) do
    case Plug.Conn.get_req_header(conn, name) do
      [] -> {:ok, nil}
      [value] -> {:ok, value}
      _ -> {:error, {:duplicate_header, name}}
    end
  end

  defp normalize_method(method) when is_atom(method),
    do: method |> Atom.to_string() |> normalize_method()

  defp normalize_method(method) when is_binary(method) do
    method = String.upcase(method)

    if Regex.match?(~r/\A[A-Z]+\z/, method),
      do: {:ok, method},
      else: {:error, :invalid_method}
  end

  defp normalize_method(_method), do: {:error, :invalid_method}

  defp validate_conn_method(conn_method, method) do
    if String.upcase(conn_method) == method, do: :ok, else: {:error, :method_mismatch}
  end

  defp validate_path(path) when is_binary(path) do
    if String.starts_with?(path, "/") and not String.contains?(path, ["\r", "\n"]),
      do: {:ok, path},
      else: {:error, :invalid_path}
  end

  defp validate_path(_path), do: {:error, :invalid_path}

  defp normalize_timestamp(timestamp) when is_integer(timestamp) and timestamp >= 0,
    do: {:ok, Integer.to_string(timestamp)}

  defp normalize_timestamp(timestamp) when is_binary(timestamp), do: parse_timestamp(timestamp)
  defp normalize_timestamp(_timestamp), do: {:error, :invalid_timestamp}

  defp parse_timestamp(timestamp) do
    case Integer.parse(timestamp) do
      {value, ""} when value >= 0 -> canonical_integer(timestamp, value, :invalid_timestamp)
      _ -> {:error, :invalid_timestamp}
    end
  end

  defp validate_request_id(request_id) when is_binary(request_id) do
    if Regex.match?(@request_id_pattern, request_id),
      do: {:ok, request_id},
      else: {:error, :invalid_request_id}
  end

  defp validate_request_id(_request_id), do: {:error, :invalid_request_id}

  defp normalize_hash(hash) when is_binary(hash), do: hash |> String.downcase() |> validate_hash()
  defp normalize_hash(_hash), do: {:error, :invalid_hash}

  defp validate_hash(hash) when is_binary(hash) do
    if Regex.match?(@hash_pattern, hash), do: {:ok, hash}, else: {:error, :invalid_hash}
  end

  defp validate_hash(_hash), do: {:error, :invalid_hash}

  defp normalize_size("-"), do: {:ok, "-"}

  defp normalize_size(size) when is_integer(size) and size >= 0,
    do: {:ok, Integer.to_string(size)}

  defp normalize_size(size) when is_binary(size) do
    case Integer.parse(size) do
      {value, ""} when value >= 0 -> canonical_integer(size, value, :invalid_size)
      _ -> {:error, :invalid_size}
    end
  end

  defp normalize_size(_size), do: {:error, :invalid_size}

  defp canonical_integer(text, value, error) do
    if text == Integer.to_string(value), do: {:ok, text}, else: {:error, error}
  end

  defp canonical_range(nil), do: {:ok, "-"}
  defp canonical_range(""), do: {:ok, "-"}

  defp canonical_range(range) when is_binary(range) do
    case Regex.run(@range_pattern, range, capture: :all_but_first) do
      ["", ""] -> {:error, :invalid_range}
      [first, last] -> {:ok, "bytes=#{first}-#{last}"}
      _ -> {:error, :invalid_range}
    end
  end

  defp canonical_range(_range), do: {:error, :invalid_range}

  defp parse_authorization(value) do
    case Regex.run(@authorization_pattern, value, capture: :all_but_first) do
      [signature] -> {:ok, signature}
      _ -> {:error, :invalid_authorization}
    end
  end

  defp validate_secret(secret) when is_binary(secret) and byte_size(secret) > 0, do: :ok
  defp validate_secret(_secret), do: {:error, :invalid_secret}

  defp fetch_secret(opts) do
    with {:ok, secret} <- Keyword.fetch(opts, :secret),
         :ok <- validate_secret(secret) do
      {:ok, secret}
    else
      :error -> {:error, :missing_secret}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_now_seconds(opts) do
    case Keyword.get(opts, :now_seconds, System.system_time(:second)) do
      now when is_integer(now) and now >= 0 -> {:ok, now}
      now when is_function(now, 0) -> fetch_now_seconds(Keyword.put(opts, :now_seconds, now.()))
      _ -> {:error, :invalid_now}
    end
  end

  defp fetch_skew_seconds(opts) do
    case Keyword.get(opts, :skew_seconds, 60) do
      skew when is_integer(skew) and skew > 0 -> {:ok, skew}
      _ -> {:error, :invalid_skew}
    end
  end

  defp fetch_replay_table(opts) do
    case Keyword.fetch(opts, :replay_table) do
      {:ok, table} -> {:ok, table}
      :error -> {:error, :missing_replay_table}
    end
  end

  defp validate_clock(timestamp, now_seconds, skew_seconds) do
    if abs(now_seconds - String.to_integer(timestamp)) <= skew_seconds,
      do: :ok,
      else: {:error, :clock_skew}
  end

  defp equal_field(value, value, _reason), do: :ok
  defp equal_field(_actual, _expected, reason), do: {:error, reason}

  defp compare_signatures(expected, claimed) when byte_size(expected) == byte_size(claimed) do
    if Plug.Crypto.secure_compare(expected, claimed),
      do: :ok,
      else: {:error, :invalid_signature}
  end

  defp compare_signatures(_expected, _claimed), do: {:error, :invalid_signature}

  defp claim_request(table, request_id, timestamp_seconds, now_seconds, skew_seconds, opts) do
    now_ms =
      case Keyword.get(opts, :monotonic_now_ms, System.monotonic_time(:millisecond)) do
        fun when is_function(fun, 0) -> fun.()
        value -> value
      end

    remaining_validity_ms = (timestamp_seconds + skew_seconds - now_seconds + 1) * 1_000
    ReplayCache.claim(table, request_id, now_ms + remaining_validity_ms)
  end

  defp signature(canonical, secret) do
    :crypto.mac(:hmac, :sha256, secret, canonical)
    |> Base.encode16(case: :lower)
  end

  defp maybe_add_range(headers, "-"), do: headers
  defp maybe_add_range(headers, range), do: headers ++ [{"range", range}]

  defp generate_request_id do
    18
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end
end
