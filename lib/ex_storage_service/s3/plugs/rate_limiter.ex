defmodule ExStorageService.S3.Plugs.RateLimiter do
  @moduledoc """
  Rate limiting plug using a token bucket algorithm backed by ETS.

  Limits requests per access key (from conn.assigns[:access_key_id]) or
  by remote IP if unauthenticated.

  Configuration (via application env :ex_storage_service, :rate_limit):
    - :max_tokens - maximum burst size (default: 100)
    - :refill_rate - tokens added per second (default: 20)
    - :enabled - whether rate limiting is active (default: true)
  """

  import Plug.Conn
  @behaviour Plug

  @table :ex_storage_service_rate_limiter

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    config = rate_limit_config()

    if config.enabled do
      key = bucket_key(conn)
      now = System.monotonic_time(:millisecond)

      case check_rate(key, now, config) do
        :ok ->
          conn

        :rate_limited ->
          request_id =
            conn.assigns[:request_id] ||
              :crypto.strong_rand_bytes(8) |> Base.encode16(case: :upper)

          body =
            ExStorageService.S3.XML.error_response(
              "SlowDown",
              "Please reduce your request rate.",
              conn.request_path,
              request_id
            )

          conn
          |> put_resp_header("content-type", "application/xml")
          |> put_resp_header("x-amz-request-id", request_id)
          |> put_resp_header("retry-after", "1")
          |> send_resp(429, body)
          |> halt()
      end
    else
      conn
    end
  end

  @doc false
  def ensure_table do
    case :ets.info(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          :set,
          read_concurrency: true,
          write_concurrency: true
        ])

        @table

      _ ->
        @table
    end
  end

  defp check_rate(key, now, config) do
    ensure_table()

    case :ets.lookup(@table, key) do
      [{^key, tokens, last_refill}] ->
        elapsed_ms = now - last_refill
        new_tokens = min(config.max_tokens, tokens + elapsed_ms * config.refill_rate / 1000)

        if new_tokens >= 1.0 do
          :ets.insert(@table, {key, new_tokens - 1.0, now})
          :ok
        else
          :ets.insert(@table, {key, new_tokens, now})
          :rate_limited
        end

      [] ->
        :ets.insert(@table, {key, config.max_tokens - 1.0, now})
        :ok
    end
  end

  defp bucket_key(conn) do
    case conn.assigns do
      %{access_key_id: ak} when is_binary(ak) and ak != "" ->
        {:access_key, ak}

      _ ->
        ip = conn.remote_ip |> :inet.ntoa() |> to_string()
        {:ip, ip}
    end
  end

  defp rate_limit_config do
    config = Application.get_env(:ex_storage_service, :rate_limit, [])

    %{
      max_tokens: Keyword.get(config, :max_tokens, 100),
      refill_rate: Keyword.get(config, :refill_rate, 20),
      enabled: Keyword.get(config, :enabled, true)
    }
  end
end
