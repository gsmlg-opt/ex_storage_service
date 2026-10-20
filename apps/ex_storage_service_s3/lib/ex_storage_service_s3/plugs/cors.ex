defmodule ExStorageServiceS3.Plugs.Cors do
  @moduledoc "Bucket-scoped CORS; only explicit preflights bypass request authentication."
  import Plug.Conn
  alias ExStorageService.{Metadata, Storage.BucketCORS}
  alias ExStorageServiceS3.Handlers.Shared

  def init(opts), do: opts

  def call(conn, _opts) do
    case {bucket(conn), get_req_header(conn, "origin")} do
      {bucket, [origin]} when is_binary(bucket) ->
        if conn.method == "OPTIONS" and
             get_req_header(conn, "access-control-request-method") != [],
           do: preflight(conn, bucket, origin),
           else: actual(conn, bucket, origin)

      _ ->
        conn
    end
  end

  defp preflight(conn, bucket, origin) do
    conn =
      vary(conn, ["Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"])

    with :ok <- head_bucket(bucket),
         [method] <- get_req_header(conn, "access-control-request-method"),
         true <- method in ~w(GET PUT POST DELETE HEAD),
         {:ok, headers} <- requested_headers(conn),
         {:ok, rules} when is_list(rules) <- BucketCORS.get(bucket),
         {:ok, rule} <- BucketCORS.match(rules, origin, method, headers) do
      conn =
        conn
        |> put_resp_header("access-control-allow-origin", allowed_origin(rule, origin))
        |> put_resp_header("access-control-allow-methods", Enum.join(rule.allowed_methods, ", "))

      conn =
        if headers == [],
          do: conn,
          else: put_resp_header(conn, "access-control-allow-headers", Enum.join(headers, ", "))

      conn =
        if is_nil(rule.max_age_seconds),
          do: conn,
          else: put_resp_header(conn, "access-control-max-age", to_string(rule.max_age_seconds))

      conn |> Shared.put_s3_headers(Shared.request_id(conn)) |> send_resp(200, "") |> halt()
    else
      {:error, reason} when reason not in [:not_found, :invalid_headers] ->
        conn
        |> Shared.storage_error_response(reason, conn.request_path, Shared.request_id(conn))
        |> halt()

      _ ->
        conn
        |> Shared.error_response(
          "AccessDenied",
          "The CORS request is not allowed.",
          conn.request_path,
          Shared.request_id(conn)
        )
        |> halt()
    end
  end

  defp head_bucket(bucket) do
    case Metadata.head_bucket(bucket) do
      {:error, :not_found} -> {:error, :bucket_not_found}
      result -> result
    end
  end

  defp actual(conn, bucket, origin) do
    conn = vary(conn, ["Origin"])

    case BucketCORS.get(bucket) do
      {:ok, rules} when is_list(rules) ->
        case BucketCORS.match(rules, origin, conn.method) do
          {:ok, rule} ->
            register_before_send(conn, fn response ->
              response =
                put_resp_header(
                  response,
                  "access-control-allow-origin",
                  allowed_origin(rule, origin)
                )

              if rule.expose_headers == [],
                do: response,
                else:
                  put_resp_header(
                    response,
                    "access-control-expose-headers",
                    Enum.join(rule.expose_headers, ", ")
                  )
            end)

          :no_match ->
            conn
        end

      _ ->
        conn
    end
  end

  defp requested_headers(conn) do
    case get_req_header(conn, "access-control-request-headers") do
      [] ->
        {:ok, []}

      [value] when byte_size(value) <= 16_384 ->
        headers = value |> String.split(",") |> Enum.map(&(String.trim(&1) |> String.downcase()))

        if Enum.all?(headers, &Regex.match?(~r/\A[!#$%&'*+.^_`|~0-9a-z-]+\z/, &1)),
          do: {:ok, Enum.uniq(headers)},
          else: {:error, :invalid_headers}

      _ ->
        {:error, :invalid_headers}
    end
  end

  defp allowed_origin(rule, origin), do: if("*" in rule.allowed_origins, do: "*", else: origin)

  defp bucket(conn) do
    case String.split(String.trim_leading(conn.request_path, "/"), "/", parts: 2) do
      [""] -> nil
      [bucket | _] -> URI.decode(bucket)
    end
  rescue
    ArgumentError -> nil
  end

  defp vary(conn, values) do
    existing =
      conn
      |> get_resp_header("vary")
      |> Enum.flat_map(&String.split(&1, ","))
      |> Enum.map(&String.trim/1)

    put_resp_header(
      conn,
      "vary",
      Enum.join(Enum.uniq_by(existing ++ values, &String.downcase/1), ", ")
    )
  end
end
