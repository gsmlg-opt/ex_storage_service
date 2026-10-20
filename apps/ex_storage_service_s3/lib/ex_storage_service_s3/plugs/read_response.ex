defmodule ExStorageServiceS3.Plugs.ReadResponse do
  @moduledoc false
  import Plug.Conn
  alias ExStorageServiceS3.Handlers.Shared

  @overrides %{
    "response-content-type" => "content-type",
    "response-content-language" => "content-language",
    "response-expires" => "expires",
    "response-cache-control" => "cache-control",
    "response-content-disposition" => "content-disposition",
    "response-content-encoding" => "content-encoding"
  }

  def init(opts), do: opts

  def call(%{method: "GET", assigns: %{s3_operation: %{name: name}}} = conn, _opts)
      when name in [:get_object, :get_object_version] do
    overrides =
      conn.query_string
      |> URI.query_decoder()
      |> Enum.filter(fn {key, _} -> Map.has_key?(@overrides, key) end)

    cond do
      overrides == [] ->
        conn

      not is_binary(conn.assigns[:user_id]) ->
        reject(conn, "AccessDenied", "Response overrides require a signed request.")

      length(Enum.uniq_by(overrides, &elem(&1, 0))) != length(overrides) or
          Enum.any?(overrides, fn {_, value} ->
            byte_size(value) > 2048 or not String.valid?(value) or
                String.contains?(value, ["\r", "\n", <<0>>])
          end) ->
        reject(conn, "InvalidArgument", "The response header override is invalid.")

      true ->
        register_before_send(conn, fn response ->
          if response.status in [200, 206] do
            Enum.reduce(overrides, response, fn {key, value}, acc ->
              put_resp_header(acc, Map.fetch!(@overrides, key), value)
            end)
          else
            response
          end
        end)
    end
  end

  def call(conn, _opts), do: conn

  defp reject(conn, code, message) do
    conn
    |> Shared.error_response(code, message, conn.request_path, Shared.request_id(conn))
    |> halt()
  end

  @doc false
  def condition(conn, etag, modified) do
    cond do
      Enum.any?(get_req_header(conn, "if-match"), &(not etag_matches?(&1, etag, false))) ->
        :failed

      get_req_header(conn, "if-match") == [] and unmodified_failed?(conn, modified) ->
        :failed

      Enum.any?(get_req_header(conn, "if-none-match"), &etag_matches?(&1, etag, true)) ->
        :not_modified

      get_req_header(conn, "if-none-match") == [] and Shared.not_modified_since?(conn, modified) ->
        :not_modified

      true ->
        :ok
    end
  end

  defp etag_matches?(value, etag, weak?) do
    Enum.any?(String.split(value, ","), fn item ->
      item = String.trim(item)
      item = if weak?, do: String.replace_prefix(item, "W/", ""), else: item
      item in ["*", etag]
    end)
  end

  defp unmodified_failed?(conn, modified) do
    with [value | _] <- get_req_header(conn, "if-unmodified-since"),
         {:ok, limit} <- Shared.parse_http_date(value),
         {:ok, actual} <- Shared.parse_object_datetime(modified) do
      DateTime.compare(actual, limit) == :gt
    else
      _ -> false
    end
  end
end
