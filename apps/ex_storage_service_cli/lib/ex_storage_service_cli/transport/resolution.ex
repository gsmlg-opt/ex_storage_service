defmodule ExStorageServiceCli.Transport.Resolution do
  @moduledoc "Invocation-local address pins that preserve the signed HTTP and TLS authority."
  alias ExStorageServiceCli.Error

  def parse(entries) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, pins} ->
      case parse_entry(entry) do
        {:ok, authority, address} -> {:cont, {:ok, Map.put(pins, authority, address)}}
        error -> {:halt, error}
      end
    end)
  end

  def parse(_), do: invalid()

  def options(url, opts) do
    pins = opts[:resolve_entries] || %{}

    with true <- is_map(pins),
         {:ok, %URI{host: host, port: port}} when is_binary(host) <- URI.new(url) do
      host = String.downcase(host)

      case Map.get(pins, {host, port}) || Map.get(pins, {host, nil}) do
        nil -> {:ok, opts}
        address -> {:ok, Keyword.put(opts, :connect_address, address)}
      end
    else
      _ -> invalid()
    end
  end

  defp parse_entry(value) when is_binary(value) do
    with true <- String.valid?(value),
         [authority, literal] <- String.split(value, "=", parts: 2),
         %{"host" => host, "port" => port} <-
           Regex.named_captures(
             ~r/\A(?<host>\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9_.-]+)(?::(?<port>[0-9]{1,5}))?\z/,
             authority
           ),
         {:ok, host} <- hostname(host),
         {:ok, port} <- port_number(port),
         true <- not String.contains?(literal, "%"),
         {:ok, address} <- :inet.parse_strict_address(String.to_charlist(literal)) do
      {:ok, {host, port}, address}
    else
      _ -> invalid()
    end
  end

  defp parse_entry(_), do: invalid()

  defp hostname("[" <> rest) do
    host = String.trim_trailing(rest, "]")

    case :inet.parse_strict_address(String.to_charlist(host)) do
      {:ok, address} when tuple_size(address) == 8 -> {:ok, String.downcase(host)}
      _ -> invalid()
    end
  end

  defp hostname(host) do
    if Regex.match?(~r/\A[A-Za-z0-9_](?:[A-Za-z0-9_.-]*[A-Za-z0-9_])?\z/, host),
      do: {:ok, String.downcase(host)},
      else: invalid()
  end

  defp port_number(""), do: {:ok, nil}

  defp port_number(value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _ -> invalid()
    end
  end

  defp invalid,
    do:
      {:error,
       Error.new(:syntax, "Resolve entries require HOST[:PORT]=literal-IP",
         code: :invalid_resolve
       )}
end
