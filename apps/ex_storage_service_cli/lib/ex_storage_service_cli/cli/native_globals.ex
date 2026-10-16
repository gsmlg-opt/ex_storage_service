defmodule ExStorageServiceCli.CLI.NativeGlobals do
  @moduledoc "Invocation-scoped native configuration and validated pre-sign request headers."
  alias ExStorageServiceCli.{CLI.Invocation, Error}

  @reserved ~w(authorization proxy-authorization host content-length transfer-encoding
    connection trailer te upgrade expect content-md5 content-encoding
    x-amz-date x-amz-security-token x-amz-s3session-token x-amz-content-sha256
    x-amz-decoded-content-length x-amz-trailer x-amz-trailer-signature
    x-amz-algorithm x-amz-credential x-amz-signature x-amz-signedheaders
    x-amz-expires x-amz-region-set)

  def prepare(options) do
    with :ok <- config_directory(options[:config_dir]),
         {:ok, headers} <- headers(options[:custom_headers] || []),
         {:ok, options} <- rates(options),
         {:ok, options} <- deadlines(options),
         {:ok, pins} <- ExStorageServiceCli.Transport.Resolution.parse(options[:resolve] || []) do
      options = if pins == %{}, do: options, else: Map.put(options, :resolve_entries, pins)
      {:ok, if(headers == [], do: options, else: Map.put(options, :request_headers, headers))}
    end
  end

  def config_options(options, opts \\ []) do
    case options[:config_dir] do
      nil -> opts
      directory -> Keyword.put(opts, :path, Path.join(Path.expand(directory), "config.toml"))
    end
  end

  def warn(%Invocation{frontend: :native, help: false, options: %{no_verify_ssl: true}}),
    do: IO.puts(:stderr, "Warning: TLS certificate verification is disabled (--insecure).")

  def warn(_invocation), do: :ok

  def apply_headers(request, options) do
    custom = options[:request_headers] || []
    names = MapSet.new(custom, &elem(&1, 0))

    existing =
      Enum.reject(request.headers, fn {name, _} ->
        MapSet.member?(names, String.downcase(name))
      end)

    %{request | headers: existing ++ custom}
  end

  defp config_directory(nil), do: :ok

  defp config_directory(directory) when is_binary(directory) and directory != "" do
    if String.contains?(directory, <<0>>), do: invalid("Invalid --config-dir"), else: :ok
  end

  defp config_directory(_), do: invalid("Invalid --config-dir")

  defp headers(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, result} ->
      case header(value) do
        {:ok, header} -> {:cont, {:ok, [header | result]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, result} -> {:ok, Enum.reverse(result)}
      error -> error
    end
  end

  defp rates(options) do
    Enum.reduce_while([:limit_upload, :limit_download], {:ok, options}, fn key, {:ok, result} ->
      case Map.fetch(result, key) do
        :error ->
          {:cont, {:ok, result}}

        {:ok, value} ->
          case ExStorageServiceCli.Transfer.RateLimiter.parse_rate(value) do
            {:ok, rate} -> {:cont, {:ok, Map.put(result, key, rate)}}
            error -> {:halt, error}
          end
      end
    end)
  end

  defp deadlines(options) do
    Enum.reduce_while(
      [{:conn_read_deadline, :read_idle_timeout}, {:conn_write_deadline, :write_idle_timeout}],
      {:ok, options},
      fn {source, target}, {:ok, result} ->
        case Map.fetch(options, source) do
          :error ->
            {:cont, {:ok, result}}

          {:ok, value} ->
            case duration(value) do
              {:ok, timeout} ->
                {:cont, {:ok, Map.put(result, target, timeout)}}

              :error ->
                {:halt,
                 invalid(
                   "Invalid --#{source |> Atom.to_string() |> String.replace("_", "-")} duration; use zero or a positive Go duration exactly representable in whole milliseconds up to 4294967295ms"
                 )}
            end
        end
      end
    )
  end

  @duration_units %{
    "ns" => 1,
    "us" => 1_000,
    "µs" => 1_000,
    "μs" => 1_000,
    "ms" => 1_000_000,
    "s" => 1_000_000_000,
    "m" => 60_000_000_000,
    "h" => 3_600_000_000_000
  }
  defp duration(value) when is_binary(value) and byte_size(value) <= 256 do
    if String.valid?(value) do
      {sign, value} =
        case value do
          "+" <> rest -> {1, rest}
          "-" <> rest -> {-1, rest}
          rest -> {1, rest}
        end

      parts = Regex.scan(~r/([0-9]+(?:\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)/u, value)

      if value == "0" or (parts != [] and Enum.map_join(parts, &hd/1) == value) do
        nanoseconds =
          Enum.reduce(parts, 0, fn [_, number, unit], total ->
            [whole | fraction] = String.split(number, ".", parts: 2)
            fractional = List.first(fraction) || ""

            total +
              div(
                String.to_integer(if(whole == "", do: "0", else: whole) <> fractional) *
                  @duration_units[unit],
                Integer.pow(10, byte_size(fractional))
              )
          end)

        cond do
          nanoseconds > 9_223_372_036_854_775_807 -> :error
          nanoseconds == 0 -> {:ok, nil}
          sign < 0 -> :error
          rem(nanoseconds, 1_000_000) != 0 -> :error
          div(nanoseconds, 1_000_000) <= 4_294_967_295 -> {:ok, div(nanoseconds, 1_000_000)}
          true -> :error
        end
      else
        :error
      end
    else
      :error
    end
  end

  defp duration(_), do: :error

  defp header(value) do
    if valid_value?(value),
      do: parse_header(value),
      else: invalid("Invalid --custom-header value")
  end

  defp parse_header(value) do
    case String.split(value, ":", parts: 2) do
      [name, value] ->
        name = name |> String.trim() |> String.downcase()
        value = Regex.replace(~r/\A[ \t]+|[ \t]+\z/, value, "")

        cond do
          not Regex.match?(~r/\A[!#$%&'*+.^_`|~0-9a-z-]+\z/, name) ->
            invalid("Invalid --custom-header name")

          reserved?(name) ->
            invalid("Reserved --custom-header cannot override authentication or framing")

          not valid_value?(value) ->
            invalid("Invalid --custom-header value")

          true ->
            {:ok, {name, value}}
        end

      _ ->
        invalid("Custom headers require a name:value pair")
    end
  end

  defp reserved?(name),
    do: name in @reserved or String.starts_with?(name, ["x-amz-checksum-", "x-amz-sdk-checksum-"])

  defp valid_value?(value),
    do: Enum.all?(:binary.bin_to_list(value), &(&1 == 9 or &1 in 32..126 or &1 >= 128))

  defp invalid(message), do: {:error, Error.new(:syntax, message)}
end
