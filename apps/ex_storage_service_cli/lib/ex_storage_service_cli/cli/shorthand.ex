defmodule ExStorageServiceCli.CLI.Shorthand do
  @moduledoc "Bounded AWS shorthand syntax with nested lists/maps, quoting and escaped commas."
  alias ExStorageServiceCli.Error
  @key ~r/\A\s*([a-zA-Z0-9_.#\/:\-]+)\s*(@?)=\s*/

  def parse(value, opts \\ []) when is_binary(value) do
    if byte_size(value) > Keyword.get(opts, :max_input_bytes, 8_388_608) do
      invalid()
    else
      try do
        {parsed, rest} = pairs(String.trim(value), nil, 0, %{}, opts)
        if String.trim(rest) == "", do: {:ok, parsed}, else: invalid()
      catch
        :shorthand -> invalid()
        {:shorthand_error, error} -> {:error, error}
      end
    end
  end

  defp pairs(_value, _end, depth, _map, _opts) when depth > 64, do: throw(:shorthand)

  defp pairs(value, ending, depth, map, opts) do
    value = String.trim_leading(value)

    case value do
      <<"}", rest::binary>> when ending == ?} ->
        {map, rest}

      "" when ending == nil and map_size(map) > 0 ->
        {map, ""}

      _ ->
        case Regex.run(@key, value) do
          [prefix, key, file_marker] ->
            if Map.has_key?(map, key), do: throw(:shorthand)
            rest = binary_part(value, byte_size(prefix), byte_size(value) - byte_size(prefix))
            {item, rest} = value(rest, depth + 1, opts)
            {item, rest} = csv(item, rest, ending, depth, opts)
            item = if file_marker == "@", do: resolve(item, opts), else: item
            map = Map.put(map, key, item)

            case String.trim_leading(rest) do
              <<",", rest::binary>> -> pairs(rest, ending, depth, map, opts)
              <<"}", rest::binary>> when ending == ?} -> {map, rest}
              "" when ending == nil -> {map, ""}
              _ -> throw(:shorthand)
            end

          _ ->
            throw(:shorthand)
        end
    end
  end

  defp csv(item, <<",", rest::binary>> = original, ending, depth, opts)
       when ending == nil and is_binary(item) do
    if String.trim(rest) == "", do: throw(:shorthand)

    if Regex.match?(@key, rest) do
      {item, original}
    else
      {next, remaining} = value(String.trim_leading(rest), depth + 1, opts)
      {values, remaining} = csv_list([next, item], remaining, depth, opts)
      {values, remaining}
    end
  end

  defp csv(item, rest, _ending, _depth, _opts), do: {item, rest}

  defp csv_list(values, <<",", rest::binary>> = original, depth, opts) do
    if String.trim(rest) == "", do: throw(:shorthand)

    if Regex.match?(@key, rest) do
      {Enum.reverse(values), original}
    else
      {next, remaining} = value(String.trim_leading(rest), depth + 1, opts)
      csv_list([next | values], remaining, depth, opts)
    end
  end

  defp csv_list(values, rest, _depth, _opts), do: {Enum.reverse(values), rest}

  defp value(_value, depth, _opts) when depth > 64, do: throw(:shorthand)
  defp value(<<"{", rest::binary>>, depth, opts), do: pairs(rest, ?}, depth, %{}, opts)
  defp value(<<"[", rest::binary>>, depth, opts), do: list(rest, depth, [], opts)

  defp value(<<quote, rest::binary>>, _depth, _opts) when quote in [?', ?"],
    do: quoted(rest, quote, [])

  defp value(value, _depth, _opts), do: scalar(value, [])

  defp list(value, depth, items, opts) do
    case String.trim_leading(value) do
      <<"]", rest::binary>> ->
        {Enum.reverse(items), rest}

      value ->
        {item, rest} = value(value, depth + 1, opts)

        case String.trim_leading(rest) do
          <<",", rest::binary>> -> list(rest, depth, [item | items], opts)
          <<"]", rest::binary>> -> {Enum.reverse([item | items]), rest}
          _ -> throw(:shorthand)
        end
    end
  end

  defp quoted(<<"\\", quote, rest::binary>>, quote, acc),
    do: quoted(rest, quote, [<<quote>> | acc])

  defp quoted(<<"\\\\", rest::binary>>, quote, acc), do: quoted(rest, quote, ["\\" | acc])
  defp quoted(<<quote, rest::binary>>, quote, acc), do: {bytes(acc), String.trim_leading(rest)}
  defp quoted(<<byte, rest::binary>>, quote, acc), do: quoted(rest, quote, [<<byte>> | acc])
  defp quoted("", _quote, _acc), do: throw(:shorthand)

  defp scalar(<<"\\,", rest::binary>>, acc), do: scalar(rest, ["," | acc])

  defp scalar(<<byte, _::binary>> = rest, acc) when byte in [?,, ?], ?}],
    do: {bytes(acc) |> String.trim_trailing(), rest}

  defp scalar(<<byte, rest::binary>>, acc), do: scalar(rest, [<<byte>> | acc])
  defp scalar("", acc), do: {bytes(acc) |> String.trim_trailing(), ""}
  defp bytes(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  defp resolve(values, opts) when is_list(values), do: Enum.map(values, &resolve(&1, opts))

  defp resolve(value, opts) do
    case Keyword.get(opts, :resolve_file) do
      resolver when is_function(resolver, 1) ->
        case resolver.(value) do
          {:ok, value} -> value
          {:error, error} -> throw({:shorthand_error, error})
        end

      _ ->
        throw(:shorthand)
    end
  end

  defp invalid,
    do: {:error, Error.new(:syntax, "Invalid AWS shorthand syntax", code: :invalid_shorthand)}
end
