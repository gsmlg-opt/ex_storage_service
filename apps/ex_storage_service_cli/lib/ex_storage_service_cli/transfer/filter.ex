defmodule ExStorageServiceCli.Transfer.Filter do
  @moduledoc "Ordered AWS glob filters with bounded, non-backtracking matching."
  alias ExStorageServiceCli.Error

  def compile(rules) when length(rules) <= 128 do
    if Enum.all?(rules, fn {action, pattern} ->
         action in [:include, :exclude] and is_binary(pattern) and byte_size(pattern) <= 8192 and
           String.valid?(pattern)
       end) do
      {:ok,
       Enum.map(rules, fn {action, pattern} ->
         {action, pattern |> String.to_charlist() |> tokens([]) |> List.to_tuple()}
       end)}
    else
      invalid()
    end
  end

  def compile(_), do: invalid()

  def include?(rules, name) do
    Enum.reduce(rules, true, fn {action, pattern}, included ->
      if matches?(pattern, name), do: action == :include, else: included
    end)
  end

  def matches?(tokens, name) do
    states = closure(tokens, MapSet.new([0]))

    states =
      for <<char::utf8 <- name>>, reduce: states do
        states ->
          next =
            Enum.reduce(states, MapSet.new(), fn index, acc ->
              if index == tuple_size(tokens) do
                acc
              else
                case elem(tokens, index) do
                  :star -> MapSet.put(acc, index)
                  token -> if match_char?(token, char), do: MapSet.put(acc, index + 1), else: acc
                end
              end
            end)

          closure(tokens, next)
      end

    MapSet.member?(states, tuple_size(tokens))
  end

  defp closure(tokens, states) do
    Enum.reduce(states, states, fn index, acc -> skip(tokens, index, acc) end)
  end

  defp skip(tokens, index, states) when index == tuple_size(tokens), do: states

  defp skip(tokens, index, states) do
    if elem(tokens, index) == :star,
      do: skip(tokens, index + 1, MapSet.put(states, index + 1)),
      else: states
  end

  defp match_char?(:any, _), do: true

  defp match_char?({:class, ranges, negated}, char),
    do: Enum.any?(ranges, fn {first, last} -> char >= first and char <= last end) != negated

  defp match_char?(literal, char), do: literal == char
  defp tokens([], acc), do: Enum.reverse(acc)
  defp tokens([?* | rest], [:star | _] = acc), do: tokens(rest, acc)
  defp tokens([?* | rest], acc), do: tokens(rest, [:star | acc])
  defp tokens([?? | rest], acc), do: tokens(rest, [:any | acc])

  defp tokens([?[ | rest], acc) do
    {negated, rest} =
      case rest do
        [?! | tail] -> {true, tail}
        _ -> {false, rest}
      end

    {prefix, rest} =
      case rest do
        [?] | tail] -> {[?]], tail}
        _ -> {[], rest}
      end

    case Enum.split_while(rest, &(&1 != ?])) do
      {_, []} -> tokens(if(negated, do: [?!], else: []) ++ prefix ++ rest, [?[ | acc])
      {chars, [_ | tail]} -> tokens(tail, [{:class, ranges(prefix ++ chars), negated} | acc])
    end
  end

  defp tokens([char | rest], acc), do: tokens(rest, [char | acc])
  defp ranges([first, ?-, last | rest]), do: [{first, last} | ranges(rest)]
  defp ranges([char | rest]), do: [{char, char} | ranges(rest)]
  defp ranges([]), do: []

  defp invalid,
    do:
      {:error,
       Error.new(:syntax, "Filter count or pattern exceeds its input budget",
         code: :filter_budget
       )}
end
