defmodule ExStorageServiceCli.Presentation.Text do
  @moduledoc false
  alias ExStorageServiceCli.Presentation.Scalar

  # AWS CLI 2.31.18 awscli/text.py algorithm; see presentation/NOTICE.txt.
  def render(data), do: format(data, nil, nil)

  defp format(data, identifier, keys) when is_map(data) do
    {scalars, containers} = partition(data, keys)
    prefix = if identifier, do: [String.upcase(Scalar.safe(identifier))], else: []
    row = if scalars == [], do: [], else: [Enum.join(prefix ++ scalars, "\t"), "\n"]
    [row, Enum.map(containers, fn {name, value} -> format(value, name, nil) end)]
  end

  defp format([], _, _), do: []

  defp format(data, identifier, _) when is_list(data) do
    cond do
      Enum.any?(data, &is_map/1) ->
        if not Enum.all?(data, &is_map/1), do: throw(:invalid_presentation)

        keys =
          data
          |> Enum.flat_map(fn item -> for {k, v} <- item, not container?(v), do: k end)
          |> Enum.uniq()
          |> Enum.sort()

        Enum.map(data, &format(&1, identifier, keys))

      Enum.any?(data, &is_list/1) ->
        {scalars, nested} = Enum.split_with(data, &(not container?(&1)))
        [scalar_list(scalars, identifier), Enum.map(nested, &format(&1, identifier, nil))]

      true ->
        scalar_list(data, identifier)
    end
  end

  defp format(value, _, _), do: [Scalar.safe(value), "\n"]
  defp scalar_list([], _), do: []
  defp scalar_list(values, nil), do: [Enum.map_join(values, "\t", &Scalar.safe/1), "\n"]

  defp scalar_list(values, name),
    do: Enum.map(values, &[String.upcase(Scalar.safe(name)), "\t", Scalar.safe(&1), "\n"])

  defp partition(data, nil) do
    {scalars, containers} =
      data |> Enum.sort() |> Enum.split_with(fn {_, value} -> not container?(value) end)

    {Enum.map(scalars, fn {_, value} -> Scalar.safe(value) end), containers}
  end

  defp partition(data, keys) do
    {Enum.map(keys, &Scalar.safe(Map.get(data, &1, ""))), data |> Map.drop(keys) |> Enum.sort()}
  end

  defp container?(value), do: is_map(value) or is_list(value)
end
