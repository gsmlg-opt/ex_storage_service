defmodule ExStorageServiceCli.Presentation.JSON do
  @moduledoc false
  alias ExStorageServiceCli.Presentation.Scalar

  def render(data), do: [encode(data, 0), "\n"]
  defp encode(value, _) when is_map(value) and map_size(value) == 0, do: "{}"
  defp encode([], _), do: "[]"

  defp encode(value, depth) when is_map(value) do
    items =
      value
      |> Enum.sort()
      |> Enum.map(fn {key, data} ->
        [indent(depth + 1), JSON.encode!(key), ": ", encode(data, depth + 1)]
      end)

    ["{\n", Enum.intersperse(items, ",\n"), "\n", indent(depth), "}"]
  end

  defp encode(value, depth) when is_list(value) do
    items = Enum.map(value, &[indent(depth + 1), encode(&1, depth + 1)])
    ["[\n", Enum.intersperse(items, ",\n"), "\n", indent(depth), "]"]
  end

  defp encode(value, _) when is_float(value), do: Scalar.float(value)
  defp encode(value, _), do: JSON.encode!(value)
  defp indent(depth), do: String.duplicate(" ", depth * 4)
end
