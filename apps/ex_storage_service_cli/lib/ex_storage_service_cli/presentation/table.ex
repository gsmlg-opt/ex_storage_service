defmodule ExStorageServiceCli.Presentation.Table do
  @moduledoc false
  alias ExStorageServiceCli.Presentation.{Scalar, Width}

  # AWS CLI 2.31.18 formatter/table algorithms; see presentation/NOTICE.txt.
  def render(data, options) do
    sections = build([], options[:command_name] || "", data, 0) |> Enum.reverse()

    if sections == [] do
      []
    else
      width = max_width(sections)
      terminal = options[:terminal_width] || terminal_width()
      sections = if width > terminal, do: Enum.map(sections, &vertical/1), else: sections
      width = max_width(sections)
      color = color?(options[:color] || "auto")
      [String.duplicate("-", width), "\n", Enum.map(sections, &section(&1, width, color))]
    end
  end

  defp empty?(value),
    do: value in [nil, false, 0, 0.0, "", []] or (is_map(value) and map_size(value) == 0)

  defp container?(value), do: is_list(value) or is_map(value)

  defp build(sections, title, data, indent) do
    if empty?(data) do
      sections
    else
      sections = if title == nil, do: sections, else: [new(title, indent) | sections]
      contents(sections, title, data, indent)
    end
  end

  defp contents(sections, _, data, indent) when is_map(data) do
    {headers, more} = keys(data)

    sections =
      case headers do
        [] -> sections
        [key] -> row(sections, [key, Map.fetch!(data, key)])
        _ -> sections |> header(headers) |> row(Enum.map(headers, &Map.fetch!(data, &1)))
      end

    Enum.reduce(more, sections, fn key, acc -> build(acc, key, data[key], indent + 1) end)
  end

  defp contents(sections, title, [first | _] = data, indent) when is_map(first) do
    if not Enum.all?(data, &is_map/1), do: throw(:invalid_presentation)
    {headers, more} = data |> Enum.map(&keys/1) |> Enum.unzip()
    headers = headers |> List.flatten() |> Enum.uniq() |> Enum.sort()
    more = more |> List.flatten() |> Enum.uniq() |> Enum.sort()
    sections = header(sections, headers)

    data
    |> Enum.with_index()
    |> Enum.reduce(sections, fn {element, index}, acc ->
      acc =
        if index > 0 and more != [], do: [new(title, indent) | acc] |> header(headers), else: acc

      acc = row(acc, Enum.map(headers, &Map.get(element, &1, "")))

      Enum.reduce(more, acc, fn key, acc ->
        if Map.has_key?(element, key), do: build(acc, key, element[key], indent + 1), else: acc
      end)
    end)
  end

  defp contents(sections, _, data, _) when is_list(data) do
    Enum.reduce(data, sections, fn item, acc ->
      cond do
        not container?(item) -> row(acc, [item])
        Enum.all?(item, &(not container?(&1))) -> row(acc, item)
        true -> build(acc, nil, item, 0)
      end
    end)
  end

  defp contents(sections, _, _, _), do: sections

  defp keys(data) do
    {scalars, nested} =
      data |> Enum.sort() |> Enum.split_with(fn {_, value} -> not container?(value) end)

    {Enum.map(scalars, &elem(&1, 0)), Enum.map(nested, &elem(&1, 0))}
  end

  defp new(title, indent),
    do: %{
      title: Scalar.safe(title || ""),
      indent: indent,
      headers: [],
      rows: [],
      widths: [],
      columns: nil
    }

  defp header([current | rest], headers) do
    headers = Enum.map(headers, &Scalar.safe/1)

    [
      %{
        current
        | headers: headers,
          columns: current.columns || length(headers),
          widths: widths(current.widths, headers)
      }
      | rest
    ]
  end

  defp row([current | rest], values) do
    if current.columns != nil and length(values) != current.columns,
      do: throw(:invalid_presentation)

    values = Enum.map(values, &Scalar.safe/1)

    [
      %{
        current
        | rows: [values | current.rows],
          columns: current.columns || length(values),
          widths: widths(current.widths, values)
      }
      | rest
    ]
  end

  defp widths([], values), do: Enum.map(values, &Width.length/1)

  defp widths(previous, values),
    do: Enum.zip_with(previous, values, fn width, value -> max(width, Width.length(value)) end)

  defp max_width(sections),
    do:
      Enum.max(
        Enum.map(sections, fn s ->
          max(Width.length(s.title) + 2, Enum.sum(s.widths) + 4 * length(s.widths) + 2) +
            2 * s.indent
        end)
      )

  defp vertical(%{rows: [row], headers: [_ | _]} = s) do
    rows = Enum.zip_with(s.headers, row, fn key, value -> [key, value] end)

    %{
      s
      | headers: [],
        rows: Enum.reverse(rows),
        widths: Enum.reduce(rows, [], &widths(&2, &1)),
        columns: 2
    }
  end

  defp vertical(section), do: section

  defp section(s, max_width, color) do
    width = max_width - 2 * s.indent
    columns = scale(s.widths, width)
    title = if s.title == "", do: [], else: [center(s.title, width, "|"), "\n"]

    title_border =
      if s.title != "" and s.headers == [] and s.rows == [],
        do: ["+", spaces(width - 2, "-"), "+\n"],
        else: []

    headers =
      if s.headers == [],
        do: [],
        else: [border(columns), cells(s.headers, columns, :center, false), "\n"]

    rows =
      if s.rows == [] or columns == [],
        do: [],
        else: [
          border(columns),
          Enum.map(Enum.reverse(s.rows), &[cells(&1, columns, :left, color), "\n"]),
          border(columns)
        ]

    content = IO.iodata_to_binary([title, title_border, headers, rows])
    indentation = String.duplicate(if(color, do: "\e[2m\e[33m|\e[0m", else: "|"), s.indent)
    content |> String.split("\n", trim: true) |> Enum.map(&[indentation, &1, indentation, "\n"])
  end

  defp cells(values, widths, alignment, color) do
    Enum.zip(values, widths)
    |> Enum.with_index()
    |> Enum.map(fn {{value, width}, index} ->
      left = if index == 0, do: "|", else: ""

      case alignment do
        :center ->
          center(value, width, left)

        :left ->
          size = Width.length(value)
          padding = if width - size - 2 - byte_size(left) - 1 >= 0, do: 2, else: 0
          styled = if color, do: ["\e[1m\e[34m", value, "\e[0m"], else: value

          [
            left,
            spaces(padding),
            styled,
            spaces(width - byte_size(left) - padding - size - 1),
            "|"
          ]
      end
    end)
  end

  defp center(value, width, left) do
    size = Width.length(value)
    start = div(width, 2) - div(size, 2) - 1
    [left, spaces(start), value, spaces(width - byte_size(left) - start - size - 1), "|"]
  end

  defp border(widths) do
    [
      Enum.with_index(widths)
      |> Enum.map(fn {width, index} ->
        [
          if(index == 0, do: "+", else: ""),
          spaces(width - if(index == 0, do: 2, else: 1), "-"),
          "+"
        ]
      end),
      "\n"
    ]
  end

  defp scale([], _), do: []

  defp scale(widths, target) do
    widths = Enum.map(widths, &(&1 + 4))
    total = Enum.sum(widths)
    scaled = Enum.map(widths, &round_even(target / total * &1))
    adjust(scaled, Enum.sum(scaled) - target)
  end

  defp adjust(widths, 0), do: widths

  defp adjust(widths, difference) do
    indices = 0..(length(widths) - 1) |> Enum.to_list()
    indices = if difference < 0, do: Enum.reverse(indices), else: indices

    {widths, difference} =
      Enum.reduce_while(indices, {widths, difference}, fn index, {widths, remaining} ->
        delta = if remaining > 0, do: -1, else: 1
        next = {List.update_at(widths, index, &(&1 + delta)), remaining + delta}
        if elem(next, 1) == 0, do: {:halt, next}, else: {:cont, next}
      end)

    adjust(widths, difference)
  end

  defp round_even(value) do
    lower = floor(value)
    if value - lower == 0.5, do: lower + rem(lower, 2), else: round(value)
  end

  defp spaces(count, char \\ " "), do: String.duplicate(char, max(count, 0))

  defp terminal_width do
    case :io.columns() do
      {:ok, width} -> width
      _ -> 80
    end
  end

  defp color?("on"), do: true
  defp color?("off"), do: false
  defp color?("auto"), do: match?({:ok, _}, :io.columns())
end
