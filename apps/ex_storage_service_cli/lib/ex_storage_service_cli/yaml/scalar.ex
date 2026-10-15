defmodule ExStorageServiceCli.YAML.Scalar do
  @moduledoc false

  def timestamp(text) do
    case Regex.run(
           ~r/\A(\d{4})-(\d{1,2})-(\d{1,2})(?:[Tt \t]+(\d{1,2}):(\d{2}):(\d{2})(\.\d*)?(?:[ \t]*(Z|[-+]\d{1,2}(?::?\d{2})?))?)?\z/,
           text
         ) do
      [_, year, month, day] ->
        with {:ok, date} <- Date.new(number(year), number(month), number(day)) do
          {:ok, {:yaml_date, Date.to_iso8601(date)}}
        else
          _ -> :error
        end

      [_, year, month, day, hour, minute, second | rest] ->
        fraction = Enum.at(rest, 0, "")
        zone = Enum.at(rest, 1, "")

        iso =
          "#{year}-#{pad(month)}-#{pad(day)}T#{pad(hour)}:#{minute}:#{second}#{fraction}#{zone(zone)}"

        case DateTime.from_iso8601(iso) do
          {:ok, datetime, _} -> {:ok, {:yaml_timestamp, DateTime.to_iso8601(datetime)}}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp zone(""), do: "Z"
  defp zone("Z"), do: "Z"

  defp zone(<<sign, rest::binary>>) do
    case String.split(rest, ":") do
      [hour, minute] ->
        <<sign>> <> pad(hour) <> ":" <> minute

      [digits] when byte_size(digits) > 2 ->
        size = byte_size(digits) - 2
        <<hour::binary-size(size), minute::binary>> = digits
        <<sign>> <> pad(hour) <> ":" <> minute

      [hour] ->
        <<sign>> <> pad(hour) <> ":00"
    end
  end

  defp pad(value), do: String.pad_leading(value, 2, "0")
  defp number(value), do: String.to_integer(value)
end
