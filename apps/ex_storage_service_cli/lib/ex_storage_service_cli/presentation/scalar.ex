defmodule ExStorageServiceCli.Presentation.Scalar do
  @moduledoc false

  def python(nil), do: "None"
  def python(true), do: "True"
  def python(false), do: "False"
  def python(value) when is_binary(value), do: value
  def python(value) when is_float(value), do: float(value)
  def python(value) when is_integer(value), do: Integer.to_string(value)
  def python(value) when is_list(value), do: "[" <> Enum.map_join(value, ", ", &repr/1) <> "]"

  def python(value) when is_map(value),
    do:
      "{" <>
        Enum.map_join(Enum.sort(value), ", ", fn {k, v} -> repr(k) <> ": " <> repr(v) end) <> "}"

  def safe(value) do
    value
    |> python()
    |> String.to_charlist()
    |> Enum.map(fn
      ?\n ->
        "\\n"

      ?\r ->
        "\\r"

      ?\t ->
        "\\t"

      code
      when code < 32 or code in 127..159 or code in 0x202A..0x202E or code in 0x2066..0x2069 ->
        "\\u" <> (Integer.to_string(code, 16) |> String.downcase() |> String.pad_leading(4, "0"))

      code ->
        <<code::utf8>>
    end)
    |> IO.iodata_to_binary()
  end

  # Both runtimes use shortest-roundtrip digits; Python chooses decimal form for
  # exponents -4 through 15 and pads a scientific exponent to at least two digits.
  def float(value) do
    text = :erlang.float_to_binary(value, [:short])

    case String.split(text, "e") do
      [decimal] ->
        decimal

      [mantissa, exponent] ->
        exponent = String.to_integer(exponent)

        if exponent >= -4 and exponent < 16 do
          decimal(mantissa, exponent)
        else
          mantissa = String.replace_suffix(mantissa, ".0", "")
          sign = if exponent < 0, do: "-", else: "+"
          mantissa <> "e" <> sign <> String.pad_leading(Integer.to_string(abs(exponent)), 2, "0")
        end
    end
  end

  defp decimal(mantissa, exponent) do
    sign = if String.starts_with?(mantissa, "-"), do: "-", else: ""

    digits =
      mantissa |> String.trim_leading("-") |> String.replace(".", "") |> String.trim_trailing("0")

    position = exponent + 1

    cond do
      position <= 0 ->
        sign <> "0." <> String.duplicate("0", -position) <> digits

      position >= byte_size(digits) ->
        sign <> String.pad_trailing(digits, position, "0") <> ".0"

      true ->
        sign <>
          binary_part(digits, 0, position) <>
          "." <> binary_part(digits, position, byte_size(digits) - position)
    end
  end

  defp repr(value) when is_binary(value), do: "'" <> String.replace(value, "'", "\\'") <> "'"
  defp repr(value), do: python(value)
end
