defmodule ExStorageServiceCli.Transfer.Planner do
  @moduledoc "Pure transfer sizing using explicitly selected provider byte limits."
  alias ExStorageServiceCli.Error
  defstruct [:mode, :size, :part_size, :part_count, parts: []]
  @required ~w(max_single_put_bytes min_nonfinal_part_bytes max_part_bytes max_parts)a

  @doc "Plans a single PUT or at most 10000 exact multipart ranges without reading data."
  def plan(size, limits, opts \\ []) do
    with :ok <- limits_valid(limits),
         :ok <- size_valid(size, limits),
         :ok <- options_valid(opts) do
      multipart? =
        size > value(limits, :max_single_put_bytes) or opts[:force_multipart] == true or
          (is_integer(opts[:multipart_threshold]) and size >= opts[:multipart_threshold])

      if size == 0 or not multipart? do
        {:ok,
         %__MODULE__{
           mode: :single,
           size: size,
           part_size: size,
           part_count: 1,
           parts: [%{number: 1, offset: 0, length: size}]
         }}
      else
        multipart(size, limits, opts)
      end
    end
  end

  defp multipart(size, limits, opts) do
    min_part = value(limits, :min_nonfinal_part_bytes)
    max_part = value(limits, :max_part_bytes)
    max_count = value(limits, :max_parts)
    requested = opts[:part_size] || min_part
    part_size = max(max(min_part, requested), ceil_div(size, max_count))

    if part_size > max_part do
      failure(:multipart_limit, "Transfer exceeds the selected provider multipart limits")
    else
      count = ceil_div(size, part_size)

      parts =
        for number <- 1..count do
          offset = (number - 1) * part_size
          %{number: number, offset: offset, length: min(part_size, size - offset)}
        end

      {:ok,
       %__MODULE__{
         mode: :multipart,
         size: size,
         part_size: part_size,
         part_count: count,
         parts: parts
       }}
    end
  end

  defp limits_valid(limits) when is_map(limits) do
    known? =
      Enum.all?(@required, fn key -> is_integer(value(limits, key)) and value(limits, key) > 0 end)

    if known? and value(limits, :max_parts) <= 10_000 and
         value(limits, :min_nonfinal_part_bytes) <= value(limits, :max_part_bytes),
       do: :ok,
       else: failure(:unknown_provider_limits, "Select explicit valid provider transfer limits")
  end

  defp limits_valid(_),
    do: failure(:unknown_provider_limits, "Select explicit valid provider transfer limits")

  defp size_valid(size, limits) do
    maximum = value(limits, :max_object_bytes)

    cond do
      not is_integer(size) or size < 0 ->
        failure(:invalid_transfer_size, "Transfer size must be a nonnegative integer")

      not is_nil(maximum) and (not is_integer(maximum) or maximum < 0) ->
        failure(:unknown_provider_limits, "Provider object size limit is invalid")

      is_integer(maximum) and size > maximum ->
        failure(:object_size_limit, "Transfer exceeds the selected provider object size limit")

      true ->
        :ok
    end
  end

  defp options_valid(opts) do
    invalid? =
      Enum.any?([:part_size, :multipart_threshold], fn key ->
        Keyword.has_key?(opts, key) and (not is_integer(opts[key]) or opts[key] <= 0)
      end)

    if invalid?,
      do:
        failure(
          :invalid_transfer_option,
          "Part size and multipart threshold must be positive byte counts"
        ),
      else: :ok
  end

  defp value(limits, key), do: Map.get(limits, key, Map.get(limits, Atom.to_string(key)))
  defp ceil_div(value, divisor), do: div(value + divisor - 1, divisor)
  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
