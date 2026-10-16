defmodule ExStorageServiceCli.CLI.NativeTransferSelection do
  @moduledoc "Bounded native transfer predicates evaluated against one invocation clock."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.CLI.Find

  def compile(%{frontend: :native, command: [command], options: options}, opts)
      when command in ["cp", "mv", "mirror", "rm"] do
    now = DateTime.to_unix(opts[:now] || DateTime.utc_now(), :nanosecond)
    classes = if command == "mirror", do: options[:exclude_storageclass] || [], else: []

    with {:ok, older} <- cutoff(options[:older_than], now),
         {:ok, newer} <- cutoff(options[:newer_than], now),
         true <-
           is_list(classes) and length(classes) <= 1000 and
             Enum.all?(
               classes,
               &(is_binary(&1) and byte_size(&1) in 1..256 and String.valid?(&1))
             ) do
      {:ok, %{older: older, newer: newer, excluded_classes: MapSet.new(classes)}}
    else
      false ->
        failure(
          :native_transfer_storageclass,
          "Storage-class exclusions must be bounded nonempty strings"
        )

      error ->
        error
    end
  end

  def compile(_, _), do: {:ok, %{older: nil, newer: nil, excluded_classes: MapSet.new()}}

  def age?(selection), do: selection.older != nil or selection.newer != nil

  def include?(selection, entry) do
    # An omitted remote class is S3 STANDARD; local files have no storage class.
    storage_class =
      if entry["key"] != nil,
        do: entry["storage_class"] || "STANDARD",
        else: entry["storage_class"]

    cond do
      MapSet.member?(selection.excluded_classes, storage_class) ->
        {:ok, false}

      not age?(selection) ->
        {:ok, true}

      is_integer(entry["modified"]) and entry["modified"] > 0 ->
        stamp = entry["modified"] * 1000
        {:ok, older?(selection.older, stamp) and newer?(selection.newer, stamp)}

      true ->
        failure(
          :native_transfer_timestamp,
          "Transfer age predicates require an available modification timestamp"
        )
    end
  end

  defp cutoff(value, _now) when value in [nil, ""], do: {:ok, nil}

  defp cutoff(value, now) do
    case Find.time_cutoff(value, now) do
      {:ok, stamp} ->
        {:ok,
         %{cutoff: stamp, absolute: Regex.match?(~r/\A[0-9]{4}[.-][0-9]{2}[.-][0-9]{2}/, value)}}

      {:error, _} ->
        failure(
          :native_transfer_time,
          "Transfer age requires a bounded duration or absolute UTC/RFC3339 timestamp"
        )
    end
  end

  defp older?(nil, _), do: true
  defp older?(%{cutoff: cutoff, absolute: true}, stamp), do: stamp < cutoff
  defp older?(%{cutoff: cutoff}, stamp), do: stamp <= cutoff
  defp newer?(nil, _), do: true
  defp newer?(%{cutoff: cutoff, absolute: true}, stamp), do: stamp >= cutoff
  defp newer?(%{cutoff: cutoff}, stamp), do: stamp > cutoff
  defp failure(code, message), do: {:error, Error.new(:transfer, message, code: code)}
end
