defmodule ExStorageService.Metadata.ListIndex.Codec do
  @moduledoc """
  Byte-ordered keys for the listing radix trees.

  Components use an escaped, terminated encoding so a component's prefix sorts
  before its extension. Complementing the entire encoding reverses that order.
  These are tree keys; metadata record names use their own safe encoding.
  """

  def current(key) when is_binary(key), do: key

  def ascending(value) when is_binary(value), do: key_prefix(value) <> <<0>>

  @doc "The lower bound for every component beginning with the raw prefix."
  def key_prefix(value) when is_binary(value) do
    for <<byte <- value>>, into: <<>>, do: <<1, byte>>
  end

  def descending(value) when is_binary(value), do: complement(ascending(value))

  @doc "The prefix shared by all versions of exactly this object key."
  def version_prefix(key) when is_binary(key), do: ascending(key)

  def version(key, version_id, metadata, is_latest)
      when is_binary(key) and is_binary(version_id) and is_map(metadata) and
             is_boolean(is_latest) do
    timestamp = last_modified(metadata)
    rank = if is_latest, do: 0, else: 1
    ascending(key) <> <<rank>> <> descending(timestamp) <> descending(version_id)
  end

  @doc "Matches VersionListing's created_at, updated_at, then empty fallback."
  def last_modified(metadata) when is_map(metadata) do
    case Map.get(metadata, :created_at, Map.get(metadata, :updated_at, "")) do
      value when is_binary(value) -> value
      _ -> raise ArgumentError, "invalid listing timestamp"
    end
  end

  def upload_prefix(key) when is_binary(key), do: ascending(key)

  def upload(key, upload_id) when is_binary(key) and is_binary(upload_id),
    do: ascending(key) <> ascending(upload_id)

  def part(number) when is_integer(number) and number >= 1 and number <= 10_000,
    do: <<number::unsigned-big-16>>

  def decode_version(encoded) when is_binary(encoded) do
    with {:ok, key, <<rank, rest::binary>>} when rank in [0, 1] <- component(encoded, :asc),
         {:ok, timestamp, rest} <- component(rest, :desc),
         {:ok, version_id, <<>>} <- component(rest, :desc) do
      {:ok, %{key: key, version_id: version_id, last_modified: timestamp, is_latest: rank == 0}}
    else
      _ -> {:error, :invalid_index_key}
    end
  end

  def decode_upload(encoded) when is_binary(encoded) do
    with {:ok, key, rest} <- component(encoded, :asc),
         {:ok, upload_id, <<>>} <- component(rest, :asc) do
      {:ok, %{key: key, upload_id: upload_id}}
    else
      _ -> {:error, :invalid_index_key}
    end
  end

  def decode_part(<<number::unsigned-big-16>>) when number >= 1 and number <= 10_000,
    do: {:ok, number}

  def decode_part(_), do: {:error, :invalid_index_key}

  @doc "The exclusive end of a raw prefix range, or nil if no finite bound exists."
  def prefix_successor(prefix) when is_binary(prefix),
    do: successor(prefix, byte_size(prefix))

  defp successor(_prefix, 0), do: nil

  defp successor(prefix, size) do
    case :binary.at(prefix, size - 1) do
      255 -> successor(prefix, size - 1)
      byte -> binary_part(prefix, 0, size - 1) <> <<byte + 1>>
    end
  end

  defp component(encoded, direction), do: component(encoded, direction, [])
  defp component(<<0, rest::binary>>, :asc, acc), do: decoded(acc, rest)
  defp component(<<255, rest::binary>>, :desc, acc), do: decoded(acc, rest)

  defp component(<<1, byte, rest::binary>>, :asc, acc),
    do: component(rest, :asc, [byte | acc])

  defp component(<<254, byte, rest::binary>>, :desc, acc),
    do: component(rest, :desc, [255 - byte | acc])

  defp component(_, _, _), do: {:error, :invalid_index_key}

  defp decoded(acc, rest), do: {:ok, acc |> Enum.reverse() |> :erlang.list_to_binary(), rest}

  defp complement(value) do
    for <<byte <- value>>, into: <<>>, do: <<255 - byte>>
  end
end
