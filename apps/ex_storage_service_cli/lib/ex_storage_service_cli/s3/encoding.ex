defmodule ExStorageServiceCli.S3.Encoding do
  @moduledoc "RFC3986 byte encoding for S3 keys, query pairs, and copy sources."

  @doc "Encodes every byte except RFC3986 unreserved characters."
  def encode(value) when is_binary(value), do: URI.encode(value, &unreserved?/1)

  @doc "Encodes a raw object path without normalizing slash or dot segments."
  def path(value) when is_binary(value), do: URI.encode(value, &(&1 == ?/ or unreserved?(&1)))

  @doc "Encodes and sorts raw query pairs, retaining repeated names and values."
  def query(pairs) do
    pairs
    |> Enum.map(fn {key, value} -> {encode(key), encode(value)} end)
    |> Enum.sort()
    |> Enum.map_join("&", fn {key, value} -> key <> "=" <> value end)
  end

  @doc "Builds the x-amz-copy-source value from raw bucket, key, and optional version."
  def copy_source(bucket, key, version_id \\ nil) do
    source = "/" <> encode(bucket) <> "/" <> path(key)
    if is_nil(version_id), do: source, else: source <> "?" <> query([{"versionId", version_id}])
  end

  @doc false
  def url_path(nil), do: "/"
  def url_path(""), do: "/"
  def url_path(path), do: encode_url_path(path, []) |> Enum.reverse() |> IO.iodata_to_binary()

  # A URL already contains escaped bytes. Preserve those triplets, while
  # escaping raw/invalid percent bytes without treating them as key syntax.
  defp encode_url_path(<<"%", a, b, rest::binary>>, acc)
       when a in ?0..?9 or a in ?a..?f or a in ?A..?F do
    if b in ?0..?9 or b in ?a..?f or b in ?A..?F do
      encode_url_path(rest, [<<"%", a, b>> | acc])
    else
      encode_url_path(<<a, b, rest::binary>>, ["%25" | acc])
    end
  end

  defp encode_url_path(<<byte, rest::binary>>, acc) do
    encoded = if byte == ?/ or unreserved?(byte), do: <<byte>>, else: encode(<<byte>>)
    encode_url_path(rest, [encoded | acc])
  end

  defp encode_url_path(<<>>, acc), do: acc

  @doc false
  def query_pairs(nil), do: []
  def query_pairs(""), do: []

  def query_pairs(query) do
    query
    |> String.split("&")
    |> Enum.map(fn pair ->
      case String.split(pair, "=", parts: 2) do
        [key, value] -> {URI.decode(key), URI.decode(value)}
        [key] -> {URI.decode(key), ""}
      end
    end)
  end

  defp unreserved?(byte) do
    byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in ~c"-._~"
  end
end
