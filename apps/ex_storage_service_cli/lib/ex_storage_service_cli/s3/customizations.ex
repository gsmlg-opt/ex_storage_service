defmodule ExStorageServiceCli.S3.Customizations do
  @moduledoc "Explicit pinned SDK customizations outside the REST-XML model."
  alias ExStorageServiceCli.{Error, S3.Encoding}
  @listings ~w(ListObjects ListObjectsV2 ListObjectVersions)

  def input(operation, input) when operation in @listings and is_map(input) do
    automatic = not Map.has_key?(input, "EncodingType")
    {:ok, Map.put_new(input, "EncodingType", "url"), %{automatic_encoding: automatic}}
  end

  def input(operation, %{"CopySource" => source} = input)
      when operation in ~w(CopyObject UploadPartCopy) do
    with {:ok, source} <- copy_source(source),
         do: {:ok, Map.put(input, "CopySource", source), %{}}
  end

  def input(_operation, input), do: {:ok, input, %{}}

  def output(operation, %{"EncodingType" => "url"} = result, %{automatic_encoding: true})
      when operation in @listings do
    {fields, collections} =
      case operation do
        "ListObjects" ->
          {~w(Delimiter Marker NextMarker), [{"Contents", "Key"}, {"CommonPrefixes", "Prefix"}]}

        "ListObjectsV2" ->
          {~w(Delimiter Prefix StartAfter), [{"Contents", "Key"}, {"CommonPrefixes", "Prefix"}]}

        "ListObjectVersions" ->
          {~w(KeyMarker NextKeyMarker Prefix Delimiter),
           [{"Versions", "Key"}, {"DeleteMarkers", "Key"}, {"CommonPrefixes", "Prefix"}]}
      end

    result = Enum.reduce(fields, result, &decode_field(&2, &1))

    Enum.reduce(collections, result, fn {collection, field}, result ->
      case Map.fetch(result, collection) do
        {:ok, values} -> Map.put(result, collection, Enum.map(values, &decode_field(&1, field)))
        :error -> result
      end
    end)
  end

  def output(_operation, result, _state), do: result

  defp decode_field(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} -> Map.put(map, field, URI.decode(value))
      :error -> map
    end
  end

  defp copy_source(source) when is_binary(source) do
    # The raw API spelling preserves a final, already escaped version suffix.
    # A native key uses the structured form below, avoiding this ambiguity.
    case Regex.run(~r/\?versionId=[^\s]+$/u, source, return: :index) do
      [{offset, length}] ->
        {:ok,
         Encoding.path(binary_part(source, 0, offset)) <> binary_part(source, offset, length)}

      nil ->
        {:ok, Encoding.path(source)}
    end
  end

  defp copy_source(%{"Bucket" => bucket, "Key" => key} = source)
       when is_binary(bucket) and bucket != "" and is_binary(key) and key != "" do
    version = source["VersionId"]

    if Enum.all?(Map.keys(source), &(&1 in ~w(Bucket Key VersionId))) and
         (is_nil(version) or is_binary(version)) do
      path =
        if String.starts_with?(bucket, "arn:"),
          do: bucket <> "/object/" <> key,
          else: "/" <> bucket <> "/" <> key

      suffix = if version, do: "?versionId=" <> Encoding.encode(version), else: ""
      {:ok, Encoding.path(path) <> suffix}
    else
      invalid_source()
    end
  end

  defp copy_source(_source), do: invalid_source()

  defp invalid_source,
    do:
      {:error,
       Error.new(:syntax, "CopySource must be a string or a complete native source identity")}
end
