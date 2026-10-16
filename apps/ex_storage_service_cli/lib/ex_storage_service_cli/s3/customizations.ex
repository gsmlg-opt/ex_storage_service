defmodule ExStorageServiceCli.S3.Customizations do
  @moduledoc "Explicit pinned SDK customizations outside the REST-XML model."
  alias ExStorageServiceCli.{Error, S3.Encoding}
  @listings ~w(ListObjects ListObjectsV2 ListObjectVersions)
  @sse_operations ~w(HeadObject GetObject PutObject CopyObject CreateMultipartUpload UploadPart UploadPartCopy CompleteMultipartUpload)
  @metadata_operations ~w(PutObject CopyObject CreateMultipartUpload)

  def input(operation, input) when is_map(input) do
    with :ok <- ascii_metadata(operation, input) do
      input = if operation in @sse_operations, do: sse_key(input, "SSECustomer"), else: input

      input =
        if operation in ~w(CopyObject UploadPartCopy),
          do: sse_key(input, "CopySourceSSECustomer"),
          else: input

      prepare_input(operation, input)
    end
  end

  def input(_operation, input), do: {:ok, input, %{}}

  defp prepare_input("RenameObject", input) do
    # Client applies input customizations once, before entering its retry loop.
    {:ok, Map.put_new_lazy(input, "ClientToken", &idempotency_token/0), %{}}
  end

  defp prepare_input(operation, input) when operation in @listings do
    automatic = not Map.has_key?(input, "EncodingType")
    {:ok, Map.put_new(input, "EncodingType", "url"), %{automatic_encoding: automatic}}
  end

  defp prepare_input(operation, %{"CopySource" => source} = input)
       when operation in ~w(CopyObject UploadPartCopy) do
    with {:ok, source} <- copy_source(source),
         do: {:ok, Map.put(input, "CopySource", source), %{}}
  end

  defp prepare_input(_operation, input), do: {:ok, input, %{}}

  # Explicit MD5 presence means the caller owns both wire values. High-level
  # transfers already provide this pair, so they must never be encoded twice.
  defp sse_key(input, prefix) do
    key = prefix <> "Key"
    md5 = prefix <> "KeyMD5"

    if is_binary(input[key]) and not Map.has_key?(input, md5) do
      input
      |> Map.put(key, Base.encode64(input[key]))
      |> Map.put(md5, Base.encode64(:crypto.hash(:md5, input[key])))
    else
      input
    end
  end

  defp ascii_metadata(operation, %{"Metadata" => metadata})
       when operation in @metadata_operations and is_map(metadata) do
    if Enum.all?(metadata, fn {key, value} -> ascii?(key) and ascii?(value) end) do
      :ok
    else
      {:error,
       Error.new(
         :syntax,
         "Non ascii characters found in S3 metadata. S3 metadata can only contain ASCII characters.",
         operation: operation,
         code: :invalid_parameter
       )}
    end
  end

  defp ascii_metadata(_, _), do: :ok

  defp ascii?(<<byte, rest::binary>>) when byte < 128, do: ascii?(rest)
  defp ascii?(<<>>), do: true
  defp ascii?(value) when is_binary(value), do: false
  defp ascii?(_), do: true

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

  defp idempotency_token do
    <<first::48, _::4, second::12, _::2, last::62>> = :crypto.strong_rand_bytes(16)

    encoded =
      Base.encode16(<<first::48, 4::4, second::12, 2::2, last::62>>, case: :lower)

    <<a::binary-size(8), b::binary-size(4), c::binary-size(4), d::binary-size(4), e::binary>> =
      encoded

    Enum.join([a, b, c, d, e], "-")
  end

  defp invalid_source,
    do:
      {:error,
       Error.new(:syntax, "CopySource must be a string or a complete native source identity")}
end
