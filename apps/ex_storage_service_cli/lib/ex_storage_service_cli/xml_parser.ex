defmodule ExStorageServiceCli.XmlParser do
  @moduledoc """
  Bounded S3 XML response parser using binary element names.

  Parses standard S3 XML responses into Elixir maps.
  """
  alias ExStorageServiceCli.{Error, S3.XML}

  @doc """
  Parses a ListAllMyBucketsResult XML response.

  Returns a list of bucket maps with `:name` and `:creation_date` keys.
  """
  def parse_list_buckets(xml) do
    case decode_list_buckets(xml) do
      {:ok, buckets} -> buckets
      {:error, _error} -> raise ArgumentError, "Invalid S3 bucket-list response"
    end
  end

  @doc false
  def decode_list_buckets(xml) do
    with {:ok, doc} <- document(xml, "ListAllMyBucketsResult") do
      buckets =
        Enum.map(descendants(doc, "Bucket"), fn bucket ->
          %{
            name: XML.child_text(bucket, "Name"),
            creation_date: XML.child_text(bucket, "CreationDate")
          }
        end)

      {:ok, buckets}
    end
  end

  @doc """
  Parses a ListBucketResult (V2) XML response.

  Returns a map with:
    * `:contents` - list of object maps
    * `:common_prefixes` - list of prefix strings
    * `:is_truncated` - boolean
    * `:next_continuation_token` - string or nil
    * `:key_count` - integer
  """
  def parse_list_objects(xml) do
    case decode_list_objects(xml) do
      {:ok, objects} -> objects
      {:error, _error} -> raise ArgumentError, "Invalid S3 object-list response"
    end
  end

  @doc false
  def decode_list_objects(xml) do
    with {:ok, doc} <- document(xml, "ListBucketResult") do
      contents =
        XML.children(doc, "Contents")
        |> Enum.map(fn elem ->
          %{
            key: XML.child_text(elem, "Key"),
            size: XML.child_text(elem, "Size") |> parse_integer(),
            etag: XML.child_text(elem, "ETag") |> String.trim("\""),
            last_modified: XML.child_text(elem, "LastModified"),
            storage_class: XML.child_text(elem, "StorageClass")
          }
        end)

      common_prefixes =
        XML.children(doc, "CommonPrefixes")
        |> Enum.map(fn elem ->
          XML.child_text(elem, "Prefix")
        end)

      is_truncated = XML.child_text(doc, "IsTruncated") == "true"

      next_token =
        case XML.child_text(doc, "NextContinuationToken") do
          "" -> nil
          value -> value
        end

      key_count = XML.child_text(doc, "KeyCount") |> parse_integer()

      {:ok,
       %{
         contents: contents,
         common_prefixes: common_prefixes,
         is_truncated: is_truncated,
         next_continuation_token: next_token,
         key_count: key_count
       }}
    end
  end

  @doc """
  Parses an S3 Error XML response.

  Returns `{:ok, %{code: String, message: String}}` or `:error`.
  """
  def parse_error(xml) do
    case document(xml, "Error") do
      {:ok, doc} ->
        code = XML.child_text(doc, "Code")
        message = XML.child_text(doc, "Message")

        if code != "" do
          {:ok, %{code: code, message: message}}
        else
          :error
        end

      {:error, _error} ->
        :error
    end
  end

  # Private helpers

  defp document(xml, expected) do
    with {:ok, doc} <- XML.parse(xml) do
      if XML.local_name(doc.name) == expected,
        do: {:ok, doc},
        else: {:error, Error.new(:protocol, "Unexpected S3 XML result")}
    end
  end

  defp descendants(%XML.Node{content: content}, name) do
    Enum.flat_map(content, fn
      %XML.Node{} = child ->
        if XML.local_name(child.name) == name, do: [child], else: descendants(child, name)

      _text ->
        []
    end)
  end

  defp parse_integer(""), do: 0

  defp parse_integer(str) do
    case Integer.parse(str) do
      {n, _} -> n
      :error -> 0
    end
  end
end
