defmodule ExStorageServiceCli.XmlParser do
  @moduledoc """
  Lightweight S3 XML response parser using `:xmerl`.

  Parses standard S3 XML responses into Elixir maps.
  """

  @doc """
  Parses a ListAllMyBucketsResult XML response.

  Returns a list of bucket maps with `:name` and `:creation_date` keys.
  """
  def parse_list_buckets(xml) do
    {doc, _} = :xmerl_scan.string(String.to_charlist(xml), quiet: true)
    buckets_elements = xpath(doc, ~c"//Bucket")

    Enum.map(buckets_elements, fn bucket ->
      %{
        name: xpath_text(bucket, ~c"Name"),
        creation_date: xpath_text(bucket, ~c"CreationDate")
      }
    end)
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
    {doc, _} = :xmerl_scan.string(String.to_charlist(xml), quiet: true)

    contents =
      xpath(doc, ~c"//Contents")
      |> Enum.map(fn elem ->
        %{
          key: xpath_text(elem, ~c"Key"),
          size: xpath_text(elem, ~c"Size") |> parse_integer(),
          etag: xpath_text(elem, ~c"ETag") |> String.trim("\""),
          last_modified: xpath_text(elem, ~c"LastModified"),
          storage_class: xpath_text(elem, ~c"StorageClass")
        }
      end)

    common_prefixes =
      xpath(doc, ~c"//CommonPrefixes")
      |> Enum.map(fn elem ->
        xpath_text(elem, ~c"Prefix")
      end)

    is_truncated = xpath_text(doc, ~c"//IsTruncated") == "true"
    next_token = xpath_text_or_nil(doc, ~c"//NextContinuationToken")
    key_count = xpath_text(doc, ~c"//KeyCount") |> parse_integer()

    %{
      contents: contents,
      common_prefixes: common_prefixes,
      is_truncated: is_truncated,
      next_continuation_token: next_token,
      key_count: key_count
    }
  end

  @doc """
  Parses an S3 Error XML response.

  Returns `{:ok, %{code: String, message: String}}` or `:error`.
  """
  def parse_error(xml) do
    case safe_parse(xml) do
      {:ok, doc} ->
        code = xpath_text(doc, ~c"//Code")
        message = xpath_text(doc, ~c"//Message")

        if code != "" do
          {:ok, %{code: code, message: message}}
        else
          :error
        end

      :error ->
        :error
    end
  end

  # Private helpers

  defp safe_parse(xml) when is_binary(xml) do
    try do
      {doc, _} = :xmerl_scan.string(String.to_charlist(xml), quiet: true)
      {:ok, doc}
    rescue
      _ -> :error
    catch
      :exit, _ -> :error
    end
  end

  defp xpath(node, path) do
    :xmerl_xpath.string(path, node)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp xpath_text(node, path) do
    case xpath(node, path) do
      [elem | _] -> extract_text(elem)
      [] -> ""
    end
  end

  defp xpath_text_or_nil(node, path) do
    case xpath_text(node, path) do
      "" -> nil
      text -> text
    end
  end

  defp extract_text({:xmlElement, _, _, _, _, _, _, _, children, _, _, _}) do
    children
    |> Enum.map(&extract_text/1)
    |> Enum.join()
  end

  defp extract_text({:xmlText, _, _, _, value, _}) do
    to_string(value)
  end

  defp extract_text(_), do: ""

  defp parse_integer(""), do: 0

  defp parse_integer(str) do
    case Integer.parse(str) do
      {n, _} -> n
      :error -> 0
    end
  end
end
