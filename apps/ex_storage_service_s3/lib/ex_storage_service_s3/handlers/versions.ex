defmodule ExStorageServiceS3.Handlers.Versions do
  @moduledoc false
  import ExStorageServiceS3.Handlers.Shared
  alias ExStorageService.CloudCache.Config
  alias ExStorageService.Storage.VersionListing
  alias ExStorageServiceS3.XML

  def list_object_versions(conn, bucket) do
    with {:ok, opts} <- request_options(conn.query_params),
         :ok <- hosted_bucket(bucket),
         {:ok, page} <- VersionListing.list(bucket, opts) do
      xml_response(conn, 200, document(bucket, page), request_id(conn))
    else
      {:error, :invalid_argument} ->
        error_response(
          conn,
          "InvalidArgument",
          "The version listing parameters are invalid.",
          "/#{bucket}",
          request_id(conn)
        )

      {:error, :unsupported_gateway_versions} ->
        error_response(
          conn,
          "NotImplemented",
          "Version listing is unavailable for gateway buckets.",
          "/#{bucket}",
          request_id(conn)
        )

      {:error, reason} ->
        storage_error_response(conn, reason, "/#{bucket}", request_id(conn))
    end
  end

  def document(bucket, page) do
    encode = fn value ->
      value = value || ""
      if page.encoding_type == "url", do: URI.encode(value, &URI.char_unreserved?/1), else: value
    end

    entries =
      Enum.map(page.entries, fn entry ->
        name = if entry.delete_marker, do: "DeleteMarker", else: "Version"
        metadata = entry.metadata

        [
          "<",
          name,
          ">",
          element("Key", encode.(entry.key)),
          element("VersionId", entry.version_id),
          element("IsLatest", to_string(entry.is_latest)),
          element("LastModified", timestamp(entry.last_modified)),
          "<Owner>",
          element("ID", Map.get(metadata, :owner_id, "owner-id")),
          element("DisplayName", Map.get(metadata, :owner_display_name, "owner")),
          "</Owner>",
          if(entry.delete_marker,
            do: [],
            else: [
              element("ETag", "\"" <> String.trim(Map.get(metadata, :etag, ""), "\"") <> "\""),
              element("Size", to_string(Map.get(metadata, :size, 0))),
              element("StorageClass", "STANDARD")
            ]
          ),
          "</",
          name,
          ">"
        ]
      end)

    IO.iodata_to_binary([
      ~s(<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">),
      element("Name", bucket),
      element("Prefix", encode.(page.prefix)),
      element("KeyMarker", encode.(page.key_marker)),
      element("VersionIdMarker", page.version_id_marker || ""),
      if(page.is_truncated,
        do: [
          element("NextKeyMarker", encode.(page.next_key_marker)),
          if(page.next_version_id_marker,
            do: element("NextVersionIdMarker", page.next_version_id_marker),
            else: []
          )
        ],
        else: []
      ),
      element("MaxKeys", to_string(page.max_keys)),
      if(page.delimiter, do: element("Delimiter", encode.(page.delimiter)), else: []),
      if(page.encoding_type, do: element("EncodingType", page.encoding_type), else: []),
      element("IsTruncated", to_string(page.is_truncated)),
      entries,
      Enum.map(
        page.common_prefixes,
        &["<CommonPrefixes>", element("Prefix", encode.(&1)), "</CommonPrefixes>"]
      ),
      "</ListVersionsResult>"
    ])
  end

  defp request_options(params) do
    value = Map.get(params, "max-keys", "1000")

    case Integer.parse(value) do
      {maximum, ""} when maximum >= 0 ->
        {:ok,
         [
           prefix: Map.get(params, "prefix", ""),
           delimiter: params["delimiter"],
           max_keys: maximum,
           key_marker: params["key-marker"],
           version_id_marker: params["version-id-marker"],
           encoding_type: params["encoding-type"]
         ]}

      _ ->
        {:error, :invalid_argument}
    end
  end

  defp hosted_bucket(bucket) do
    case Config.get_config(bucket) do
      {:ok, %{enabled: true}} -> {:error, :unsupported_gateway_versions}
      {:ok, _} -> :ok
      {:error, :not_found} -> :ok
      error -> error
    end
  end

  defp timestamp(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp timestamp(value), do: value
  defp element(name, value), do: ["<", name, ">", XML.escape(value), "</", name, ">"]
end
