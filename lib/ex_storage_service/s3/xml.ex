defmodule ExStorageService.S3.XML do
  @moduledoc """
  XML serialization for S3-compatible API responses.

  Uses IO lists for efficient XML construction with proper S3 namespacing.
  """

  @s3_xmlns "http://s3.amazonaws.com/doc/2006-03-01/"

  @doc """
  Builds ListAllMyBucketsResult XML.

  `buckets` is a list of maps with `:name` and `:creation_date` keys.
  """
  def list_buckets_response(buckets, owner \\ %{id: "owner-id", display_name: "owner"}) do
    bucket_elements =
      Enum.map(buckets, fn b ->
        [
          "<Bucket>",
          "<Name>", escape(b.name), "</Name>",
          "<CreationDate>", escape(b.creation_date), "</CreationDate>",
          "</Bucket>"
        ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListAllMyBucketsResult xmlns="#{@s3_xmlns}">),
      "<Owner>",
      "<ID>", escape(owner.id), "</ID>",
      "<DisplayName>", escape(owner.display_name), "</DisplayName>",
      "</Owner>",
      "<Buckets>",
      bucket_elements,
      "</Buckets>",
      "</ListAllMyBucketsResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds ListBucketResult XML (ListObjectsV2).

  `objects` is a list of maps with `:key`, `:last_modified`, `:etag`, `:size`, `:storage_class` keys.

  `opts` supports:
    - :prefix
    - :delimiter
    - :max_keys
    - :continuation_token
    - :next_continuation_token
    - :is_truncated
    - :key_count
    - :common_prefixes (list of prefix strings)
  """
  def list_objects_response(bucket, objects, opts \\ %{}) do
    prefix = Map.get(opts, :prefix, "")
    delimiter = Map.get(opts, :delimiter, "")
    max_keys = Map.get(opts, :max_keys, 1000)
    is_truncated = Map.get(opts, :is_truncated, false)
    key_count = Map.get(opts, :key_count, length(objects))
    continuation_token = Map.get(opts, :continuation_token)
    next_continuation_token = Map.get(opts, :next_continuation_token)
    common_prefixes = Map.get(opts, :common_prefixes, [])

    contents =
      Enum.map(objects, fn obj ->
        [
          "<Contents>",
          "<Key>", escape(obj.key), "</Key>",
          "<LastModified>", escape(obj.last_modified), "</LastModified>",
          "<ETag>", escape(obj.etag), "</ETag>",
          "<Size>", to_string(obj.size), "</Size>",
          "<StorageClass>", escape(Map.get(obj, :storage_class, "STANDARD")), "</StorageClass>",
          "</Contents>"
        ]
      end)

    prefix_elements =
      Enum.map(common_prefixes, fn p ->
        ["<CommonPrefixes><Prefix>", escape(p), "</Prefix></CommonPrefixes>"]
      end)

    continuation =
      if continuation_token do
        ["<ContinuationToken>", escape(continuation_token), "</ContinuationToken>"]
      else
        []
      end

    next_continuation =
      if next_continuation_token do
        ["<NextContinuationToken>", escape(next_continuation_token), "</NextContinuationToken>"]
      else
        []
      end

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListBucketResult xmlns="#{@s3_xmlns}">),
      "<Name>", escape(bucket), "</Name>",
      "<Prefix>", escape(prefix), "</Prefix>",
      if(delimiter != "", do: ["<Delimiter>", escape(delimiter), "</Delimiter>"], else: []),
      "<MaxKeys>", to_string(max_keys), "</MaxKeys>",
      "<KeyCount>", to_string(key_count), "</KeyCount>",
      "<IsTruncated>", to_string(is_truncated), "</IsTruncated>",
      "<EncodingType>url</EncodingType>",
      continuation,
      next_continuation,
      contents,
      prefix_elements,
      "</ListBucketResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds CopyObjectResult XML.
  """
  def copy_object_response(etag, last_modified) do
    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<CopyObjectResult>",
      "<ETag>", escape(etag), "</ETag>",
      "<LastModified>", escape(last_modified), "</LastModified>",
      "</CopyObjectResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds DeleteResult XML for multi-object delete.

  `results` is a list of:
    - `{:deleted, key}` for successfully deleted objects
    - `{:error, key, code, message}` for failed deletions
  """
  def delete_objects_response(results) do
    elements =
      Enum.map(results, fn
        {:deleted, key} ->
          [
            "<Deleted>",
            "<Key>", escape(key), "</Key>",
            "</Deleted>"
          ]

        {:error, key, code, message} ->
          [
            "<Error>",
            "<Key>", escape(key), "</Key>",
            "<Code>", escape(code), "</Code>",
            "<Message>", escape(message), "</Message>",
            "</Error>"
          ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<DeleteResult>",
      elements,
      "</DeleteResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds an S3 Error XML response.

  Common error codes:
    - NoSuchBucket
    - NoSuchKey
    - BucketAlreadyExists
    - BucketNotEmpty
    - AccessDenied
    - InvalidArgument
  """
  def error_response(code, message, resource, request_id) do
    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<Error>",
      "<Code>", escape(code), "</Code>",
      "<Message>", escape(message), "</Message>",
      "<Resource>", escape(resource), "</Resource>",
      "<RequestId>", escape(request_id), "</RequestId>",
      "</Error>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Returns the HTTP status code for a given S3 error code.
  """
  def error_status_code(code) do
    case code do
      "NoSuchBucket" -> 404
      "NoSuchKey" -> 404
      "BucketAlreadyExists" -> 409
      "BucketAlreadyOwnedByYou" -> 409
      "BucketNotEmpty" -> 409
      "AccessDenied" -> 403
      "InvalidArgument" -> 400
      "InvalidBucketName" -> 400
      "MalformedXML" -> 400
      "NoSuchUpload" -> 404
      "NoSuchLifecycleConfiguration" -> 404
      "MethodNotAllowed" -> 405
      "SlowDown" -> 429
      "EntityTooLarge" -> 413
      "InvalidRange" -> 416
      "InternalError" -> 500
      _ -> 500
    end
  end

  @doc """
  Builds InitiateMultipartUploadResult XML.
  """
  def initiate_multipart_upload_response(bucket, key, upload_id) do
    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<InitiateMultipartUploadResult xmlns="#{@s3_xmlns}">),
      "<Bucket>", escape(bucket), "</Bucket>",
      "<Key>", escape(key), "</Key>",
      "<UploadId>", escape(upload_id), "</UploadId>",
      "</InitiateMultipartUploadResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds CompleteMultipartUploadResult XML.
  """
  def complete_multipart_upload_response(bucket, key, etag, location) do
    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<CompleteMultipartUploadResult xmlns="#{@s3_xmlns}">),
      "<Location>", escape(location), "</Location>",
      "<Bucket>", escape(bucket), "</Bucket>",
      "<Key>", escape(key), "</Key>",
      "<ETag>\"", escape(etag), "\"</ETag>",
      "</CompleteMultipartUploadResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds ListPartsResult XML.

  `parts` is a list of maps with `:part_number`, `:etag`, `:size`, and optionally `:uploaded_at`.
  """
  def list_parts_response(bucket, key, upload_id, parts) do
    part_elements =
      Enum.map(parts, fn p ->
        [
          "<Part>",
          "<PartNumber>", to_string(p.part_number), "</PartNumber>",
          "<ETag>\"", escape(p.etag), "\"</ETag>",
          "<Size>", to_string(p.size), "</Size>",
          if(Map.has_key?(p, :uploaded_at),
            do: ["<LastModified>", escape(p.uploaded_at), "</LastModified>"],
            else: []
          ),
          "</Part>"
        ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListPartsResult xmlns="#{@s3_xmlns}">),
      "<Bucket>", escape(bucket), "</Bucket>",
      "<Key>", escape(key), "</Key>",
      "<UploadId>", escape(upload_id), "</UploadId>",
      part_elements,
      "</ListPartsResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Escapes special XML characters in a string.
  """
  def escape(nil), do: ""
  def escape(value) when is_binary(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&apos;")
  end
  def escape(value), do: escape(to_string(value))
end
