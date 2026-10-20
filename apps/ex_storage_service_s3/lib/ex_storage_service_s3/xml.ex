defmodule ExStorageServiceS3.XML do
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
          "<Name>",
          escape(b.name),
          "</Name>",
          "<CreationDate>",
          escape(b.creation_date),
          "</CreationDate>",
          "</Bucket>"
        ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListAllMyBucketsResult xmlns="#{@s3_xmlns}">),
      "<Owner>",
      "<ID>",
      escape(owner.id),
      "</ID>",
      "<DisplayName>",
      escape(owner.display_name),
      "</DisplayName>",
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

    list_type = Map.get(opts, :list_type, 2)
    encoding_type = Map.get(opts, :encoding_type)

    encode = fn value ->
      if encoding_type == "url", do: URI.encode(value, &URI.char_unreserved?/1), else: value
    end

    contents =
      Enum.map(objects, fn obj ->
        [
          "<Contents>",
          "<Key>",
          escape(encode.(obj.key)),
          "</Key>",
          "<LastModified>",
          escape(obj.last_modified),
          "</LastModified>",
          "<ETag>",
          escape(obj.etag),
          "</ETag>",
          "<Size>",
          to_string(obj.size),
          "</Size>",
          "<StorageClass>",
          escape(Map.get(obj, :storage_class, "STANDARD")),
          "</StorageClass>",
          "</Contents>"
        ]
      end)

    prefix_elements =
      Enum.map(common_prefixes, fn p ->
        ["<CommonPrefixes><Prefix>", escape(encode.(p)), "</Prefix></CommonPrefixes>"]
      end)

    continuation =
      if list_type == 2 and continuation_token do
        ["<ContinuationToken>", escape(continuation_token), "</ContinuationToken>"]
      else
        []
      end

    next_continuation =
      if list_type == 2 and next_continuation_token do
        ["<NextContinuationToken>", escape(next_continuation_token), "</NextContinuationToken>"]
      else
        []
      end

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListBucketResult xmlns="#{@s3_xmlns}">),
      "<Name>",
      escape(bucket),
      "</Name>",
      "<Prefix>",
      escape(encode.(prefix)),
      "</Prefix>",
      if(delimiter != "",
        do: ["<Delimiter>", escape(encode.(delimiter)), "</Delimiter>"],
        else: []
      ),
      "<MaxKeys>",
      to_string(max_keys),
      "</MaxKeys>",
      if(list_type == 2, do: element("KeyCount", key_count), else: []),
      if(list_type == 1, do: element("Marker", encode.(Map.get(opts, :marker) || "")), else: []),
      if(list_type == 1 and next_continuation_token,
        do: element("NextMarker", encode.(next_continuation_token)),
        else: []
      ),
      if(list_type == 2 and Map.get(opts, :start_after),
        do: element("StartAfter", encode.(opts.start_after)),
        else: []
      ),
      "<IsTruncated>",
      to_string(is_truncated),
      "</IsTruncated>",
      if(encoding_type == "url", do: element("EncodingType", "url"), else: []),
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
  def copy_object_response(etag, last_modified, attributes \\ %{}) do
    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<CopyObjectResult>",
      "<ETag>",
      escape(etag),
      "</ETag>",
      "<LastModified>",
      escape(last_modified),
      "</LastModified>",
      checksum_elements(Map.get(attributes, :checksums, %{})),
      element("ChecksumType", Map.get(attributes, :checksum_type)),
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
  def delete_objects_response(results, opts \\ []) do
    version_xml = fn
      nil -> []
      version -> ["<VersionId>", escape(version), "</VersionId>"]
    end

    elements =
      results
      |> Enum.reject(fn result ->
        Keyword.get(opts, :quiet, false) and elem(result, 0) == :deleted
      end)
      |> Enum.map(fn
        {:deleted, %{key: key} = entry} ->
          marker =
            if entry.delete_marker do
              [
                "<DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>",
                escape(entry.delete_marker_version_id),
                "</DeleteMarkerVersionId>"
              ]
            else
              []
            end

          [
            "<Deleted><Key>",
            escape(key),
            "</Key>",
            version_xml.(entry.version_id),
            marker,
            "</Deleted>"
          ]

        {:deleted, key} ->
          ["<Deleted><Key>", escape(key), "</Key></Deleted>"]

        {:error, %{key: key, code: code, message: message} = entry} ->
          [
            "<Error><Key>",
            escape(key),
            "</Key>",
            version_xml.(entry.version_id),
            "<Code>",
            escape(code),
            "</Code><Message>",
            escape(message),
            "</Message></Error>"
          ]

        {:error, key, code, message} ->
          [
            "<Error><Key>",
            escape(key),
            "</Key><Code>",
            escape(code),
            "</Code><Message>",
            escape(message),
            "</Message></Error>"
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
      "<Code>",
      escape(code),
      "</Code>",
      "<Message>",
      escape(message),
      "</Message>",
      "<Resource>",
      escape(resource),
      "</Resource>",
      "<RequestId>",
      escape(request_id),
      "</RequestId>",
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
      "NoSuchVersion" -> 404
      "NoSuchTagSet" -> 404
      "NoSuchCORSConfiguration" -> 404
      "InvalidTag" -> 400
      "BucketAlreadyExists" -> 409
      "BucketAlreadyOwnedByYou" -> 409
      "BucketNotEmpty" -> 409
      "AccessDenied" -> 403
      "InvalidArgument" -> 400
      "InvalidBucketName" -> 400
      "MalformedXML" -> 400
      "NoSuchUpload" -> 404
      "NoSuchLifecycleConfiguration" -> 404
      "InvalidPart" -> 400
      "InvalidPartOrder" -> 400
      "MethodNotAllowed" -> 405
      "SlowDown" -> 503
      "EntityTooLarge" -> 413
      "EntityTooSmall" -> 400
      "InvalidRequest" -> 400
      "InvalidRange" -> 416
      "ServiceUnavailable" -> 503
      "NotImplemented" -> 501
      "PreconditionFailed" -> 412
      "ConditionalRequestConflict" -> 409
      "BadDigest" -> 400
      "InvalidDigest" -> 400
      "SignatureDoesNotMatch" -> 403
      "XAmzContentSHA256Mismatch" -> 400
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
      "<Bucket>",
      escape(bucket),
      "</Bucket>",
      "<Key>",
      escape(key),
      "</Key>",
      "<UploadId>",
      escape(upload_id),
      "</UploadId>",
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
      "<Location>",
      escape(location),
      "</Location>",
      "<Bucket>",
      escape(bucket),
      "</Bucket>",
      "<Key>",
      escape(key),
      "</Key>",
      "<ETag>\"",
      escape(etag),
      "\"</ETag>",
      "</CompleteMultipartUploadResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Builds ListPartsResult XML.

  `parts` is a list of maps with `:part_number`, `:etag`, `:size`, and optionally `:uploaded_at`.
  """
  def list_parts_response(bucket, key, upload_id, parts) when is_list(parts),
    do: list_parts_response(bucket, key, upload_id, %{parts: parts})

  def list_parts_response(bucket, key, upload_id, page) when is_map(page) do
    part_elements =
      Enum.map(page.parts, fn p ->
        [
          "<Part>",
          "<PartNumber>",
          to_string(p.part_number),
          "</PartNumber>",
          "<ETag>\"",
          escape(p.etag),
          "\"</ETag>",
          "<Size>",
          to_string(p.size),
          "</Size>",
          if(Map.has_key?(p, :uploaded_at),
            do: ["<LastModified>", escape(p.uploaded_at), "</LastModified>"],
            else: []
          ),
          checksum_elements(Map.get(p, :checksums, %{})),
          "</Part>"
        ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListPartsResult xmlns="#{@s3_xmlns}">),
      "<Bucket>",
      escape(bucket),
      "</Bucket>",
      "<Key>",
      escape(key),
      "</Key>",
      "<UploadId>",
      escape(upload_id),
      "</UploadId>",
      element("PartNumberMarker", Map.get(page, :part_number_marker, 0)),
      element("NextPartNumberMarker", Map.get(page, :next_part_number_marker)),
      element("MaxParts", Map.get(page, :max_parts, 1000)),
      element("IsTruncated", Map.get(page, :is_truncated, false)),
      element("ChecksumAlgorithm", Map.get(page, :checksum_algorithm)),
      element("ChecksumType", Map.get(page, :checksum_type)),
      part_elements,
      "</ListPartsResult>"
    ]
    |> IO.iodata_to_binary()
  end

  @doc "Builds a paginated ListMultipartUploads result."
  def list_multipart_uploads_response(bucket, page) do
    encode = fn value ->
      if Map.get(page, :encoding_type) == "url" and is_binary(value),
        do: URI.encode(value, &URI.char_unreserved?/1),
        else: value
    end

    uploads =
      Enum.map(page.uploads, fn upload ->
        [
          "<Upload>",
          element("Key", encode.(upload.key)),
          element("UploadId", upload.upload_id),
          "<Initiator><ID>owner-id</ID><DisplayName>owner</DisplayName></Initiator>",
          "<Owner><ID>owner-id</ID><DisplayName>owner</DisplayName></Owner>",
          element("StorageClass", "STANDARD"),
          element("Initiated", upload.created_at),
          "</Upload>"
        ]
      end)

    [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      ~s(<ListMultipartUploadsResult xmlns="#{@s3_xmlns}">),
      element("Bucket", bucket),
      element("KeyMarker", encode.(Map.get(page, :key_marker, ""))),
      element("UploadIdMarker", Map.get(page, :upload_id_marker, "")),
      element("NextKeyMarker", encode.(Map.get(page, :next_key_marker))),
      element("NextUploadIdMarker", Map.get(page, :next_upload_id_marker)),
      element("Prefix", encode.(Map.get(page, :prefix, ""))),
      element("Delimiter", encode.(Map.get(page, :delimiter))),
      element("EncodingType", Map.get(page, :encoding_type)),
      element("MaxUploads", Map.get(page, :max_uploads, 1000)),
      element("IsTruncated", Map.get(page, :is_truncated, false)),
      uploads,
      Enum.map(Map.get(page, :common_prefixes, []), fn prefix ->
        ["<CommonPrefixes>", element("Prefix", encode.(prefix)), "</CommonPrefixes>"]
      end),
      "</ListMultipartUploadsResult>"
    ]
    |> IO.iodata_to_binary()
  end

  defp element(_name, nil), do: []
  defp element(name, value), do: ["<", name, ">", escape(value), "</", name, ">"]

  defp checksum_elements(checksums) do
    for algorithm <- ~w(CRC32 CRC32C CRC64NVME SHA1 SHA256),
        value = Map.get(checksums, String.downcase(algorithm), Map.get(checksums, algorithm)),
        not is_nil(value),
        do: element("Checksum" <> algorithm, value)
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
