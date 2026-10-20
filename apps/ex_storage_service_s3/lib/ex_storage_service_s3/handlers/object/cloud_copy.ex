defmodule ExStorageServiceS3.Handlers.Object.CloudCopy do
  @moduledoc """
  Bounded COPY staging for operations involving an authoritative gateway.

  Authorization and source conditions are evaluated by the request handler against
  `head_source/1`. A gateway download is pinned to that exact HEAD identity; a
  hosted source uses the immutable metadata already selected by ObjectService.
  """
  alias ExStorageService.BlobStore.{LocalCAS, Source}
  alias ExStorageService.CloudCache.{Client, Config, WriteThrough}
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.BucketLifecycle
  alias ExStorageService.ObjectService
  alias ExStorageServiceS3.Handlers.Shared

  def head_source(source) do
    case Config.get_active_config(source.bucket) do
      :disabled -> ObjectService.head(source.bucket, source.key, source.version_id, [])
      {:ok, config} -> gateway_head(source, config)
    end
  end

  defp gateway_head(%{version_id: version} = _source, _config) when not is_nil(version),
    do: {:error, :unsupported_gateway_version}

  defp gateway_head(source, config) do
    case Shared.latest_delete_marker(source.bucket, source.key) do
      {:ok, _} ->
        {:error, :object_not_found}

      :no_such_bucket ->
        {:error, :bucket_not_found}

      {:error, _} = error ->
        error

      :not_found ->
        with {:ok, meta} <- Client.head_object(config, source.key),
             true <- valid_identity?(meta) do
          modified =
            case Shared.parse_http_date(meta.last_modified || "") do
              {:ok, value} -> DateTime.to_iso8601(value)
              _ -> nil
            end

          {:ok,
           %{
             delete_marker: false,
             version_id: nil,
             metadata: %{
               etag: meta.etag,
               size: meta.content_length,
               content_type: meta.content_type,
               metadata: meta.metadata,
               system_headers: meta.headers,
               updated_at: modified,
               gateway_identity: %{
                 metadata: meta,
                 endpoint: Config.endpoint_url(config),
                 remote_bucket: config.bucket
               }
             }
           }}
        else
          false -> {:error, :invalid_upstream_metadata}
          {:error, :not_found} -> {:error, :object_not_found}
          {:error, _} = error -> error
        end
    end
  end

  def copy(
        source,
        destination_bucket,
        destination_key,
        metadata,
        attributes,
        operation_id,
        opts \\ []
      ) do
    context =
      case Keyword.get(opts, :context) do
        %ExStorageService.Context{} = context ->
          context

        nil ->
          {:ok, context} = ExStorageService.Context.default()
          context
      end

    opts = Keyword.merge(ExStorageService.Context.blob_store_options(context), opts)

    with {:ok, bucket} <- Metadata.get_bucket(destination_bucket),
         generation = BucketLifecycle.generation(bucket),
         destination_config = Config.get_active_config(destination_bucket),
         :ok <- allowed_destination(destination_config),
         {:ok, staged} <- stage_source(source, metadata, opts) do
      case copy_attributes_from_source(
             Source.file(staged.path, 0, staged.size),
             attributes,
             metadata,
             opts
           ) do
        {:ok, attributes} ->
          case destination_config do
            {:ok, config} ->
              case WriteThrough.put(
                     destination_bucket,
                     destination_key,
                     staged,
                     config,
                     Map.put_new(attributes, :headers, Map.get(attributes, :system_headers, [])),
                     operation_id,
                     Keyword.put(opts, :bucket_generation, generation)
                   ) do
                {:ok, meta} -> {:ok, %{version_id: "null", metadata: meta}}
                {:error, _} = error -> error
              end

            :disabled ->
              try do
                put_hosted(
                  destination_bucket,
                  destination_key,
                  staged,
                  attributes,
                  operation_id,
                  opts
                  |> Keyword.put(:bucket_generation, generation)
                  |> Keyword.update(
                    :metadata_opts,
                    [bucket_generation: generation],
                    &Keyword.put(&1, :bucket_generation, generation)
                  )
                )
              after
                LocalCAS.discard(staged)
              end
          end

        {:error, _} = error ->
          LocalCAS.discard(staged)
          error
      end
    else
      {:error, :not_found} -> {:error, :bucket_not_found}
      {:error, _} = error -> error
    end
  end

  @doc false
  def copy_attributes_from_source(source, attributes, metadata, opts \\ []) do
    attributes = Map.put_new(attributes, :headers, Map.get(attributes, :system_headers, []))

    source_checksums =
      Map.merge(Map.get(metadata, :checksums, %{}), Map.get(attributes, :checksums, %{}))

    source_names =
      Map.keys(source_checksums) ++
        for {"x-amz-checksum-" <> name, _} <- Map.get(metadata, :system_headers, []),
            name in ExStorageServiceS3.Integrity.algorithms(),
            do: name

    requested = Keyword.get(opts, :checksum_algorithm, Map.get(attributes, :checksum_algorithm))

    names =
      if requested,
        do: [String.downcase(requested)],
        else: Enum.uniq(source_names) -- ["md5"]

    if Enum.all?(names, &(&1 in ExStorageServiceS3.Integrity.algorithms())) do
      with {:ok, checksums} <-
             ExStorageServiceS3.MultipartChecksums.checksum_source(source, ["md5" | names]) do
        {md5, checksums} = Map.pop!(checksums, "md5")
        etag = md5 |> Base.decode64!() |> Base.encode16(case: :lower)
        attributes = Map.put(attributes, :etag, etag)

        checksum_headers =
          Enum.map(checksums, fn {name, value} -> {"x-amz-checksum-" <> name, value} end)

        attributes =
          Enum.reduce([:headers, :system_headers], attributes, fn key, current ->
            headers =
              Enum.reject(Map.get(current, key, []), fn {name, _} ->
                String.starts_with?(name, "x-amz-checksum-")
              end)

            Map.put(current, key, headers ++ checksum_headers)
          end)

        result = attributes |> Map.put(:checksums, checksums) |> Map.delete(:checksum_algorithm)

        result =
          if names == [],
            do: Map.delete(result, :checksum_type),
            else: Map.put(result, :checksum_type, "FULL_OBJECT")

        {:ok, result}
      end
    else
      {:error, :unsupported_checksum}
    end
  end

  @doc false
  def with_source(source, metadata, range, callback) do
    if Map.has_key?(metadata, :gateway_identity) do
      with {:ok, staged} <- stage_source(source, metadata, []) do
        try do
          {offset, length} = range || {0, staged.size}
          callback.(Source.file(staged.path, offset, length))
        after
          LocalCAS.discard(staged)
        end
      end
    else
      with {:ok, pinned} <-
             ObjectService.open_source(metadata, bucket: source.bucket, range: range),
           do: callback.(pinned)
    end
  end

  defp stage_source(source, %{gateway_identity: identity}, opts) do
    with {:ok, config} <- Config.get_active_config(source.bucket),
         true <-
           Config.endpoint_url(config) == identity.endpoint and
             config.bucket == identity.remote_bucket,
         {:ok, staged, actual} <-
           Client.get_file(
             config,
             source.key,
             Keyword.put(opts, :if_match, quote_etag(identity.metadata.etag))
           ) do
      if same_identity?(identity.metadata, actual) do
        {:ok, staged}
      else
        LocalCAS.discard(staged)
        {:error, :upstream_changed}
      end
    else
      false -> {:error, :upstream_changed}
      :disabled -> {:error, :upstream_changed}
      {:error, {:http_error, 412}} -> {:error, :precondition_failed}
      {:error, :not_found} -> {:error, :object_not_found}
      {:error, _} = error -> error
    end
  end

  defp stage_source(source, metadata, opts) do
    with {:ok, pinned} <- ObjectService.open_source(metadata, bucket: source.bucket),
         {:ok, staged} <- LocalCAS.stage(Source.request_body(pinned), opts) do
      if staged.size == metadata.size and staged.hash == metadata.content_hash do
        {:ok, staged}
      else
        LocalCAS.discard(staged)
        {:error, :source_checksum_mismatch}
      end
    end
  end

  defp put_hosted(bucket, key, staged, attributes, operation_id, opts) do
    with {:ok, io} <- File.open(staged.path, [:read, :raw, :binary]) do
      try do
        reader = fn offset ->
          case :file.pread(io, offset, 65_536) do
            {:ok, bytes} -> {:more, bytes, offset + byte_size(bytes)}
            :eof -> {:done, offset}
            {:error, reason} -> {:error, reason, offset}
          end
        end

        case ObjectService.put_from_reader(
               bucket,
               key,
               reader,
               0,
               attributes.content_type,
               attributes.metadata,
               opts
               |> Keyword.put(:operation_id, operation_id)
               |> Keyword.put(:attributes, attributes)
             ) do
          {:ok, result, _} -> {:ok, result}
          {:error, reason, _} -> {:error, reason}
        end
      after
        File.close(io)
      end
    end
  end

  defp allowed_destination(:disabled), do: :ok

  defp allowed_destination({:ok, _config}) do
    case ExStorageService.Context.default() do
      {:ok, %{config: %{mode: :cluster}}} -> {:error, :cluster_data_plane_disabled}
      _ -> :ok
    end
  end

  defp valid_identity?(%{etag: etag, content_length: size}),
    do: is_binary(etag) and etag != "" and is_integer(size) and size >= 0

  defp valid_identity?(_), do: false

  defp same_identity?(expected, actual), do: expected == actual

  defp quote_etag(etag), do: "\"" <> String.trim(etag, "\"") <> "\""
end
