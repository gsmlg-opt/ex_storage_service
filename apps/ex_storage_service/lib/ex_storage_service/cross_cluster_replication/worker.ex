defmodule ExStorageService.CrossClusterReplication.Worker do
  @moduledoc """
  Streams immutable object versions to external S3-compatible replicas.

  Cross-cluster replication is eventual disaster recovery. It is not part of
  the in-cluster RF/W durability quorum. A worker resolves the pinned blob from
  any currently readable cluster location and sends it with an explicit
  content length, without assembling an object-sized binary.

  Authentication remains the legacy bearer-token scheme until replica targets
  are migrated to SigV4.
  """

  require Logger

  alias ExStorageService.BlobStore.Source
  alias ExStorageService.{Metadata, ObjectService}

  @type replica :: map()

  @doc """
  Replicates a pinned PUT to an external replica.

  `object_info` contains at least `content_hash` and `size`. Passing `nil`
  preserves the legacy behavior of pinning the current object metadata at
  execution time.
  """
  @spec replicate_put(String.t(), String.t(), replica(), map() | nil) ::
          :ok | {:error, term()}
  def replicate_put(bucket, key, replica, object_info \\ nil)

  def replicate_put(bucket, key, replica, nil) when is_map(replica) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        replicate_put(bucket, key, replica, %{
          version_id: get_field(meta, :version_id),
          content_hash: get_field(meta, :content_hash),
          etag: get_field(meta, :etag),
          size: get_field(meta, :size),
          content_type: get_field(meta, :content_type) || "application/octet-stream"
        })

      {:error, reason} ->
        Logger.error("Cannot replicate PUT #{bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def replicate_put(bucket, key, replica, object_info) when is_map(replica),
    do: replicate_put(bucket, key, replica, object_info, [])

  @doc false
  @spec replicate_put(String.t(), String.t(), replica(), map(), keyword()) ::
          :ok | {:error, term()}
  def replicate_put(bucket, key, replica, object_info, opts)
      when is_map(replica) and is_map(object_info) and is_list(opts) do
    content_hash = get_field(object_info, :content_hash)
    etag = get_field(object_info, :etag)
    size = get_field(object_info, :size)
    content_type = get_field(object_info, :content_type) || "application/octet-stream"

    with :ok <- validate_identity(content_hash, size),
         remote_bucket = get_field(replica, :bucket) || bucket,
         url = build_url(get_field(replica, :endpoint), remote_bucket, key),
         headers = auth_headers(replica),
         false <- destination_has_content?(url, headers, etag, size, opts),
         {:ok, source} <- open_source(bucket, object_info, opts),
         :ok <- validate_source_size(source, size) do
      push_object(bucket, key, url, headers, source, size, content_type, replica, opts)
    else
      true ->
        Logger.info(
          "Replication PUT #{bucket}/#{key}: already present at #{get_field(replica, :endpoint)}"
        )

        :ok

      {:error, reason} when reason in [:blob_not_found, :not_found] ->
        handle_missing_content(bucket, key, content_hash)

      {:error, reason} ->
        Logger.error(
          "Replication PUT #{bucket}/#{key}: failed to open content #{inspect(content_hash)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  @doc """
  Replicates a DELETE to the external replica.

  Successful responses and a missing destination key are both idempotent
  success.
  """
  @spec replicate_delete(String.t(), String.t(), replica()) :: :ok | {:error, term()}
  def replicate_delete(bucket, key, replica) when is_map(replica),
    do: replicate_delete(bucket, key, replica, [])

  @doc false
  @spec replicate_delete(String.t(), String.t(), replica(), keyword()) ::
          :ok | {:error, term()}
  def replicate_delete(bucket, key, replica, opts) when is_map(replica) and is_list(opts) do
    remote_bucket = get_field(replica, :bucket) || bucket
    url = build_url(get_field(replica, :endpoint), remote_bucket, key)
    headers = auth_headers(replica)

    case request([method: :delete, url: url, headers: headers], opts) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 ->
        Logger.debug("Replicated DELETE #{bucket}/#{key} to #{get_field(replica, :endpoint)}")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "Replication DELETE failed for #{bucket}/#{key} to #{get_field(replica, :endpoint)}: HTTP #{status} - #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error(
          "Replication DELETE failed for #{bucket}/#{key} to #{get_field(replica, :endpoint)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp open_source(bucket, object_info, opts) do
    source_opts = Keyword.merge([bucket: bucket], Keyword.get(opts, :source_opts, []))
    source_opener = Keyword.get(opts, :source_opener, &ObjectService.open_source/2)
    source_opener.(object_info, source_opts)
  end

  defp push_object(bucket, key, url, headers, source, size, content_type, replica, opts) do
    request_opts = [
      method: :put,
      url: url,
      headers: [
        {"content-type", content_type},
        {"content-length", Integer.to_string(size)}
        | headers
      ],
      body: Source.request_body(source),
      connect_options: [protocols: [:http1]],
      pool_max_idle_time: 0,
      retry: false,
      redirect: false,
      decode_body: false
    ]

    case request(request_opts, opts) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.debug("Replicated PUT #{bucket}/#{key} to #{get_field(replica, :endpoint)}")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{get_field(replica, :endpoint)}: HTTP #{status} - #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{get_field(replica, :endpoint)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp destination_has_content?(_url, _headers, etag, _size, _opts)
       when not is_binary(etag),
       do: false

  defp destination_has_content?(url, headers, etag, size, opts) do
    case request([method: :head, url: url, headers: headers], opts) do
      {:ok, %{status: 200} = response} ->
        remote_etag =
          response
          |> Req.Response.get_header("etag")
          |> List.first()
          |> unquote_etag()

        remote_size =
          response
          |> Req.Response.get_header("content-length")
          |> List.first()
          |> parse_integer()

        remote_etag == etag and remote_size == size

      _ ->
        false
    end
  end

  defp request(request_opts, opts) do
    request = Keyword.get(opts, :request, &Req.request/1)

    try do
      request.(request_opts)
    rescue
      exception -> {:error, {:request_failed, exception}}
    catch
      kind, reason -> {:error, {:request_failed, {kind, reason}}}
    end
  end

  defp validate_identity(hash, size)
       when is_binary(hash) and is_integer(size) and size >= 0,
       do: :ok

  defp validate_identity(_hash, _size), do: {:error, :invalid_object_snapshot}

  defp validate_source_size(source, expected_size) do
    if Source.content_length(source) == expected_size,
      do: :ok,
      else: {:error, :source_size_mismatch}
  end

  defp handle_missing_content(bucket, key, content_hash) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, meta} ->
        if get_field(meta, :content_hash) == content_hash do
          Logger.error(
            "Replication PUT #{bucket}/#{key}: content #{inspect(content_hash)} missing from storage"
          )

          {:error, :content_missing}
        else
          Logger.info("Replication PUT #{bucket}/#{key}: pinned version is stale, skipping")
          :ok
        end

      {:error, :not_found} ->
        Logger.info(
          "Replication PUT #{bucket}/#{key}: object gone, pinned version is stale, skipping"
        )

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_field(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, to_string(key))

  defp build_url(endpoint, bucket, key) do
    endpoint = String.trim_trailing(endpoint, "/")
    "#{endpoint}/#{bucket}/#{key}"
  end

  defp auth_headers(replica) do
    case get_field(replica, :access_key) do
      access_key when is_binary(access_key) and access_key != "" ->
        [{"authorization", "Bearer #{access_key}"}]

      _ ->
        []
    end
  end

  defp unquote_etag(nil), do: nil
  defp unquote_etag(etag), do: String.trim(etag, "\"")

  defp parse_integer(nil), do: nil

  defp parse_integer(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> nil
    end
  end
end
