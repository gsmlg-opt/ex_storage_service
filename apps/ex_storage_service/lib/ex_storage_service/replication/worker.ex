defmodule ExStorageService.Replication.Worker do
  @moduledoc """
  Replication worker that handles replicating objects to remote S3-compatible endpoints.

  Uses Req HTTP client with simple bearer token authentication (v1 simplification,
  not full SigV4). PUT jobs may pin the exact object version captured at enqueue
  time; the worker streams that content, skips transfer when the destination
  already has matching etag/size, and treats superseded missing pinned content as
  stale.
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Replication.Config.Replica

  @doc """
  Replicate a PUT to the replica.

  `object_info` pins the version captured when the job was enqueued
  (`%{version_id:, content_hash:, etag:, size:, content_type:}`). When
  `nil` (legacy jobs, Sync-enqueued jobs), the current object metadata is
  read instead.

  Skip semantics (idempotency):
  - destination already holds identical content (HEAD etag+size match) → `:ok`
  - pinned content is gone AND the key has moved on to a newer version → `:ok` (stale)
  """
  @spec replicate_put(String.t(), String.t(), Replica.t(), map() | nil) ::
          :ok | {:error, term()}
  def replicate_put(bucket, key, replica, object_info \\ nil)

  def replicate_put(bucket, key, %Replica{} = replica, nil) do
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

  def replicate_put(bucket, key, %Replica{} = replica, object_info) do
    content_hash = get_field(object_info, :content_hash)
    etag = get_field(object_info, :etag)
    size = get_field(object_info, :size)
    content_type = get_field(object_info, :content_type) || "application/octet-stream"

    remote_bucket = replica.bucket || bucket
    url = build_url(replica.endpoint, remote_bucket, key)
    headers = auth_headers(replica)

    case Engine.read_object(bucket, content_hash) do
      {:ok, body} ->
        if destination_has_content?(url, headers, etag, size) do
          Logger.info("Replication PUT #{bucket}/#{key}: already present at #{replica.endpoint}")
          :ok
        else
          push_object(bucket, key, url, headers, body, content_type, replica)
        end

      {:error, :not_found} ->
        handle_missing_content(bucket, key, content_hash)

      {:error, reason} ->
        Logger.error(
          "Replication PUT #{bucket}/#{key}: failed to read content #{inspect(content_hash)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  @doc """
  Replicate a DELETE operation to the replica endpoint.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec replicate_delete(String.t(), String.t(), Replica.t()) :: :ok | {:error, term()}
  def replicate_delete(bucket, key, %Replica{} = replica) do
    remote_bucket = replica.bucket || bucket
    url = build_url(replica.endpoint, remote_bucket, key)
    headers = auth_headers(replica)

    case Req.delete(url, headers: headers) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 ->
        Logger.debug("Replicated DELETE #{bucket}/#{key} to #{replica.endpoint}")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "Replication DELETE failed for #{bucket}/#{key} to #{replica.endpoint}: HTTP #{status} - #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error(
          "Replication DELETE failed for #{bucket}/#{key} to #{replica.endpoint}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  ## Private

  defp push_object(bucket, key, url, headers, body, content_type, replica) do
    # Buffered body: an enumerable (chunked) body corrupts the pooled
    # HTTP/1.1 connection with Bandit targets (subsequent requests on the
    # connection fail chunk parsing). Streaming with explicit
    # content-length is a follow-up.
    case Req.put(url, body: body, headers: [{"content-type", content_type} | headers]) do
      {:ok, %{status: status}} when status in 200..299 ->
        Logger.debug("Replicated PUT #{bucket}/#{key} to #{replica.endpoint}")
        :ok

      {:ok, %{status: status, body: resp_body}} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{replica.endpoint}: HTTP #{status} - #{inspect(resp_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error(
          "Replication PUT failed for #{bucket}/#{key} to #{replica.endpoint}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # The destination is generic S3, so content-hash skip is implemented as a
  # key-level HEAD etag+size comparison.
  defp destination_has_content?(url, headers, etag, size) when is_binary(etag) do
    case Req.head(url, headers: headers) do
      {:ok, %{status: 200} = resp} ->
        remote_etag =
          resp
          |> Req.Response.get_header("etag")
          |> List.first()
          |> case do
            nil -> nil
            quoted -> String.trim(quoted, "\"")
          end

        remote_size =
          resp
          |> Req.Response.get_header("content-length")
          |> List.first()
          |> case do
            nil -> nil
            len -> String.to_integer(len)
          end

        remote_etag == etag and remote_size == size

      _ ->
        false
    end
  end

  defp destination_has_content?(_url, _headers, _etag, _size), do: false

  # Pinned content is gone from local storage. If the key has moved on to a
  # newer version, the job is stale (a newer job will replicate the newer
  # version) — succeed without transfer. If the key still points at the
  # pinned content, that is genuine content loss — error for retry/repair.
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
    end
  end

  defp get_field(map, key) when is_map(map), do: map[key] || map[to_string(key)]

  defp build_url(endpoint, bucket, key) do
    endpoint = String.trim_trailing(endpoint, "/")
    "#{endpoint}/#{bucket}/#{key}"
  end

  defp auth_headers(%Replica{access_key: access_key})
       when is_binary(access_key) and access_key != "" do
    [{"authorization", "Bearer #{access_key}"}]
  end

  defp auth_headers(_), do: []
end
