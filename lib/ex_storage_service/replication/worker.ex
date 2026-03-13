defmodule ExStorageService.Replication.Worker do
  @moduledoc """
  Replication worker that handles replicating objects to remote S3-compatible endpoints.

  Uses Req HTTP client with simple bearer token authentication (v1 simplification,
  not full SigV4).
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.Engine
  alias ExStorageService.Replication.Config.Replica

  @doc """
  Replicate a PUT operation: read the object from local storage and PUT it to the replica.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  @spec replicate_put(String.t(), String.t(), Replica.t()) :: :ok | {:error, term()}
  def replicate_put(bucket, key, %Replica{} = replica) do
    with {:ok, meta} <- Metadata.get_object_meta(bucket, key),
         content_hash <- meta[:content_hash] || meta["content_hash"],
         {:ok, file_path} <- Engine.get_object(bucket, content_hash) do
      body = File.read!(file_path)
      remote_bucket = replica.bucket || bucket
      url = build_url(replica.endpoint, remote_bucket, key)

      headers = auth_headers(replica)

      content_type =
        meta[:content_type] || meta["content_type"] || "application/octet-stream"

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
    else
      {:error, reason} ->
        Logger.error("Cannot replicate PUT #{bucket}/#{key}: #{inspect(reason)}")
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
