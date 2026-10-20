defmodule ExStorageService.Storage.Versioning do
  @moduledoc """
  Compatibility facade for bucket versioning.

  New writes use immutable v2 version records and one atomic metadata
  transaction. Legacy v1 records remain readable, but no mutable v2 version
  list exists.
  """

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.ObjectCommit

  @type versioning_state :: :disabled | :enabled | :suspended

  @spec get_versioning(String.t()) :: versioning_state()
  def get_versioning(bucket) do
    case fetch_versioning(bucket) do
      {:ok, state} -> state
      _ -> :disabled
    end
  end

  @doc "Reads bucket versioning without treating an unavailable metadata read as disabled."
  @spec fetch_versioning(String.t()) :: {:ok, versioning_state()} | {:error, term()}
  def fetch_versioning(bucket) do
    case Concord.get("bucket_versioning:#{bucket}", consistency: :linearizable) do
      {:ok, nil} -> {:ok, :disabled}
      {:error, :not_found} -> {:ok, :disabled}
      {:ok, state} when state in [:enabled, :suspended] -> {:ok, state}
      {:ok, state} when state in ["enabled", "Enabled"] -> {:ok, :enabled}
      {:ok, state} when state in ["suspended", "Suspended"] -> {:ok, :suspended}
      {:ok, _} -> {:error, :invalid_versioning_state}
      {:error, _} = error -> error
    end
  end

  @spec set_versioning(String.t(), versioning_state()) :: :ok | {:error, term()}
  def set_versioning(bucket, state, opts \\ [])

  def set_versioning(bucket, state, opts) when state in [:enabled, :suspended] do
    ExStorageService.Metadata.BucketConfig.put(bucket, "bucket_versioning", state, opts)
  end

  def set_versioning(_bucket, :disabled, _opts), do: {:error, :invalid_state_transition}

  @spec put_version(String.t(), String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def put_version(bucket, key, metadata), do: put_version(bucket, key, metadata, [])

  @doc false
  @spec put_version(String.t(), String.t(), map(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def put_version(bucket, key, metadata, opts) do
    case put_version_result(bucket, key, metadata, opts) do
      {:ok, %{version_id: version_id}} ->
        {:ok, version_id}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  def put_version_result(bucket, key, metadata, opts) do
    ObjectCommit.put(bucket, key, metadata, Keyword.put(opts, :bucket_versioning, true))
  end

  @spec get_version(String.t(), String.t(), String.t() | nil) ::
          {:ok, map()} | {:error, :not_found | term()}
  def get_version(bucket, key, nil) do
    case ObjectCommit.get_head(bucket, key) do
      {:ok, version} ->
        {:ok, version}

      {:error, :not_found} ->
        with {:ok, false} <- ObjectCommit.has_state(bucket, key) do
          get_v1_latest(bucket, key)
        else
          {:ok, true} -> {:error, :not_found}
          error -> error
        end

      error ->
        error
    end
  end

  def get_version(bucket, key, version_id) do
    case ObjectCommit.get_version(bucket, key, version_id) do
      {:ok, version} ->
        {:ok, version}

      {:error, :not_found} ->
        with {:ok, false} <- ObjectCommit.has_state(bucket, key) do
          get_v1_version(bucket, key, version_id)
        else
          {:ok, true} -> {:error, :not_found}
          error -> error
        end

      error ->
        error
    end
  end

  @spec delete_version(String.t(), String.t(), String.t() | nil) ::
          {:ok, String.t(), :delete_marker | :deleted} | {:error, term()}
  def delete_version(bucket, key), do: delete_version(bucket, key, nil, [])
  def delete_version(bucket, key, version_id), do: delete_version(bucket, key, version_id, [])

  @doc false
  @spec delete_version(String.t(), String.t(), String.t() | nil, keyword()) ::
          {:ok, String.t(), :delete_marker | :deleted} | {:error, term()}
  def delete_version(bucket, key, nil, opts) do
    case ObjectCommit.delete_marker(bucket, key, Keyword.put(opts, :bucket_versioning, true)) do
      {:ok, %{version_id: version_id, kind: kind}} ->
        {:ok, version_id, kind}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def delete_version(bucket, key, version_id, opts) do
    case ObjectCommit.delete_version(
           bucket,
           key,
           version_id,
           Keyword.put(opts, :bucket_versioning, true)
         ) do
      {:ok, _result} -> {:ok, version_id, :deleted}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec list_versions(String.t(), String.t()) :: {:ok, [map()]}
  def list_versions(bucket, key) do
    with {:ok, v2_versions} <- ObjectCommit.list_versions(bucket, key) do
      case v2_versions do
        [] ->
          with {:ok, false} <- ObjectCommit.has_state(bucket, key) do
            list_v1_versions(bucket, key)
          else
            {:ok, true} -> {:ok, []}
            error -> error
          end

        versions ->
          {:ok, versions}
      end
    end
  end

  defp get_v1_latest(bucket, key) do
    case get_v1_version_list(bucket, key) do
      [latest_id | _] -> get_v1_version(bucket, key, latest_id)
      [] -> Metadata.get_v1_object_meta(bucket, key)
    end
  end

  defp get_v1_version(bucket, key, "null") do
    case Metadata.get_v1_object_meta(bucket, key) do
      {:ok, metadata} ->
        if Map.get(metadata, :version_id, "null") == "null",
          do: {:ok, Map.put(metadata, :version_id, "null")},
          else: {:error, :not_found}

      error ->
        error
    end
  end

  defp get_v1_version(bucket, key, version_id) do
    case Concord.get("obj_ver:#{bucket}:#{key}:#{version_id}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, metadata} -> {:ok, Map.put(metadata, :version_id, version_id)}
      error -> error
    end
  end

  defp list_v1_versions(bucket, key) do
    versions =
      get_v1_version_list(bucket, key)
      |> Enum.reduce([], fn version_id, versions ->
        case get_v1_version(bucket, key, version_id) do
          {:ok, version} -> [version | versions]
          _ -> versions
        end
      end)
      |> Enum.reverse()

    {:ok, versions}
  end

  defp get_v1_version_list(bucket, key) do
    case Concord.get("obj_ver_list:#{bucket}:#{key}") do
      {:ok, version_ids} when is_list(version_ids) -> version_ids
      _ -> []
    end
  end
end
