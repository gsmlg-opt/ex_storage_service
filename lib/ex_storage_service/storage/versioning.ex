defmodule ExStorageService.Storage.Versioning do
  @moduledoc """
  Bucket versioning support for S3-compatible storage.

  Versioning states:
    - :disabled (default) — no versioning
    - :enabled — new versions created on PutObject, delete markers on DeleteObject
    - :suspended — new objects get version_id "null", existing versions preserved

  Metadata key schemas:
    - "bucket_versioning:{bucket}" — versioning state
    - "obj_ver:{bucket}:{key}:{version_id}" — versioned object metadata
    - "obj_ver_list:{bucket}:{key}" — ordered list of version IDs (newest first)
  """

  alias ExStorageService.Metadata

  @type versioning_state :: :disabled | :enabled | :suspended

  @doc """
  Get the versioning state for a bucket.
  """
  @spec get_versioning(String.t()) :: versioning_state()
  def get_versioning(bucket) do
    case Concord.get("bucket_versioning:#{bucket}") do
      {:ok, nil} -> :disabled
      {:ok, state} when state in [:enabled, :suspended] -> state
      {:ok, state} when state in ["enabled", "Enabled"] -> :enabled
      {:ok, state} when state in ["suspended", "Suspended"] -> :suspended
      _ -> :disabled
    end
  end

  @doc """
  Set the versioning state for a bucket.
  Only :enabled and :suspended are valid transitions.
  """
  @spec set_versioning(String.t(), versioning_state()) :: :ok | {:error, term()}
  def set_versioning(bucket, state) when state in [:enabled, :suspended] do
    Concord.put("bucket_versioning:#{bucket}", state)
  end

  def set_versioning(_bucket, :disabled) do
    {:error, :invalid_state_transition}
  end

  @doc """
  Create a new version of an object. Returns the version_id.

  When versioning is enabled, generates a unique version ID.
  When suspended, uses "null" as the version ID.
  """
  @spec put_version(String.t(), String.t(), map()) :: {:ok, String.t()}
  def put_version(bucket, key, meta) do
    state = get_versioning(bucket)

    case state do
      :enabled ->
        version_id = generate_version_id()
        store_version(bucket, key, version_id, meta)
        {:ok, version_id}

      :suspended ->
        # Suspended: use "null" as version_id, overwrite any existing "null" version
        store_version(bucket, key, "null", meta)
        {:ok, "null"}

      :disabled ->
        # No versioning — just store normally via Metadata
        Metadata.put_object_meta(bucket, key, meta)
        {:ok, "null"}
    end
  end

  @doc """
  Get object metadata. Without version_id, returns the latest version.
  With version_id, returns that specific version.
  """
  @spec get_version(String.t(), String.t(), String.t() | nil) :: {:ok, map()} | {:error, :not_found}
  def get_version(bucket, key, nil) do
    case get_versioning(bucket) do
      :disabled ->
        Metadata.get_object_meta(bucket, key)

      _ ->
        case get_version_list(bucket, key) do
          [] ->
            # Fall back to non-versioned metadata
            Metadata.get_object_meta(bucket, key)

          [latest_id | _] ->
            get_version(bucket, key, latest_id)
        end
    end
  end

  def get_version(bucket, key, version_id) do
    case Concord.get("obj_ver:#{bucket}:#{key}:#{version_id}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, meta} -> {:ok, Map.put(meta, :version_id, version_id)}
      error -> error
    end
  end

  @doc """
  Delete an object version. When versioning is enabled, creates a delete marker
  instead of actually deleting. Returns `{:ok, version_id, :delete_marker | :deleted}`.
  """
  @spec delete_version(String.t(), String.t(), String.t() | nil) ::
          {:ok, String.t(), :delete_marker | :deleted}
  def delete_version(bucket, key, version_id \\ nil) do
    state = get_versioning(bucket)

    case {state, version_id} do
      {:disabled, _} ->
        Metadata.delete_object_meta(bucket, key)
        {:ok, "null", :deleted}

      {:enabled, nil} ->
        # Create a delete marker
        marker_version_id = generate_version_id()

        marker_meta = %{
          is_delete_marker: true,
          created_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        store_version(bucket, key, marker_version_id, marker_meta)
        {:ok, marker_version_id, :delete_marker}

      {_, vid} when not is_nil(vid) ->
        # Delete specific version
        Concord.delete("obj_ver:#{bucket}:#{key}:#{vid}")
        remove_from_version_list(bucket, key, vid)
        {:ok, vid, :deleted}

      {:suspended, nil} ->
        # Create a delete marker with "null" version
        marker_meta = %{
          is_delete_marker: true,
          created_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        store_version(bucket, key, "null", marker_meta)
        {:ok, "null", :delete_marker}
    end
  end

  @doc """
  List all versions of an object, newest first.
  """
  @spec list_versions(String.t(), String.t()) :: {:ok, [map()]}
  def list_versions(bucket, key) do
    version_ids = get_version_list(bucket, key)

    versions =
      Enum.reduce(version_ids, [], fn vid, acc ->
        case Concord.get("obj_ver:#{bucket}:#{key}:#{vid}") do
          {:ok, nil} -> acc
          {:ok, meta} -> [Map.put(meta, :version_id, vid) | acc]
          _ -> acc
        end
      end)
      |> Enum.reverse()

    {:ok, versions}
  end

  # Private helpers

  defp generate_version_id do
    timestamp = System.system_time(:microsecond)
    random = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    "#{timestamp}-#{random}"
  end

  defp store_version(bucket, key, version_id, meta) do
    # Store the version metadata
    meta_with_version = Map.put(meta, :version_id, version_id)
    Concord.put("obj_ver:#{bucket}:#{key}:#{version_id}", meta_with_version)

    # Also store in the main object metadata for non-versioned access
    Metadata.put_object_meta(bucket, key, meta_with_version)

    # Update version list
    add_to_version_list(bucket, key, version_id)
  end

  defp get_version_list(bucket, key) do
    case Concord.get("obj_ver_list:#{bucket}:#{key}") do
      {:ok, nil} -> []
      {:ok, list} when is_list(list) -> list
      _ -> []
    end
  end

  defp add_to_version_list(bucket, key, version_id) do
    list = get_version_list(bucket, key)

    # For "null" version, replace existing "null" entry
    list =
      if version_id == "null" do
        Enum.reject(list, &(&1 == "null"))
      else
        list
      end

    new_list = [version_id | list]
    Concord.put("obj_ver_list:#{bucket}:#{key}", new_list)
  end

  defp remove_from_version_list(bucket, key, version_id) do
    list = get_version_list(bucket, key)
    new_list = Enum.reject(list, &(&1 == version_id))
    Concord.put("obj_ver_list:#{bucket}:#{key}", new_list)
  end
end
