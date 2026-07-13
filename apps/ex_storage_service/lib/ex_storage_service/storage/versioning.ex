defmodule ExStorageService.Storage.Versioning do
  @moduledoc """
  Bucket versioning support for S3-compatible storage.

  Versioning states:
    - :disabled (default) — no versioning
    - :enabled — new versions created on PutObject, delete markers on DeleteObject
    - :suspended — new objects get version_id "null", existing versions preserved

  Metadata key schemas:
    - "obj:{bucket}:{key}" — mutable latest-view ref; absent when latest is a delete marker
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
    meta = stamp_version_fields(bucket, key, meta)

    case state do
      :enabled ->
        version_id = generate_version_id()
        store_version(bucket, key, version_id, meta)
        {:ok, version_id}

      :suspended ->
        store_version(bucket, key, "null", meta)
        {:ok, "null"}

      :disabled ->
        Metadata.put_object_meta(bucket, key, meta)
        {:ok, "null"}
    end
  end

  # Every version record carries its object type (Phase 3 adds :manifest)
  # and a parent pointer to the version it superseded.
  defp stamp_version_fields(bucket, key, meta) do
    parent =
      case Metadata.get_object_meta(bucket, key) do
        {:ok, %{version_id: vid}} -> vid
        _ -> nil
      end

    meta
    |> Map.put_new(:object_type, :blob)
    |> Map.put(:parent_version_id, parent)
  end

  @doc """
  Get object metadata. Without version_id, returns the latest version.
  With version_id, returns that specific version.
  """
  @spec get_version(String.t(), String.t(), String.t() | nil) ::
          {:ok, map()} | {:error, :not_found}
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
        marker_version_id = generate_version_id()
        create_delete_marker(bucket, key, marker_version_id)
        {:ok, marker_version_id, :delete_marker}

      {:suspended, nil} ->
        create_delete_marker(bucket, key, "null")
        {:ok, "null", :delete_marker}

      {_, vid} ->
        Concord.delete("obj_ver:#{bucket}:#{key}:#{vid}")
        remove_from_version_list(bucket, key, vid)
        repoint_latest(bucket, key, vid)
        {:ok, vid, :deleted}
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
    meta_with_version = Map.put(meta, :version_id, version_id)

    # Write order matters for crash safety: immutable version record first,
    # then the version index, then the mutable obj: ref last so the latest
    # view never points at a version record that does not exist.
    Concord.put("obj_ver:#{bucket}:#{key}:#{version_id}", meta_with_version)
    add_to_version_list(bucket, key, version_id)
    Metadata.put_object_meta(bucket, key, meta_with_version)
  end

  # A delete marker is an immutable version record; the mutable obj: latest
  # view is removed so GET/HEAD/list treat the key as absent (PRD §10.2).
  defp create_delete_marker(bucket, key, marker_version_id) do
    marker_meta = %{
      is_delete_marker: true,
      object_type: :blob,
      parent_version_id: current_version_id(bucket, key),
      created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      version_id: marker_version_id
    }

    Concord.put("obj_ver:#{bucket}:#{key}:#{marker_version_id}", marker_meta)
    add_to_version_list(bucket, key, marker_version_id)
    Metadata.delete_object_meta(bucket, key)
  end

  defp current_version_id(bucket, key) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, %{version_id: vid}} -> vid
      _ -> nil
    end
  end

  # After permanently deleting a specific version, the newest remaining
  # version becomes latest: a normal version repopulates obj:; a delete
  # marker (or nothing) leaves the key absent. Only needed when the
  # deleted version was the current latest.
  defp repoint_latest(bucket, key, deleted_vid) do
    case Metadata.get_object_meta(bucket, key) do
      {:ok, %{version_id: current}} when current != deleted_vid ->
        :ok

      _ ->
        case get_version_list(bucket, key) do
          [] ->
            Metadata.delete_object_meta(bucket, key)

          [head | _] ->
            case Concord.get("obj_ver:#{bucket}:#{key}:#{head}") do
              {:ok, %{is_delete_marker: true}} ->
                Metadata.delete_object_meta(bucket, key)

              {:ok, head_meta} when is_map(head_meta) ->
                Metadata.put_object_meta(bucket, key, head_meta)

              _ ->
                Metadata.delete_object_meta(bucket, key)
            end
        end
    end
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
