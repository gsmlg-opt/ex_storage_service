defmodule ExStorageService.Metadata do
  @moduledoc """
  Metadata operations backed by Concord key-value store.

  Legacy keys are namespaced:
  - Buckets: `"bucket:{name}"`
  - Objects: `"obj:{bucket}:{key}"`

  Object reads prefer the atomic v2 schema and fall back to these v1 records.
  """

  alias ExStorageService.Metadata.ObjectCommit

  ## Bucket operations

  def create_bucket(name), do: ExStorageService.Metadata.BucketLifecycle.create(name)

  def delete_bucket(name), do: ExStorageService.Metadata.BucketLifecycle.delete(name)

  def get_bucket(name) do
    case Concord.get("bucket:#{name}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  def head_bucket(name) do
    case Concord.get("bucket:#{name}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, _value} -> :ok
      error -> error
    end
  end

  def list_buckets do
    case Concord.get_all() do
      {:ok, all} ->
        buckets =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "bucket:") end)
          |> Enum.map(fn {_k, v} -> v end)

        {:ok, buckets}

      error ->
        error
    end
  end

  @doc false
  def check_object_mutation(bucket, opts \\ []) do
    backend = Keyword.get(opts, :backend, ExStorageService.Metadata.Backend.Concord)

    case ExStorageService.Metadata.BucketLifecycle.guard(backend, bucket, opts) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  ## Object metadata operations

  def put_object_meta(bucket, key, meta) do
    case ObjectCommit.put(bucket, key, meta, bucket_versioning: true) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def get_object_meta(bucket, key) do
    case ObjectCommit.get_head(bucket, key) do
      {:ok, %{is_delete_marker: true}} ->
        {:error, :not_found}

      {:ok, value} ->
        {:ok, normalize_public_object_meta(bucket, value)}

      {:error, :not_found} ->
        with {:ok, false} <- ObjectCommit.has_state(bucket, key) do
          get_v1_object_meta(bucket, key)
        else
          {:ok, true} -> {:error, :not_found}
          error -> error
        end

      error ->
        error
    end
  end

  @doc false
  def get_v1_object_meta(bucket, key) do
    case Concord.get("obj:#{bucket}:#{key}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  def delete_object_meta(bucket, key) do
    case ExStorageService.Storage.Versioning.delete_version(bucket, key) do
      {:ok, _, _} -> :ok
      error -> error
    end
  end

  @doc """
  List objects in a bucket with support for prefix, delimiter, max_keys,
  and continuation_token (pagination).
  """
  def list_objects(bucket, opts \\ []) do
    ExStorageService.Metadata.ListIndex.Listing.objects(bucket, opts)
  end

  ## Blob metadata operations (global CAS)
  # Key schema: "blob:sha256:{hash}" — see docs/prd/git-style-data-model.md §7.4

  def put_blob_meta(content_hash, meta) do
    Concord.put("blob:sha256:#{content_hash}", meta)
  end

  def get_blob_meta(content_hash) do
    case Concord.get("blob:sha256:#{content_hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  @doc """
  Creates the blob metadata record if it does not exist yet. Dedup hits
  (same content committed again) keep the original record.
  """
  def ensure_blob_meta(content_hash, size) do
    case get_blob_meta(content_hash) do
      {:ok, _meta} ->
        :ok

      {:error, :not_found} ->
        <<prefix::binary-size(2), rest::binary>> = content_hash
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        put_blob_meta(content_hash, %{
          hash: "sha256:#{content_hash}",
          size: size,
          physical_path: Path.join(["cas", "objects", "sha256", prefix, rest]),
          state: :active,
          created_at: now,
          last_seen_at: now
        })
    end
  end

  ## Private

  defp normalize_public_object_meta(bucket, metadata) do
    case Concord.get("bucket_versioning:#{bucket}") do
      {:ok, state} when state in [:enabled, "enabled", "Enabled"] ->
        metadata

      _ ->
        Map.delete(metadata, :version_id)
    end
  end
end
