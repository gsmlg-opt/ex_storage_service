defmodule ExStorageService.Storage.CAS do
  @moduledoc """
  Global content-addressable blob store under the reserved `cas/` root.

  Layout: `{data_root}/cas/objects/sha256/{first_two_hex}/{rest}`.

  Blobs are immutable and shared across all buckets, keys, and versions.
  Commit is a synced atomic rename from a tmp file on the same filesystem.
  All functions are plain path/filesystem operations executed
  in the caller's process — this module deliberately has no process.

  `cas` is a reserved name: `BucketValidator` rejects it as a bucket name
  and `ContentGC` skips it when scanning the legacy bucket-local layout.
  """

  @reserved_root "cas"

  alias ExStorageService.BlobStore.{LocalCAS, StagedBlob}

  def reserved_root, do: @reserved_root

  def data_root do
    Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
  end

  def blob_path(content_hash) do
    LocalCAS.blob_path(content_hash, root: cas_root())
  end

  def has_blob?(content_hash), do: File.exists?(blob_path(content_hash))

  @doc """
  Returns a unique path inside `cas/tmp/uploads/`, creating the directory.
  The tmp dir shares a filesystem with `cas/objects/`, so `commit_blob/2`
  can rename atomically.
  """
  def tmp_upload_path do
    dir = Path.join([data_root(), @reserved_root, "tmp", "uploads"])
    File.mkdir_p!(dir)
    Path.join(dir, "upload-#{:erlang.unique_integer([:positive])}")
  end

  @doc """
  Atomically moves `tmp_path` into the CAS. If the blob already exists
  (dedup hit), the tmp file is discarded and the existing blob is kept.
  """
  def commit_blob(tmp_path, content_hash) do
    with {:ok, %File.Stat{size: size}} <- File.stat(tmp_path),
         {:ok, _ready} <-
           LocalCAS.commit(
             %StagedBlob{path: tmp_path, hash: content_hash, etag: nil, size: size},
             root: cas_root()
           ) do
      :ok
    end
  end

  @doc """
  Re-hashes the blob file and compares against its content hash.
  """
  def verify_blob(content_hash) do
    case LocalCAS.verify(content_hash, root: cas_root()) do
      :ok -> :ok
      {:error, :checksum_mismatch} -> {:error, :corrupt}
      {:error, :not_found} -> {:error, :missing}
      {:error, reason} -> {:error, reason}
    end
  end

  defp cas_root, do: Path.join(data_root(), @reserved_root)
end
