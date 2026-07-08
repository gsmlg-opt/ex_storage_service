defmodule ExStorageService.Storage.Engine do
  @moduledoc """
  Storage engine facade over the global content-addressable store
  (`ExStorageService.Storage.CAS`).

  PUTs stream data to a CAS tmp file — computing SHA-256 and MD5 in a
  single pass in the *calling* process — then commit with an atomic
  rename. GETs resolve the global CAS path first and fall back to the
  legacy bucket-local layout (`{data_root}/{bucket}/objects/...`) for
  content written before the global-CAS migration
  (see `ExStorageService.Storage.Migration`).

  The GenServer exists only to create the storage directories at boot;
  every read/write operation is a plain function.
  """

  use GenServer

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  ## Client API

  def start_link(opts) do
    data_root = Keyword.fetch!(opts, :data_root)
    GenServer.start_link(__MODULE__, data_root, name: __MODULE__)
  end

  @doc """
  Returns the data_root path configured for the engine.
  """
  def data_root, do: CAS.data_root()

  @doc """
  Store object data in the global CAS, computing SHA-256 and MD5 in a
  single pass. `data_or_stream` can be a binary or an `Enumerable` of
  binary chunks; streams are enumerated in the calling process, so this
  is safe for `Plug.Conn.read_body/2`-backed streams.

  Returns `{:ok, {content_hash, etag, size}}` on success.
  """
  def put_object(
        _bucket,
        _key,
        data_or_stream,
        _content_type \\ "application/octet-stream",
        _metadata \\ %{}
      ) do
    tmp_path = CAS.tmp_upload_path()

    case stream_to_file(data_or_stream, tmp_path) do
      {:ok, {content_hash, md5, size}} ->
        :ok = CAS.commit_blob(tmp_path, content_hash)
        Metadata.ensure_blob_meta(content_hash, size)
        etag = Base.encode16(md5, case: :lower)
        {:ok, {content_hash, etag, size}}

      {:error, reason} ->
        File.rm(tmp_path)
        {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Stream-aware PUT. Kept as a separate name for existing callers; the
  write always happens in the calling process now, so this is equivalent
  to `put_object/5`.
  """
  def put_object_stream(
        bucket,
        key,
        stream,
        content_type \\ "application/octet-stream",
        metadata \\ %{}
      ) do
    put_object(bucket, key, stream, content_type, metadata)
  end

  @doc """
  Returns the file path for the given content hash, suitable for sendfile.

  Resolves the global CAS first, then falls back to the legacy
  bucket-local path for content that predates the CAS migration.
  """
  def get_object(bucket, content_hash) do
    legacy = legacy_content_path(CAS.data_root(), bucket, content_hash)

    cond do
      CAS.has_blob?(content_hash) -> {:ok, CAS.blob_path(content_hash)}
      File.exists?(legacy) -> {:ok, legacy}
      true -> {:error, :not_found}
    end
  end

  @doc """
  Ensures the given content hash is present in the global CAS, promoting
  it from the legacy bucket-local layout when necessary (used by
  metadata-only CopyObject and by the migration task).

  Reads of the legacy source keep working after promotion because
  `get_object/2` checks the CAS first.
  """
  def promote_to_global(bucket, content_hash) do
    if CAS.has_blob?(content_hash) do
      :ok
    else
      legacy = legacy_content_path(CAS.data_root(), bucket, content_hash)

      case File.stat(legacy) do
        {:ok, %File.Stat{size: size}} ->
          dest = CAS.blob_path(content_hash)
          File.mkdir_p!(Path.dirname(dest))
          File.rename!(legacy, dest)
          Metadata.ensure_blob_meta(content_hash, size)
          :ok

        {:error, _} ->
          {:error, :not_found}
      end
    end
  end

  @doc """
  Construct the legacy bucket-local filesystem path for a content hash.
  Only pre-migration content lives here; new writes go to the CAS.
  """
  def legacy_content_path(data_root, bucket, content_hash) do
    <<prefix::binary-size(2), rest::binary>> = content_hash
    Path.join([data_root, bucket, "objects", prefix, rest])
  end

  # Transitional delegate for callers not yet migrated to the global CAS
  # (removed once handlers/object.ex and bucket_live/files.ex are updated).
  def content_path(data_root, bucket, content_hash) do
    legacy_content_path(data_root, bucket, content_hash)
  end

  # Transitional no-op: content files are removed by GC only (PRD §10.3).
  def delete_content(_bucket, _content_hash) do
    :ok
  end

  @doc """
  Ensure the bucket directory structure exists (legacy layout; still used
  for multipart part staging).
  """
  def ensure_bucket_dirs(bucket) do
    File.mkdir_p!(Path.join([CAS.data_root(), bucket, "objects"]))
    :ok
  end

  ## Server Callbacks

  @impl true
  def init(data_root) do
    data_root = Path.expand(data_root)
    File.mkdir_p!(data_root)
    File.mkdir_p!(Path.join([data_root, CAS.reserved_root(), "objects", "sha256"]))
    File.mkdir_p!(Path.join([data_root, CAS.reserved_root(), "tmp", "uploads"]))
    Logger.info("Storage engine started with data root: #{data_root}")
    {:ok, %{data_root: data_root}}
  end

  ## Private

  defp stream_to_file(data, tmp_path) when is_binary(data) do
    sha256 = :crypto.hash(:sha256, data)
    md5 = :crypto.hash(:md5, data)
    size = byte_size(data)

    case File.write(tmp_path, data) do
      :ok ->
        content_hash = Base.encode16(sha256, case: :lower)
        {:ok, {content_hash, md5, size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_to_file(stream, tmp_path) do
    try do
      file = File.open!(tmp_path, [:write, :raw, :binary])
      sha256_ctx = :crypto.hash_init(:sha256)
      md5_ctx = :crypto.hash_init(:md5)

      {sha256_ctx, md5_ctx, size} =
        Enum.reduce(stream, {sha256_ctx, md5_ctx, 0}, fn chunk, {sha_ctx, md_ctx, acc_size} ->
          :ok = IO.binwrite(file, chunk)

          sha_ctx = :crypto.hash_update(sha_ctx, chunk)
          md_ctx = :crypto.hash_update(md_ctx, chunk)
          {sha_ctx, md_ctx, acc_size + byte_size(chunk)}
        end)

      File.close(file)

      sha256 = :crypto.hash_final(sha256_ctx)
      md5 = :crypto.hash_final(md5_ctx)
      content_hash = Base.encode16(sha256, case: :lower)

      {:ok, {content_hash, md5, size}}
    rescue
      e -> {:error, Exception.message(e)}
    end
  end
end
