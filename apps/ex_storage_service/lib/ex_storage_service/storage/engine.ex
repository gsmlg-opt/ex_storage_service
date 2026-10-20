defmodule ExStorageService.Storage.Engine do
  @moduledoc """
  Storage engine facade over the global content-addressable store
  (`ExStorageService.Storage.CAS`).

  PUTs stream data to a CAS tmp file — computing SHA-256 and MD5 in a
  single pass in the *calling* process — then commit with an atomic
  rename. GETs resolve packed blobs first, then fall back to loose global
  CAS and legacy bucket-local paths (`{data_root}/{bucket}/objects/...`)
  for content written before the global-CAS migration
  (see `ExStorageService.Storage.Migration`).

  The GenServer initializes directories and drives bounded anonymous-staging
  recovery; every read/write operation is a plain function.
  """

  use GenServer

  require Logger

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS
  alias ExStorageService.Storage.Pack
  alias ExStorageService.Storage.StagingRecovery

  ## Client API

  def start_link(opts) do
    data_root = Keyword.fetch!(opts, :data_root)
    blob_root = Keyword.get(opts, :blob_root, CAS.blob_root())
    tmp_root = Keyword.get(opts, :tmp_root, CAS.tmp_root())
    name = Keyword.get(opts, :name, __MODULE__)

    GenServer.start_link(
      __MODULE__,
      %{
        data_root: data_root,
        blob_root: blob_root,
        tmp_root: tmp_root,
        recovery_context: Keyword.get(opts, :recovery_context)
      },
      name: name
    )
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
    case LocalCAS.stage(data_or_stream, blob_store_opts()) do
      {:ok, staged} ->
        case LocalCAS.commit(staged, blob_store_opts()) do
          {:ok, ready} ->
            Metadata.ensure_blob_meta(ready.hash, ready.size)
            {:ok, {ready.hash, ready.etag, ready.size}}

          {:error, _} = error ->
            _ = LocalCAS.discard(staged, blob_store_opts())
            error
        end

      {:error, _} = error ->
        normalize_put_error(error)
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
    case LocalCAS.stat(content_hash, blob_store_opts(bucket: bucket, pack_module: nil)) do
      {:ok, %{storage: storage, source: {:file, path, 0, _size}}}
      when storage in [:loose, :legacy] ->
        {:ok, path}

      {:ok, %{storage: :packed}} ->
        {:error, :not_found}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Resolve a blob to a servable location: a whole file (loose CAS or legacy
  layout) or a slice of a pack file. Serving code uses the offset/length
  forms of `send_file` for pack slices, preserving zero-copy and Range.
  """
  def get_object_location(bucket, content_hash) do
    case LocalCAS.stat(content_hash, blob_store_opts(bucket: bucket)) do
      {:ok, %{storage: :packed, source: {:file, path, offset, size}}} ->
        {:ok, {:pack, path, offset, size}}

      {:ok, %{source: {:file, path, 0, _size}}} ->
        {:ok, {:file, path}}

      {:error, _} = error ->
        error
    end
  end

  @doc "Read a blob's bytes regardless of physical location."
  def read_object(bucket, content_hash) do
    case get_object_location(bucket, content_hash) do
      {:ok, {:file, path}} -> File.read(path)
      {:ok, {:pack, _path, _offset, _size}} -> Pack.read(content_hash)
      {:error, reason} -> {:error, reason}
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
    case LocalCAS.ensure_ready(content_hash, blob_store_opts(bucket: bucket)) do
      {:ok, %{size: size}} ->
        Metadata.ensure_blob_meta(content_hash, size)
        :ok

      {:error, _} = error ->
        error
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

  @doc """
  Ensure the bucket directory structure exists (legacy layout; still used
  for multipart part staging).
  """
  def ensure_bucket_dirs(bucket) do
    File.mkdir_p(Path.join([CAS.data_root(), bucket, "objects"]))
  end

  ## Server Callbacks

  @impl true
  def init(settings) do
    Process.flag(:trap_exit, true)

    settings =
      Enum.reduce([:data_root, :blob_root, :tmp_root], settings, fn key, acc ->
        Map.update!(acc, key, &Path.expand/1)
      end)

    ready_dir = Path.join([settings.blob_root, "objects", "sha256"])
    staging_dir = Path.join(settings.tmp_root, "uploads")

    with :ok <- File.mkdir_p(settings.data_root),
         :ok <- File.mkdir_p(ready_dir),
         :ok <- File.mkdir_p(staging_dir),
         :ok <- ensure_same_filesystem(staging_dir, ready_dir) do
      Logger.info(
        "Storage engine started with data root #{settings.data_root} and blob root #{settings.blob_root}"
      )

      send(self(), :staging_recovery_start)
      {:ok, Map.put(settings, :staging_recovery, nil)}
    else
      {:error, reason} -> {:stop, {:storage_root_unavailable, reason}}
    end
  end

  @impl true
  def handle_info(:staging_recovery_start, %{staging_recovery: recovery} = state)
      when recovery != nil,
      do: {:noreply, state}

  def handle_info(:staging_recovery_start, state) do
    context = state.recovery_context

    if context != nil and context.config.node_role != :metadata and
         ExStorageService.InstanceConfig.worker_enabled?(context.config, :cas_gc) and
         not ExStorageService.Operations.Backup.quarantined?(context.config) do
      case StagingRecovery.new(ExStorageService.Context.blob_store_options(context)) do
        {:ok, recovery} ->
          {:noreply, %{state | staging_recovery: recovery}}

        {:disabled, reason} ->
          Logger.warning("Anonymous staging recovery disabled: #{reason}")
          {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:staging_recovery_step, %{staging_recovery: recovery} = state)
      when recovery != nil do
    recovery = StagingRecovery.step(recovery)
    if recovery.candidate != nil, do: send(self(), :staging_recovery_step)
    {:noreply, %{state | staging_recovery: recovery}}
  end

  def handle_info(message, %{staging_recovery: recovery} = state) when recovery != nil do
    case StagingRecovery.message(recovery, message) do
      {:ok, recovery} ->
        if recovery.candidate != nil, do: send(self(), :staging_recovery_step)
        {:noreply, %{state | staging_recovery: recovery}}

      {:ignore, _} ->
        {:noreply, state}

      {:done, _recovery} ->
        Process.send_after(self(), :staging_recovery_start, 60_000)
        {:noreply, %{state | staging_recovery: nil}}

      {:error, reason, _recovery} ->
        Logger.warning("Anonymous staging recovery stopped: #{reason}")
        Process.send_after(self(), :staging_recovery_start, 60_000)
        {:noreply, %{state | staging_recovery: nil}}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{staging_recovery: recovery}) do
    if recovery != nil, do: StagingRecovery.close(recovery)
    :ok
  end

  ## Private

  defp blob_store_opts(extra \\ []) do
    Keyword.merge(
      case ExStorageService.Context.default() do
        {:ok, context} -> ExStorageService.Context.blob_store_options(context)
        {:error, _} -> [root: CAS.blob_root(), tmp_dir: Path.join(CAS.tmp_root(), "uploads")]
      end,
      extra
    )
  end

  defp normalize_put_error({:error, {:stage, reason}}), do: {:error, reason}
  defp normalize_put_error(error), do: error

  defp ensure_same_filesystem(staging_dir, ready_dir) do
    with {:ok, staging_stat} <- File.stat(staging_dir),
         {:ok, ready_stat} <- File.stat(ready_dir) do
      if {staging_stat.major_device, staging_stat.minor_device} ==
           {ready_stat.major_device, ready_stat.minor_device},
         do: :ok,
         else: {:error, {:cross_device_storage_roots, staging_dir, ready_dir}}
    end
  end
end
