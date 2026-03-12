defmodule ExStorageService.Storage.Engine do
  @moduledoc """
  Storage engine providing content-addressable file storage using SHA-256.

  Files are stored under `data_root/bucket/objects/` organized by the first
  two characters of their content hash for even directory distribution.
  """

  use GenServer

  require Logger

  ## Client API

  def start_link(opts) do
    data_root = Keyword.fetch!(opts, :data_root)
    GenServer.start_link(__MODULE__, data_root, name: __MODULE__)
  end

  @doc """
  Store object data, computing SHA-256 and MD5 in a single pass.

  Returns `{:ok, {content_hash, etag, size}}` on success.
  """
  def put_object(bucket, key, data_or_stream, content_type \\ "application/octet-stream", metadata \\ %{}) do
    GenServer.call(__MODULE__, {:put_object, bucket, key, data_or_stream, content_type, metadata}, :infinity)
  end

  @doc """
  Returns the file path for the given content hash, suitable for sendfile.
  """
  def get_object(bucket, content_hash) do
    GenServer.call(__MODULE__, {:get_object, bucket, content_hash})
  end

  @doc """
  Delete content file if it is no longer referenced.
  """
  def delete_content(bucket, content_hash) do
    GenServer.call(__MODULE__, {:delete_content, bucket, content_hash})
  end

  @doc """
  Construct the filesystem path for a content-addressed object.
  """
  def content_path(data_root, bucket, content_hash) do
    <<prefix::binary-size(2), rest::binary>> = content_hash
    Path.join([data_root, bucket, "objects", prefix, rest])
  end

  @doc """
  Ensure the bucket directory structure exists.
  """
  def ensure_bucket_dirs(bucket) do
    GenServer.call(__MODULE__, {:ensure_bucket_dirs, bucket})
  end

  ## Server Callbacks

  @impl true
  def init(data_root) do
    data_root = Path.expand(data_root)
    File.mkdir_p!(data_root)
    Logger.info("Storage engine started with data root: #{data_root}")
    {:ok, %{data_root: data_root}}
  end

  @impl true
  def handle_call({:put_object, bucket, _key, data_or_stream, _content_type, _metadata}, _from, state) do
    %{data_root: data_root} = state

    ensure_bucket_dirs!(data_root, bucket)

    tmp_dir = Path.join([data_root, bucket, "tmp"])
    File.mkdir_p!(tmp_dir)
    tmp_path = Path.join(tmp_dir, "upload_#{:erlang.unique_integer([:positive])}")

    result = stream_to_file(data_or_stream, tmp_path)

    case result do
      {:ok, {sha256_hash, md5_hash, size}} ->
        dest = content_path(data_root, bucket, sha256_hash)
        dest_dir = Path.dirname(dest)
        File.mkdir_p!(dest_dir)
        File.rename!(tmp_path, dest)

        etag = Base.encode16(md5_hash, case: :lower)
        {:reply, {:ok, {sha256_hash, etag, size}}, state}

      {:error, reason} ->
        File.rm(tmp_path)
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_object, bucket, content_hash}, _from, state) do
    %{data_root: data_root} = state
    path = content_path(data_root, bucket, content_hash)

    if File.exists?(path) do
      {:reply, {:ok, path}, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_content, bucket, content_hash}, _from, state) do
    %{data_root: data_root} = state
    path = content_path(data_root, bucket, content_hash)

    case File.rm(path) do
      :ok -> {:reply, :ok, state}
      {:error, :enoent} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:ensure_bucket_dirs, bucket}, _from, state) do
    %{data_root: data_root} = state
    ensure_bucket_dirs!(data_root, bucket)
    {:reply, :ok, state}
  end

  ## Private

  defp ensure_bucket_dirs!(data_root, bucket) do
    objects_dir = Path.join([data_root, bucket, "objects"])
    File.mkdir_p!(objects_dir)
  end

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
