defmodule ExStorageService.BlobStore.LocalCAS do
  @moduledoc """
  Durable local content-addressed blob storage.

  Staging and ready files default to directories below the same `cas` root.
  Commit syncs staged bytes, closes the file, atomically renames it, and syncs
  both directories affected by the rename where the platform supports
  directory handles. The configured staging and ready roots must share a
  filesystem; a cross-device rename is rejected without copying or publishing
  partial content.

  Filesystem operations and boundary faults are injectable per call through
  `:fs_module` and `:faults`; no mutable global test state is used.
  """

  @behaviour ExStorageService.BlobStore

  alias ExStorageService.BlobStore.{ReadyBlob, Source, StagedBlob}
  alias ExStorageService.Storage.Pack

  @chunk_size 262_144

  defmodule FileSystem do
    @moduledoc false

    def mkdir_p(path), do: File.mkdir_p(path)
    def open(path, modes), do: File.open(path, modes)
    def write(io, data), do: IO.binwrite(io, data)
    def sync(io), do: :file.sync(io)
    def close(io), do: File.close(io)
    def rename(source, destination), do: File.rename(source, destination)
    def rm(path), do: File.rm(path)
    def stat(path), do: File.stat(path)
    def pread(io, offset, length), do: :file.pread(io, offset, length)

    def open_directory(path) do
      :file.open(String.to_charlist(path), [:read, :raw, :directory])
    end
  end

  @impl true
  def stage(data, opts \\ []) do
    fs = fs(opts)
    path = staging_path(opts)

    with :ok <- phase(:stage, opts),
         :ok <- tagged(:stage, fs.mkdir_p(Path.dirname(path))),
         {:ok, io} <- tagged(:stage, fs.open(path, [:write, :raw, :binary])) do
      result = write_staged(io, data, fs)
      close_result = tagged(:stage, fs.close(io))

      case {result, close_result} do
        {{:ok, {hash, etag, size}}, :ok} ->
          {:ok, %StagedBlob{path: path, hash: hash, etag: etag, size: size}}

        {{:error, _} = error, _} ->
          _ = fs.rm(path)
          error

        {{:ok, _digest}, {:error, _} = error} ->
          _ = fs.rm(path)
          error
      end
    else
      {:error, _} = error ->
        _ = fs.rm(path)
        error
    end
  end

  @impl true
  def commit(%StagedBlob{} = staged, opts \\ []) do
    fs = fs(opts)

    with :ok <- validate_staged(staged),
         destination = blob_path(staged.hash, opts),
         :ok <- tagged(:rename, fs.mkdir_p(Path.dirname(destination))) do
      case fs.stat(destination) do
        {:ok, %File.Stat{type: :regular, size: size}} when size == staged.size ->
          with :ok <- verify_source(Source.file(destination, 0, size), staged.hash, opts),
               :ok <- discard(staged, opts) do
            {:ok, ready(staged, destination)}
          end

        {:ok, %File.Stat{}} ->
          {:error, {:commit, :existing_blob_mismatch}}

        {:error, :enoent} ->
          publish(staged, destination, opts)

        {:error, reason} ->
          {:error, {:stat, reason}}
      end
    end
  end

  @impl true
  def discard(%StagedBlob{path: path}, opts \\ []) do
    case fs(opts).rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:discard, reason}}
    end
  end

  @impl true
  def stat(hash, opts \\ []) do
    with {:ok, {storage, source}} <- resolve(hash, opts) do
      {:file, _path, _offset, size} = source
      {:ok, %{hash: hash, size: size, source: source, storage: storage}}
    end
  end

  @impl true
  def open(hash, range \\ nil, opts \\ []) do
    with {:ok, {_storage, source}} <- resolve(hash, opts),
         {:ok, ranged_source} <- apply_range(source, range) do
      {:ok, ranged_source}
    end
  end

  @impl true
  def delete(hash, opts \\ []) do
    with :ok <- validate_hash(hash) do
      case fs(opts).rm(blob_path(hash, opts)) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, {:delete, reason}}
      end
    end
  end

  @impl true
  def verify(hash, opts \\ []) do
    with {:ok, source} <- open(hash, nil, opts),
         :ok <- verify_source(source, hash, opts) do
      :ok
    end
  end

  @doc """
  Ensures a hash is ready in the global CAS.

  Existing loose or packed blobs are returned unchanged. With `:bucket`,
  legacy content is streamed for checksum verification, synced, and renamed
  into the CAS using the normal durable commit path.
  """
  @spec ensure_ready(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def ensure_ready(hash, opts \\ []) do
    case stat(hash, Keyword.delete(opts, :bucket)) do
      {:ok, ready_stat} ->
        {:ok, ready_stat}

      {:error, :not_found} ->
        promote_legacy(hash, opts)

      {:error, _} = error ->
        error
    end
  end

  @doc false
  def blob_path(hash, opts \\ []) do
    <<prefix::binary-size(2), rest::binary>> = hash
    Path.join([root(opts), "objects", "sha256", prefix, rest])
  end

  @doc false
  def staging_path(opts \\ []) do
    directory =
      Keyword.get_lazy(opts, :tmp_dir, fn ->
        tmp_root =
          Application.get_env(:ex_storage_service, :tmp_root, Path.join(root(opts), "tmp"))

        Path.join(tmp_root, "uploads")
      end)

    Path.join(directory, "upload-#{System.unique_integer([:positive, :monotonic])}")
  end

  defp publish(staged, destination, opts) do
    fs = fs(opts)

    with {:ok, io} <- tagged(:sync, fs.open(staged.path, [:read, :raw, :binary])),
         :ok <- sync_and_close(io, fs, opts),
         :ok <- phase(:rename, opts),
         :ok <- rename(fs, staged.path, destination),
         :ok <-
           sync_directories(
             fs,
             [
               Path.dirname(destination),
               destination |> Path.dirname() |> Path.dirname(),
               Path.dirname(staged.path)
             ],
             opts
           ) do
      {:ok, ready(staged, destination)}
    end
  end

  defp promote_legacy(hash, opts) do
    with {:ok, %{size: size, source: {:file, path, 0, size}}} <- stat(hash, opts),
         :ok <- verify(hash, opts),
         {:ok, _ready} <-
           commit(%StagedBlob{path: path, hash: hash, etag: nil, size: size}, opts) do
      stat(hash, Keyword.delete(opts, :bucket))
    end
  end

  defp sync_and_close(io, fs, opts) do
    result =
      with :ok <- phase(:sync, opts),
           :ok <- tagged(:sync, fs.sync(io)) do
        :ok
      end

    close_result = tagged(:sync, fs.close(io))
    if result == :ok, do: close_result, else: result
  end

  defp rename(fs, source, destination) do
    case fs.rename(source, destination) do
      :ok -> :ok
      {:error, :exdev} -> {:error, {:rename, :cross_device}}
      {:error, reason} -> {:error, {:rename, reason}}
    end
  end

  defp sync_directories(fs, directories, opts) do
    directories
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn directory, :ok ->
      case sync_directory(fs, directory, opts) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp sync_directory(fs, directory, opts) do
    with :ok <- phase(:directory_sync, opts) do
      case fs.open_directory(directory) do
        {:ok, io} ->
          result = tagged(:directory_sync, fs.sync(io))
          close_result = tagged(:directory_sync, fs.close(io))
          if result == :ok, do: close_result, else: result

        {:error, reason} when reason in [:enotsup, :eisdir] ->
          :ok

        {:error, reason} ->
          {:error, {:directory_sync, reason}}
      end
    end
  end

  defp write_staged(io, data, fs) when is_binary(data) do
    with :ok <- tagged(:stage, fs.write(io, data)) do
      {:ok, digest_result(:crypto.hash(:sha256, data), :crypto.hash(:md5, data), byte_size(data))}
    end
  end

  defp write_staged(io, enumerable, fs) do
    try do
      enumerable
      |> Enum.reduce_while(
        {:crypto.hash_init(:sha256), :crypto.hash_init(:md5), 0},
        fn
          chunk, {sha, md5, size} when is_binary(chunk) ->
            case fs.write(io, chunk) do
              :ok ->
                {:cont,
                 {:crypto.hash_update(sha, chunk), :crypto.hash_update(md5, chunk),
                  size + byte_size(chunk)}}

              {:error, reason} ->
                {:halt, {:error, {:stage, reason}}}
            end

          _chunk, _acc ->
            {:halt, {:error, {:stage, :invalid_chunk}}}
        end
      )
      |> case do
        {sha, md5, size} ->
          {:ok, digest_result(:crypto.hash_final(sha), :crypto.hash_final(md5), size)}

        {:error, _} = error ->
          error
      end
    rescue
      error -> {:error, {:stage, Exception.message(error)}}
    catch
      {:error, reason} -> {:error, {:stage, reason}}
    end
  end

  defp digest_result(sha, md5, size) do
    {
      Base.encode16(sha, case: :lower),
      Base.encode16(md5, case: :lower),
      size
    }
  end

  defp resolve(hash, opts) do
    with :ok <- validate_hash(hash) do
      case Keyword.get(opts, :pack_module, Pack) do
        nil ->
          resolve_file(hash, opts)

        pack_module ->
          case pack_module.locate(hash) do
            {:ok, {path, offset, size}} ->
              {:ok, {:packed, Source.file(path, offset, size)}}

            {:error, :not_found} ->
              resolve_file(hash, opts)

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  defp resolve_file(hash, opts) do
    path = blob_path(hash, opts)

    case regular_file_source(path, opts) do
      {:ok, source} ->
        {:ok, {:loose, source}}

      {:error, :not_found} ->
        resolve_legacy(hash, opts)

      {:error, _} = error ->
        error
    end
  end

  defp resolve_legacy(hash, opts) do
    case Keyword.get(opts, :bucket) do
      bucket when is_binary(bucket) ->
        <<prefix::binary-size(2), rest::binary>> = hash

        data_root =
          Keyword.get(
            opts,
            :data_root,
            Application.get_env(
              :ex_storage_service,
              :data_root,
              "/tmp/ex_storage_service/data"
            )
          )

        path = Path.join([data_root, bucket, "objects", prefix, rest])

        case regular_file_source(path, opts) do
          {:ok, source} -> {:ok, {:legacy, source}}
          {:error, _} = error -> error
        end

      _ ->
        {:error, :not_found}
    end
  end

  defp regular_file_source(path, opts) do
    case fs(opts).stat(path) do
      {:ok, %File.Stat{type: :regular, size: size}} -> {:ok, Source.file(path, 0, size)}
      {:ok, %File.Stat{}} -> {:error, :not_found}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, {:stat, reason}}
    end
  end

  defp apply_range(source, nil), do: {:ok, source}
  defp apply_range(source, :all), do: {:ok, source}

  defp apply_range({:file, path, base_offset, total}, {offset, length})
       when is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 and
              offset <= total and length <= total - offset do
    {:ok, Source.file(path, base_offset + offset, length)}
  end

  defp apply_range(_source, _range), do: {:error, :invalid_range}

  defp hash_source({:file, path, offset, length}, opts) do
    fs = fs(opts)

    with {:ok, io} <- tagged(:verify, fs.open(path, [:read, :raw, :binary])) do
      result = hash_chunks(io, offset, length, :crypto.hash_init(:sha256), fs)
      close_result = tagged(:verify, fs.close(io))
      if match?({:ok, _}, result), do: merge_close(result, close_result), else: result
    end
  end

  defp verify_source(source, expected_hash, opts) do
    with {:ok, actual_hash} <- hash_source(source, opts) do
      if actual_hash == expected_hash, do: :ok, else: {:error, :checksum_mismatch}
    end
  end

  defp hash_chunks(_io, _offset, 0, context, _fs) do
    {:ok, context |> :crypto.hash_final() |> Base.encode16(case: :lower)}
  end

  defp hash_chunks(io, offset, remaining, context, fs) do
    length = min(remaining, @chunk_size)

    case fs.pread(io, offset, length) do
      {:ok, data} when byte_size(data) == length ->
        hash_chunks(
          io,
          offset + length,
          remaining - length,
          :crypto.hash_update(context, data),
          fs
        )

      {:ok, _short} ->
        {:error, {:verify, :unexpected_eof}}

      :eof ->
        {:error, {:verify, :unexpected_eof}}

      {:error, reason} ->
        {:error, {:verify, reason}}
    end
  end

  defp merge_close({:ok, value}, :ok), do: {:ok, value}
  defp merge_close({:ok, _value}, {:error, _} = error), do: error

  defp ready(staged, destination) do
    %ReadyBlob{
      path: destination,
      hash: staged.hash,
      etag: staged.etag,
      size: staged.size,
      source: Source.file(destination, 0, staged.size)
    }
  end

  defp root(opts) do
    Keyword.get_lazy(opts, :root, fn ->
      Application.get_env(
        :ex_storage_service,
        :blob_root,
        Path.join(
          Application.get_env(
            :ex_storage_service,
            :data_root,
            "/tmp/ex_storage_service/data"
          ),
          "cas"
        )
      )
    end)
  end

  defp fs(opts), do: Keyword.get(opts, :fs_module, FileSystem)

  defp validate_hash(hash) when is_binary(hash) and byte_size(hash) == 64 do
    case Base.decode16(hash, case: :mixed) do
      {:ok, decoded} when byte_size(decoded) == 32 -> :ok
      _ -> {:error, :invalid_hash}
    end
  end

  defp validate_hash(_hash), do: {:error, :invalid_hash}

  defp validate_staged(%StagedBlob{path: path, hash: hash, size: size})
       when is_binary(path) and is_integer(size) and size >= 0,
       do: validate_hash(hash)

  defp validate_staged(%StagedBlob{}), do: {:error, :invalid_staged_blob}

  defp phase(name, opts) do
    faults = Keyword.get(opts, :faults, %{})
    hook = if is_map(faults), do: Map.get(faults, name), else: Keyword.get(faults, name)

    case hook do
      nil -> :ok
      :ok -> :ok
      function when is_function(function, 0) -> tagged(name, function.())
      {:error, reason} -> {:error, {name, reason}}
      reason -> {:error, {name, reason}}
    end
  end

  defp tagged(_phase, :ok), do: :ok
  defp tagged(_phase, {:ok, value}), do: {:ok, value}
  defp tagged(phase, {:error, reason}), do: {:error, {phase, reason}}
end
