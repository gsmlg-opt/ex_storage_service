defmodule ExStorageService.BlobStore.ReadRepair do
  @moduledoc """
  Bounded tee-to-local repair for full remote blob streams.

  Each upstream chunk is written before it is forwarded, so memory remains
  bounded by one transport chunk. Final checksum verification, CAS publication,
  and metadata repair run under the instance replica task supervisor after the
  client stream completes.
  """

  alias ExStorageService.BlobStore.{LocalCAS, Source, StagedBlob}

  @chunk_size 262_144

  @spec wrap(Source.t(), binary(), non_neg_integer(), keyword()) :: Source.t()
  def wrap({:stream, _producer, length} = source, hash, size, opts)
      when length == size do
    Source.stateful_stream(
      fn initial, reducer ->
        case open_staging(opts) do
          {:ok, staging} ->
            try do
              source
              |> Source.reduce({initial, staging}, tee_reducer(reducer))
              |> finish_stream(hash, size, opts)
            catch
              kind, reason ->
                _ = close_staging(staging)
                _ = File.rm(staging.path)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          {:error, _reason} ->
            Source.reduce(source, initial, reducer)
        end
      end,
      size
    )
  end

  def wrap(source, _hash, _size, _opts), do: source

  defp open_staging(opts) do
    path = LocalCAS.staging_path(blob_opts(opts))

    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, io} <- File.open(path, [:write, :raw, :binary]) do
      {:ok, %{path: path, io: io, bytes: 0, error: nil}}
    end
  end

  defp tee_reducer(reducer) do
    fn chunk, {consumer, staging} ->
      staging = write_chunk(staging, chunk)

      case reducer.(chunk, consumer) do
        {:cont, next} -> {:cont, {next, staging}}
        {:halt, reason, next} -> {:halt, reason, {next, staging}}
      end
    end
  end

  defp write_chunk(%{error: nil} = staging, chunk) do
    case IO.binwrite(staging.io, chunk) do
      :ok -> %{staging | bytes: staging.bytes + byte_size(chunk)}
      {:error, reason} -> %{staging | error: {:write, reason}}
    end
  end

  defp write_chunk(staging, _chunk), do: staging

  defp finish_stream({:ok, {consumer, staging}}, hash, size, opts) do
    case close_staging(staging) do
      {:ok, %{bytes: ^size, error: nil} = complete} ->
        schedule_finalize(complete.path, hash, size, opts)

      {:ok, incomplete} ->
        _ = File.rm(incomplete.path)

      {:error, path} ->
        _ = File.rm(path)
    end

    {:ok, consumer}
  end

  defp finish_stream({:error, reason, {consumer, staging}}, _hash, _size, _opts) do
    _ = close_staging(staging)
    _ = File.rm(staging.path)
    {:error, reason, consumer}
  end

  defp close_staging(staging) do
    sync_result = :file.sync(staging.io)
    close_result = File.close(staging.io)

    case {sync_result, close_result} do
      {:ok, :ok} -> {:ok, staging}
      _ -> {:error, staging.path}
    end
  end

  defp schedule_finalize(path, hash, size, opts) do
    work = fn -> finalize(path, hash, size, opts) end

    case Keyword.get(opts, :finalizer, :async) do
      :inline ->
        work.()

      :async ->
        case Keyword.get(opts, :task_supervisor) do
          nil ->
            _ = File.rm(path)

          supervisor ->
            if process_alive?(supervisor) do
              case Task.Supervisor.start_child(supervisor, work) do
                {:ok, _pid} -> :ok
                {:error, _reason} -> File.rm(path)
              end
            else
              File.rm(path)
            end
        end
    end
  end

  defp finalize(path, hash, size, opts) do
    store = Keyword.get(opts, :blob_store, LocalCAS)
    staged = %StagedBlob{path: path, hash: hash, etag: nil, size: size}

    try do
      with {:ok, ^size} <- file_size(path),
           {:ok, ^hash} <- file_hash(path),
           {:ok, ready} <- commit_repair(store, staged, blob_opts(opts)) do
        _ = on_ready(opts, ready)
        {:committed, ready}
      end
    after
      _ = File.rm(path)
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %File.Stat{type: :regular, size: size}} -> {:ok, size}
      {:ok, _stat} -> {:error, :invalid_staged_file}
      {:error, reason} -> {:error, reason}
    end
  end

  defp file_hash(path) do
    digest =
      path
      |> File.stream!([], @chunk_size)
      |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
      |> :crypto.hash_final()
      |> Base.encode16(case: :lower)

    {:ok, digest}
  end

  defp on_ready(opts, ready) do
    case Keyword.get(opts, :on_ready) do
      callback when is_function(callback, 1) -> callback.(ready)
      _ -> {:ok, ready}
    end
  end

  defp commit_repair(store, staged, opts) do
    if function_exported?(store, :commit_repair, 2),
      do: store.commit_repair(staged, opts),
      else: store.commit(staged, opts)
  end

  defp process_alive?(name) when is_atom(name), do: Process.whereis(name) != nil

  defp process_alive?({:via, module, term}) do
    case module.whereis_name(term) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end

  defp process_alive?(_name), do: false

  defp blob_opts(opts), do: Keyword.get(opts, :blob_store_opts, [])
end
