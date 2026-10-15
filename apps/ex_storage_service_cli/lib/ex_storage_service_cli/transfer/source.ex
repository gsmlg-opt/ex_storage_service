defmodule ExStorageServiceCli.Transfer.Source do
  @moduledoc "Replayable bounded file ranges with identity and incremental content validation."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.Transfer.{Coordinator, RemoteSource}
  @max_chunk 65_536
  @identity_keys [:major_device, :minor_device, :inode, :size, :mtime, :ctime, :type]
  defstruct [
    :path,
    :offset,
    :length,
    :identity,
    :payload_hash,
    :coordinator,
    chunk_size: @max_chunk,
    temporary: false
  ]

  defmodule ReadError do
    @moduledoc "An enumerable read failure carrying the same typed error as the tuple APIs."
    defexception [:error, message: "Transfer source could not be read"]
  end

  @doc "Captures a file range and hashes it, unless hash: false is explicitly requested."
  def file(path, opts \\ []) do
    with {:ok, stat} <- stat(path),
         :ok <- regular_file(stat),
         {:ok, offset, length, chunk_size} <- bounds(stat.size, opts),
         source = %__MODULE__{
           path: Path.expand(path),
           offset: offset,
           length: length,
           identity: identity(stat),
           coordinator: opts[:coordinator],
           chunk_size: chunk_size
         },
         {:ok, source} <- maybe_hash(source, opts) do
      {:ok, source}
    end
  end

  @doc "Creates a subrange using absolute file offsets, bounded by the original descriptor."
  def range(source, offset, length, opts \\ [])

  def range(%RemoteSource{} = source, offset, length, opts),
    do: RemoteSource.range(source, offset, length, opts)

  def range(%__MODULE__{} = source, offset, length, opts) do
    if is_integer(offset) and is_integer(length) and length >= 0 and
         offset >= source.offset and offset + length <= source.offset + source.length do
      with :ok <- validate_identity(source) do
        maybe_hash(%{source | offset: offset, length: length, payload_hash: nil}, opts)
      end
    else
      failure(:invalid_source_range, "Transfer source range is invalid")
    end
  end

  @doc "Checks file identity and, when captured, the content hash (including same-second rewrites)."
  def validate(%__MODULE__{} = source) do
    with :ok <- validate_identity(source), {:ok, _hash} <- hash(source), do: :ok
  end

  def validate(%RemoteSource{} = source), do: RemoteSource.validate(source)

  def restore(context, %{"kind" => "remote"} = record, opts),
    do: RemoteSource.restore(context, record, opts)

  def restore(context, record, opts),
    do:
      file(
        record["path"],
        Keyword.merge(opts,
          offset: record["offset"],
          length: record["length"],
          coordinator: context.coordinator
        )
      )

  @doc "Removes only bridge staging files captured by this source descriptor."
  def cleanup(%__MODULE__{temporary: true} = source) do
    if file_identity(source) == :ok, do: File.rm(source.path)
    :ok
  end

  def cleanup(_source), do: :ok

  @doc "Checks inexpensive metadata only; use validate/1 at transfer and resume boundaries."
  def validate_identity(%__MODULE__{} = source) do
    with :ok <- Coordinator.check(source.coordinator), do: file_identity(source)
  end

  defp file_identity(source) do
    with {:ok, current} <- stat(source.path) do
      if identity(current) == source.identity, do: :ok, else: changed()
    end
  end

  @doc "Returns reopenable binary chunks; halts, failures and normal completion close the file."
  def enumerable(%__MODULE__{} = source) do
    Stream.resource(
      fn -> open!(source) end,
      fn state -> next(source, state) end,
      fn {file, _, _} -> File.close(file) end
    )
  end

  @doc "Computes SHA256 incrementally, returning a lowercase hex digest or a typed failure."
  def hash(%__MODULE__{} = source) do
    with {:ok, context} <-
           reduce(source, :crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1)) do
      {:ok, context |> :crypto.hash_final() |> Base.encode16(case: :lower)}
    end
  end

  @doc "Bounded fold usable as an incremental checksum hook; no object binary is materialized."
  def reduce(%__MODULE__{} = source, initial, reducer) when is_function(reducer, 2) do
    {:ok, Enum.reduce(enumerable(source), initial, reducer)}
  rescue
    error in ReadError -> {:error, error.error}
  end

  defp maybe_hash(source, opts) do
    if Keyword.get(opts, :hash, true) do
      with {:ok, hash} <- hash(source), do: {:ok, %{source | payload_hash: hash}}
    else
      {:ok, source}
    end
  end

  defp open!(source) do
    check!(validate_identity(source))

    case File.open(source.path, [:read, :binary, :raw]) do
      {:ok, file} ->
        # Stream.resource cannot run its cleanup when initialization raises.
        try do
          check_file!(source, file)

          case :file.position(file, source.offset) do
            {:ok, _} ->
              {file, source.length, :crypto.hash_init(:sha256)}

            {:error, _} ->
              raise_read(failure(:source_read, "Transfer source could not be positioned"))
          end
        rescue
          error ->
            File.close(file)
            reraise error, __STACKTRACE__
        end

      {:error, _} ->
        raise_read(failure(:source_read, "Transfer source could not be opened"))
    end
  end

  defp next(source, {file, 0, digest} = state) do
    check_file!(source, file)
    hash = digest |> :crypto.hash_final() |> Base.encode16(case: :lower)
    if source.payload_hash && hash != source.payload_hash, do: raise_read(changed())
    {:halt, state}
  end

  defp next(source, {file, remaining, digest}) do
    check_file!(source, file)
    length = min(remaining, source.chunk_size)

    case :file.read(file, length) do
      {:ok, chunk} when byte_size(chunk) == length ->
        {[chunk], {file, remaining - length, :crypto.hash_update(digest, chunk)}}

      :eof ->
        raise_read(changed())

      {:ok, _short} ->
        raise_read(changed())

      {:error, _} ->
        raise_read(failure(:source_read, "Transfer source read failed"))
    end
  end

  defp check_file!(source, file) do
    check!(validate_identity(source))

    case :file.read_file_info(file, time: :posix) do
      {:ok, record} ->
        if record |> File.Stat.from_record() |> identity() != source.identity,
          do: raise_read(changed())

      {:error, _} ->
        raise_read(failure(:source_read, "Transfer source identity could not be read"))
    end
  end

  defp check!(:ok), do: :ok
  defp check!({:error, _} = error), do: raise_read(error)
  defp raise_read({:error, error}), do: raise(ReadError, error: error)

  defp stat(path) do
    case File.stat(path, time: :posix) do
      {:ok, stat} -> {:ok, stat}
      {:error, _} -> failure(:source_unavailable, "Transfer source is unavailable")
    end
  end

  defp regular_file(%{type: :regular}), do: :ok
  defp regular_file(_), do: failure(:invalid_source, "Transfer source must be a regular file")
  defp identity(stat), do: Map.take(stat, @identity_keys)

  defp bounds(size, opts) do
    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, if(is_integer(offset), do: size - offset, else: nil))
    chunk = Keyword.get(opts, :chunk_size, @max_chunk)

    if is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 and
         offset + length <= size and is_integer(chunk) and chunk in 1..@max_chunk,
       do: {:ok, offset, length, chunk},
       else: failure(:invalid_source_range, "Transfer source range or chunk size is invalid")
  end

  defp changed, do: failure(:source_changed, "Transfer source changed after it was captured")
  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
