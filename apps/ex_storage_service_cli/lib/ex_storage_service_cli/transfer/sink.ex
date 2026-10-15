defmodule ExStorageServiceCli.Transfer.Sink do
  @moduledoc """
  Exclusive download staging followed by verified atomic publication.

  Parent components and the destination are checked with lstat; symlinks are
  rejected. Captured directory identities are rechecked before publication.
  Portable BEAM file APIs do not provide openat/renameat directory capabilities:
  the destination tree must be controlled by the invoking user, without another
  process concurrently renaming its parent directories.
  """
  alias ExStorageServiceCli.Error
  defstruct [:path, :temporary, :file, :original, :temporary_identity, parents: []]

  @doc "Maps a relative object key without normalizing or silently renaming it."
  def map(root, key) when is_binary(key) do
    parts = String.split(key, "/", trim: false)

    if key == "" or Enum.any?(parts, &unsafe_component?/1),
      do: unsafe(),
      else: {:ok, Path.join([Path.expand(root) | parts])}
  end

  def map(_root, _key), do: unsafe()

  def open(path) do
    path = Path.expand(path)

    with {:ok, parents} <- parents(Path.dirname(path)),
         {:ok, original} <- destination(path),
         temporary =
           Path.join(
             Path.dirname(path),
             ".ess-download-" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
           ),
         {:ok, file} <- file_open(temporary) do
      case File.chmod(temporary, 0o600) do
        :ok ->
          {:ok, stat} = File.lstat(temporary, time: :posix)

          {:ok,
           %__MODULE__{
             path: path,
             temporary: temporary,
             file: file,
             original: original,
             temporary_identity: identity(stat),
             parents: parents
           }}

        {:error, _} ->
          File.close(file)
          File.rm(temporary)
          failure("Cannot restrict download staging permissions")
      end
    end
  end

  def write(%__MODULE__{file: file}, bytes) do
    case :file.write(file, bytes) do
      :ok -> :ok
      {:error, _} -> failure("Cannot write download staging file")
    end
  end

  def publish(%__MODULE__{} = sink) do
    with :ok <- sync(sink.file),
         :ok <- unchanged_parents(sink.parents),
         :ok <- unchanged_temporary(sink),
         {:ok, current} <- destination(sink.path),
         :ok <- unchanged_destination(sink.original, current),
         :ok <- close_file(sink.file),
         :ok <- rename(sink.temporary, sink.path) do
      :ok
    end
  end

  @doc "Closes and removes only the captured staging file, never a foreign replacement."
  def abort(%__MODULE__{} = sink) do
    File.close(sink.file)

    case File.lstat(sink.temporary, time: :posix) do
      {:ok, stat} ->
        if identity(stat) == sink.temporary_identity, do: File.rm(sink.temporary)

      {:error, :enoent} ->
        :ok

      {:error, _} ->
        :ok
    end

    :ok
  end

  defp parents(directory) do
    directory
    |> Path.split()
    |> Enum.reduce_while({:ok, [], ""}, fn part, {:ok, records, previous} ->
      path = if previous == "", do: part, else: Path.join(previous, part)

      case ensure_directory(path) do
        {:ok, stat} -> {:cont, {:ok, [{path, identity(stat)} | records], path}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, records, _} -> {:ok, records}
      error -> error
    end
  end

  defp ensure_directory(path) do
    case File.lstat(path, time: :posix) do
      {:ok, %{type: :directory} = stat} ->
        {:ok, stat}

      {:ok, _} ->
        unsafe()

      {:error, :enoent} ->
        case File.mkdir(path) do
          :ok ->
            with :ok <- chmod_directory(path), do: ensure_directory(path)

          {:error, :eexist} ->
            ensure_directory(path)

          {:error, _} ->
            failure("Cannot create destination directory")
        end

      {:error, _} ->
        failure("Cannot inspect destination directory")
    end
  end

  defp destination(path) do
    case File.lstat(path, time: :posix) do
      {:ok, %{type: :regular} = stat} ->
        {:ok, Map.take(stat, [:major_device, :minor_device, :inode, :size, :mtime, :ctime])}

      {:ok, _} ->
        unsafe()

      {:error, :enoent} ->
        {:ok, nil}

      {:error, _} ->
        failure("Cannot inspect download destination")
    end
  end

  defp unchanged_parents(records) do
    Enum.reduce_while(records, :ok, fn {path, captured}, :ok ->
      case File.lstat(path, time: :posix) do
        {:ok, stat} -> if identity(stat) == captured, do: {:cont, :ok}, else: {:halt, unsafe()}
        {:error, _} -> {:halt, unsafe()}
      end
    end)
  end

  defp unchanged_destination(value, value), do: :ok

  defp unchanged_destination(_old, _new),
    do:
      {:error,
       Error.new(:client, "Download destination changed during transfer",
         code: :destination_changed
       )}

  defp identity(stat), do: Map.take(stat, [:major_device, :minor_device, :inode, :type])

  defp unchanged_temporary(sink) do
    case File.lstat(sink.temporary, time: :posix) do
      {:ok, %{type: :regular} = stat} ->
        if identity(stat) == sink.temporary_identity, do: :ok, else: unsafe()

      _ ->
        unsafe()
    end
  end

  defp chmod_directory(path),
    do: normalize(File.chmod(path, 0o700), "Cannot restrict destination directory permissions")

  defp file_open(path) do
    case File.open(path, [:write, :binary, :raw, :exclusive]) do
      {:ok, file} -> {:ok, file}
      {:error, _} -> failure("Cannot create exclusive download staging file")
    end
  end

  defp sync(file), do: normalize(:file.sync(file), "Cannot flush download staging file")
  defp close_file(file), do: normalize(File.close(file), "Cannot close download staging file")

  defp rename(source, target),
    do: normalize(File.rename(source, target), "Cannot publish download atomically")

  defp normalize(:ok, _message), do: :ok
  defp normalize({:error, _}, message), do: failure(message)
  defp failure(message), do: {:error, Error.new(:client, message, code: :destination_io)}

  defp unsafe,
    do:
      {:error, Error.new(:client, "Unsafe local download destination", code: :unsafe_destination)}

  defp unsafe_component?(part) do
    part in ["", ".", ".."] or String.contains?(part, ["\\", ":", <<0>>]) or
      String.ends_with?(part, [".", " "]) or
      Regex.match?(~r/^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)/i, part)
  end
end
