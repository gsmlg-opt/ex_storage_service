defmodule ExStorageServiceCli.Config.Store do
  @moduledoc false
  alias ExStorageServiceCli.Error
  @max_bytes 1_048_576

  def read(path) do
    with :ok <- safe_path(path), {:ok, contents} <- read_bytes(path) do
      case contents do
        nil ->
          {:ok, %{}}

        contents ->
          case Toml.decode(contents, keys: :strings) do
            {:ok, data} ->
              {:ok, data}

            {:error, _reason} ->
              failure("Configuration is malformed; original file was preserved", :malformed)
          end
      end
    end
  end

  def update(path, function) do
    lock = path <> ".lock"

    with :ok <- safe_path(path),
         :ok <- ensure_directory(Path.dirname(path)),
         :ok <- acquire(lock) do
      try do
        with {:ok, data} <- read(path),
             {:ok, updated} <- function.(data),
             {:ok, contents} <- encode(updated),
             do: atomic_write(path, contents)
      after
        File.rmdir(lock)
      end
    end
  end

  def backup(path) do
    with :ok <- safe_path(path), {:ok, contents} <- read_bytes(path) do
      if is_binary(contents) do
        destination = path <> ".backup." <> Integer.to_string(System.system_time(:microsecond))
        with :ok <- atomic_write(destination, contents), do: {:ok, destination}
      else
        failure("Configuration file does not exist", :missing)
      end
    end
  end

  defp read_bytes(path) do
    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, nil}

      {:ok, %{type: :regular, size: size}} when size <= @max_bytes ->
        case File.open(path, [:read, :binary], &IO.binread(&1, @max_bytes + 1)) do
          {:ok, :eof} ->
            {:ok, ""}

          {:ok, contents} when is_binary(contents) and byte_size(contents) <= @max_bytes ->
            {:ok, contents}

          _ ->
            failure("Configuration could not be read within its size limit", :read_failed)
        end

      {:ok, _stat} ->
        failure("Configuration must be a regular file within its size limit", :unsafe_path)

      {:error, _reason} ->
        failure("Configuration could not be inspected", :read_failed)
    end
  end

  defp safe_path(path) do
    [root | segments] = path |> Path.expand() |> Path.split()

    segments
    |> Enum.reduce_while(root, fn segment, parent ->
      current = Path.join(parent, segment)

      case File.lstat(current) do
        {:ok, %{type: :symlink}} ->
          {:halt, failure("Configuration path must not contain symlinks", :unsafe_path)}

        {:ok, _stat} ->
          {:cont, current}

        {:error, :enoent} ->
          {:cont, current}

        {:error, _reason} ->
          {:halt, failure("Configuration path could not be inspected", :unsafe_path)}
      end
    end)
    |> case do
      {:error, error} -> {:error, error}
      _path -> :ok
    end
  end

  defp ensure_directory(directory) do
    case File.mkdir_p(directory) do
      :ok ->
        case File.chmod(directory, 0o700) do
          :ok ->
            :ok

          {:error, _reason} ->
            failure("Configuration directory permissions could not be set", :write_failed)
        end

      {:error, _reason} ->
        failure("Configuration directory could not be created", :write_failed)
    end
  end

  defp acquire(lock) do
    case File.mkdir(lock) do
      :ok ->
        case File.chmod(lock, 0o700) do
          :ok ->
            :ok

          {:error, _reason} ->
            File.rmdir(lock)
            failure("Configuration lock permissions could not be set", :write_failed)
        end

      {:error, :eexist} ->
        failure(
          "Configuration is locked by another writer; inspect the lock before retrying",
          :locked
        )

      {:error, _reason} ->
        failure("Configuration lock could not be acquired", :write_failed)
    end
  end

  defp atomic_write(path, contents) do
    temporary = path <> ".tmp." <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

    with :ok <- safe_path(path),
         {:ok, file} <- File.open(temporary, [:write, :binary, :exclusive]) do
      try do
        result =
          try do
            with :ok <- File.chmod(temporary, 0o600),
                 :ok <- IO.binwrite(file, contents),
                 :ok <- :file.sync(file),
                 do: :ok
          after
            File.close(file)
          end

        with :ok <- result, :ok <- safe_path(path), :ok <- File.rename(temporary, path), do: :ok
      after
        File.rm(temporary)
      end
    end
    |> case do
      :ok ->
        :ok

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, _reason} ->
        failure("Configuration could not be written; original file was preserved", :write_failed)
    end
  end

  defp encode(data) do
    try do
      contents = IO.iodata_to_binary(table(data, []))

      if byte_size(contents) <= @max_bytes,
        do: {:ok, contents},
        else: failure("Configuration exceeds its size limit", :too_large)
    rescue
      _error ->
        failure("Configuration contains values that cannot be encoded safely", :unsupported_value)
    end
  end

  defp table(data, path) do
    {tables, fields} =
      Enum.sort(data)
      |> Enum.split_with(fn {_key, value} -> is_map(value) and not is_struct(value) end)

    header = if path == [], do: [], else: ["\n[", Enum.map_join(path, ".", &string/1), "]\n"]

    [
      header,
      Enum.map(fields, fn {key, value} -> [string(key), " = ", value(value), "\n"] end),
      Enum.map(tables, fn {key, value} -> table(value, path ++ [key]) end)
    ]
  end

  defp value(value) when is_binary(value), do: string(value)
  defp value(value) when is_number(value) or is_boolean(value), do: to_string(value)
  defp value(%Date{} = value), do: Date.to_iso8601(value)
  defp value(%Time{} = value), do: Time.to_iso8601(value)
  defp value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)

  defp value(values) when is_list(values),
    do: ["[", Enum.map_intersperse(values, ", ", &value/1), "]"]

  defp value(map) when is_map(map),
    do: [
      "{",
      Enum.map_intersperse(Enum.sort(map), ", ", fn {key, item} ->
        [string(key), " = ", value(item)]
      end),
      "}"
    ]

  defp string(value) when is_binary(value), do: JSON.encode!(value)
  defp failure(message, code), do: {:error, Error.new(:config, message, code: code)}
end
