defmodule ExStorageService.Metadata.ListIndex.Manifest do
  @moduledoc false

  # Only the explicit offline exporter materializes Concord's source snapshot.
  # Sorting, merging, verification and replay use bounded file buffers.
  @magic "ESSIDX1\n"
  @frame_limit 16_777_216
  @run_bytes 1_048_576
  @fan_in 16

  def create(path, entries) do
    stage =
      path <> ".partial-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

    with {:error, :enoent} <- File.lstat(path),
         :ok <- File.mkdir(stage),
         :ok <- File.chmod(stage, 0o700) do
      try do
        with {:ok, runs} <- runs(entries, stage),
             {:ok, final} <- merge_levels(runs, stage, 0),
             :ok <- File.rename(final, path),
             :ok <- sync_directory(Path.dirname(path)),
             {:ok, digest, size} <- digest(path) do
          {:ok, %{path: Path.expand(path), digest: digest, bytes: size}}
        end
      after
        File.rm_rf(stage)
      end
    else
      {:ok, _} -> {:error, :manifest_exists}
      error -> error
    end
  end

  def stream(path) do
    Stream.resource(
      fn ->
        case open(path) do
          {:ok, file} -> file
          {:error, reason} -> throw({:manifest_error, reason})
        end
      end,
      fn file ->
        case read_frame(file) do
          :eof -> {:halt, file}
          {:ok, entry} -> {[entry], file}
          {:error, reason} -> throw({:manifest_error, reason})
        end
      end,
      &File.close/1
    )
  end

  def batch(path, offset, count) when is_integer(count) and count in 1..100 do
    with {:ok, file} <- open(path) do
      try do
        with {:ok, _} <- :file.position(file, max(offset, byte_size(@magic))) do
          read_batch(file, count, [], 0)
        end
      after
        File.close(file)
      end
    end
  end

  def digest(path) do
    with {:ok, %{type: :regular}} <- File.lstat(path),
         {:ok, file} <- File.open(path, [:read, :raw, :binary]) do
      try do
        hash(file, :crypto.hash_init(:sha256), 0)
      after
        File.close(file)
      end
    else
      {:ok, _} -> {:error, :invalid_manifest_file}
      error -> error
    end
  end

  defp hash(file, state, size) do
    case IO.binread(file, 262_144) do
      :eof -> {:ok, Base.encode16(:crypto.hash_final(state), case: :lower), size}
      {:error, _} = error -> error
      bytes -> hash(file, :crypto.hash_update(state, bytes), size + byte_size(bytes))
    end
  end

  defp runs(entries, stage) do
    result =
      Enum.reduce_while(entries, {:ok, [], [], 0, 0}, fn entry, {:ok, files, items, bytes, n} ->
        size = :erlang.external_size(entry)

        cond do
          size > @frame_limit ->
            {:halt, {:error, :manifest_record_too_large}}

          bytes + size > @run_bytes and items != [] ->
            path = Path.join(stage, "run-#{n}")

            case write(path, Enum.sort(items)) do
              :ok -> {:cont, {:ok, [path | files], [entry], size, n + 1}}
              error -> {:halt, error}
            end

          true ->
            {:cont, {:ok, files, [entry | items], bytes + size, n}}
        end
      end)

    with {:ok, files, items, _bytes, n} <- result do
      path = Path.join(stage, "run-#{n}")
      with :ok <- write(path, Enum.sort(items)), do: {:ok, Enum.reverse([path | files])}
    end
  catch
    {:manifest_error, reason} -> {:error, reason}
  end

  defp merge_levels([path], _stage, _level), do: {:ok, path}

  defp merge_levels(paths, stage, level) do
    result =
      paths
      |> Enum.chunk_every(@fan_in)
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, []}, fn {group, n}, {:ok, merged} ->
        path = Path.join(stage, "merge-#{level}-#{n}")

        case merge(group, path) do
          :ok ->
            Enum.each(group, &File.rm/1)
            {:cont, {:ok, [path | merged]}}

          error ->
            {:halt, error}
        end
      end)

    with {:ok, paths} <- result, do: merge_levels(Enum.reverse(paths), stage, level + 1)
  end

  defp merge(paths, destination) do
    opened = Enum.map(paths, &open/1)
    files = for {:ok, file} <- opened, do: file

    try do
      with true <- length(files) == length(paths),
           {:ok, output} <- File.open(destination, [:write, :exclusive, :raw, :binary]) do
        try do
          with :ok <- File.chmod(destination, 0o600),
               :ok <- IO.binwrite(output, @magic),
               :ok <- merge_next(output, Enum.map(files, &{&1, read_frame(&1)})),
               :ok <- :file.sync(output),
               do: :ok
        after
          File.close(output)
        end
      else
        false -> {:error, :manifest_open_failed}
        error -> error
      end
    after
      Enum.each(files, &File.close/1)
    end
  end

  defp merge_next(output, heads) do
    case Enum.find(heads, fn {_file, result} -> match?({:error, _}, result) end) do
      {_file, error} ->
        error

      nil ->
        live = Enum.reject(heads, fn {_file, value} -> value == :eof end)

        if live == [] do
          :ok
        else
          {file, {:ok, entry}} = Enum.min_by(live, fn {_file, {:ok, entry}} -> entry end)

          with :ok <- frame(output, entry) do
            merge_next(output, List.keyreplace(heads, file, 0, {file, read_frame(file)}))
          end
        end
    end
  end

  defp write(path, entries) do
    with {:ok, file} <- File.open(path, [:write, :exclusive, :raw, :binary]) do
      try do
        with :ok <- File.chmod(path, 0o600),
             :ok <- IO.binwrite(file, @magic),
             :ok <-
               Enum.reduce_while(entries, :ok, fn entry, :ok ->
                 case frame(file, entry) do
                   :ok -> {:cont, :ok}
                   error -> {:halt, error}
                 end
               end),
             do: :file.sync(file)
      after
        File.close(file)
      end
    end
  end

  defp frame(file, entry) do
    bytes = :erlang.term_to_binary(entry)

    if byte_size(bytes) <= @frame_limit do
      IO.binwrite(file, [
        <<byte_size(bytes)::unsigned-big-32>>,
        :crypto.hash(:sha256, bytes),
        bytes
      ])
    else
      {:error, :manifest_record_too_large}
    end
  end

  defp open(path) do
    with {:ok, %{type: :regular}} <- File.lstat(path),
         {:ok, file} <- File.open(path, [:read, :raw, :binary]) do
      if IO.binread(file, byte_size(@magic)) == @magic do
        {:ok, file}
      else
        File.close(file)
        {:error, :invalid_manifest_header}
      end
    else
      {:ok, _} -> {:error, :invalid_manifest_file}
      error -> error
    end
  end

  defp read_frame(file) do
    case IO.binread(file, 4) do
      :eof ->
        :eof

      <<size::unsigned-big-32>> when size > 0 and size <= @frame_limit ->
        with checksum when is_binary(checksum) and byte_size(checksum) == 32 <-
               IO.binread(file, 32),
             bytes when is_binary(bytes) and byte_size(bytes) == size <- IO.binread(file, size),
             true <- :crypto.hash(:sha256, bytes) == checksum do
          {:ok, :erlang.binary_to_term(bytes, [:safe])}
        else
          _ -> {:error, :manifest_checksum_mismatch}
        end

      _ ->
        {:error, :invalid_manifest_frame}
    end
  rescue
    ArgumentError -> {:error, :invalid_manifest_term}
  end

  defp read_batch(file, remaining, entries, bytes) when remaining == 0 or bytes >= @run_bytes do
    with {:ok, offset} <- :file.position(file, :cur),
         do: {:ok, %{entries: Enum.reverse(entries), offset: offset, eof: false}}
  end

  defp read_batch(file, remaining, entries, bytes) do
    case read_frame(file) do
      :eof ->
        with {:ok, offset} <- :file.position(file, :cur),
             do: {:ok, %{entries: Enum.reverse(entries), offset: offset, eof: true}}

      {:ok, entry} ->
        read_batch(file, remaining - 1, [entry | entries], bytes + :erlang.external_size(entry))

      error ->
        error
    end
  end

  defp sync_directory(path) do
    case :file.open(String.to_charlist(path), [:read, :raw, :directory]) do
      {:ok, file} ->
        try do
          :file.sync(file)
        after
          :file.close(file)
        end

      error ->
        error
    end
  end
end
