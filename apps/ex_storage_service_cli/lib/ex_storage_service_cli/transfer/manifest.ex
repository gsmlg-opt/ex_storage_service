defmodule ExStorageServiceCli.Transfer.Manifest do
  @moduledoc "Private disk-backed inventory with bounded runs and two-file external merges."
  alias ExStorageServiceCli.Error
  defstruct [:root, :path, count: 0, bytes: 0]
  @max_record 1_048_576

  def build(entries, opts \\ []) do
    root =
      Path.join(
        opts[:tmp_dir] || System.tmp_dir!(),
        ".ess-inventory-" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
      )

    with :ok <- io_result(File.mkdir(root)) do
      try do
        result =
          with :ok <- io_result(File.chmod(root, 0o700)) do
            Enum.reduce_while(
              entries,
              {:ok,
               %{
                 root: root,
                 levels: %{},
                 batch: [],
                 batch_bytes: 0,
                 count: 0,
                 bytes: 0,
                 key: opts[:key] || "name"
               }},
              fn
                {:error, _} = error, _ -> {:halt, error}
                {:ok, entry}, {:ok, state} -> append(entry, state)
                entry, {:ok, state} when is_map(entry) -> append(entry, state)
              end
            )
          end

        result =
          case result do
            {:ok, state} ->
              with {:ok, state} <- flush(state), {:ok, path} <- finish(state) do
                {:ok, %__MODULE__{root: root, path: path, count: state.count, bytes: state.bytes}}
              end

            error ->
              error
          end

        case result do
          {:ok, _} ->
            result

          error ->
            File.rm_rf(root)
            error
        end
      rescue
        _ ->
          File.rm_rf(root)
          failure(:inventory_io, "Cannot complete a private inventory")
      catch
        kind, reason ->
          File.rm_rf(root)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  def stream(%__MODULE__{path: path}) do
    path |> File.stream!([], :line) |> Stream.map(&JSON.decode!/1)
  end

  def cleanup(%__MODULE__{root: root}), do: File.rm_rf(root)

  @doc "Merge two sorted disk inventories with at most two records in memory."
  def pairs(%__MODULE__{path: first}, %__MODULE__{path: second}) do
    Stream.resource(
      fn ->
        {:ok, a} = File.open(first, [:read, :binary])

        case File.open(second, [:read, :binary]) do
          {:ok, b} ->
            try do
              {a, b, read(a), read(b)}
            rescue
              error ->
                File.close(a)
                File.close(b)
                reraise error, __STACKTRACE__
            end

          {:error, reason} ->
            File.close(a)
            raise File.Error, reason: reason, action: "open", path: second
        end
      end,
      fn
        {_, _, :eof, :eof} = state ->
          {:halt, state}

        {a, b, :eof, right} ->
          {[{nil, right}], {a, b, :eof, read(b)}}

        {a, b, left, :eof} ->
          {[{left, nil}], {a, b, read(a), :eof}}

        {a, b, left, right} ->
          cond do
            left["name"] == right["name"] -> {[{left, right}], {a, b, read(a), read(b)}}
            left["name"] < right["name"] -> {[{left, nil}], {a, b, read(a), right}}
            true -> {[{nil, right}], {a, b, left, read(b)}}
          end
      end,
      fn {a, b, _, _} ->
        File.close(a)
        File.close(b)
      end
    )
  end

  defp read(file) do
    case IO.binread(file, :line) do
      :eof -> :eof
      line when is_binary(line) -> JSON.decode!(line)
    end
  end

  defp append(entry, state) do
    line = JSON.encode!(entry) <> "\n"

    if byte_size(line) > @max_record or not is_binary(entry[state.key]) do
      {:halt, failure(:inventory_budget, "Inventory entry exceeds its record budget")}
    else
      state = %{
        state
        | batch: [{entry[state.key], line} | state.batch],
          batch_bytes: state.batch_bytes + byte_size(line),
          count: state.count + 1,
          bytes: state.bytes + (entry["size"] || 0)
      }

      if length(state.batch) >= 256 or state.batch_bytes >= @max_record do
        case flush(state) do
          {:ok, state} -> {:cont, {:ok, state}}
          error -> {:halt, error}
        end
      else
        {:cont, {:ok, state}}
      end
    end
  end

  defp flush(%{batch: []} = state), do: {:ok, state}

  defp flush(state) do
    records = Enum.sort_by(state.batch, &elem(&1, 0))

    if adjacent_duplicate?(records) do
      failure(:inventory_collision, "Inventory contains duplicate or conflicting mapped names")
    else
      path = name(state.root)

      with :ok <- write_private(path, Enum.map(records, &elem(&1, 1))),
           {:ok, levels} <- carry(state.levels, 0, path, state) do
        {:ok, %{state | levels: levels, batch: [], batch_bytes: 0}}
      end
    end
  end

  defp adjacent_duplicate?([{key, _}, {key, _} | _]), do: true
  defp adjacent_duplicate?([_ | rest]), do: adjacent_duplicate?(rest)
  defp adjacent_duplicate?(_), do: false

  defp carry(levels, level, path, state) do
    case Map.pop(levels, level) do
      {nil, levels} ->
        {:ok, Map.put(levels, level, path)}

      {old, levels} ->
        with {:ok, path} <- merge(old, path, state), do: carry(levels, level + 1, path, state)
    end
  end

  defp finish(state) do
    Enum.reduce_while(Map.values(state.levels), {:ok, nil}, fn
      path, {:ok, nil} ->
        {:cont, {:ok, path}}

      path, {:ok, old} ->
        case merge(old, path, state) do
          {:ok, path} -> {:cont, {:ok, path}}
          error -> {:halt, error}
        end
    end)
    |> case do
      {:ok, nil} ->
        path = name(state.root)
        with :ok <- write_private(path, ""), do: {:ok, path}

      result ->
        result
    end
  end

  defp merge(first, second, state) do
    path = name(state.root)

    with {:ok, output} <- File.open(path, [:write, :binary, :exclusive]) do
      try do
        with :ok <- io_result(File.chmod(path, 0o600)) do
          result =
            File.open(first, [:read, :binary], fn a ->
              File.open(second, [:read, :binary], fn b ->
                merge_lines(a, b, IO.binread(a, :line), IO.binread(b, :line), output, state.key)
              end)
            end)

          case result do
            {:ok, {:ok, :ok}} ->
              File.rm(first)
              File.rm(second)
              {:ok, path}

            {:ok, {:ok, {:error, _} = error}} ->
              error

            _ ->
              failure(:inventory_io, "Cannot merge inventory runs")
          end
        end
      after
        File.close(output)
      end
    else
      _ -> failure(:inventory_io, "Cannot open private inventory run")
    end
  end

  defp merge_lines(_, _, :eof, :eof, _, _), do: :ok

  defp merge_lines(a, b, :eof, line, output, key) when is_binary(line) do
    with :ok <- io_result(IO.binwrite(output, line)),
         do: merge_lines(a, b, :eof, IO.binread(b, :line), output, key)
  end

  defp merge_lines(a, b, line, :eof, output, key) when is_binary(line) do
    with :ok <- io_result(IO.binwrite(output, line)),
         do: merge_lines(a, b, IO.binread(a, :line), :eof, output, key)
  end

  defp merge_lines(a, b, left, right, output, key) when is_binary(left) and is_binary(right) do
    first = JSON.decode!(left)[key]
    second = JSON.decode!(right)[key]

    cond do
      first == second ->
        failure(:inventory_collision, "Inventory contains duplicate or conflicting mapped names")

      first < second ->
        with :ok <- io_result(IO.binwrite(output, left)),
             do: merge_lines(a, b, IO.binread(a, :line), right, output, key)

      true ->
        with :ok <- io_result(IO.binwrite(output, right)),
             do: merge_lines(a, b, left, IO.binread(b, :line), output, key)
    end
  end

  defp merge_lines(_, _, _, _, _, _), do: failure(:inventory_io, "Cannot read inventory run")

  defp write_private(path, data) do
    with {:ok, file} <- File.open(path, [:write, :binary, :exclusive]) do
      try do
        with :ok <- io_result(File.chmod(path, 0o600)), do: io_result(IO.binwrite(file, data))
      after
        File.close(file)
      end
    end
  end

  defp name(root),
    do: Path.join(root, Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))

  defp io_result(:ok), do: :ok
  defp io_result({:error, _}), do: failure(:inventory_io, "Cannot write private inventory")
  defp failure(code, message), do: {:error, Error.new(:client, message, code: code)}
end
