defmodule ExStorageServiceCli.Transfer.Spool do
  @moduledoc "Private disk-backed stdin capture with bounded cancellable reads and deterministic cleanup."
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.Transfer.{Coordinator, Manifest, Source}

  def with_source(context, opts, callback) do
    with {:ok, manifest} <- Manifest.build([], opts) do
      try do
        path = Path.join(manifest.root, "stdin")

        with {:ok, file} <- File.open(path, [:write, :binary, :exclusive]) do
          try do
            with :ok <- File.chmod(path, 0o600),
                 {:ok, length} <- capture(file, context, opts),
                 :ok <- expected(length, opts[:expected_size]),
                 :ok <- :file.sync(file),
                 :ok <- File.close(file),
                 {:ok, source} <- Source.file(path, coordinator: context.coordinator) do
              callback.(source)
            end
          after
            File.close(file)
          end
        else
          _ -> failure(:stdin_spool, "Cannot open private stdin staging file")
        end
      after
        Manifest.cleanup(manifest)
      end
    end
  end

  defp capture(file, context, opts) do
    case opts[:task_supervisor] do
      nil ->
        {:ok, supervisor} = Task.Supervisor.start_link()

        try do
          read(file, context.coordinator, supervisor, opts[:stdin] || :stdio, 0)
        after
          Supervisor.stop(supervisor)
        end

      supervisor ->
        read(file, context.coordinator, supervisor, opts[:stdin] || :stdio, 0)
    end
  end

  defp read(file, coordinator, supervisor, input, total) do
    with :ok <- Coordinator.check(coordinator) do
      task = Task.Supervisor.async_nolink(supervisor, fn -> IO.binread(input, 65_536) end)

      result =
        try do
          await(task, coordinator)
        after
          Task.shutdown(task, :brutal_kill)
        end

      case result do
        {:ok, :eof} ->
          {:ok, total}

        {:ok, bytes} when is_binary(bytes) and byte_size(bytes) <= 65_536 ->
          with :ok <- Coordinator.check(coordinator),
               :ok <- write(file, bytes),
               do: read(file, coordinator, supervisor, input, total + byte_size(bytes))

        {:error, %Error{}} = error ->
          error

        _ ->
          failure(:stdin_read, "Cannot read stdin")
      end
    end
  end

  defp await(task, coordinator) do
    with :ok <- Coordinator.check(coordinator) do
      case Task.yield(task, 100) do
        nil -> await(task, coordinator)
        {:ok, bytes} -> {:ok, bytes}
        _ -> failure(:stdin_read, "Stdin reader stopped")
      end
    end
  end

  defp write(file, bytes) do
    case IO.binwrite(file, bytes) do
      :ok -> :ok
      _ -> failure(:stdin_spool, "Cannot write stdin staging file")
    end
  end

  defp expected(_, nil), do: :ok

  defp expected(actual, expected) when is_integer(expected) do
    if actual == expected,
      do: :ok,
      else: failure(:stdin_size, "Stdin length differs from expected size")
  end

  defp expected(actual, value) when is_binary(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> expected(actual, number)
      _ -> failure(:stdin_size, "Expected size must be a nonnegative integer")
    end
  end

  defp expected(_, _), do: failure(:stdin_size, "Expected size must be a nonnegative integer")
  defp failure(code, message), do: {:error, Error.new(:transfer, message, code: code)}
end
