defmodule ExStorageServiceCli.CLI.OutputSession do
  @moduledoc "Owned structured-output session with bounded private pager spool and literal process arguments."
  use GenServer
  alias ExStorageServiceCli.{Error, Credentials.Process, Transfer.Coordinator, CLI.Signals.Native}

  def open(options, opts \\ []) do
    GenServer.start(__MODULE__, {options, Keyword.put_new(opts, :owner, self())})
  end

  def write(nil, data), do: output(data, nil)
  def write(server, data), do: GenServer.call(server, {:write, data}, :infinity)
  def finish(server), do: GenServer.call(server, :finish, :infinity)

  def abort(server) do
    if Elixir.Process.alive?(server), do: GenServer.call(server, :abort, :infinity), else: :ok
  catch
    :exit, _ -> :ok
  end

  def prepare_options(options, opts \\ []) do
    color = options[:color] || "auto"

    cond do
      color not in ~w(auto on off) -> failure(:invalid_color)
      options[:raw_stdout] -> {:ok, Map.put(options, :color, "off")}
      color == "auto" -> {:ok, Map.put(options, :color, if(tty?(opts), do: "on", else: "off"))}
      true -> {:ok, options}
    end
  end

  def pager(options, opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())

    cond do
      options[:no_cli_pager] || options[:raw_stdout] || not tty?(opts) -> nil
      Map.has_key?(env, "AWS_PAGER") -> env["AWS_PAGER"]
      is_binary(options[:cli_pager]) -> options[:cli_pager]
      Map.has_key?(env, "PAGER") -> env["PAGER"]
      :os.type() == {:win32, :nt} -> "more"
      true -> "less"
    end
  end

  @impl true
  def init({options, opts}) do
    selected = pager(options, opts)

    state = %{
      owner: Elixir.Process.monitor(opts[:owner]),
      opts: opts,
      options: options,
      pager: selected,
      file: nil,
      directory: nil,
      port: nil,
      bytes: 0,
      from: nil,
      failure: nil,
      deadline: nil,
      aborted: false
    }

    if selected in [nil, ""], do: {:ok, state}, else: spool(state)
  end

  defp spool(state) do
    directory =
      Path.join(
        state.opts[:tmp_dir] || System.tmp_dir!(),
        "ess-output-" <> Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
      )

    with :ok <- File.mkdir(directory),
         :ok <- File.chmod(directory, 0o700),
         path = Path.join(directory, "output"),
         {:ok, file} <- File.open(path, [:write, :binary, :exclusive, :raw]),
         :ok <- File.chmod(path, 0o600) do
      {:ok, %{state | directory: directory, file: file}}
    else
      _ ->
        File.rm(Path.join(directory, "output"))
        File.rmdir(directory)
        {:stop, error(:pager_spool_unavailable)}
    end
  end

  @impl true
  def handle_call({:write, _data}, _from, %{port: port} = state) when not is_nil(port),
    do: {:reply, failure(:output_session_closed), state}

  def handle_call({:write, data}, _from, state) do
    size = IO.iodata_length(data)
    limit = state.opts[:max_spool_bytes] || 67_108_864

    result =
      cond do
        state.failure -> {:error, state.failure}
        state.file != nil and state.bytes + size > limit -> failure(:pager_spool_limit)
        true -> output(data, state.file || state.opts[:write])
      end

    state =
      case result do
        :ok -> %{state | bytes: state.bytes + size}
        {:error, error} -> %{state | failure: error}
      end

    {:reply, result, state}
  end

  def handle_call(:finish, _from, %{port: port} = state) when not is_nil(port),
    do: {:reply, failure(:output_session_closed), state}

  def handle_call(:finish, _from, %{failure: %Error{} = error} = state),
    do: {:stop, :normal, {:error, error}, cleanup(state)}

  def handle_call(:finish, _from, %{file: nil} = state), do: {:stop, :normal, :ok, cleanup(state)}
  def handle_call(:finish, _from, %{bytes: 0} = state), do: {:stop, :normal, :ok, cleanup(state)}

  def handle_call(:finish, from, state) do
    File.close(state.file)
    state = %{state | file: nil}

    case start_pager(state) do
      {:ok, port} ->
        Elixir.Process.send_after(self(), :check, 25)

        {:noreply,
         %{
           state
           | port: port,
             from: from,
             deadline: now() + (state.opts[:pager_timeout] || 300_000)
         }}

      {:error, _} = error ->
        {:stop, :normal, error, cleanup(state)}
    end
  end

  def handle_call(:abort, _from, %{port: nil} = state), do: {:stop, :normal, :ok, cleanup(state)}

  def handle_call(:abort, from, state) do
    if state.from, do: GenServer.reply(state.from, failure(:interrupted))
    cancel(state.port)
    {:noreply, %{state | from: from, aborted: true, failure: error(:interrupted)}}
  end

  @impl true
  def handle_info(:check, state) do
    reason =
      cond do
        state.failure ->
          state.failure

        now() >= state.deadline ->
          error(:pager_timeout)

        true ->
          case check_coordinator(state.options[:coordinator] || state.opts[:coordinator]) do
            :ok -> nil
            {:error, error} -> error
          end
      end

    if reason, do: cancel(state.port)

    case Native.pager_poll(state.port) do
      :running ->
        Elixir.Process.send_after(self(), :check, 25)
        {:noreply, %{state | failure: reason}}

      {:exit, status} ->
        result =
          cond do
            state.aborted -> :ok
            reason -> {:error, reason}
            status == 0 -> :ok
            status in [130, 141] -> failure(:broken_pipe)
            true -> failure(:pager_failed)
          end

        from = state.from
        state = cleanup(%{state | port: nil, from: nil})
        if from, do: GenServer.reply(from, result)
        {:stop, :normal, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{owner: ref, port: nil} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, ref, :process, _, _}, %{owner: ref} = state) do
    cancel(state.port)
    {:noreply, %{state | failure: error(:interrupted), from: nil}}
  end

  def handle_info({:ess_cancel, _}, state) do
    if state.port, do: cancel(state.port)
    {:noreply, %{state | failure: error(:interrupted)}}
  end

  @impl true
  def terminate(_, state) do
    cleanup(state)
    :ok
  end

  defp cleanup(state) do
    if state.file, do: File.close(state.file)
    if state.port, do: Native.pager_close(state.port)

    if state.directory do
      Enum.each(["output", "ess_pager"], &File.rm(Path.join(state.directory, &1)))
      File.rmdir(state.directory)
    end

    %{state | file: nil, port: nil, directory: nil}
  end

  defp start_pager(state) do
    env = Keyword.get(state.opts, :env, System.get_env()) |> Map.put_new("LESS", "FRX")

    with {:ok, [program | arguments]} <- Process.command(state.pager),
         {:ok, executable} <- executable(program, env),
         {:ok, helper} <- helper(state.directory) do
      environment = Enum.map(env, fn {key, value} -> key <> "=" <> value end)

      Native.pager_start(
        [helper, Path.join(state.directory, "output"), executable | arguments],
        environment
      )
    else
      _ -> failure(:pager_unavailable)
    end
  rescue
    _ -> failure(:pager_unavailable)
  end

  defp executable(program, env) do
    candidates =
      if String.contains?(program, "/"),
        do: [Path.expand(program)],
        else:
          Enum.map(
            String.split(env["PATH"] || System.get_env("PATH") || "", ":"),
            &Path.expand(Path.join(&1, program))
          )

    case Enum.find(candidates, fn path ->
           case File.stat(path) do
             {:ok, %{type: :regular, mode: mode}} -> Bitwise.band(mode, 0o111) != 0
             _ -> false
           end
         end) do
      nil -> failure(:pager_unavailable)
      path -> {:ok, path}
    end
  end

  defp helper(directory) do
    with {:unix, :linux} <- :os.type(),
         priv when is_list(priv) <- :code.priv_dir(:ex_storage_service_cli),
         {:ok, metadata, _} <-
           :erl_prim_loader.get_file(
             to_charlist(Path.join(to_string(priv), "native/ess_pager.json"))
           ),
         {:ok, metadata} <- JSON.decode(metadata),
         true <- metadata["os"] == "linux" and metadata["architecture"] == architecture(),
         {:ok, binary, _} <-
           :erl_prim_loader.get_file(to_charlist(Path.join(to_string(priv), "native/ess_pager"))),
         true <- Base.encode16(:crypto.hash(:sha256, binary), case: :lower) == metadata["sha256"],
         path = Path.join(directory, "ess_pager"),
         :ok <- File.write(path, binary, [:binary, :exclusive]),
         :ok <- File.chmod(path, 0o700),
         do: {:ok, path},
         else: (_ -> failure(:pager_native_unavailable))
  end

  defp output(data, writer) do
    result =
      cond do
        is_function(writer, 1) -> writer.(data)
        writer != nil -> :file.write(writer, data)
        true -> IO.write(:stdio, data)
      end

    case result do
      :ok -> :ok
      {:error, reason} when reason in [:epipe, :terminated, :closed] -> failure(:broken_pipe)
      _ -> failure(:output_write_failed)
    end
  rescue
    _ -> failure(:broken_pipe)
  catch
    :exit, _ -> failure(:broken_pipe)
  end

  defp cancel(port) do
    if port, do: Native.pager_close(port)
    :ok
  rescue
    _ -> :ok
  end

  defp check_coordinator(coordinator) do
    Coordinator.check(coordinator)
  catch
    :exit, _ -> failure(:interrupted)
  end

  defp tty?(opts),
    do: Keyword.get_lazy(opts, :tty, fn -> match?({:ok, _}, :io.columns(:standard_io)) end)

  defp architecture,
    do: :erlang.system_info(:system_architecture) |> to_string() |> String.split("-") |> hd()

  defp now, do: System.monotonic_time(:millisecond)

  defp error(code),
    do:
      Error.new(
        if(code == :interrupted, do: :interrupted, else: :client),
        "CLI output session failed",
        code: code
      )

  defp failure(code), do: {:error, error(code)}
end
