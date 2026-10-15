defmodule ExStorageServiceCli.CLI.Signals.Native do
  @moduledoc "Invocation-owned POSIX SIGINT/SIGTERM notifications through a nonblocking self-pipe."
  alias ExStorageServiceCli.Error
  @loaded {__MODULE__, :loaded}

  @doc "Starts the resource owned by the calling process; select messages go to that owner."
  def start(owner) do
    with :ok <- load(), do: result(nif_start(owner))
  end

  @doc "Drains/rearms {:select, resource, :ess_sigint, :ready_input} for either signal; returns [:sigint, :sigterm] subsets."
  def drain(resource), do: result(nif_drain(resource))
  @doc "Restores the previous SIGINT disposition and closes owned descriptors, idempotently."
  def close(resource), do: result(nif_close(resource))

  @doc "Opens an invocation-owned bounded directory iterator without following the final symlink by default."
  def open_directory(path, follow_symlinks \\ false) do
    with :ok <- load(), do: nif_open_directory(path, follow_symlinks)
  end

  @doc "Reads at most 1024 binary names, excluding dot entries."
  def read_directory(resource, maximum \\ 128), do: nif_read_directory(resource, maximum)
  @doc "Closes a directory iterator idempotently."
  def close_directory(resource), do: nif_close_directory(resource)

  @doc "Suppresses terminal input echo until explicit restore or owner death."
  def suppress_stdin_echo do
    with :ok <- load(), do: nif_suppress_stdin_echo()
  end

  @doc "Restores the invocation's original terminal settings, idempotently."
  def restore_terminal(resource), do: nif_restore_terminal(resource)

  @doc "Starts a literal-argv Linux pager helper while preserving the controlling session."
  def pager_start(arguments, environment) do
    with :ok <- load(), do: nif_pager_start(arguments, environment)
  end

  @doc "Returns :running or {:exit, status} after terminal restoration and child reaping."
  def pager_poll(resource), do: nif_pager_poll(resource)

  @doc "Closes the pager lifetime pipe; poll until completion before removing its files."
  def pager_close(resource), do: nif_pager_close(resource)

  def load do
    if :persistent_term.get(@loaded, false) do
      :ok
    else
      with {:ok, priv} <- priv(),
           {:ok, metadata} <- asset(Path.join(priv, "native/ess_signals.json")),
           {:ok, metadata} <- JSON.decode(metadata),
           :ok <- platform(metadata),
           {:ok, binary} <- asset(Path.join(priv, "native/ess_signals.so")),
           true <-
             Base.encode16(:crypto.hash(:sha256, binary), case: :lower) == metadata["sha256"] do
        load_binary(binary)
      else
        {:error, %Error{}} = error -> error
        _ -> failure(:native_signal_unavailable)
      end
    end
  end

  @doc false
  def load_path(path) do
    case :erlang.load_nif(String.to_charlist(path), 0) do
      :ok ->
        :persistent_term.put(@loaded, true)
        :ok

      {:error, {:reload, _}} ->
        :persistent_term.put(@loaded, true)
        :ok

      _ ->
        failure(:native_signal_unavailable)
    end
  end

  defp load_binary(binary) do
    random = :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
    directory = Path.join(System.tmp_dir!(), "ess-native-" <> random)

    with :ok <- File.mkdir(directory) do
      try do
        with :ok <- File.chmod(directory, 0o700),
             :ok <-
               File.write(Path.join(directory, "ess_signals.so"), binary, [:binary, :exclusive]),
             :ok <- File.chmod(Path.join(directory, "ess_signals.so"), 0o600) do
          load_path(Path.join(directory, "ess_signals"))
        else
          _ -> failure(:native_signal_unavailable)
        end
      after
        File.rm(Path.join(directory, "ess_signals.so"))
        File.rmdir(directory)
      end
    else
      _ -> failure(:native_signal_unavailable)
    end
  end

  defp priv do
    case :code.priv_dir(:ex_storage_service_cli) do
      directory when is_list(directory) -> {:ok, List.to_string(directory)}
      _ -> failure(:native_signal_unavailable)
    end
  end

  defp asset(path) do
    case :erl_prim_loader.get_file(String.to_charlist(path)) do
      {:ok, bytes, _path} -> {:ok, bytes}
      _ -> failure(:native_signal_unavailable)
    end
  end

  defp platform(metadata) do
    architecture =
      :erlang.system_info(:system_architecture) |> to_string() |> String.split("-") |> hd()

    valid =
      metadata["os"] == :os.type() |> elem(1) |> Atom.to_string() and
        metadata["architecture"] == architecture and
        metadata["otp_release"] == to_string(:erlang.system_info(:otp_release))

    if valid, do: :ok, else: failure(:native_signal_platform_mismatch)
  end

  @doc false
  def handler_restored?, do: nif_restored()
  @doc false
  def nif_start(_owner), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_drain(_resource), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_close(_resource), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_restored, do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_open_directory(_path, _follow), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_read_directory(_resource, _maximum), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_close_directory(_resource), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_suppress_stdin_echo, do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_restore_terminal(_resource), do: :erlang.nif_error(:native_signal_unavailable)

  @doc false
  def nif_pager_start(_arguments, _environment), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_pager_poll(_resource), do: :erlang.nif_error(:native_signal_unavailable)
  @doc false
  def nif_pager_close(_resource), do: :erlang.nif_error(:native_signal_unavailable)

  defp result({:error, code}) when is_atom(code), do: failure(code)
  defp result(result), do: result

  defp failure(code),
    do:
      {:error, Error.new(:client, "Native invocation signal handling is unavailable", code: code)}
end
