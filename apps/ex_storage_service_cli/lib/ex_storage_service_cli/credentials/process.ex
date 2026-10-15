defmodule ExStorageServiceCli.Credentials.Process do
  @moduledoc "Bounded credential_process execution with literal argv, discarded stderr, and owned POSIX cleanup."
  alias ExStorageServiceCli.Credentials.Record
  alias ExStorageServiceCli.Error

  def run(command, opts \\ []) do
    env = Keyword.get(opts, :env, System.get_env())

    with {:ok, [program | args]} <- command(command),
         {:ok, executable} <- executable(program, env),
         {:ok, shell, kill} <- runtime(),
         {:ok, output} <- execute(shell, kill, executable, args, env, opts),
         {:ok, record} <- response(output, opts) do
      {:ok, record}
    end
  end

  @doc "POSIX-style quotes and backslashes, without expansion or shell interpretation."
  def command(value) when is_binary(value) do
    if String.valid?(value) and byte_size(value) <= 65_536 and not String.contains?(value, <<0>>),
      do: words(value, :plain, [], [], false),
      else: failure(:invalid_credential_process_command)
  end

  def command(_value), do: failure(:invalid_credential_process_command)

  defp words("", :plain, current, words, started) do
    words =
      if started, do: [current |> Enum.reverse() |> IO.iodata_to_binary() | words], else: words

    case Enum.reverse(words) do
      [program | _] = result when program != "" -> {:ok, result}
      _ -> failure(:invalid_credential_process_command)
    end
  end

  defp words("", _mode, _current, _words, _started),
    do: failure(:invalid_credential_process_command)

  defp words(<<char::utf8, rest::binary>>, :plain, current, words, started)
       when char in [32, 9, 10, 13] do
    words =
      if started, do: [current |> Enum.reverse() |> IO.iodata_to_binary() | words], else: words

    words(rest, :plain, [], words, false)
  end

  defp words(<<39, rest::binary>>, :plain, current, words, _started),
    do: words(rest, :single, current, words, true)

  defp words(<<34, rest::binary>>, :plain, current, words, _started),
    do: words(rest, :double, current, words, true)

  defp words(<<39, rest::binary>>, :single, current, words, _started),
    do: words(rest, :plain, current, words, true)

  defp words(<<34, rest::binary>>, :double, current, words, _started),
    do: words(rest, :plain, current, words, true)

  defp words(<<92, rest::binary>>, mode, current, words, started)
       when mode in [:plain, :double] do
    case rest do
      <<10, remaining::binary>> ->
        words(remaining, mode, current, words, started)

      <<char::utf8, remaining::binary>> ->
        escaped =
          if mode == :double and char not in [36, 96, 34, 92],
            do: <<92, char::utf8>>,
            else: <<char::utf8>>

        words(remaining, mode, [escaped | current], words, true)

      "" ->
        failure(:invalid_credential_process_command)
    end
  end

  defp words(<<char::utf8, rest::binary>>, mode, current, words, _started),
    do: words(rest, mode, [<<char::utf8>> | current], words, true)

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
      nil -> failure(:credential_process_not_found)
      path -> {:ok, path}
    end
  end

  defp runtime do
    # Linux ERTS spawn_executable creates a process group owned by the port child.
    case {:os.type(), System.find_executable("sh"), System.find_executable("kill")} do
      {{:unix, :linux}, shell, kill} when is_binary(shell) and is_binary(kill) ->
        {:ok, shell, kill}

      _ ->
        failure(:credential_process_platform_unavailable)
    end
  end

  defp execute(shell, kill, executable, args, env, opts) do
    timeout = opts[:timeout] || 5_000
    limit = min(opts[:max_output] || 65_536, 65_536)
    environment = Map.merge(Map.new(System.get_env(), fn {key, _value} -> {key, false} end), env)

    environment =
      Enum.map(environment, fn {key, value} ->
        {String.to_charlist(key), if(value == false, do: false, else: String.to_charlist(value))}
      end)

    # Only this constant wrapper is shell source. Every configured byte is argv.
    port =
      Port.open({:spawn_executable, shell}, [
        :binary,
        :exit_status,
        :use_stdio,
        :hide,
        args: ["-c", "exec \"$@\" 2>/dev/null", "ess-credential-process", executable | args],
        env: environment
      ])

    pid =
      case Port.info(port, :os_pid) do
        {:os_pid, pid} -> pid
        nil -> nil
      end

    owner = self()
    cleanup = if is_integer(pid), do: spawn(fn -> watch_owner(owner, pid, kill) end)
    result = collect(port, System.monotonic_time(:millisecond) + timeout, limit, 0, [])

    if match?({:error, _}, result) and is_integer(pid) and Port.info(port) != nil do
      System.cmd(kill, ["-KILL", "--", "-" <> Integer.to_string(pid)], stderr_to_stdout: true)
    end

    if cleanup, do: send(cleanup, :finished)
    close(port)
    drain(port)
    result
  rescue
    ErlangError -> failure(:credential_process_start_failed)
  end

  defp close(port) do
    # This owned port may exit after collection or process-group termination.
    Port.close(port)
  catch
    :error, :badarg -> :ok
  end

  defp watch_owner(owner, pid, kill) do
    monitor = Elixir.Process.monitor(owner)

    receive do
      :finished ->
        Elixir.Process.demonitor(monitor, [:flush])

      {:DOWN, ^monitor, :process, ^owner, _} ->
        System.cmd(kill, ["-KILL", "--", "-" <> Integer.to_string(pid)], stderr_to_stdout: true)
    end
  end

  defp collect(port, deadline, limit, size, chunks) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, chunk}} ->
        if size + byte_size(chunk) > limit,
          do: failure(:credential_process_output_limit),
          else: collect(port, deadline, limit, size + byte_size(chunk), [chunk | chunks])

      {^port, {:exit_status, 0}} ->
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

      {^port, {:exit_status, _status}} ->
        failure(:credential_process_failed)
    after
      remaining -> failure(:credential_process_timeout)
    end
  end

  defp drain(port) do
    receive do
      {^port, _message} -> drain(port)
    after
      0 -> :ok
    end
  end

  defp response(output, opts) do
    case JSON.decode(output) do
      {:ok, %{"Version" => 1} = data} ->
        with {:ok, record} <-
               Record.new(
                 %{
                   access_key_id: data["AccessKeyId"],
                   secret_access_key: data["SecretAccessKey"],
                   session_token: data["SessionToken"],
                   expires_at: data["Expiration"]
                 },
                 %{provider: :credential_process, profile: opts[:profile]}
               ),
             :ok <- expiry(record, Keyword.get(opts, :now, DateTime.utc_now())) do
          {:ok, record}
        end

      _ ->
        failure(:invalid_credential_process_response)
    end
  end

  @doc false
  def expiry(%Record{expires_at: nil}, _now), do: :ok

  def expiry(%Record{expires_at: expires}, now),
    do: if(DateTime.compare(expires, now) == :gt, do: :ok, else: failure(:expired_credentials))

  defp failure(code),
    do:
      {:error,
       Error.new(
         :credentials,
         "Credential process or temporary credentials are unavailable or invalid",
         code: code
       )}
end
