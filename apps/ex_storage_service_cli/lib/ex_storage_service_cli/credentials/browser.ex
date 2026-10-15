defmodule ExStorageServiceCli.Credentials.Browser do
  @moduledoc false

  def notify(verification, opts) do
    if is_function(opts[:on_verification], 1) do
      opts[:on_verification].(verification)
    else
      code =
        if verification.user_code, do: "\nThen enter code: " <> verification.user_code, else: ""

      IO.puts(:stderr, "Open this URL to sign in:\n" <> verification.verification_uri <> code)
    end

    if opts[:no_browser] != true do
      url = verification.verification_uri_complete || verification.verification_uri

      if safe_url?(url) do
        open(url, opts)
      end
    end

    :ok
  end

  defp open(url, opts) do
    if is_function(opts[:open_browser], 1), do: opts[:open_browser].(url), else: launch(url, opts)
  rescue
    _ -> :failed
  catch
    _, _ -> :failed
  end

  defp safe_url?(url) when is_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: "https", host: host, port: port, userinfo: nil, fragment: nil}}
      when is_binary(host) and host != "" and is_integer(port) and port > 0 and port <= 65_535 ->
        not String.contains?(url, <<0>>)

      _ ->
        false
    end
  end

  defp safe_url?(_url), do: false

  def launch(url, opts \\ []) do
    # Linux ERTS creates an owned process group for spawn_executable. Other
    # platforms keep the displayed URL until their cancellation is validated.
    case {:os.type(), opts[:browser_executable] || System.find_executable("xdg-open"),
          System.find_executable("kill")} do
      {{:unix, :linux}, executable, kill} when is_binary(executable) and is_binary(kill) ->
        if safe_url?(url), do: execute(executable, kill, url, opts), else: :unavailable

      _ ->
        :unavailable
    end
  rescue
    _ -> :unavailable
  end

  defp execute(executable, kill, url, opts) do
    timeout = min(max(opts[:browser_timeout] || 5000, 1), 5000)

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        :use_stdio,
        :stderr_to_stdout,
        :hide,
        args: [url],
        env: [{~c"BROWSER", false}]
      ])

    {:os_pid, pid} = Port.info(port, :os_pid)
    owner = self()
    cleanup = spawn(fn -> watch_owner(owner, pid, kill) end)
    result = collect(port, System.monotonic_time(:millisecond) + timeout, 0)

    if result != :opened and Port.info(port) != nil, do: stop(kill, pid)
    send(cleanup, :finished)
    if Port.info(port) != nil, do: Port.close(port)
    drain(port)
    result
  end

  defp watch_owner(owner, pid, kill) do
    monitor = Process.monitor(owner)

    receive do
      :finished -> Process.demonitor(monitor, [:flush])
      {:DOWN, ^monitor, :process, ^owner, _} -> stop(kill, pid)
    end
  end

  defp stop(kill, pid),
    do: System.cmd(kill, ["-KILL", "--", "-" <> Integer.to_string(pid)], stderr_to_stdout: true)

  defp collect(port, deadline, size) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {^port, {:data, chunk}} ->
        if size + byte_size(chunk) > 65_536,
          do: :failed,
          else: collect(port, deadline, size + byte_size(chunk))

      {^port, {:exit_status, 0}} ->
        :opened

      {^port, {:exit_status, _status}} ->
        :failed
    after
      remaining -> :timeout
    end
  end

  defp drain(port) do
    receive do
      {^port, _message} -> drain(port)
    after
      0 -> :ok
    end
  end
end
