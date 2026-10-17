defmodule ExStorageServiceCli.Credentials.ProcessTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Credentials.Process, as: CredentialProcess
  alias ExStorageServiceCli.Credentials.Record
  alias ExStorageServiceCli.Error
  @moduletag :tmp_dir
  @response ~s({"Version":1,"AccessKeyId":"process-access","SecretAccessKey":"process-secret","SessionToken":"process-token","Expiration":"2030-01-01T00:00:00Z"})

  test "POSIX quotes and escapes produce literal arguments without expansions" do
    assert {:ok,
            ["helper", "two words", "", "a\\q", "a$b", "one two", "$(literal)", "`literal`", ";"]} =
             CredentialProcess.command(
               ~S|helper 'two words' "" "a\q" "a\$b" one\ two '$(literal)' '`literal`' ;|
             )

    for command <- ["", "'unterminated", <<"helper ", 92>>, <<0>>],
        do:
          assert(
            {:error, %Error{code: :invalid_credential_process_command}} =
              CredentialProcess.command(command)
          )
  end

  test "actual helper preserves metacharacters and succeeds with secret stderr diagnostics", %{
    tmp_dir: root
  } do
    args_file = Path.join(root, "arguments")
    marker = Path.join(root, "must-not-exist")

    path =
      helper(
        root,
        "printf '%s\\n' \"$@\" > '#{args_file}'\nprintf '%s' '#{@response}'\nprintf '%s' 'stderr-secret' >&2\n"
      )

    arguments = [
      "two words",
      "",
      "$(touch #{marker})",
      "literal; touch #{marker}",
      "`touch #{marker}`"
    ]

    command = Enum.map_join([path | arguments], " ", &quote_arg/1)

    assert {:ok,
            %Record{
              access_key_id: "process-access",
              session_token: "process-token",
              expires_at: ~U[2030-01-01 00:00:00Z]
            }} = CredentialProcess.run(command, now: ~U[2026-10-15 00:00:00Z])

    assert File.read!(args_file) == Enum.join(arguments, "\n") <> "\n"
    refute File.exists?(marker)
  end

  test "nonzero exits, malformed JSON, versions, tokens and expirations are typed and redacted",
       %{tmp_dir: root} do
    cases = [
      {"printf '%s' '#{@response}'; printf 'private diagnostic' >&2; exit 7",
       :credential_process_failed},
      {"printf '%s' 'process-secret'", :invalid_credential_process_response},
      {"printf '%s' '{\"Version\":1.0}'", :invalid_credential_process_response},
      {"printf '%s' '{\"Version\":2}'", :invalid_credential_process_response},
      {"printf '%s' '{\"Version\":1,\"AccessKeyId\":\"process-access\"}'", :invalid_credentials},
      {"printf '%s' '{\"Version\":1,\"AccessKeyId\":\"a\",\"SecretAccessKey\":\"s\",\"SessionToken\":false}'",
       :invalid_credentials},
      {"printf '%s' '{\"Version\":1,\"AccessKeyId\":\"a\",\"SecretAccessKey\":\"s\",\"Expiration\":\"tomorrow\"}'",
       :invalid_credentials},
      {"printf '%s' '{\"Version\":1,\"AccessKeyId\":\"a\",\"SecretAccessKey\":\"s\",\"Expiration\":\"2000-01-01T00:00:00Z\"}'",
       :expired_credentials}
    ]

    for {{script, code}, index} <- Enum.with_index(cases) do
      path = helper(root, script, "failure-#{index}")

      assert {:error, %Error{code: ^code} = error} =
               CredentialProcess.run(quote_arg(path), now: ~U[2026-10-15 00:00:00Z])

      for secret <- ["process-access", "process-secret", "private diagnostic"],
          do: refute(inspect(error) =~ secret)
    end
  end

  test "output is capped at 64KiB", %{tmp_dir: root} do
    path = helper(root, "printf '%s' '#{String.duplicate("a", 65_537)}'")

    assert {:error, %Error{code: :credential_process_output_limit}} =
             CredentialProcess.run(quote_arg(path))
  end

  test "timeout kills the owned helper and its sleeping descendant", %{tmp_dir: root} do
    own_file = Path.join(root, "helper-pid")
    child_file = Path.join(root, "child-pid")
    group_file = Path.join(root, "helper-group")
    child_group_file = Path.join(root, "child-group")
    sleep = System.find_executable("sleep")

    path =
      helper(
        root,
        "printf '%s' \"$$\" > '#{own_file}'\nps -o pgid= -p \"$$\" > '#{group_file}'\n" <>
          "'#{sleep}' 60 &\nprintf '%s' \"$!\" > '#{child_file}'\n" <>
          "ps -o pgid= -p \"$!\" > '#{child_group_file}'\nwait\n"
      )

    started = System.monotonic_time(:millisecond)

    assert {:error, %Error{code: :credential_process_timeout}} =
             CredentialProcess.run(quote_arg(path), timeout: 500)

    assert System.monotonic_time(:millisecond) - started < 3_000

    helper_pid = File.read!(own_file)
    assert String.trim(File.read!(group_file)) == helper_pid
    assert String.trim(File.read!(child_group_file)) == helper_pid
    {beam_group, 0} = System.cmd("ps", ["-o", "pgid=", "-p", System.pid()])
    refute String.trim(beam_group) == helper_pid

    for file <- [own_file, child_file] do
      pid = File.read!(file)
      assert stopped?(pid)
    end

    refute_receive {_port, {:data, _}}
  end

  test "output overflow kills the owned helper and its descendant", %{tmp_dir: root} do
    own_file = Path.join(root, "overflow-helper-pid")
    child_file = Path.join(root, "overflow-child-pid")
    sleep = System.find_executable("sleep")

    path =
      helper(
        root,
        "printf '%s' \"$$\" > '#{own_file}'\n'#{sleep}' 60 &\n" <>
          "printf '%s' \"$!\" > '#{child_file}'\nprintf '%s' '#{String.duplicate("x", 1024)}'\nwait\n"
      )

    assert {:error, %Error{code: :credential_process_output_limit}} =
             CredentialProcess.run(quote_arg(path), max_output: 32)

    for file <- [own_file, child_file], do: assert(stopped?(File.read!(file)))
    refute_receive {_port, {:data, _}}
  end

  test "credential owner death kills its helper group", %{tmp_dir: root} do
    own_file = Path.join(root, "owner-helper-pid")
    child_file = Path.join(root, "owner-child-pid")
    sleep = System.find_executable("sleep")

    path =
      helper(
        root,
        "printf '%s' \"$$\" > '#{own_file}'\n'#{sleep}' 60 &\nprintf '%s' \"$!\" > '#{child_file}'\nwait\n"
      )

    runner = spawn(fn -> CredentialProcess.run(quote_arg(path), timeout: 10_000) end)
    assert await_file(child_file)
    Elixir.Process.exit(runner, :kill)
    for file <- [own_file, child_file], do: assert(stopped?(File.read!(file)))
  end

  test "the port owner monitors the caller before launching its helper", %{tmp_dir: root} do
    own_file = Path.join(root, "startup-helper-pid")
    child_file = Path.join(root, "startup-child-pid")

    path =
      helper(
        root,
        "printf '%s' \"$$\" > '#{own_file}'\n'#{System.find_executable("sleep")}' 60 &\n" <>
          "printf '%s' \"$!\" > '#{child_file}'\nwait\n"
      )

    runner =
      spawn(fn ->
        receive do
          :start -> CredentialProcess.run(quote_arg(path), timeout: 10_000)
        end
      end)

    session = :trace.session_create(:credential_startup, self(), [])

    try do
      # Trace only this invocation; suppress call arguments, including its environment.
      :trace.function(
        session,
        {:erlang, :monitor, 2},
        [
          {[:process, runner], [], [{:message, false}, {:return_trace}]}
        ],
        []
      )

      :trace.function(
        session,
        {:erlang, :open_port, 2},
        [
          {:_, [], [{:message, false}, {:return_trace}]}
        ],
        []
      )

      :trace.process(session, runner, true, [:call, :set_on_spawn])
      send(runner, :start)
      {worker, port, monitored_before_launch} = await_launch()
      {:os_pid, pid} = Port.info(port, :os_pid)

      try do
        assert worker in monitored_before_launch
        refute worker == runner
        assert :erlang.suspend_process(worker)

        try do
          assert await_file(own_file)
          assert await_file(child_file)
          monitor = Elixir.Process.monitor(runner)
          Elixir.Process.exit(runner, :kill)
          assert_receive {:DOWN, ^monitor, :process, ^runner, :killed}
        after
          if Elixir.Process.alive?(worker), do: :erlang.resume_process(worker)
        end

        for file <- [own_file, child_file], do: assert(stopped?(File.read!(file)))
        refute_receive {_port, {:data, _}}
      after
        Elixir.Process.exit(runner, :kill)

        System.cmd(System.find_executable("kill"), ["-KILL", "--", "-#{pid}"],
          stderr_to_stdout: true
        )
      end
    after
      Elixir.Process.exit(runner, :kill)
      :trace.session_destroy(session)
    end
  end

  test "concurrent timeout cleanup preserves the error when owned ports exit before close" do
    command = quote_arg(System.find_executable("sleep")) <> " 60"

    1..256
    |> Task.async_stream(fn _ -> CredentialProcess.run(command, timeout: 5) end,
      max_concurrency: 16,
      timeout: 5_000
    )
    |> Enum.each(fn result ->
      assert {:ok, {:error, %Error{code: :credential_process_timeout}}} = result
    end)
  end

  test "genuine port launch errors remain typed and redacted" do
    command = quote_arg(System.find_executable("sleep")) <> " 60"

    assert {:error, %Error{code: :credential_process_start_failed} = error} =
             CredentialProcess.run(command, env: %{"INVALID\0KEY" => "launch-secret"})

    refute inspect(error) =~ "launch-secret"
    refute inspect(error) =~ "INVALID"
  end

  defp await_file(path, attempts \\ 100)
  defp await_file(_path, 0), do: false

  defp await_file(path, attempts) do
    if match?({:ok, pid} when byte_size(pid) > 0, File.read(path)),
      do: true,
      else:
        (
          Elixir.Process.sleep(10)
          await_file(path, attempts - 1)
        )
  end

  defp await_launch(monitored \\ []) do
    receive do
      {:trace, worker, :return_from, {:erlang, :monitor, 2}, _reference} ->
        await_launch([worker | monitored])

      {:trace, worker, :return_from, {:erlang, :open_port, 2}, port} ->
        {worker, port, monitored}
    after
      1_000 -> flunk("credential helper did not open its port")
    end
  end

  test "missing programs and invalid command text do not launch anything", %{tmp_dir: root} do
    assert {:error, %Error{code: :credential_process_not_found}} =
             CredentialProcess.run(quote_arg(Path.join(root, "missing")))
  end

  defp stopped?(pid, attempts \\ 20)
  defp stopped?(_pid, 0), do: false

  defp stopped?(pid, attempts) do
    {status, _} = System.cmd("ps", ["-o", "stat=", "-p", pid], stderr_to_stdout: true)

    if String.trim(status) == "" or String.starts_with?(String.trim(status), "Z"),
      do: true,
      else:
        (
          Elixir.Process.sleep(10)
          stopped?(pid, attempts - 1)
        )
  end

  defp helper(root, body, name \\ "credential helper") do
    path = Path.join(root, name)
    File.write!(path, "#!/bin/sh\n" <> body <> "\n")
    File.chmod!(path, 0o700)
    path
  end

  defp quote_arg(value), do: "'" <> String.replace(value, "'", "'\\''") <> "'"
end
