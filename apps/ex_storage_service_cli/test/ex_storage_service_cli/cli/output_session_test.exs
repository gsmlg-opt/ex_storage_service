defmodule ExStorageServiceCli.CLI.OutputSessionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, CLI.OutputSession, Transfer.Coordinator}
  @moduletag :tmp_dir

  test "pager precedence retains explicit empty environment values and non-TTY bypass" do
    assert OutputSession.pager(%{cli_pager: "profile"}, tty: true, env: %{"AWS_PAGER" => ""}) ==
             ""

    assert OutputSession.pager(%{cli_pager: "profile"}, tty: true, env: %{"PAGER" => "fallback"}) ==
             "profile"

    assert OutputSession.pager(%{}, tty: true, env: %{"PAGER" => "fallback"}) == "fallback"
    assert OutputSession.pager(%{}, tty: true, env: %{}) == "less"
    assert OutputSession.pager(%{}, tty: false, env: %{"AWS_PAGER" => "configured"}) == nil

    assert OutputSession.pager(%{no_cli_pager: true},
             tty: true,
             env: %{"AWS_PAGER" => "configured"}
           ) == nil

    assert OutputSession.pager(%{raw_stdout: true},
             tty: true,
             env: %{"AWS_PAGER" => "configured"}
           ) == nil

    assert {:ok, %{color: "off"}} = OutputSession.prepare_options(%{}, tty: false)
    assert {:ok, %{color: "on"}} = OutputSession.prepare_options(%{color: "on"}, tty: false)
    assert {:ok, %{color: "on"}} = OutputSession.prepare_options(%{}, tty: true)
  end

  test "direct writes preserve bytes and translate broken pipes" do
    assert ExUnit.CaptureIO.capture_io(fn ->
             assert :ok = OutputSession.write(nil, "Unicode 雪 é\n")
           end) == "Unicode 雪 é\n"

    owner = self()

    {:ok, session} =
      OutputSession.open(%{},
        tty: false,
        write: fn bytes ->
          send(owner, {:output, IO.iodata_to_binary(bytes)})
          :ok
        end
      )

    assert :ok = OutputSession.write(session, [<<0, 255>>, "data"])
    assert_receive {:output, <<0, 255, "data">>}
    assert :ok = OutputSession.finish(session)
    {:ok, session} = OutputSession.open(%{}, tty: false, write: fn _ -> {:error, :epipe} end)
    assert {:error, %Error{code: :broken_pipe}} = OutputSession.write(session, "data")
    assert {:error, %Error{code: :broken_pipe}} = OutputSession.finish(session)
  end

  test "pager spool is private, bounded, and removed on failure", %{tmp_dir: root} do
    {:ok, session} =
      OutputSession.open(%{},
        tty: true,
        env: %{"AWS_PAGER" => "cat"},
        tmp_dir: root,
        max_spool_bytes: 3
      )

    directory = :sys.get_state(session).directory
    assert Bitwise.band(File.stat!(directory).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(Path.join(directory, "output")).mode, 0o777) == 0o600
    assert :ok = OutputSession.write(session, "abc")
    assert {:error, %Error{code: :pager_spool_limit}} = OutputSession.write(session, "d")
    assert {:error, %Error{code: :pager_spool_limit}} = OutputSession.finish(session)
    refute File.exists?(directory)
  end

  test "actual pager receives literal argv and exact spool stdin without a shell wrapper", %{
    tmp_dir: root
  } do
    output = Path.join(root, "output file")
    marker = Path.join(root, "must-not-run")
    args = Path.join(root, "arguments")
    script = script(root, "cat > \"$1\"\nprintf '%s' \"$3\" > \"$2\"\n")

    command =
      "#{System.find_executable("sh")} '#{script}' '#{output}' '#{args}' '$(touch #{marker})'"

    {:ok, session} =
      OutputSession.open(%{},
        tty: true,
        env: Map.put(System.get_env(), "AWS_PAGER", command),
        tmp_dir: root
      )

    directory = :sys.get_state(session).directory
    bytes = <<0, 255, 13, 10, "literal output">>
    assert :ok = OutputSession.write(session, bytes)
    assert :ok = OutputSession.finish(session)
    assert File.read!(output) == bytes
    assert File.read!(args) == "$(touch #{marker})"
    refute File.exists?(marker)
    refute File.exists?(directory)
  end

  test "coordinator cancellation kills owned pager processes and removes spool", %{tmp_dir: root} do
    pids = Path.join(root, "pids")
    script = script(root, "sleep 30 &\nprintf '%s %s' \"$$\" \"$!\" > \"$1\"\nwait\n")
    command = "#{System.find_executable("sh")} '#{script}' '#{pids}'"
    {:ok, coordinator} = start_supervised({Coordinator, []})

    {:ok, session} =
      OutputSession.open(%{coordinator: coordinator},
        tty: true,
        env: Map.put(System.get_env(), "AWS_PAGER", command),
        tmp_dir: root
      )

    directory = :sys.get_state(session).directory
    :ok = OutputSession.write(session, "data")
    worker = Task.async(fn -> OutputSession.finish(session) end)
    assert wait_file(pids)
    :ok = Coordinator.cancel(coordinator)
    assert {:error, %Error{category: :interrupted}} = Task.await(worker, 1000)
    refute File.exists?(directory)
    Enum.each(String.split(File.read!(pids)), fn pid -> assert stopped?(pid) end)
  end

  test "owner death closes an unfinished spool without launching a pager", %{tmp_dir: root} do
    caller = self()

    owner =
      spawn(fn ->
        {:ok, session} =
          OutputSession.open(%{},
            tty: true,
            env: %{"AWS_PAGER" => "never-launched"},
            tmp_dir: root
          )

        :ok = OutputSession.write(session, "data")
        send(caller, {:session, session, :sys.get_state(session).directory})

        receive do
          :finish -> :ok
        end
      end)

    assert_receive {:session, session, directory}
    monitor = Process.monitor(session)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^session, :normal}
    refute File.exists?(directory)
  end

  test "owner death during paging cancels the child before spool cleanup", %{tmp_dir: root} do
    caller = self()
    pids = Path.join(root, "active-pids")
    script = script(root, "sleep 30 &\nprintf '%s %s' \"$$\" \"$!\" > \"$1\"\nwait\n")
    command = "#{System.find_executable("sh")} '#{script}' '#{pids}'"

    owner =
      spawn(fn ->
        {:ok, session} =
          OutputSession.open(%{},
            tty: true,
            env: Map.put(System.get_env(), "AWS_PAGER", command),
            tmp_dir: root
          )

        :ok = OutputSession.write(session, "data")
        send(caller, {:active_session, session, :sys.get_state(session).directory})
        OutputSession.finish(session)
      end)

    assert_receive {:active_session, session, directory}
    monitor = Process.monitor(session)
    assert wait_file(pids)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^session, :normal}, 1000
    refute File.exists?(directory)
    Enum.each(String.split(File.read!(pids)), fn pid -> assert stopped?(pid) end)
  end

  test "pager failure, missing executable, and deadline are typed and cleaned", %{tmp_dir: root} do
    for {command, code, timeout} <- [
          {"#{System.find_executable("sh")} -c 'exit 7'", :pager_failed, 1000},
          {"/definitely/missing/ess-pager", :pager_unavailable, 1000},
          {"#{System.find_executable("sleep")} 30", :pager_timeout, 25}
        ] do
      {:ok, session} =
        OutputSession.open(%{},
          tty: true,
          env: Map.put(System.get_env(), "AWS_PAGER", command),
          tmp_dir: root,
          pager_timeout: timeout
        )

      directory = :sys.get_state(session).directory
      :ok = OutputSession.write(session, "data")
      assert {:error, %Error{code: ^code}} = OutputSession.finish(session)
      refute File.exists?(directory)
    end
  end

  test "real terminal foreground, cancellation, owner death, and sensitive prompt restore" do
    root = Path.expand("../../..", __DIR__)
    build = Path.dirname(:code.lib_dir(:ex_storage_service_cli) |> to_string())

    {output, status} =
      System.cmd(
        "python3",
        [Path.join(root, "test/support/command_session_terminal_probe.py"), build],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "PASS command sessions"
  end

  defp script(root, body) do
    path = Path.join(root, "pager-script")
    File.write!(path, body)
    path
  end

  defp wait_file(path, retries \\ 100)
  defp wait_file(_, 0), do: false

  defp wait_file(path, retries) do
    if File.exists?(path),
      do: true,
      else:
        (
          Process.sleep(10)
          wait_file(path, retries - 1)
        )
  end

  defp stopped?(pid) do
    case File.read("/proc/#{pid}/stat") do
      {:error, :enoent} -> true
      {:ok, value} -> Regex.match?(~r/^\d+ \(.*\) Z /, value)
      _ -> false
    end
  end
end
