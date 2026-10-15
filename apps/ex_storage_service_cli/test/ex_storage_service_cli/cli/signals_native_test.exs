defmodule ExStorageServiceCli.CLI.SignalsNativeTest do
  use ExUnit.Case, async: true
  @moduletag :tmp_dir
  @root Path.expand("../../..", __DIR__)

  setup_all do
    directory =
      Path.join(System.tmp_dir!(), "ess-signal-test-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    library = Path.join(directory, "ess_signals")
    include = Path.join(to_string(:code.root_dir()), "usr/include")

    flags =
      if :os.type() == {:unix, :darwin},
        do: ["-dynamiclib", "-undefined", "dynamic_lookup"],
        else: ["-shared", "-fPIC"]

    {output, status} =
      System.cmd(
        "cc",
        ["-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-I", include] ++
          flags ++ [Path.join(@root, "c_src/ess_signal.c"), "-o", library <> ".so"],
        stderr_to_stdout: true
      )

    assert status == 0, output
    on_exit(fn -> File.rm_rf!(directory) end)
    %{library: library}
  end

  test "real SIGINT is delivered to the invocation and restores the previous handler", context do
    port =
      child(context, """
      {:ok, resource} = Native.start(self())
      IO.puts("READY " <> System.pid())
      receive do
        {:select, ^resource, :ess_sigint, :ready_input} ->
          [:sigint] = Native.drain(resource)
          :ok = Native.close(resource)
          true = Native.handler_restored?()
          IO.puts("RESTORED")
          System.halt(130)
      after 5_000 -> System.halt(77)
      end
      """)

    ready = line(port)
    assert "READY " <> pid = ready
    assert {"", 0} = System.cmd("kill", ["-INT", String.trim(pid)])
    assert {output, 130} = collect(port, "")
    assert output =~ "RESTORED"
  end

  test "real SIGTERM uses the invocation resource and restores both dispositions", context do
    port =
      child(context, """
      {:ok, resource} = Native.start(self())
      IO.puts("READY " <> System.pid())
      receive do
        {:select, ^resource, :ess_sigint, :ready_input} ->
          [:sigterm] = Native.drain(resource)
          :ok = Native.close(resource)
          true = Native.handler_restored?()
          IO.puts("RESTORED")
          System.halt(143)
      after 5_000 -> System.halt(77)
      end
      """)

    assert "READY " <> pid = line(port)
    assert {"", 0} = System.cmd("kill", ["-TERM", String.trim(pid)])
    assert {output, 143} = collect(port, "")
    assert output =~ "RESTORED"
  end

  test "owner death restores handler and 100 lifetimes do not leak descriptors", context do
    port =
      child(context, """
      parent = self()
      owner = spawn(fn ->
        {:ok, resource} = Native.start(self())
        send(parent, {:owned, resource})
        receive do :stop -> :ok end
      end)
      receive do {:owned, _resource} -> :ok after 1_000 -> raise "not owned" end
      monitor = Process.monitor(owner)
      send(owner, :stop)
      receive do {:DOWN, ^monitor, _, _, _} -> :ok after 1_000 -> raise "owner alive" end
      wait = fn wait, n ->
        if Native.handler_restored?(), do: :ok, else:
          (if n == 0, do: raise("not restored"), else: (Process.sleep(10); wait.(wait, n - 1)))
      end
      wait.(wait, 100)
      count = fn -> if :os.type() == {:unix, :linux}, do: length(File.ls!("/proc/self/fd")), else: 0 end
      before_count = count.()
      for _ <- 1..100 do
        {:ok, resource} = Native.start(self())
        [] = Native.drain(resource)
        :ok = Native.close(resource)
        :ok = Native.close(resource)
        true = Native.handler_restored?()
      end
      Process.sleep(50)
      true = count.() <= before_count + 2
      IO.puts("CLEAN")
      """)

    assert {output, 0} = collect(port, "")
    assert output =~ "CLEAN"
  end

  test "directory reads are bounded, retain binary names, and reject final symlinks", context do
    names = Enum.map(1..260, &"entry-#{&1}") ++ [<<255, 254>>]
    Enum.each(names, &File.write!(Path.join(context.tmp_dir, &1), ""))
    link = context.tmp_dir <> "-link"
    File.ln_s!(context.tmp_dir, link)
    on_exit(fn -> File.rm(link) end)

    port =
      child(context, """
      {:ok, resource} = Native.open_directory(#{inspect(context.tmp_dir)})
      {:ok, first} = Native.read_directory(resource, 128)
      128 = length(first)
      parent = self()
      spawn(fn -> send(parent, {:other, Native.read_directory(resource, 1)}) end)
      receive do {:other, {:error, :directory_owner_required}} -> :ok after 1_000 -> raise "ownership" end
      read = fn read, acc ->
        case Native.read_directory(resource, 128) do
          {:ok, batch} when length(batch) <= 128 -> read.(read, acc ++ batch)
          :eof -> acc
        end
      end
      names = first ++ read.(read, [])
      true = Enum.sort(names) == Enum.sort(#{inspect(names, limit: :infinity)})
      :ok = Native.close_directory(resource)
      :ok = Native.close_directory(resource)
      {:error, :directory_closed} = Native.read_directory(resource, 1)
      {:error, reason} = Native.open_directory(#{inspect(link)}, false)
      true = reason in [:enotdir, :eloop]
      {:ok, followed} = Native.open_directory(#{inspect(link)}, true)
      :ok = Native.close_directory(followed)
      IO.puts("BOUNDED")
      """)

    assert {output, 0} = collect(port, "")
    assert output =~ "BOUNDED"
  end

  test "directory owner death closes the descriptor while another process retains the resource",
       context do
    port =
      child(context, """
      count = fn -> if :os.type() == {:unix, :linux}, do: length(File.ls!("/proc/self/fd")), else: 0 end
      before_count = count.()
      parent = self()
      owner = spawn(fn ->
        {:ok, resource} = Native.open_directory(#{inspect(context.tmp_dir)})
        send(parent, {:directory, resource})
        receive do :stop -> :ok end
      end)
      resource = receive do {:directory, resource} -> resource after 1_000 -> raise "no directory" end
      monitor = Process.monitor(owner)
      send(owner, :stop)
      receive do {:DOWN, ^monitor, _, _, _} -> :ok after 1_000 -> raise "owner alive" end
      Process.sleep(50)
      true = count.() <= before_count
      :ok = Native.close_directory(resource)
      IO.puts("OWNER-CLEAN")
      """)

    assert {output, 0} = collect(port, "")
    assert output =~ "OWNER-CLEAN"
  end

  test "actual terminal hides entered MFA and restores echo on close and owner death", context do
    {output, status} =
      System.cmd(
        "python3",
        [Path.join(@root, "test/support/native_terminal_probe.py"), @root, context.library],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "PASS terminal"
  end

  defp child(context, script) do
    source = """
    Code.require_file(#{inspect(Path.join(@root, "lib/ex_storage_service_cli/error.ex"))})
    Code.require_file(#{inspect(Path.join(@root, "lib/ex_storage_service_cli/cli/signals/native.ex"))})
    alias ExStorageServiceCli.CLI.Signals.Native
    :ok = Native.load_path(#{inspect(context.library)})
    #{script}
    """

    Port.open({:spawn_executable, String.to_charlist(System.find_executable("elixir"))}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      args: ["--erl", "+S 2:2", "-e", source]
    ])
  end

  defp line(port) do
    receive do
      {^port, {:data, data}} -> data
    after
      10_000 ->
        Port.close(port)
        flunk("Signal subprocess did not become ready")
    end
  end

  defp collect(port, output) do
    receive do
      {^port, {:data, data}} -> collect(port, output <> data)
      {^port, {:exit_status, status}} -> {output, status}
    after
      10_000 ->
        Port.close(port)
        flunk("Signal subprocess exceeded deadline")
    end
  end
end
