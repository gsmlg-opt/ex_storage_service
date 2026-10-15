"""Exercise real POSIX terminal echo and owner cleanup without entering secrets in logs."""
import os
import pty
import select
import subprocess
import sys
import termios
import time

root, library = sys.argv[1:]
prefix = f'''
Code.require_file({root + "/lib/ex_storage_service_cli/error.ex"!r})
Code.require_file({root + "/lib/ex_storage_service_cli/cli/signals/native.ex"!r})
alias ExStorageServiceCli.CLI.Signals.Native
:ok = Native.load_path({library!r})
'''.replace("'", '"')
for owner_dies in [False, True]:
    script = '''
    {:ok, terminal} = Native.suppress_stdin_echo()
    {:error, :terminal_busy} = Native.suppress_stdin_echo()
    IO.puts("READY")
    "654321\\n" = IO.gets("")
    :ok = Native.restore_terminal(terminal)
    :ok = Native.restore_terminal(terminal)
    IO.puts("RESTORED")
    '''
    if owner_dies:
        script = '''
        parent = self()
        owner = spawn(fn ->
          {:ok, terminal} = Native.suppress_stdin_echo()
          send(parent, {:terminal, terminal})
          receive do :stop -> :ok end
        end)
        terminal = receive do {:terminal, terminal} -> terminal end
        monitor = Process.monitor(owner)
        IO.puts("READY")
        "654321\\n" = IO.gets("")
        send(owner, :stop)
        receive do {:DOWN, ^monitor, _, _, _} -> :ok end
        Process.sleep(50)
        IO.puts("OWNER-RESTORED")
        "done\\n" = IO.gets("")
        :ok = Native.restore_terminal(terminal)
        IO.puts("RESTORED")
        '''
    master, slave = pty.openpty()
    original = termios.tcgetattr(slave)
    child = subprocess.Popen(
        ["elixir", "--erl", "+S 2:2", "-e", prefix + script],
        stdin=slave, stdout=slave, stderr=slave, close_fds=True,
    )
    try:
        output = b""
        deadline = time.monotonic() + 10
        while b"READY" not in output and time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output += os.read(master, 8192)
        assert b"READY" in output, output
        assert not termios.tcgetattr(slave)[3] & termios.ECHO
        os.write(master, b"654321\n")
        if owner_dies:
            while b"OWNER-RESTORED" not in output and time.monotonic() < deadline:
                if select.select([master], [], [], 0.1)[0]:
                    output += os.read(master, 8192)
            assert b"OWNER-RESTORED" in output, output
            assert termios.tcgetattr(slave) == original
            os.write(master, b"done\n")
        while b"RESTORED" not in output and time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                output += os.read(master, 8192)
        child.wait(timeout=3)
        assert child.returncode == 0 and b"RESTORED" in output, output
        assert b"654321" not in output, output
        assert termios.tcgetattr(slave) == original
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        os.close(master)
        os.close(slave)
print("PASS terminal no-echo explicit-restore owner-death retained-resource")
