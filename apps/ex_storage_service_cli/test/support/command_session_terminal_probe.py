"""Linux PTY acceptance for owned pager sessions and pre-label secret suppression."""
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import shlex
import subprocess
import sys
import tempfile
import termios
import time

build = sys.argv[1]


def controlling_terminal():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)


def run(expression, on_output):
    master, slave = pty.openpty()
    child = subprocess.Popen(
        ["elixir", "--erl", "+S 2:2", "-pa", build + "/*/ebin", "-e", expression],
        stdin=slave, stdout=slave, stderr=slave, close_fds=True,
        preexec_fn=controlling_terminal,
    )
    output = b""
    try:
        deadline = time.monotonic() + 12
        baseline = None
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.05)[0]:
                output += os.read(master, 65536)
            if b"BEFORE" in output and baseline is None:
                baseline = termios.tcgetattr(slave)
            on_output(output, master, slave)
            if b"RESULT" in output:
                assert baseline == termios.tcgetattr(slave), output
                assert os.tcgetpgrp(master) == child.pid, output
                assert child.wait(timeout=3) == 0, output
                return output
            if child.poll() is not None:
                break
        raise AssertionError("terminal probe did not finish: " + repr(output))
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        os.close(master)
        os.close(slave)


for mode in ["finish", "cancel", "owner-death"]:
    with tempfile.TemporaryDirectory(prefix="ess-session-pty-") as temporary:
        directory = Path(temporary)
        pager = directory / "pager.py"
        ready = directory / "ready"
        pager.write_text('''import os,sys,termios
from pathlib import Path
f=open('/dev/tty','r+b',buffering=0)
assert os.tcgetpgrp(f.fileno()) == os.getpgrp()
a=termios.tcgetattr(f.fileno()); a[3] &= ~termios.ECHO; termios.tcsetattr(f.fileno(),termios.TCSANOW,a)
assert sys.stdin.buffer.read() == b'payload\\x00\\xff'
Path(sys.argv[1]).write_text(str(os.getpid()))
f.write(b'PAGER_READY\\n')
assert f.read(1) == b'q'
''')
        command = " ".join(shlex.quote(value) for value in [sys.executable, str(pager), str(ready)])
        expression = f'''
        alias ExStorageServiceCli.CLI.OutputSession
        alias ExStorageServiceCli.Transfer.Coordinator
        {{:ok, coordinator}} = Coordinator.start_link([])
        parent = self()
        IO.puts("BEFORE")
        Process.sleep(100)
        owner = spawn(fn ->
          {{:ok, session}} = OutputSession.open(%{{cli_pager: {json.dumps(command)}, coordinator: coordinator}}, tty: true, env: %{{}}, tmp_dir: {json.dumps(temporary)})
          :ok = OutputSession.write(session, <<"payload", 0, 255>>)
          send(parent, {{:session, session}})
          result = OutputSession.finish(session)
          send(parent, {{:result, result}})
        end)
        session = receive do {{:session, session}} -> session end
        monitor = Process.monitor(session)
        wait = fn wait -> if File.exists?({json.dumps(str(ready))}), do: :ok, else: (Process.sleep(5); wait.(wait)) end
        wait.(wait)
        '''
        if mode == "cancel":
            expression += ':ok = Coordinator.cancel(coordinator)\nreceive do {:result, {:error, %{category: :interrupted}}} -> :ok end\n'
        elif mode == "owner-death":
            expression += 'Process.exit(owner, :kill)\n'
        else:
            expression += 'receive do {:result, :ok} -> :ok end\n'
        expression += 'receive do {:DOWN, ^monitor, _, _, :normal} -> :ok end\nIO.puts("RESULT")\nProcess.sleep(100)'
        sent = [False]

        def pager_output(output, master, _slave):
            if mode == "finish" and b"PAGER_READY" in output and not sent[0]:
                os.write(master, b"q\n")
                sent[0] = True

        run(expression, pager_output)
        assert not list(directory.glob("ess-output-*"))
        pid = ready.read_text()
        state = Path("/proc") / pid / "stat"
        assert not state.exists() or ") Z " in state.read_text()

expression = '''
alias ExStorageServiceCli.CLI.AutoPrompt
IO.puts("BEFORE")
Process.sleep(100)
{:error, %{code: :auto_prompt_declined}} = AutoPrompt.run("WriteGetObjectResponse", %{"RequestRoute" => "route"}, %{cli_auto_prompt: true})
IO.puts("RESULT")
Process.sleep(100)
'''
sent = [False, False]


def prompt_output(output, master, slave):
    if b"RequestToken: " in output and not sent[0]:
        assert not termios.tcgetattr(slave)[3] & termios.ECHO
        os.write(master, b"private-input-probe\n")
        sent[0] = True
    if b"Run WriteGetObjectResponse?" in output and not sent[1]:
        assert termios.tcgetattr(slave)[3] & termios.ECHO
        os.write(master, b"no\n")
        sent[1] = True


output = run(expression, prompt_output)
assert b"private-input-probe" not in output
print("PASS command sessions foreground exact-bytes cancellation owner-death sensitive-no-echo restoration")
