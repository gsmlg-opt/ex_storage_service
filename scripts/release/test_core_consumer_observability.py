"""Consumer observability tests use Python fixtures, never Mix or native builds."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import core_package


class FlushedOutput(io.StringIO):
    def __init__(self, marker=None, callback=None):
        super().__init__()
        self.marker = marker
        self.callback = callback
        self.observed = False

    def flush(self):
        super().flush()
        if self.marker and not self.observed and self.marker in self.getvalue():
            self.observed = True
            self.callback()


class CoreConsumerObservabilityTests(unittest.TestCase):
    version = "0.6.9"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ess-observe-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "consumer"
        self.package = self.root / "package.tar"
        self.trace = self.root / "trace.jsonl"
        self.finished = self.root / "finished"
        self.socket_path = self.root / "handshake.sock"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        fake_mix = self.bin / "mix"
        fake_mix.write_text(f"#!{sys.executable}\n" + '''
import json
import os
from pathlib import Path
import socket
import sys

command = sys.argv[1:]
with open(os.environ["ESS_TEST_TRACE"], "a") as trace:
    trace.write(json.dumps({"argv": command, "cwd": str(Path.cwd()),
                           "mix_env": os.environ["MIX_ENV"],
                           "build": os.environ["MIX_BUILD_PATH"],
                           "deps": os.environ["MIX_DEPS_PATH"],
                           "inherited": os.environ["ESS_TEST_INHERITED"]}) + "\\n")
if command[0] == os.environ.get("ESS_TEST_HANDSHAKE_COMMAND"):
    print("live:" + command[0], flush=True)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(5)
        client.connect(os.environ["ESS_TEST_SOCKET"])
        if client.recv(1) != b"x":
            raise RuntimeError("missing parent acknowledgement")
    Path(os.environ["ESS_TEST_FINISHED"]).write_text("acknowledged")
os.write(1, ("stdout:" + command[0] + "\\r\\n").encode())
os.write(2, ("stderr:" + command[0] + "\\n").encode())
os.write(1, ("tail:" + command[0]).encode())
if command[0] == os.environ.get("ESS_TEST_FAIL_COMMAND"):
    sys.exit(7)
''')
        fake_mix.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ.get("PATH", ""),
                        ESS_TEST_TRACE=str(self.trace), ESS_TEST_SOCKET=str(self.socket_path),
                        ESS_TEST_FINISHED=str(self.finished), ESS_TEST_INHERITED="preserved")
        for key in ("ESS_TEST_HANDSHAKE_COMMAND", "ESS_TEST_FAIL_COMMAND"):
            self.env.pop(key, None)
        contents = io.BytesIO()
        with tarfile.open(fileobj=contents, mode="w:gz") as archive:
            data = b'version: "0.6.9"\n'
            member = tarfile.TarInfo("mix.exs")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
        self.contents = contents.getvalue()
        self.children = []
        self.calls = []
        self.real_popen = subprocess.Popen
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)

    def spawn(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        child = self.real_popen(*args, **kwargs)
        self.children.append(child)
        return child

    def consume(self, sink):
        with patch.dict(os.environ, self.env, clear=True), \
                patch.object(core_package, "read_package", return_value=("checksum", self.contents)), \
                patch.object(core_package.subprocess, "Popen", side_effect=self.spawn), \
                redirect_stdout(sink):
            core_package.consumer(self.package, self.version, self.output)

    def records(self):
        return json.loads((self.output / "commands.json").read_text())

    def trace_records(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()]

    def listener(self):
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.settimeout(5)
        listener.bind(str(self.socket_path))
        listener.listen(1)
        self.addCleanup(listener.close)
        return listener

    def acknowledge(self, listener, command, expected_count):
        self.assertFalse(self.finished.exists(), "child exited before output was observed")
        self.assertIsNone(self.children[-1].poll())
        records = self.records()
        self.assertEqual(len(records), expected_count)
        self.assertEqual(records[-1], {"argv": ["mix", *command], "status": None, "output": ""})
        self.assertIn("live:" + command[0], (self.output / "consumer.log").read_text())
        with listener.accept()[0] as connection:
            connection.sendall(b"x")

    @staticmethod
    def expected_commands(version):
        return [["mix", "deps.get", "--only", "prod"],
                ["mix", "compile", "--warnings-as-errors"],
                ["mix", "run", "--no-start", "-e",
                 'Application.load(:ex_storage_service); '
                 f'"{version}" = to_string(Application.spec(:ex_storage_service, :vsn)); '
                 'Code.ensure_loaded!(ExStorageService.Storage.Engine); '
                 'if Code.ensure_loaded?(ExStorageServiceWeb.Endpoint), do: raise("server dependency"); '
                 'IO.puts("standalone-core-consumer-ok")']]

    def test_live_output_checkpoint_and_exact_success_contract(self):
        self.env["ESS_TEST_HANDSHAKE_COMMAND"] = "deps.get"
        listener = self.listener()
        sink = FlushedOutput("live:deps.get", lambda: self.acknowledge(
            listener, ["deps.get", "--only", "prod"], 1))
        self.consume(sink)
        self.assertTrue(sink.observed)
        self.assertEqual(self.finished.read_text(), "acknowledged")
        commands = self.expected_commands(self.version)
        self.assertEqual([record["argv"] for record in self.records()], commands)
        expected_env = dict(self.env, MIX_ENV="prod", MIX_BUILD_PATH=str(self.output / "build"),
                            MIX_DEPS_PATH=str(self.output / "deps"))
        for (args, kwargs), command, trace, result in zip(
                self.calls, commands, self.trace_records(), self.records(), strict=True):
            self.assertEqual(args, (command,))
            self.assertEqual(kwargs, {"cwd": self.output / "source", "env": expected_env,
                                      "text": True, "bufsize": 1, "stdout": subprocess.PIPE,
                                      "stderr": subprocess.STDOUT})
            self.assertEqual(trace, {"argv": command[1:], "cwd": str(self.output / "source"),
                                     "mix_env": "prod", "build": str(self.output / "build"),
                                     "deps": str(self.output / "deps"), "inherited": "preserved"})
            name = command[1]
            text = f"stdout:{name}\nstderr:{name}\ntail:{name}"
            if name == "deps.get":
                text = "live:deps.get\n" + text
            self.assertEqual(result, {"argv": command, "status": 0, "output": text})
            self.assertEqual(sink.getvalue().count(f"Running {json.dumps(command)} (cwd="), 1)
            self.assertIn(text, sink.getvalue())
        self.assertEqual((self.output / "consumer.log").read_text(), sink.getvalue())
        self.assertEqual(json.loads((self.output / "manifest.json").read_text()),
                         {"version": self.version, "hex_inner_checksum": "checksum", "commands": 3})
        self.assertTrue(all(child.poll() == 0 for child in self.children))

    def test_failed_command_is_streamed_recorded_and_stops_later_commands(self):
        self.env.update(ESS_TEST_HANDSHAKE_COMMAND="compile", ESS_TEST_FAIL_COMMAND="compile")
        listener = self.listener()
        sink = FlushedOutput("live:compile", lambda: self.acknowledge(
            listener, ["compile", "--warnings-as-errors"], 2))
        with self.assertRaisesRegex(RuntimeError, "Fresh core consumer failed"):
            self.consume(sink)
        self.assertTrue(sink.observed)
        records = self.records()
        self.assertEqual([record["argv"] for record in records], self.expected_commands(self.version)[:2])
        self.assertEqual([record["status"] for record in records], [0, 7])
        self.assertEqual(records[1]["output"], "live:compile\nstdout:compile\nstderr:compile\ntail:compile")
        self.assertEqual(sink.getvalue().count("stderr:compile"), 1)
        self.assertEqual((self.output / "consumer.log").read_text(), sink.getvalue())
        self.assertEqual(len(self.trace_records()), 2)
        self.assertFalse((self.output / "manifest.json").exists())
        self.assertTrue(all(child.poll() is not None for child in self.children))

    def test_handled_interrupt_retains_partial_output_and_reaps_child(self):
        self.env["ESS_TEST_HANDSHAKE_COMMAND"] = "deps.get"
        self.listener()

        def interrupt():
            self.assertIsNone(self.children[0].poll())
            self.assertEqual(self.records()[0]["status"], None)
            raise KeyboardInterrupt()

        sink = FlushedOutput("live:deps.get", interrupt)
        with self.assertRaises(KeyboardInterrupt):
            self.consume(sink)
        self.assertTrue(sink.observed)
        record = self.records()[0]
        self.assertEqual(record["argv"], self.expected_commands(self.version)[0])
        self.assertEqual(record["output"], "live:deps.get\n")
        self.assertEqual(record["status"], self.children[0].returncode)
        self.assertLess(record["status"], 0)
        self.assertEqual(len(self.children), 1)
        self.assertIsNotNone(self.children[0].poll())
        self.assertIn("live:deps.get\n", (self.output / "consumer.log").read_text())
        self.assertFalse((self.output / "manifest.json").exists())
        self.assertFalse(self.finished.exists())

    def test_launch_failure_retains_active_command_and_flushed_header(self):
        sink = FlushedOutput()
        with patch.dict(os.environ, self.env, clear=True), \
                patch.object(core_package, "read_package", return_value=("checksum", self.contents)), \
                patch.object(core_package.subprocess, "Popen", side_effect=FileNotFoundError("fixture")), \
                redirect_stdout(sink):
            with self.assertRaises(FileNotFoundError):
                core_package.consumer(self.package, self.version, self.output)
        self.assertEqual(self.records(), [{"argv": self.expected_commands(self.version)[0],
                                           "status": None, "output": ""}])
        self.assertIn('Running ["mix", "deps.get", "--only", "prod"]', sink.getvalue())
        self.assertEqual((self.output / "consumer.log").read_text(), sink.getvalue())
        self.assertFalse((self.output / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
