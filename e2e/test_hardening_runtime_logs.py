"""Retain startup failures without exporting private fixture credentials."""
import ast
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.parse
import uuid


ROOT = Path(__file__).parent


def runtime_class():
    source = ast.parse((ROOT / "gateway_hardening.py").read_text())
    runtime = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == "Runtime")
    namespace = dict(globals())
    exec(compile(ast.Module(body=[runtime], type_ignores=[]), "gateway_hardening.py", "exec"), namespace)
    return namespace["Runtime"], namespace


def finalizer(source):
    module = ast.parse((ROOT / source).read_text())
    main = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    body = next(node.finalbody for node in main.body if isinstance(node, ast.Try) and node.finalbody)
    return compile(ast.Module(body=body, type_ignores=[]), source, "exec")


class RuntimeLogTest(unittest.TestCase):
    def fixture(self, directory):
        root, evidence = Path(directory) / "private", Path(directory) / "evidence"
        root.mkdir()
        evidence.mkdir()
        runtime, namespace = runtime_class()
        service = runtime(root, 12345)
        service.env["AWS_ACCESS_KEY_ID"] = "private-environment-access"
        credentials = {"access_key": "private-rpc-access", "secret_key": 'private-rpc/secret+"value'}
        (root / "rpc-seed-result.json").write_text(json.dumps(credentials))
        (root / "backlog-configure-request.json").write_text(json.dumps({"token": "private-peer-token"}))
        (root / "control.log").write_text("private controller output")
        (root / "private-state.json").write_text("private fixture state")
        return service, evidence, credentials, namespace

    def test_failed_service_start_retains_diagnostics_and_redacts_private_values(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime, evidence, credentials, namespace = self.fixture(directory)
            values = [runtime.cookie, runtime.env["ESS_MASTER_KEY"], runtime.env["AWS_ACCESS_KEY_ID"],
                      *credentials.values(), "private-peer-token"]
            text = ("** (MatchError) listener startup failed: :eaddrinuse\n"
                    "    e2e/scripts/start.exs:38: (file)\n" + "\n".join(values) + "\n" +
                    urllib.parse.quote(credentials["secret_key"], safe="") + "\n" +
                    json.dumps(credentials["secret_key"])[1:-1] + "\n" +
                    "http://unrecognized-user:unrecognized-password@127.0.0.1:9000\n" +
                    "::add-mask::unrecognized-masked-secret\n")
            launch = subprocess.Popen

            def child(_argv, **options):
                return launch([sys.executable, "-c", "import sys; sys.stdout.write(sys.argv[1])", text], **options)

            def failed_ready(_url, process):
                process.wait(timeout=5)
                raise RuntimeError("owned service exited; inspect retained log")

            namespace["wait_http"] = failed_ready
            with patch.object(subprocess, "Popen", side_effect=child):
                with self.assertRaisesRegex(RuntimeError, "owned service exited"):
                    runtime.start()
            runtime.close()
            (runtime.root / "beam-2.log").write_text("second generation: :eacces\n" + runtime.cookie)
            (runtime.root / "beam-private.log").write_text("private non-service output")
            (runtime.root / "beam-99.log").symlink_to(runtime.root / "control.log")
            runtime.retain_logs(evidence)
            retained = (evidence / "beam-1.log").read_text()
            self.assertIn(":eaddrinuse", retained)
            self.assertIn("e2e/scripts/start.exs:38", retained)
            for value in values + ["unrecognized-user", "unrecognized-password", "unrecognized-masked-secret",
                                   urllib.parse.quote(credentials["secret_key"], safe=""),
                                   json.dumps(credentials["secret_key"])[1:-1]]:
                self.assertNotIn(value, retained)
            self.assertEqual(["beam-1.log", "beam-2.log"], sorted(path.name for path in evidence.iterdir()))
            self.assertIn(":eacces", (evidence / "beam-2.log").read_text())
            self.assertNotIn(runtime.cookie, (evidence / "beam-2.log").read_text())
            self.assertIsNone(runtime.process)

    def test_backlog_failure_finalizer_exports_service_log(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime, evidence, _, _ = self.fixture(directory)
            (runtime.root / "beam-1.log").write_text("listener failed: :eaddrinuse\n" + runtime.cookie)
            closed = []
            result = {"status": "failed", "error_type": "RuntimeError"}
            namespace = {"runtime": runtime, "replica": SimpleNamespace(close=lambda: closed.append("replica")),
                         "evidence": evidence, "root": runtime.root, "result": result, "json": json}
            with redirect_stdout(io.StringIO()):
                exec(finalizer("s3_hardening_replication_backlog.py"), namespace)
            self.assertEqual(["replica"], closed)
            self.assertIn(":eaddrinuse", (evidence / "beam-1.log").read_text())
            self.assertNotIn(runtime.cookie, (evidence / "beam-1.log").read_text())
            self.assertEqual("failed", json.loads((evidence / "results.json").read_text())["status"])

    def test_backlog_cleanup_failure_preserves_replica_cleanup_and_failed_receipt(self):
        for component in ("close", "retain_logs"):
            with self.subTest(component=component), tempfile.TemporaryDirectory() as directory:
                runtime, evidence, _, _ = self.fixture(directory)
                (runtime.root / "beam-1.log").write_text("listener failed: :eaddrinuse\n")
                error = OSError("owned diagnostic export failed")

                def fail(*_args):
                    raise error

                setattr(runtime, component, fail)
                closed = []
                namespace = {"runtime": runtime, "replica": SimpleNamespace(close=lambda: closed.append("replica")),
                             "evidence": evidence, "root": runtime.root, "result": {"status": "passed"}, "json": json}
                with redirect_stdout(io.StringIO()), self.assertRaises(OSError) as caught:
                    exec(finalizer("s3_hardening_replication_backlog.py"), namespace)
                self.assertIs(error, caught.exception)
                self.assertEqual(["replica"], closed)
                self.assertEqual("failed", json.loads((evidence / "results.json").read_text())["status"])
                if component == "close":
                    self.assertIn(":eaddrinuse", (evidence / "beam-1.log").read_text())

    def test_resource_finalizer_exports_service_log_even_when_observer_shutdown_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime, evidence, _, _ = self.fixture(directory)
            (runtime.root / "beam-1.log").write_text("listener failed: :eaddrinuse\n" + runtime.cookie)

            def failed_close():
                raise RuntimeError("observer shutdown failed")

            runtime.close = failed_close
            removed = []
            result = {"status": "failed", "error_type": "RuntimeError"}
            namespace = {"runtime": runtime, "proxy": None, "evidence": evidence, "root": runtime.root,
                         "samples_path": runtime.root / "samples.jsonl", "container": "owned-fixture",
                         "result": result, "json": json,
                         "subprocess": SimpleNamespace(DEVNULL=-3, check_output=lambda *_args, **_kwargs: "candidate\n",
                                                       run=lambda argv, **_kwargs: removed.append(argv))}
            with redirect_stdout(io.StringIO()):
                exec(finalizer("s3_hardening_resources.py"), namespace)
            self.assertEqual([["docker", "rm", "-f", "owned-fixture"]], removed)
            self.assertIn(":eaddrinuse", (evidence / "beam-1.log").read_text())
            self.assertNotIn(runtime.cookie, (evidence / "beam-1.log").read_text())
            receipt = json.loads((evidence / "results.json").read_text())
            self.assertEqual("failed", receipt["status"])
            self.assertIn({"component": "runtime", "error_type": "RuntimeError"}, receipt["cleanup_errors"])


if __name__ == "__main__":
    unittest.main()
