"""Controller failures retain useful classifications without disclosing payloads."""
import ast
import json
import os
from pathlib import Path
import subprocess
import tempfile
import traceback
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid


class ControlRedactionTest(unittest.TestCase):
    def test_remote_index_failure_diagnostics_exclude_payloads(self):
        root = Path(__file__).resolve().parents[1]
        controller = (root / "e2e/scripts/index_cluster_control.exs").read_text()
        source = "defmodule ESSIndexRPCFailureDiagnostic do\n" + controller.split(
            "defmodule ESSIndexRPCFailureDiagnostic do\n", 1)[1].split("\ncall = fn target ->", 1)[0]
        secret = "private-rpc-key-value-cookie-and-exception-message"
        exercise = r'''
secret = "private-rpc-key-value-cookie-and-exception-message"
frame = {ExStorageService.Metadata.ListIndex.Backfill, :resume, [secret],
         [line: 144, file: secret]}
stack = List.duplicate(frame, 12)
for reason <- [%MatchError{term: {:error, :build_conflict}},
               %MatchError{term: false}, %MatchError{term: {:error, secret}},
               %RuntimeError{message: secret}, %{private: secret}] do
  ESSIndexRPCFailureDiagnostic.emit("resume", {:badrpc, {:EXIT, {reason, stack}}})
end
ESSIndexRPCFailureDiagnostic.emit(secret, {:badrpc, secret})
ESSIndexRPCFailureDiagnostic.emit("resume", %{private: secret})
'''
        environment = dict(os.environ, PAGER="cat", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "index_rpc_failure_probe.exs"
            path.write_text(source + exercise)
            result = subprocess.run(["elixir", str(path)], cwd=root, env=environment,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, timeout=30, check=True)
        self.assertNotIn(secret, result.stdout + result.stderr)
        lines = result.stderr.splitlines()
        self.assertEqual(7, len(lines))
        records = [json.loads(line.removeprefix("A29_RPC_FAILURE ")) for line in lines]
        self.assertEqual("build_conflict", records[0]["rhs_shape"]["detail"]["tag"])
        self.assertEqual("false", records[1]["rhs_shape"]["tag"])
        self.assertEqual({"kind": "redacted"}, records[2]["rhs_shape"]["detail"])
        self.assertEqual("Elixir.RuntimeError", records[3]["exception"])
        self.assertEqual("other", records[5]["action"])
        self.assertEqual("unexpected_rpc_result", records[6]["kind"])
        for record in records[:5]:
            self.assertEqual(8, len(record["stack"]))
            self.assertEqual({"module": "Elixir.ExStorageService.Metadata.ListIndex.Backfill",
                              "function": "resume", "arity": 1, "line": 144}, record["stack"][0])

    def test_failure_and_timeout_tracebacks_exclude_private_cookie(self):
        for source, owner, method, arguments, log in [
            ("gateway_hardening.py", "Runtime", "control", ("inspect", {}), "control.log"),
            ("s3_hardening_faults.py", "FaultRuntime", "fault_control", ("snapshot",), "fault-control.log"),
        ]:
            module = ast.parse((Path(__file__).parent / source).read_text())
            cls = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == owner)
            function = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == method)
            namespace = {"json": json, "subprocess": subprocess, "uuid": uuid}
            # Compile the actual method alone; importing these runners would
            # unnecessarily require Boto3 merely to inspect error handling.
            exec(compile(ast.Module(body=[function], type_ignores=[]), source, "exec"), namespace)
            cookie = "private-cookie-must-never-enter-public-traceback"
            for failure_type in (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                with self.subTest(source=source, failure=failure_type.__name__), tempfile.TemporaryDirectory() as root:
                    service = SimpleNamespace(root=Path(root), cookie=cookie, env={})

                    def fail(argv, **kwargs):
                        self.assertIn(cookie, argv)
                        kwargs["stdout"].write(b"private diagnostic preserved\n")
                        if failure_type is subprocess.CalledProcessError:
                            raise subprocess.CalledProcessError(1, argv)
                        raise subprocess.TimeoutExpired(argv, 45)

                    with patch.object(subprocess, "run", side_effect=fail):
                        try:
                            namespace[method](service, *arguments)
                        except RuntimeError as failure:
                            public = "".join(traceback.format_exception(failure))
                            self.assertNotIn(cookie, public)
                            self.assertIn("inspect private", public)
                        else:
                            self.fail("controller failure was swallowed")
                    self.assertEqual(b"private diagnostic preserved\n", (Path(root) / log).read_bytes())


if __name__ == "__main__":
    unittest.main()
