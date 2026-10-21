"""Synthetic terminal-driver checks: no driver import, subprocess or workload."""
import ast
import datetime
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock


DRIVER = Path(__file__).with_name("run.py")


class FreezeGateTest(unittest.TestCase):
    def terminal(self, child_exit=0, changed=None):
        tree = ast.parse(DRIVER.read_text())
        owned_command = next(index for index, node in enumerate(tree.body)
                             if isinstance(node, ast.With) and any(
                                 isinstance(item, ast.Constant) and item.value == "authoritative-run.log"
                                 for item in ast.walk(node)))
        terminal = ast.Module(body=tree.body[owned_command + 1:], type_ignores=[])
        self.assertFalse(any(isinstance(node, (ast.Import, ast.ImportFrom, ast.With))
                             for node in ast.walk(terminal)))
        self.assertFalse(any(isinstance(node, ast.Name) and node.id in ("subprocess", "process", "os")
                             for node in ast.walk(terminal)))
        envelope = Path("/synthetic-repo/docs/plans/s3-hardening-resource-envelope.md")
        archive = Path("/synthetic-output/runner-inputs/run.py")
        hashes = {envelope: "envelope", archive: "runner"}
        if changed in ("envelope", "runner"):
            hashes[envelope if changed == "envelope" else archive] = "CHANGED"

        def exit_driver(code):
            raise SystemExit(code)

        values = {
            "code": child_exit, "candidate": "candidate", "fingerprint": "source",
            "acceptance": {"sha": lambda: "CHANGED" if changed == "sha" else "candidate",
                           "source_fingerprint": lambda: "CHANGED" if changed == "source" else "source"},
            "freeze": {"resource_envelope_sha256": "envelope"},
            "external_inputs": {"runner-inputs/run.py": "runner"},
            "repo": Path("/synthetic-repo"), "output": Path("/synthetic-output"),
            "digest": lambda path: hashes[path], "record": {}, "started": 100,
            "time": SimpleNamespace(monotonic=lambda: 102), "datetime": datetime,
            "json": json, "save": mock.Mock(), "print": mock.Mock(),
            "sys": SimpleNamespace(exit=exit_driver),
        }
        with self.assertRaises(SystemExit) as stopped:
            exec(compile(terminal, str(DRIVER), "exec"), values)
        values["save"].assert_called_once_with("invocation.json", values["record"])
        self.assertEqual(stopped.exception.code, values["record"]["exit_status"])
        return values["record"]

    def test_unchanged_success_preserves_child_outcome(self):
        result = self.terminal()
        self.assertEqual("passed", result["status"])
        self.assertEqual(0, result["exit_status"])
        self.assertEqual(0, result["child_exit_status"])
        self.assertEqual([], result["freeze_failures"])

    def test_each_frozen_input_drift_fails_a_successful_child(self):
        for changed in ("sha", "source", "envelope", "runner"):
            with self.subTest(changed=changed):
                result = self.terminal(changed=changed)
                self.assertEqual("failed", result["status"])
                self.assertNotEqual(0, result["exit_status"])
                self.assertEqual(0, result["child_exit_status"])
                self.assertEqual(1, len(result["freeze_failures"]))

    def test_child_failure_is_preserved(self):
        result = self.terminal(child_exit=124)
        self.assertEqual("failed", result["status"])
        self.assertEqual(124, result["exit_status"])
        self.assertEqual(124, result["child_exit_status"])
        self.assertEqual([], result["freeze_failures"])

    def test_drift_does_not_replace_an_existing_child_failure(self):
        result = self.terminal(child_exit=7, changed="envelope")
        self.assertEqual("failed", result["status"])
        self.assertEqual(7, result["exit_status"])
        self.assertEqual(7, result["child_exit_status"])
        self.assertEqual(1, len(result["freeze_failures"]))

    def test_regression_is_archived_with_driver_inputs(self):
        self.assertIn(DRIVER.with_name("test_runner_freeze_gate.py").name,
                      [node.value for node in ast.walk(ast.parse(DRIVER.read_text()))
                       if isinstance(node, ast.Constant) and isinstance(node.value, str)])


if __name__ == "__main__":
    unittest.main()
