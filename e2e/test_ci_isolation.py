"""Prepare hosted isolation without weakening global user-namespace policy."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "ci_isolation", Path(__file__).with_name("scripts") / "ci_isolation.py")
isolation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(isolation)


class HostedIsolationTest(unittest.TestCase):
    def prepare(self):
        isolation.prepare_runner(self.run)

    def setUp(self):
        self.commands = []
        self.run = self.commands.append
        for target, value in (("os.geteuid", 1001),
                              ("platform.freedesktop_os_release", {"ID": "ubuntu", "VERSION_ID": "24.04"}),
                              ("shutil.which", "/usr/bin/bwrap"),
                              ("Path.is_file", True),
                              ("Path.read_bytes", b"reviewed profile"),
                              ("Path.read_text", "Y\n")):
            owner, name = target.split(".")
            mock = patch.object(getattr(isolation, owner), name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        digest = patch.object(isolation, "PROFILE_SHA256",
                              isolation.hashlib.sha256(b"reviewed profile").hexdigest())
        digest.start()
        self.addCleanup(digest.stop)
        env = patch.dict(os.environ, GITHUB_ACTIONS="true", RUNNER_ENVIRONMENT="github-hosted")
        env.start()
        self.addCleanup(env.stop)

    def test_loads_packaged_restricted_profile_and_never_changes_global_policy(self):
        self.prepare()
        self.assertIn(["sudo", "-n", "apt-get", "install", "--yes", "apparmor",
                       "apparmor-profiles", "bubblewrap", "zsh"], self.commands)
        load = ["sudo", "-n", "apparmor_parser", "-r", str(isolation.PROFILE)]
        self.assertIn(load, self.commands)
        self.assertLess(self.commands.index(load),
                        self.commands.index(["sudo", "-n", "aa-status"]))
        self.assertFalse(any("sysctl" in command for command in self.commands))
        self.assertTrue(all(command[:2] == ["sudo", "-n"] for command in self.commands
                            if command[0] == "sudo"))

    def test_refuses_privileged_nonhosted_and_unknown_distribution_before_mutation(self):
        for target, value in (("os.geteuid", 0),
                              ("platform.freedesktop_os_release", {"ID": "ubuntu", "VERSION_ID": "26.04"})):
            owner, name = target.split(".")
            with self.subTest(target=target), \
                    patch.object(getattr(isolation, owner), name, return_value=value):
                with self.assertRaises(RuntimeError):
                    self.prepare()
                self.assertEqual([], self.commands)
        for name in ("GITHUB_ACTIONS", "RUNNER_ENVIRONMENT"):
            with self.subTest(name=name), patch.dict(os.environ, {name: "unexpected"}):
                with self.assertRaises(RuntimeError):
                    self.prepare()
                self.assertEqual([], self.commands)

    def test_missing_profile_or_different_bwrap_is_a_hard_prerequisite_failure(self):
        for target, value in (("Path.is_file", False), ("shutil.which", "/custom/bin/bwrap"),
                              ("Path.read_bytes", b"unreviewed policy")):
            owner, name = target.split(".")
            with self.subTest(target=target), \
                    patch.object(getattr(isolation, owner), name, return_value=value):
                self.commands.clear()
                with self.assertRaises(RuntimeError):
                    self.prepare()
                self.assertFalse(any(command[2:3] == ["apparmor_parser"] for command in self.commands))


class PrivateTmpfsTest(unittest.TestCase):
    def test_probe_checks_namespace_device_capacity_bytes_and_host_visibility(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            host_namespace = os.readlink("/proc/self/ns/mnt")
            commands = []

            def execute(argv, **kwargs):
                commands.append(argv)
                value = dict(uid=1001, namespace="private-mount-namespace", device=-1,
                             capacity=8 * 1024 * 1024, bytes=1024 * 1024,
                             sha256=isolation.PROBE_SHA256)
                value.update(changes)
                if leak:
                    (root / "tmp/private.bin").write_bytes(b"visible on host")
                return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")

            changes, leak = {}, False
            with patch.object(isolation.os, "geteuid", return_value=1001), \
                    patch.object(isolation.subprocess, "run", side_effect=execute):
                value = isolation.probe_private_tmpfs(root)
                self.assertEqual(8 * 1024 * 1024, value["capacity"])
                self.assertEqual("bwrap", commands[0][0])
                self.assertEqual("8388608", commands[0][commands[0].index("--size") + 1])
                self.assertNotIn("sudo", commands[0])
                for changes in ({"namespace": host_namespace}, {"device": (root / "tmp").stat().st_dev},
                                {"capacity": 16 * 1024 * 1024}, {"bytes": 0}, {"sha256": "wrong"}):
                    with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                        isolation.probe_private_tmpfs(root)
                changes, leak = {}, True
                with self.assertRaises(RuntimeError):
                    isolation.probe_private_tmpfs(root)

    def test_child_failure_and_timeout_propagate(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(isolation.os, "geteuid", return_value=1001):
            for error in (subprocess.CalledProcessError(1, ["bwrap"], stderr="uid map denied"),
                          subprocess.TimeoutExpired(["bwrap"], 15)):
                with self.subTest(error=error), \
                        patch.object(isolation.subprocess, "run", side_effect=error), \
                        self.assertRaises(type(error)):
                    isolation.probe_private_tmpfs(Path(directory))


class IsolationReceiptTest(unittest.TestCase):
    def test_failed_namespace_probe_retains_denial_and_does_not_report_success(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt"
            error = subprocess.CalledProcessError(1, ["bwrap"], stderr="uid map denied")
            with patch.object(isolation.sys, "argv", ["ci_isolation.py", "--output", str(output)]), \
                    patch.object(isolation, "prepare_runner"), \
                    patch.object(isolation, "probe_private_tmpfs", side_effect=error):
                self.assertEqual(1, isolation.main())
            receipt = json.loads((output / "isolation.json").read_text())
            self.assertEqual("failed", receipt["status"])
            self.assertEqual("uid map denied", receipt["command_stderr"])
            self.assertNotIn("private_tmpfs", receipt)


if __name__ == "__main__":
    unittest.main()
