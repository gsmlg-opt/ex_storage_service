"""Contract tests for fail-closed aggregate evidence handling; no ESS runtime."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import venv
from types import SimpleNamespace
from unittest.mock import patch

from e2e import hardening_acceptance as acceptance


class AggregateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ledger = self.root / "ledger.json"
        self.log = self.root / "executed.log"
        self.log.write_text("real assertion output fixture\n")
        self.rows = [{"id": f"A{index:02}", "status": "passed",
                      "assertion_audit": ["named test covers every required assertion"],
                      "evidence": [{"path": "executed.log"}],
                      "verification": {"command": "test", "exit_status": 0,
                                       "mode": "standalone", "toolchain": "pinned",
                                       "starting_sha": "source", "ending_sha": "source",
                                       "source_sha256": "fingerprint"}}
                     for index in range(1, 39)]
        for profile in acceptance.PROFILES:
            (self.root / f"{profile}.json").write_text(json.dumps({
                "profile": profile, "status": "passed", "ending_sha": "source",
                "source_sha256": "fingerprint",
                "commands": [{"exit_status": 0, "log": "executed.log"}]}))

    def audit(self):
        self.ledger.write_text(json.dumps({"scenarios": self.rows}))
        with patch.object(acceptance, "REPO", self.root), \
                patch.object(acceptance, "sha", return_value="source"), \
                patch.object(acceptance, "source_fingerprint", return_value="fingerprint"), \
                contextlib.redirect_stdout(io.StringIO()):
            return acceptance.audit(self.ledger, self.root)

    def test_requires_all_38_unique_audited_scenarios(self):
        self.assertEqual([], self.audit())
        self.rows[-1]["id"] = "A01"
        self.assertTrue(any("exactly once" in error for error in self.audit()))

    def test_missing_or_empty_row_source_is_rejected(self):
        self.assertEqual([], self.audit())
        for index in (0, -1):
            row = self.rows[index]
            original = row["verification"].copy()
            for value in (None, ""):
                with self.subTest(row=row["id"], value=value):
                    row["verification"] = original.copy()
                    if value is None:
                        row["verification"].pop("source_sha256")
                    else:
                        row["verification"]["source_sha256"] = value
                    try:
                        self.assertIn(f"{row['id']}: missing verification.source_sha256", self.audit())
                    finally:
                        row["verification"] = original.copy()

    def test_stale_row_source_is_rejected_with_current_lanes(self):
        self.assertEqual([], self.audit())
        for index in (0, -1):
            row = self.rows[index]
            with self.subTest(row=row["id"]):
                row["verification"]["source_sha256"] = "stale"
                try:
                    self.assertEqual(
                        [f"{row['id']}: verification belongs to different source contents"], self.audit())
                finally:
                    row["verification"]["source_sha256"] = "fingerprint"

    def test_stale_row_revision_is_rejected_with_current_source_and_lanes(self):
        self.assertEqual([], self.audit())
        for index in (0, -1):
            row = self.rows[index]
            with self.subTest(row=row["id"]):
                row["verification"]["ending_sha"] = "stale"
                try:
                    self.assertEqual([f"{row['id']}: verification belongs to another revision"], self.audit())
                finally:
                    row["verification"]["ending_sha"] = "source"

    def test_pending_blocked_and_empty_audit_never_pass(self):
        for status in ("pending", "blocked", "in_progress"):
            self.rows[0]["status"] = status
            self.assertTrue(self.audit())
        self.rows[0]["status"] = "passed"
        self.rows[0]["assertion_audit"] = []
        self.assertTrue(any("assertion audit" in error for error in self.audit()))

    def test_unexecuted_failed_or_stale_lanes_never_pass(self):
        resource = self.root / "resources.json"
        original = json.loads(resource.read_text())
        resource.unlink()
        self.assertTrue(any("no lane execution" in error for error in self.audit()))
        for update in ({"status": "blocked"}, {"source_sha256": "stale"},
                       {"commands": [{"exit_status": 124, "log": "executed.log"}]}):
            resource.write_text(json.dumps(dict(original, **update)))
            self.assertTrue(self.audit())

    def test_nonzero_exit_and_missing_or_escaping_evidence_never_pass(self):
        self.rows[0]["verification"]["exit_status"] = 1
        self.assertTrue(self.audit())
        self.rows[0]["verification"]["exit_status"] = 0
        for path in ("missing.log", "../outside.log"):
            self.rows[0]["evidence"] = [{"path": path}]
            self.assertTrue(self.audit())

    def test_selected_python_keeps_fresh_virtualenv_imports(self):
        environment = self.root / "venv"
        venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        executable = environment / "bin/python"
        self.assertTrue(executable.is_symlink())
        library = Path(subprocess.check_output(
            [str(executable), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
            text=True, timeout=10).strip())
        (library / "ess_hardening_venv_only_probe.py").write_text("value = 'selected-virtualenv'\n")

        for configured in (str(executable), os.path.relpath(executable)):
            with self.subTest(configured=configured):
                args = SimpleNamespace(python=configured, mc="/verified/mc")
                selected = acceptance.commands("gateway", args, "isolated")[0][1][0]
                self.assertTrue(Path(selected).is_absolute())
                result = subprocess.run(
                    [selected, "-I", "-c", "import ess_hardening_venv_only_probe as probe; print(probe.value)"],
                    capture_output=True, text=True, timeout=10)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertEqual("selected-virtualenv\n", result.stdout)

    def test_standalone_retains_existing_signed_sdk_admin_and_persistence_lane(self):
        python = str(Path("/usr/bin/python3").absolute())
        self.assertEqual([
            ("prod", ["bash", "e2e/run_hardening_phase2_standalone.sh", python, "isolated"]),
            ("prod", ["bash", "e2e/run_hardening_retained_standalone.sh", python, "isolated-retained"]),
        ], acceptance.commands("standalone", SimpleNamespace(python="/usr/bin/python3"), "isolated"))

    def test_index_requires_native_scale_and_actual_cluster_rebuild_lanes(self):
        python = str(Path("/usr/bin/python3").absolute())
        self.assertEqual([
            ("prod", ["bash", "e2e/run_hardening_index.sh", python, "isolated"]),
            ("prod", ["bash", "e2e/run_hardening_index_cluster.sh", python, "isolated-cluster"]),
        ], acceptance.commands("index", SimpleNamespace(python="/usr/bin/python3"), "isolated"))

    def test_cluster_preserves_finite_tagged_deadline_and_independent_s3_lane(self):
        python = str(Path("/usr/bin/python3").absolute())
        self.assertEqual([
            ("test", ["mix", "test", "apps/ex_storage_service/test/ex_storage_service/cluster/three_voter_cluster_test.exs",
                      "--include", "cluster"]),
            ("prod", ["bash", "e2e/run_hardening_phase1_cluster.sh", python, "isolated"]),
        ], acceptance.commands("cluster", SimpleNamespace(python="/usr/bin/python3"), "isolated"))

    def test_cluster_workflow_preserves_tagged_timeout(self):
        workflow = (acceptance.REPO / ".github/workflows/cluster-e2e.yml").read_text()
        tagged_step = workflow.split("- name: Run tagged three-voter test", 1)[1].split("- name:", 1)[0]
        self.assertIn("mix test apps/ex_storage_service/test/ex_storage_service/cluster/three_voter_cluster_test.exs", tagged_step)
        self.assertIn("--include cluster", tagged_step)
        self.assertNotIn("--trace", tagged_step)
        self.assertNotIn("--timeout", tagged_step)
        tagged_test = (acceptance.REPO / "apps/ex_storage_service/test/ex_storage_service/cluster/three_voter_cluster_test.exs").read_text()
        self.assertIn("@moduletag timeout: 120_000", tagged_test)

    def test_fault_crash_resilience_lanes_are_required_and_use_disposable_wrappers(self):
        for profile in ("faults", "crashes", "resilience", "resources"):
            self.assertIn(profile, acceptance.PROFILES)
            expected = [("prod", ["bash", f"e2e/run_hardening_{profile}.sh", str(Path("/usr/bin/python3").absolute()), "isolated"])]
            if profile == "crashes":
                expected.append(("prod", ["bash", "e2e/run_hardening_replication_crash.sh",
                                          str(Path("/usr/bin/python3").absolute()), "isolated-replication"]))
            if profile == "resilience":
                expected.append(("prod", ["bash", "e2e/run_hardening_replication_backlog.sh",
                                          str(Path("/usr/bin/python3").absolute()), "isolated-backlog"]))
            if profile == "resources":
                expected.append(("prod", ["bash", "e2e/run_hardening_cluster_resources.sh",
                                          str(Path("/usr/bin/python3").absolute()), "isolated-cluster"]))
            self.assertEqual(expected,
                             acceptance.commands(profile, SimpleNamespace(python="/usr/bin/python3"), "isolated"))
            path = self.root / f"{profile}.json"
            path.unlink()
            self.assertTrue(any(profile + ": pending" in error for error in self.audit()))


if __name__ == "__main__":
    unittest.main()
