"""Publication must reject stale, skipped and unrelated acceptance runs."""
import importlib.util
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("verify_hardening", Path(__file__).resolve().parents[1] /
                                            "scripts/release/verify_hardening.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
RELEASE_SPEC = importlib.util.spec_from_file_location("release_fixture_tools", SPEC.origin.replace(
    "verify_hardening.py", "release.py"))
release_tools = importlib.util.module_from_spec(RELEASE_SPEC)
RELEASE_SPEC.loader.exec_module(release_tools)
ACCEPTANCE = Path(__file__).with_name("hardening_acceptance.py")


class ReleaseGateTest(unittest.TestCase):
    def setUp(self):
        self.run = dict(head_sha="candidate", event="workflow_dispatch", status="completed",
                        conclusion="success", path=".github/workflows/s3-hardening.yml",
                        head_repository={"full_name": "gsmlg-opt/ex_storage_service"})
        self.jobs = [dict(name="Extended audit of all 38 scenarios and every lane", conclusion="success")]

    def check(self):
        gate.validate_run(self.run, self.jobs, "candidate", "gsmlg-opt/ex_storage_service")

    def test_accepts_only_successful_exact_candidate(self):
        self.check()
        for field, value in (("head_sha", "older"), ("event", "push"), ("status", "in_progress"),
                             ("conclusion", "failure"), ("path", ".github/workflows/test.yml"),
                             ("head_repository", {"full_name": "fork/ess"})):
            with self.subTest(field=field):
                original = self.run[field]
                self.run[field] = value
                with self.assertRaises(ValueError):
                    self.check()
                self.run[field] = original

    def test_daily_success_cannot_replace_the_full_job(self):
        for conclusion in ("skipped", "cancelled", "timed_out", "failure", None):
            self.jobs[0]["conclusion"] = conclusion
            with self.assertRaises(ValueError):
                self.check()
        self.jobs = []
        with self.assertRaises(ValueError):
            self.check()

    def test_duplicate_aggregate_jobs_are_rejected(self):
        self.jobs *= 2
        with self.assertRaises(ValueError):
            self.check()


class PreparedReleaseGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ess-release-gate-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-b", "main")
        self.git("config", "user.name", "Release Gate Test")
        self.git("config", "user.email", "release-gate@example.invalid")
        for name in release_tools.VERSION_FILES:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('@version "1.0.0"\n' if "_cli/" in name else 'version: "1.0.0"\n')
        self.git("add", ".")
        self.git("commit", "-m", "tested source")
        self.source = self.git("rev-parse", "HEAD")
        namespace = runpy.run_path(str(ACCEPTANCE))
        namespace["source_fingerprint"].__globals__["REPO"] = self.root
        self.fingerprint = namespace["source_fingerprint"]
        self.source_fingerprint = self.fingerprint()
        release_tools.set_versions(self.root, "1.0.1")
        self.git("add", ".")
        self.git("commit", "-m", "chore(release): v1.0.1", "-m", f"Release-Source: {self.source}")
        self.release = self.git("rev-parse", "HEAD")
        self.git("tag", "v1.0.1")
        self.prepared = dict(version="1.0.1", source_sha=self.source, release_sha=self.release,
                             reused=True, component_versions=release_tools.versions(self.root))
        self.prepared_path = Path(self.temp.name) / "prepared.json"
        self.prepared_path.write_text(json.dumps(self.prepared))
        root_patch = patch.object(gate, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], cwd=self.root, check=True,
                              capture_output=True, text=True).stdout.strip()

    def audit_command(self, failure=False):
        # Run the production fingerprint implementation against the actual
        # checked-out fixture, retaining version bytes in its ordinary scope.
        code = ("import pathlib,runpy,subprocess,sys\n"
                f"ns=runpy.run_path({str(ACCEPTANCE)!r})\n"
                "ns['source_fingerprint'].__globals__['REPO']=pathlib.Path.cwd()\n"
                f"assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()=={self.source!r}\n"
                f"assert ns['source_fingerprint']()=={self.source_fingerprint!r}\n"
                f"sys.exit({9 if failure else 0})\n")
        return [sys.executable, "-B", "-c", code]

    def assert_restored(self):
        self.assertEqual(self.release, self.git("rev-parse", "HEAD"))
        self.assertEqual("", self.git("status", "--porcelain"))
        self.assertEqual({"1.0.1"}, set(release_tools.versions(self.root).values()))

    def test_exact_version_only_retry_mapping_is_accepted(self):
        self.assertNotEqual(self.source_fingerprint, self.fingerprint())
        self.assertEqual(self.source, gate.validate_prepared(self.prepared, self.release))
        gate.audit_source(self.source, self.release, self.audit_command())
        self.assert_restored()

    def test_established_prepare_reuses_tag_from_source_and_release_checkouts(self):
        remote = Path(self.temp.name) / "origin.git"
        self.git("init", "--bare", str(remote))
        self.git("remote", "add", "origin", str(remote))
        self.git("push", "origin", "main", "refs/tags/v1.0.1")
        with patch.object(release_tools, "ROOT", self.root), \
                patch.dict(release_tools.os.environ, {"GITHUB_OUTPUT": ""}):
            for requested in (self.source, self.release):
                with self.subTest(requested=requested):
                    self.git("checkout", "--detach", requested)
                    release_tools.prepare("1.0.1", self.prepared_path)
                    prepared = json.loads(self.prepared_path.read_text())
                    self.assertTrue(prepared["reused"])
                    self.assertEqual(self.source, gate.validate_prepared(prepared, self.release))
                    gate.audit_source(self.source, self.release, self.audit_command())
                    self.assert_restored()

    def test_preparation_without_a_version_change_audits_the_same_commit(self):
        self.git("checkout", "--detach", self.source)
        prepared = dict(version="1.0.0", source_sha=self.source, release_sha=self.source,
                        component_versions=release_tools.versions(self.root))
        self.assertEqual(self.source, gate.validate_prepared(prepared, self.source))
        gate.audit_source(self.source, self.source, self.audit_command())
        self.assertEqual(self.source, self.git("rev-parse", "HEAD"))
        self.assertEqual("", self.git("status", "--porcelain"))

    def test_failed_audit_restores_clean_release_checkout(self):
        with self.assertRaises(subprocess.CalledProcessError) as failure:
            gate.audit_source(self.source, self.release, self.audit_command(failure=True))
        self.assertEqual(9, failure.exception.returncode)
        self.assert_restored()

    def test_mismatched_release_source_and_nonversion_changes_are_rejected(self):
        for value in (dict(self.prepared, release_sha=self.source),
                      dict(self.prepared, source_sha="f" * 40)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                gate.validate_prepared(value, self.release)
        (self.root / "untested").write_text("runtime change")
        self.git("add", ".")
        self.git("commit", "--amend", "--no-edit")
        changed = self.git("rev-parse", "HEAD")
        with self.assertRaises(ValueError):
            gate.validate_prepared(dict(self.prepared, release_sha=changed), changed)

    def test_version_file_whitespace_cannot_hide_in_release_mapping(self):
        path = self.root / "mix.exs"
        path.write_text(path.read_text() + "\n")
        self.git("add", ".")
        self.git("commit", "--amend", "--no-edit")
        changed = self.git("rev-parse", "HEAD")
        with self.assertRaises(ValueError):
            gate.validate_prepared(dict(self.prepared, release_sha=changed), changed)

    def test_missing_source_trailer_is_rejected(self):
        self.git("commit", "--amend", "-m", "release without provenance")
        changed = self.git("rev-parse", "HEAD")
        with self.assertRaises(ValueError):
            gate.validate_prepared(dict(self.prepared, release_sha=changed), changed)

    def test_main_requires_the_artifact_and_reaudits_original_source(self):
        actual_output, actual_run = subprocess.check_output, subprocess.run
        downloaded, audited = [], []
        run = dict(head_sha=self.source, event="workflow_dispatch", status="completed", conclusion="success",
                   path=".github/workflows/s3-hardening.yml", run_attempt=2,
                   head_repository={"full_name": "gsmlg-opt/ex_storage_service"})
        jobs = [{"jobs": [dict(name="Extended audit of all 38 scenarios and every lane", conclusion="success")]}]

        def output(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return json.dumps(jobs if "--paginate" in argv else run)
            return actual_output(argv, **kwargs)

        def execute(argv, **kwargs):
            if argv[:3] == ["gh", "run", "download"]:
                downloaded.append(argv)
                if missing_artifact:
                    raise subprocess.CalledProcessError(1, argv)
                return subprocess.CompletedProcess(argv, 0)
            if argv[:2] == [sys.executable, "e2e/hardening_acceptance.py"]:
                audited.append(argv)
                return actual_run(self.audit_command(), **kwargs)
            return actual_run(argv, **kwargs)

        argv = ["verify_hardening.py", "--repo", "gsmlg-opt/ex_storage_service", "--run-id", "123",
                "--prepared", str(self.prepared_path)]
        with patch.object(sys, "argv", argv), patch.object(subprocess, "check_output", side_effect=output), \
                patch.object(subprocess, "run", side_effect=execute):
            missing_artifact = True
            with self.assertRaises(subprocess.CalledProcessError):
                gate.main()
            self.assertEqual([], audited)
            self.assert_restored()
            missing_artifact = False
            gate.main()
            self.assertEqual(1, len(audited))
            self.assertEqual("docs/reports/s3-hardening/ci-123-2", audited[0][-1])
            self.assertIn("s3-hardening-verification", downloaded[-1])
            self.assert_restored()
            run["head_sha"] = self.release
            with self.assertRaises(ValueError):
                gate.main()
            self.assertEqual(1, len(audited))
            self.assertEqual(2, len(downloaded))
            self.assert_restored()


if __name__ == "__main__":
    unittest.main()
