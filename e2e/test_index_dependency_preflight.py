import contextlib
import hashlib
import io
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent / "scripts"))
import index_dependency_preflight as preflight


def lock(version="3.1.5"):
    return "\n".join(
        f'  "{name}": {{:hex, :{name}, "{version}", "{inner}", '
        f'[:mix], [], "hexpm", "{outer}"}},'
        for name, (inner, outer) in preflight.VERIFIED_CHECKSUMS.items())


class IndexDependencyPreflightTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.source = self.root / "deps" / "concord" / "reviewed.ex"
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(b"reviewed published source\n")
        sources = {"concord/reviewed.ex": hashlib.sha256(self.source.read_bytes()).hexdigest()}
        patch = mock.patch.dict(preflight.VERIFIED_SOURCES, sources, clear=True)
        patch.start()
        self.addCleanup(patch.stop)

    def blocker(self, contents):
        return preflight.blocker(contents, self.root / "deps")

    def test_verified_cohort_enters_unchanged_workload(self):
        self.assertIsNone(self.blocker(lock()))

    def test_reproduced_release_identifies_both_blockers(self):
        result = self.blocker(lock("3.1.4"))
        self.assertIn("#101", result)
        self.assertIn("#102", result)

    def test_historical_release_is_not_reenabled(self):
        self.assertIn("#99", self.blocker(lock("3.1.3")))

    def test_unknown_and_uncoordinated_releases_require_validation(self):
        for version in ("3.1.6", "3.2.0", "4.0.0"):
            with self.subTest(version=version):
                self.assertIn("Unvalidated", self.blocker(lock(version)))
        self.assertIn("Unvalidated", self.blocker(lock().replace(
            ':ex_turso, "3.1.5"', ':ex_turso, "3.1.4"')))

    def test_missing_or_duplicate_lock_entries_fail_closed(self):
        self.assertIn("Cannot identify", self.blocker(""))
        self.assertIn("Cannot identify", self.blocker(lock() + "\n" + lock()))

    def test_unverified_inner_or_outer_checksum_fails_closed(self):
        for package, checksums in preflight.VERIFIED_CHECKSUMS.items():
            for checksum in checksums:
                with self.subTest(package=package, checksum=checksum):
                    self.assertIn(f"Unverified {package}", self.blocker(
                        lock().replace(checksum, "0" * 64)))

    def test_malformed_or_non_hex_metadata_fails_closed(self):
        for malformed in (lock().replace('"hexpm"', '"private"', 1),
                          lock().replace(':hex, :concord', ':git, :concord', 1),
                          lock().replace('"concord": {', '"concord": invalid {', 1),
                          lock().replace('"3.1.5",', '"3.1.5", "extra",', 1)):
            with self.subTest(lock=malformed):
                self.assertIn("Cannot validate", self.blocker(malformed))

    def test_modified_or_missing_reviewed_source_fails_closed(self):
        self.source.write_bytes(b"local dependency patch\n")
        self.assertIn("Modified or unverified", self.blocker(lock()))
        self.source.unlink()
        self.assertIn("Cannot read reviewed", self.blocker(lock()))

    def run_cli(self):
        previous = pathlib.Path.cwd()
        self.addCleanup(os.chdir, previous)
        os.chdir(self.root)
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = preflight.main()
        return status, stdout.getvalue(), stderr.getvalue()

    def test_cli_success_returns_zero_without_certifying_acceptance(self):
        (self.root / "mix.lock").write_text(lock())
        status, stdout, stderr = self.run_cli()
        self.assertEqual(status, 0)
        self.assertIn("unchanged A28 workload, not acceptance", stdout)
        self.assertEqual(stderr, "")

    def test_cli_refusal_returns_one_and_explains_blocker(self):
        (self.root / "mix.lock").write_text(lock("3.1.4"))
        status, stdout, stderr = self.run_cli()
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertIn("#101", stderr)

    def test_cli_missing_lock_returns_one(self):
        status, stdout, stderr = self.run_cli()
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertIn("Cannot read mix.lock", stderr)


if __name__ == "__main__":
    unittest.main()
