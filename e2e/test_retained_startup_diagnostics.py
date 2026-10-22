"""Safe diagnostic and hermetic retained-shell regressions; no services."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import retained_startup_diagnostics as diagnostic


class DiagnosticContractTest(unittest.TestCase):
    def test_private_log_bytes_never_enter_public_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = b"Authorization: Bearer CANARY_CREDENTIAL\n** (ArgumentError) Address already in use (os error 98)\n"
            log = root / "server.log"
            log.write_bytes(raw)
            value = diagnostic.record(root, "server", "early_exit", 1, [12001, 12002])
            public = json.dumps(value)
            self.assertNotIn("CANARY_CREDENTIAL", public)
            self.assertNotIn("Authorization", public)
            self.assertEqual(raw, log.read_bytes())
            self.assertEqual(hashlib.sha256(raw).hexdigest(), value["logs"][0]["sha256"])
            self.assertEqual(["address_in_use", "argument_error"], value["logs"][0]["observed_markers"])
            self.assertEqual(1, value["child_exit_status"])
            self.assertFalse(value["acceptance_pass"])

    def test_unknown_messages_are_not_reported_as_raw_excerpts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "seed.log").write_text("unknown failure CANARY_CREDENTIAL")
            value = diagnostic.record(root, "seed", "command_failure", 2, [12001, 12002])
            self.assertEqual([], value["logs"][0]["observed_markers"])
            self.assertNotIn("CANARY_CREDENTIAL", json.dumps(value))

    def test_scanned_tail_is_bounded_and_timeout_has_no_fabricated_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = b"ArgumentError" + b"x" * 70000
            (root / "server.log").write_bytes(raw)
            value = diagnostic.record(root, "server", "readiness_timeout", None, [12001, 12002])
            self.assertIsNone(value["child_exit_status"])
            self.assertEqual(65536, value["logs"][0]["scanned_tail_bytes"])
            self.assertEqual([], value["logs"][0]["observed_markers"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), value["logs"][0]["sha256"])

    def test_unbounded_fields_are_rejected(self):
        for phase, reason, status, ports in [("credential", "early_exit", 1, [1, 2]),
                ("server", "CANARY_CREDENTIAL", 1, [1, 2]),
                ("server", "early_exit", 256, [1, 2]),
                ("server", "early_exit", 1, [1, 65536])]:
            with self.assertRaises(ValueError):
                diagnostic.record(Path("/nonexistent"), phase, reason, status, ports)


@unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("setsid")
                     and shutil.which("bash"), "Linux bash/setsid required")
class RetainedShellContractTest(unittest.TestCase):
    def test_assets_failure_retains_actual_exit_and_private_log(self):
        self.check_shell_failure("assets", 37, "command_failure", 37)

    def test_server_early_exit_retains_wait_status_and_outer_failure(self):
        self.check_shell_failure("server", 43, "early_exit", 1)

    def check_shell_failure(self, phase, child_status, reason, outer_status):
        source = Path(__file__).resolve().parent
        canary = "RETAINED_SHELL_PRIVATE_CANARY"
        raw = f"Authorization: Bearer {canary}\n** (ArgumentError) fixture failure\n".encode()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            e2e = root / "e2e"
            e2e.mkdir()
            for name in ("run_hardening_retained_standalone.sh",
                         "retained_startup_diagnostics.py", "hardening_ports.py"):
                shutil.copyfile(source / name, e2e / name)

            commands = root / "bin"
            commands.mkdir()
            scripts = {
                "mktemp": """#!/usr/bin/env bash
set -eu
[[ "$*" == '-d /tmp/ess-hardening-retained-XXXXXXXX' ]] || exit 98
mkdir -- "$PWD/private-root"
printf '%s\\n' "$PWD/private-root"
""",
                "curl": "#!/usr/bin/env bash\nexit 7\n",
                "mix": """#!/usr/bin/env bash
set -eu
printf '%s\\n' "$*" >> "$PWD/mix-calls"
case "$*" in
  assets.deploy)
    if [[ "$FAKE_FAILURE_PHASE" == assets ]]; then
      printf '%s' "$FAKE_PRIVATE_LOG"
      exit "$FAKE_CHILD_STATUS"
    fi ;;
  'run --no-start --no-compile e2e/scripts/seed_e2e.exs')
    printf 'E2E_ACCESS_KEY_ID=%s\\n' "$FAKE_CANARY" > "$E2E_GITHUB_ENV" ;;
  'run --no-compile --no-halt')
    printf '%s' "$FAKE_PRIVATE_LOG"
    exit "$FAKE_CHILD_STATUS" ;;
  *) exit 99 ;;
esac
""",
            }
            for name, content in scripts.items():
                path = commands / name
                path.write_text(content)
                path.chmod(0o700)

            result = subprocess.run(
                [shutil.which("bash"), "e2e/run_hardening_retained_standalone.sh",
                 sys.executable, "hermetic-shell"],
                cwd=root, capture_output=True, timeout=10,
                env={**os.environ, "PATH": str(commands) + os.pathsep + os.environ["PATH"],
                     "FAKE_FAILURE_PHASE": phase, "FAKE_CHILD_STATUS": str(child_status),
                     "FAKE_CANARY": canary, "FAKE_PRIVATE_LOG": raw.decode()},
            )
            self.assertEqual(outer_status, result.returncode, "retained shell exit changed")
            receipt = root / "docs/reports/s3-hardening/hermetic-shell/startup-failure.json"
            public = receipt.read_bytes()
            self.assertFalse(canary.encode() in public + result.stdout + result.stderr,
                             "private canary leaked into public output")
            value = json.loads(public)
            self.assertEqual(phase, value["phase"])
            self.assertEqual(reason, value["reason"])
            self.assertEqual(child_status, value["child_exit_status"])
            self.assertFalse(value["acceptance_pass"])
            self.assertTrue(value["raw_logs_private"])
            private_root = root / "private-root"
            log = private_root / f"{phase}.log"
            self.assertTrue(raw == log.read_bytes(), "private log bytes changed")
            summary = next(item for item in value["logs"] if item["name"] == log.name)
            self.assertEqual(hashlib.sha256(raw).hexdigest(), summary["sha256"])
            self.assertEqual(len(raw), summary["bytes"])
            self.assertEqual(["argument_error"], summary["observed_markers"])
            self.assertFalse((private_root / "credentials.env").exists())
            expected_calls = ["assets.deploy"]
            if phase == "server":
                expected_calls += ["run --no-start --no-compile e2e/scripts/seed_e2e.exs",
                                   "run --no-compile --no-halt"]
            self.assertEqual(expected_calls, (root / "mix-calls").read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
