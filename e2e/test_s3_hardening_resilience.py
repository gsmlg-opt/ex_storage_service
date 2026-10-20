"""Control failures must not expose distributed-node argv in public evidence."""
import subprocess
import sys
import tempfile
import traceback
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s3_hardening_resilience import control


class ControlErrorsTest(unittest.TestCase):
    def test_nonzero_exit_omits_command_arguments(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "resilience-state.json"
            with patch("s3_hardening_resilience.subprocess.run",
                       return_value=subprocess.CompletedProcess(["private-command"], 1)):
                with self.assertRaisesRegex(RuntimeError, "exited 1; see private control log") as caught:
                    control("rejoined-all", path, {})
            self.assertNotIn("private-command", str(caught.exception))

    def test_timeout_suppresses_command_and_exception_chain(self):
        secret = "private-cookie-regression-sentinel"
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "resilience-state.json"
            with patch("s3_hardening_resilience.subprocess.run", side_effect=
                       subprocess.TimeoutExpired(["elixir", "--cookie", secret], 60)):
                try:
                    control("rejoined-all", path, {})
                except RuntimeError:
                    rendered = traceback.format_exc()
                else:
                    self.fail("timeout unexpectedly succeeded")
            self.assertIn("see private control log", rendered)
            self.assertNotIn(secret, rendered)
            self.assertNotIn("TimeoutExpired", rendered)


if __name__ == "__main__":
    unittest.main()
