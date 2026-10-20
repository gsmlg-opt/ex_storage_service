"""Exercise redaction, event bounds and isolation with actual OTP trace sessions."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ResourceErrorTraceTest(unittest.TestCase):
    def test_real_trace_redacts_payloads_and_preserves_other_sessions(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="ess-resource-error-trace-") as directory:
            env = dict(os.environ, PAGER="cat", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
            completed = subprocess.run(
                ["elixir", "e2e/scripts/resource_error_trace_probe.exs", directory],
                cwd=repo, env=env, capture_output=True, text=True, timeout=15)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout), {
                "status": "passed", "redaction_cases": 5,
                "event_cap": 64, "sentinel_preserved": True})


if __name__ == "__main__":
    unittest.main()
