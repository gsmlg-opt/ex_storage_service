"""Run the actual resource entry point with owned inert harness/tool fixtures."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ClusterResourceCleanupTest(unittest.TestCase):
    def test_cleanup_failures_change_outer_success_and_preserve_prior_failure(self):
        runner = Path(__file__).resolve().parent / "run_hardening_cluster_resources.sh"
        outcomes = []
        for operation, verifier, stop, expected in (
                (0, 0, 0, 0), (0, 11, 0, 1), (7, 0, 0, 7),
                (7, 11, 0, 7), (0, 0, 5, 1), (7, 11, 5, 7)):
            with self.subTest(operation=operation, verifier=verifier, stop=stop), \
                    tempfile.TemporaryDirectory(prefix="ess-resource-exit-") as directory:
                root = Path(directory)
                tools = root / "tools"
                tools.mkdir()
                e2e = root / "e2e"
                e2e.mkdir()

                def executable(path, source):
                    path.write_text("#!/bin/sh\nset -eu\n" + source)
                    path.chmod(0o700)

                executable(tools / "epmd", "exit 0\n")
                executable(tools / "mktemp", '''mkdir -p "$TEST_CLUSTER_ROOT"
printf '%s\\n' "$TEST_CLUSTER_ROOT"
''')
                executable(tools / "elixir", ''': > "$E2E_GITHUB_ENV"
exit 0
''')
                executable(e2e / "cluster_harness.sh", '''case "$1" in
  start)
    mkdir -p "$ESS_CLUSTER_E2E_ROOT/logs"
    for id in meta data-a data-b; do
      mkdir -p "$ESS_CLUSTER_E2E_ROOT/resource-samples/$id"
      printf 'fixture\\n' > "$ESS_CLUSTER_E2E_ROOT/logs/$id.log"
    done
    ;;
  wait-ready) ;;
  stop-all) exit "$TEST_STOP_STATUS" ;;
  *) exit 64 ;;
esac
''')
                python = tools / "pinned-python-fixture"
                executable(python, '''if [ "$1" = "-B" ]; then
  printf 'verified\\n' >> "$TEST_VERIFY_LOG"
  exit "$TEST_VERIFIER_STATUS"
fi
exit "$TEST_OPERATION_STATUS"
''')
                environment = dict(os.environ, PAGER="cat", PATH=str(tools) + os.pathsep + os.environ["PATH"],
                    TEST_CLUSTER_ROOT=str(root / "owned-cluster"), TEST_VERIFY_LOG=str(root / "verified.log"),
                    TEST_OPERATION_STATUS=str(operation), TEST_VERIFIER_STATUS=str(verifier), TEST_STOP_STATUS=str(stop))
                completed = subprocess.run(["bash", str(runner), str(python), "cleanup-regression"],
                    cwd=root, env=environment, capture_output=True, text=True, timeout=10)
                self.assertEqual(expected, completed.returncode, completed.stdout + completed.stderr)
                self.assertEqual(["verified"] * 3, (root / "verified.log").read_text().splitlines())
                self.assertEqual(3, len(list((root / "docs/reports/s3-hardening").glob("*-*.log"))))
                outcomes.append({"operation": operation, "verifier": verifier, "stop": stop,
                                 "expected": expected, "actual": completed.returncode,
                                 "all_observers_checked": True, "all_logs_retained": True})
        if "SAMPLER_EVIDENCE_ROOT" in os.environ:
            evidence = Path(os.environ["SAMPLER_EVIDENCE_ROOT"]) / "shell-cleanup-regression.json"
            evidence.write_text(json.dumps(outcomes, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
