"""Keep automatic and release checks within the declared daily-use scope."""
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DailyWorkflowTest(unittest.TestCase):
    def workflow(self, name):
        return (ROOT / ".github/workflows" / name).read_text()

    def test_release_retains_daily_business_and_independent_artifact_gates(self):
        text = self.workflow("release.yml")
        for command in ("run_daily_unit_tests.sh all", "run_hardening_daily.sh",
                        "--verification-profile daily", "core_package.py consumer",
                        "release.py smoke", "verify_published.py"):
            self.assertIn(command, text)
        self.assertNotIn("          mix test\n", text)

    def test_reusable_publication_checks_fresh_candidates_and_public_consumers(self):
        core = self.workflow("publish-core.yml")
        cli = self.workflow("publish-cli.yml")
        self.assertIn("run_daily_unit_tests.sh core", core)
        self.assertIn("core-public-consumer", core)
        self.assertIn("run_daily_unit_tests.sh cli", cli)
        self.assertEqual(2, cli.count("--verification-profile daily"))
        self.assertIn("--package \"$RUNNER_TEMP/published-cli.tar\"", cli)

    def test_large_topologies_and_resource_jobs_require_manual_selection(self):
        for name in ("cluster-e2e.yml", "cloud-cache-e2e.yml", "e2e-test.yml"):
            text = self.workflow(name)
            self.assertIn("  workflow_dispatch:", text)
            self.assertNotIn("  push:", text)
            self.assertNotIn("  pull_request:", text)
        cli = self.workflow("cli-compatibility.yml")
        for job in ("unit-wire-executable", "resources", "providers"):
            self.assertIn(f"  {job}:\n    if: github.event_name == 'workflow_dispatch' && inputs.extended", cli)
        self.assertIn("default: false", cli)
        self.assertIn("run_daily_unit_tests.sh all", self.workflow("test.yml"))


if __name__ == "__main__":
    unittest.main()
