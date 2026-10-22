"""Retain daily interoperability and require full acceptance before publication."""
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DailyWorkflowTest(unittest.TestCase):
    def workflow(self, name):
        return (ROOT / ".github/workflows" / name).read_text()

    def test_cached_jobs_recompile_candidate_identity_before_running_child_mix(self):
        for name, job, consumer in (
                ("ci.yml", "compile", None),
                ("cluster-e2e.yml", "tagged-cluster", "mix test"),
                ("cluster-e2e.yml", "cluster-s3", "Start three-node cluster"),
                ("e2e-test.yml", "e2e", "mix assets.deploy"),
                ("cloud-cache-e2e.yml", "cloud-cache-e2e", "mix assets.deploy")):
            with self.subTest(workflow=name, job=job):
                text = self.workflow(name).split(f"  {job}:\n", 1)[1]
                # Next top-level job is outside this candidate's cache boundary.
                text = re.split(r"\n  [a-z][a-z0-9-]*:\n", text, maxsplit=1)[0]
                compile_at = text.index("mix compile --force --warnings-as-errors")
                if consumer:
                    self.assertLess(compile_at, text.index(consumer))

    def test_required_hosted_namespace_setup_retains_each_lane_diagnostics(self):
        correctness = self.workflow("test.yml")
        self.assertIn("runs-on: ubuntu-24.04", correctness)
        self.assertLess(correctness.index("ci_isolation.py --output"),
                        correctness.index("python3 -m unittest discover"))
        self.assertIn("name: correctness-isolation-prerequisite", correctness)
        extended = self.workflow("s3-hardening.yml").split("  extended-lane:\n", 1)[1]
        self.assertIn("runs-on: ubuntu-24.04", extended)
        self.assertIn("${{ matrix.profile }}-isolation'", extended)
        self.assertLess(extended.index("ci_isolation.py --output"),
                        extended.index("--profile '${{ matrix.profile }}'"))

    def test_extended_lanes_install_ripgrep_before_running_workloads(self):
        extended = self.workflow("s3-hardening.yml").split("  extended-lane:\n", 1)[1]
        extended = extended.split("  full-acceptance:\n", 1)[0]
        installation = extended.index("sudo -n apt-get install --yes ripgrep")
        # Namespace setup already updates the Ubuntu package index.
        self.assertLess(extended.index("ci_isolation.py --output"), installation)
        self.assertLess(installation, extended.index("rg --version"))
        self.assertLess(extended.index("rg --version"), extended.index("mix deps.get"))
        self.assertLess(installation, extended.index("--profile '${{ matrix.profile }}'"))

    def test_release_retains_daily_business_and_independent_artifact_gates(self):
        text = self.workflow("release.yml")
        for command in ("run_daily_unit_tests.sh all", "run_hardening_daily.sh",
                        "--verification-profile daily", "core_package.py consumer",
                        "release.py smoke", "verify_published.py"):
            self.assertIn(command, text)
        self.assertNotIn("          mix test\n", text)
        self.assertIn("verify_hardening.py", text)
        self.assertIn("inputs.acceptance_run_id", text)
        self.assertLess(text.index("release.py prepare"), text.index("verify_hardening.py"))
        self.assertLess(text.index("verify_hardening.py"), text.index("release.py commit"))
        self.assertIn('--prepared "$RUNNER_TEMP/prepared-release.json"', text)

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
        self.assertIn("run_correctness_tests.py", self.workflow("test.yml"))
        self.assertIn("if-no-files-found: error", self.workflow("test.yml"))


if __name__ == "__main__":
    unittest.main()
