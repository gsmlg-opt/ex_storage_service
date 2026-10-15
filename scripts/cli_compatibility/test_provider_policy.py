import copy
import json
import unittest

from provider_policy import ROOT, validate


class ProviderPolicyTest(unittest.TestCase):
    def setUp(self):
        self.report = json.loads((ROOT / "apps/ex_storage_service_cli/test/fixtures/providers/provider-acceptance.json").read_text())
        self.expected = json.loads((ROOT / "docs/cli/compatibility/provider-expectations.json").read_text())
        self.reference = json.loads((ROOT / "docs/cli/compatibility/reference-lock.json").read_text())["aws_cli"]
        self.artifact = self.report["artifact_sha256"]

    def test_retains_independently_proven_provider_failures(self):
        result = validate(self.report, self.expected, self.reference, self.artifact)
        self.assertEqual("known-provider-limitations", result["status"])
        self.assertEqual(4, result["provider_failures_retained"])

    def test_unexpected_or_missing_evidence_and_cleanup_are_fatal(self):
        mutations = [
            lambda r: r.update(independent_aws=False),
            lambda r: r.update(artifact_sha256="0" * 64),
            lambda r: r["aws_reference"].update(artifact_sha256="0" * 64),
            lambda r: r["providers"][0]["checks"].pop(),
            lambda r: r["providers"][0]["failures"][0].update(error="client regression"),
            lambda r: r["providers"][0]["cleanup"][0].update(status="failed"),
            lambda r: r["providers"][0]["content_encoding"]["gzip"].update(raw_bytes_verified=False),
            lambda r: r["providers"][1]["key_boundary"].update(ess_status=0),
            lambda r: r["providers"].pop(),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                report = copy.deepcopy(self.report)
                mutation(report)
                with self.assertRaises(RuntimeError):
                    validate(report, self.expected, self.reference, self.artifact)


if __name__ == "__main__":
    unittest.main()
