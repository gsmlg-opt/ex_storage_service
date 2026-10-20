"""Fail closed if complete correctness suites disappear from captured output."""
import unittest
from e2e.run_correctness_tests import MINIMUM_TESTS, counts, validate


class SelectionTest(unittest.TestCase):
    def test_actual_app_summaries_are_required(self):
        log = "\n".join(f"==> {app}\n{minimum} tests, 0 failures" for
                        app, minimum in MINIMUM_TESTS.items())
        self.assertEqual([], validate(counts(log)))
        self.assertTrue(validate(counts(log.replace("357 tests", "1 test"))))
        self.assertTrue(validate(counts(log.replace("==> ex_storage_service_s3", "==> wrong"))))

    def test_skipped_failed_and_missing_summaries_fail(self):
        self.assertTrue(validate(counts("")))
        self.assertEqual({"ex_storage_service": dict(tests=590, failures=1, excluded=1,
                                                    skipped=2, executed=587)},
                         counts("==> ex_storage_service\n590 tests, 1 failure, 1 excluded, 2 skipped"))

    def test_elixir_119_excluded_counts_match_118_discovered_and_executed_counts(self):
        old = "==> ex_storage_service\n599 tests, 0 failures, 1 excluded"
        new = "==> ex_storage_service\n598 tests, 0 failures (1 excluded)"
        expected = {"ex_storage_service": dict(tests=599, failures=0, excluded=1,
                                             skipped=0, executed=598)}
        self.assertEqual(expected, counts(old))
        self.assertEqual(expected, counts(new))
        # Hosted transport regression: 39 executed + one heavy exclusion still
        # discovers the unchanged minimum of 40 tests.
        self.assertEqual([], validate(counts(
            "==> ex_storage_service_cluster\n39 tests, 0 failures (1 excluded)"),
            {"ex_storage_service_cluster": 40}))

    def test_elixir_119_skips_fail_without_double_subtracting_exclusions(self):
        result = counts("==> ex_storage_service\n598 tests, 1 failure, 2 skipped (1 excluded)")
        self.assertEqual({"ex_storage_service": dict(tests=599, failures=1, excluded=1,
                                                    skipped=2, executed=596)}, result)
        self.assertTrue(validate(result, {"ex_storage_service": 599}))

    def test_ambiguous_or_truncated_summary_is_not_credited(self):
        for summary in ("599 tests, 0 failures, 1 excluded (1 excluded)",
                        "599 tests, 0 failures, 1 invalid"):
            with self.subTest(summary=summary):
                self.assertTrue(validate(counts("==> ex_storage_service\n" + summary),
                                         {"ex_storage_service": 599}))


if __name__ == "__main__":
    unittest.main()
