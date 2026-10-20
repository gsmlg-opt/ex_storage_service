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


if __name__ == "__main__":
    unittest.main()
