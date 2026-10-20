"""Coverage is identified by case, size and concurrency, not a passing count."""
import unittest
from resource_cases import manifest, verify_coverage


class ResourceCasesTest(unittest.TestCase):
    def test_all_required_paths_sizes_and_concurrencies(self):
        for mode, count in (("all", 56), ("cluster", 20), ("upstream", 8)):
            expected = manifest(mode)
            self.assertEqual(count, len(expected))
            verify_coverage(expected, expected)
            for groups in (expected[:-1], expected + expected[:1],
                           expected[:-1] + expected[:1]):
                with self.assertRaises(AssertionError):
                    verify_coverage(groups, expected)
            with self.assertRaises(AssertionError):
                verify_coverage([dict(row, failures=["gap"]) for row in expected], expected)


if __name__ == "__main__":
    unittest.main()
