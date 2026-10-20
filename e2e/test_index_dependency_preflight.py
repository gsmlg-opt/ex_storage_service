import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parent / "scripts"))
from index_dependency_preflight import blocker


def lock(version):
    return "\n".join(f'  "{name}": {{:hex, :{name}, "{version}", "digest"}},'
                     for name in ("concord", "viewstamped_replication", "ex_turso"))


class IndexDependencyPreflightTest(unittest.TestCase):
    def test_reproduced_release_identifies_both_blockers(self):
        result = blocker(lock("3.1.4"))
        self.assertIn("#101", result)
        self.assertIn("#102", result)

    def test_historical_release_is_not_reenabled(self):
        self.assertIn("#99", blocker(lock("3.1.3")))

    def test_unknown_and_uncoordinated_releases_require_validation(self):
        self.assertIn("Unvalidated", blocker(lock("3.1.5")))
        self.assertIn("Unvalidated", blocker(lock("3.1.4").replace(
            ':ex_turso, "3.1.4"', ':ex_turso, "3.1.5"')))

    def test_missing_or_duplicate_lock_entries_fail_closed(self):
        self.assertIn("Cannot identify", blocker(""))
        self.assertIn("Cannot identify", blocker(lock("3.1.4") + "\n" + lock("3.1.4")))


if __name__ == "__main__":
    unittest.main()
