"""Check stage-file cleanup independently of byte-only resource samples."""
from pathlib import Path
import tempfile
import unittest

import s3_hardening_resources as resources
from scripts.resource_observer import temporary_bytes


class ResourceStagingTest(unittest.TestCase):
    def test_zero_byte_stages_are_visible_without_counting_published_cache(self):
        with tempfile.TemporaryDirectory(prefix="ess-staging-inventory-") as directory:
            data = Path(directory)
            (data / "tmp").mkdir()
            cache = data / "resource-gateway/cache/ab"
            cache.mkdir(parents=True)
            (data / "tmp/hosted-protected-empty").touch()
            (cache / "hash.nonce.tmp").touch()
            (cache / "published-hash").write_bytes(b"completed cache bytes")
            self.assertEqual(0, temporary_bytes(data / "tmp", data))
            self.assertEqual({"files": 2, "bytes": 0}, resources.staging_snapshot(data))

    def test_hidden_cache_stage_and_nested_local_stage_are_counted(self):
        with tempfile.TemporaryDirectory(prefix="ess-staging-inventory-") as directory:
            data = Path(directory)
            (data / "tmp/nested").mkdir(parents=True)
            (data / "tmp/nested/stage").write_bytes(b"local")
            (data / ".cache-stage-fixture").write_bytes(b"cache")
            self.assertEqual({"files": 2, "bytes": 10}, resources.staging_snapshot(data))

    def test_inventory_rejects_symlinks_and_missing_local_stage_root(self):
        with tempfile.TemporaryDirectory(prefix="ess-staging-inventory-") as directory:
            data = Path(directory)
            with self.assertRaises(FileNotFoundError):
                resources.staging_snapshot(data)
            (data / "tmp").mkdir()
            (data / "outside").write_bytes(b"must not follow")
            (data / "tmp/link").symlink_to(data / "outside")
            with self.assertRaisesRegex(AssertionError, "symlink"):
                resources.staging_snapshot(data)
            (data / "tmp/link").unlink()
            (data / "bucket").mkdir()
            (data / "bucket/cache").symlink_to(data / "missing")
            with self.assertRaisesRegex(AssertionError, "symlink"):
                resources.staging_snapshot(data)


if __name__ == "__main__":
    unittest.main()
