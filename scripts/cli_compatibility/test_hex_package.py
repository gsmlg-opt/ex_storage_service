import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from hex_package import read_package


class HexPackageIntegrityTest(unittest.TestCase):
    def archive(self, root, changed=False, duplicate=False):
        metadata, contents = b"fixture metadata", b"fixture contents"
        checksum = hashlib.sha256(b"3" + metadata + contents).hexdigest().encode()
        values = [("VERSION", b"3"), ("CHECKSUM", checksum), ("metadata.config", metadata),
                  ("contents.tar.gz", contents + (b"tampered" if changed else b""))]
        if duplicate:
            values.append(("VERSION", b"3"))
        path = Path(root) / "package.tar"
        with tarfile.open(path, "w") as archive:
            for name, value in values:
                member = tarfile.TarInfo(name)
                member.size = len(value)
                archive.addfile(member, io.BytesIO(value))
        return path, checksum.decode(), contents

    def test_valid_payload_and_independent_corruption(self):
        with tempfile.TemporaryDirectory() as root:
            path, checksum, contents = self.archive(root)
            self.assertEqual((checksum, contents), read_package(path))
            path, _, _ = self.archive(root, changed=True)
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                read_package(path)

    def test_duplicate_entries_cannot_replace_verified_payload(self):
        with tempfile.TemporaryDirectory() as root:
            path, _, _ = self.archive(root, duplicate=True)
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                read_package(path)


if __name__ == "__main__":
    unittest.main()
