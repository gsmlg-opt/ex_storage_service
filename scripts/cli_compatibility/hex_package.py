"""Bounded integrity verification for the Hex v3 source packages we publish."""

import hashlib
from pathlib import Path
import re
import tarfile


PACKAGE_LIMIT = 64 * 1024 * 1024
MEMBERS = {"VERSION", "CHECKSUM", "metadata.config", "contents.tar.gz"}


def read_package(path):
    if Path(path).stat().st_size > PACKAGE_LIMIT:
        raise RuntimeError("Hex package exceeds the fixture budget")
    values = {}
    with tarfile.open(path) as archive:
        for member in archive:
            if member.name not in MEMBERS or member.name in values or not member.isfile():
                raise RuntimeError("Unexpected or duplicate Hex package entry")
            if member.size > PACKAGE_LIMIT:
                raise RuntimeError("Hex package entry exceeds the fixture budget")
            values[member.name] = archive.extractfile(member).read(PACKAGE_LIMIT + 1)
    if values.keys() != MEMBERS or values["VERSION"] != b"3":
        raise RuntimeError("Expected a complete Hex v3 package")
    checksum = values["CHECKSUM"].decode("ascii").strip().lower()
    actual = hashlib.sha256(
        values["VERSION"] + values["metadata.config"] + values["contents.tar.gz"]
    ).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", checksum) or checksum != actual:
        raise RuntimeError("Hex source payload checksum mismatch")
    return checksum, values["contents.tar.gz"]
