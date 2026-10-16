#!/usr/bin/env python3
"""Require the registry's Hex payload to match the independently validated tar."""

import argparse
import json
from pathlib import Path
import re
import tempfile
import time
import urllib.error
import urllib.request

from hex_package import PACKAGE_LIMIT, read_package


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--package-name", choices=("ex_storage_service_cli", "ex_storage_service"),
                        default="ex_storage_service_cli")
    parser.add_argument("--candidate-package", type=Path)
    parser.add_argument("--allow-missing", action="store_true")
    parser.add_argument("--status-file", type=Path)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?", args.version):
        parser.error("Invalid release version")
    expected = json.loads(args.manifest.read_text())
    if expected["version"] != args.version:
        parser.error("Manifest version differs from the release")
    checksum = expected["hex_inner_checksum"]
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        parser.error("Manifest has an invalid Hex inner checksum")
    if args.candidate_package:
        candidate, _contents = read_package(args.candidate_package)
        if candidate != checksum:
            raise RuntimeError("Candidate Hex source payload differs from the validated release manifest")
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError(args.output)
    url = f"https://repo.hex.pm/tarballs/{args.package_name}-{args.version}.tar"
    result = {"version": args.version, "verified_url": url}
    for attempt in range(12):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = response.read(PACKAGE_LIMIT + 1)
            break
        except urllib.error.HTTPError as error:
            if error.code == 404 and args.allow_missing:
                result["status"] = "absent"
                break
            if error.code != 404 or attempt == 11:
                raise
            time.sleep(5)
    if result.get("status") != "absent":
        if len(payload) > PACKAGE_LIMIT:
            raise RuntimeError("Registry tar exceeds its budget")
        with tempfile.TemporaryDirectory(prefix="hex-verify-") as temporary:
            downloaded = Path(temporary) / "registry.tar"
            downloaded.write_bytes(payload)
            actual, _contents = read_package(downloaded)
        if actual != checksum:
            raise RuntimeError("Published Hex source payload differs from the validated release package")
        with args.output.open("xb") as stream:
            stream.write(payload)
        result.update(status="reused" if args.allow_missing else "verified",
                      hex_inner_checksum=actual)
    if args.status_file:
        args.status_file.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))

    return result


if __name__ == "__main__":
    main()
