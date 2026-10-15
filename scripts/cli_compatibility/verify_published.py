#!/usr/bin/env python3
"""Require the registry's Hex payload to match the independently validated tar."""

import argparse
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request

from hex_package import PACKAGE_LIMIT, read_package


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?", args.version):
        parser.error("Invalid release version")
    expected = json.loads(args.manifest.read_text())
    if expected["version"] != args.version:
        parser.error("Manifest version differs from the release")
    url = f"https://repo.hex.pm/tarballs/ex_storage_service_cli-{args.version}.tar"
    for attempt in range(12):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = response.read(PACKAGE_LIMIT + 1)
            break
        except urllib.error.HTTPError as error:
            if error.code != 404 or attempt == 11:
                raise
            time.sleep(5)
    if len(payload) > PACKAGE_LIMIT:
        raise RuntimeError("Registry tar exceeds its budget")
    with args.output.open("xb") as stream:
        stream.write(payload)
    actual, _contents = read_package(args.output)
    if actual != expected["hex_inner_checksum"]:
        raise RuntimeError("Published Hex source payload differs from the validated release package")
    print(json.dumps({"version": args.version, "hex_inner_checksum": actual, "verified_url": url}))


if __name__ == "__main__":
    main()
