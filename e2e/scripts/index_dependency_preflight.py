"""Refuse expensive A28 runs until the diagnosed dependency blockers are fixed.

Version checks identify reproduced defects, not correctness. A future dependency
must be inspected and the minimal/production probes rerun before this gate is
updated to admit it; changing only the lockfile cannot certify that repair.
"""
import pathlib
import re
import sys


PACKAGES = ("concord", "viewstamped_replication", "ex_turso")


def blocker(lock):
    versions = {}
    for package in PACKAGES:
        matches = re.findall(
            rf'^\s*"{package}": \{{:hex, :{package}, "([^"]+)"',
            lock, re.MULTILINE)
        if len(matches) != 1:
            return f"Cannot identify one locked {package} release; refusing unverified index workload."
        versions[package] = matches[0]

    if set(versions.values()) == {"3.1.4"}:
        return ("BLOCKED A28: gsmlg-dev/concord#101 (singleton read history traversal) "
                "and #102 (atomic transaction full-state materialization), reproduced on "
                "coordinated 3.1.4. No 10k/100k acceptance run is valid yet.")
    if set(versions.values()) == {"3.1.3"}:
        return "BLOCKED A28: gsmlg-dev/concord#99 and #100; coordinated 3.1.3 is not admissible."
    return (f"Unvalidated index dependency versions {versions}; require released upstream fixes "
            "for gsmlg-dev/concord#101/#102, source/API review and both diagnostic probes "
            "before updating this gate. Refusing expensive acceptance workload.")


if __name__ == "__main__":
    print(blocker(pathlib.Path("mix.lock").read_text()), file=sys.stderr)
    sys.exit(1)
