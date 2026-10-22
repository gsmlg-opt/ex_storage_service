"""Admit only source-reviewed, diagnostically verified A28 dependencies.

This gate permits the unchanged expensive workload; it does not certify A28.
Future releases require source/API review and fresh minimal/production probes.
"""
import hashlib
import pathlib
import re
import sys


PACKAGES = ("concord", "viewstamped_replication", "ex_turso")
# Hex inner/package checksums for the independently verified published cohort.
VERIFIED_CHECKSUMS = {
    "concord": (
        "c46e5817e5fe50e5e0b75f5a4f569a1d352d51d9e3f10c0bc03b785d45e22bc1",
        "e10cf2b128c04f27cd3649e5020db70e4c0d3f04ca8a7aa0b89c799c10341d93"),
    "viewstamped_replication": (
        "209a01e721605daf0fd15ee848146c4098ec71eeae22fb999bab698cd60b28bf",
        "88a79917aeaa141be7caa8a99bed4f479af39e5335f43edbbe899d4790fb7833"),
    "ex_turso": (
        "355b1c21a3bc18fda0e66630bbc8b79aaa3d3e879b99876f12a4e2377b814fc8",
        "f1219b208f45bc53ef1f83c17122dab4316439bbb2e43cdf1507590fac60b66e"),
}
VERIFIED_SOURCES = {
    "viewstamped_replication/lib/viewstamped_replication/replica.ex":
        "d8672cdefc7f5ce5152091ba2e028b5c226494606deafa7fbd2374b7fd2df6a3",
    "concord/lib/concord/state_machine/core.ex":
        "85eed0f6afa87c52e239f2f8b36671338dce8ad56ad22637520248c8262b9f72",
    "concord/lib/concord/engine/vsr/state_machine.ex":
        "76da3884a9c779b0311507c2da3c9ed8dae390fd51438c71a850933a2c1fa306",
    "concord/lib/concord/state_machine.ex":
        "fb417d9970ded983b25fdd459e46d13b5c8c28a37f2d275dcb349164ce422699",
    "concord/lib/concord/state_machine/observer.ex":
        "f23fac22c4b17a4a8e66e60ad86da759455e671f93b396084c71026c965da668",
}


def blocker(lock, dependencies_root=pathlib.Path("deps")):
    versions = {}
    checksums = {}
    for package in PACKAGES:
        entries = re.findall(rf'^\s*"{package}":\s*(.+?)\s*$', lock, re.MULTILINE)
        if len(entries) != 1:
            return f"Cannot identify one locked {package} release; refusing unverified index workload."
        entry = re.fullmatch(
            rf'\{{:hex, :{package}, "([^"]+)", "([0-9a-f]{{64}})", '
            rf'\[:mix\], \[.*\], "hexpm", "([0-9a-f]{{64}})"\}},?', entries[0])
        if entry is None:
            return f"Cannot validate locked {package} Hex metadata; refusing unverified index workload."
        versions[package], inner, outer = entry.groups()
        checksums[package] = (inner, outer)

    if set(versions.values()) == {"3.1.4"}:
        return ("BLOCKED A28: gsmlg-dev/concord#101 (singleton read history traversal) "
                "and #102 (atomic transaction full-state materialization), reproduced on "
                "coordinated 3.1.4. No 10k/100k acceptance run is valid yet.")
    if set(versions.values()) == {"3.1.3"}:
        return "BLOCKED A28: gsmlg-dev/concord#99 and #100; coordinated 3.1.3 is not admissible."
    if set(versions.values()) != {"3.1.5"}:
        return (f"Unvalidated index dependency versions {versions}; require released upstream fixes "
                "for gsmlg-dev/concord#101/#102, source/API review and both diagnostic probes "
                "before updating this gate. Refusing expensive acceptance workload.")

    for package in PACKAGES:
        if checksums[package] != VERIFIED_CHECKSUMS[package]:
            return f"Unverified {package} 3.1.5 Hex checksums; refusing index workload."
    for relative, expected in VERIFIED_SOURCES.items():
        try:
            actual = hashlib.sha256((pathlib.Path(dependencies_root) / relative).read_bytes()).hexdigest()
        except OSError:
            return f"Cannot read reviewed dependency source {relative}; refusing index workload."
        if actual != expected:
            return f"Modified or unverified dependency source {relative}; refusing index workload."
    return None


def main():
    try:
        reason = blocker(pathlib.Path("mix.lock").read_text())
    except OSError as error:
        reason = f"Cannot read mix.lock: {error}; refusing index workload."
    if reason:
        print(reason, file=sys.stderr)
        return 1
    print("Verified Concord/VSR/ExTurso 3.1.5; entering the unchanged A28 workload, not acceptance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
