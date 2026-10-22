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
        "7aa89ba62ebea474421dc251b4a1ee418ea013bda0230b7cbe5f81710b365a38",
        "75bd5e3be9aaeab7d673663bd73767129bac1eda97ecf944d3ac558619520f21"),
    "viewstamped_replication": (
        "e1c5d3f18b39b2a8a65569452219d6c37a58c01f9137115cb252853cc3e83137",
        "4052659b0afa6cf351a23e5e916db9f0a159afd9d623b55dc8b6227d3ecb1963"),
    "ex_turso": (
        "c6fdaf0ef160d011a1675b70dc21fbc3ec4900d66d17a4af132601f215f483ba",
        "47d4389790a51f488fbbaf457edf7a4f8b56b0678f4329c190354cece698f0fc"),
}
VERIFIED_SOURCES = {
    "viewstamped_replication/lib/viewstamped_replication/replica.ex":
        "bb62a681344538fee05a357082d0e87721deb5a3614c1344f07b4e2aa4e16541",
    "viewstamped_replication/lib/viewstamped_replication/protocol.ex":
        "133cd6e7532b4b8a6d841e8be39d07023229e5a8f71e3cc5c060b40bd8e64fb2",
    "viewstamped_replication/lib/viewstamped_replication/protocol/state.ex":
        "b97e99e709823e37d243b22e7203398eb9f5302ec2e1c4e266f8d4be26d12142",
    "viewstamped_replication/lib/viewstamped_replication/log.ex":
        "26e992fcad95826b75ca5beeb1adc0acc9a2e223904e68142696b4ce45d34a87",
    "viewstamped_replication/lib/viewstamped_replication/log/indexed.ex":
        "2d3b160486489a6b73ce7d4d1076abffaa902ef896230f67b625f423bee49164",
    "viewstamped_replication/lib/viewstamped_replication/storage/memory.ex":
        "241e2d793ac385c8b3a92d5e88b59f9a05ee65f41bf2a62404fa8213813d850e",
    "viewstamped_replication/lib/viewstamped_replication/storage/file.ex":
        "9a3c2a8bf45bd0220b930a4907f9e128cfc8e28b9dffd3bf04d3df5086aaae96",
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
    if set(versions.values()) == {"3.1.5"}:
        return ("BLOCKED A28: gsmlg-dev/concord#104 (singleton write history traversal); "
                "coordinated 3.1.5 is not admissible.")
    if set(versions.values()) != {"3.1.6"}:
        return (f"Unvalidated index dependency versions {versions}; require released upstream fixes "
                "for gsmlg-dev/concord#101/#102/#104, source/API review and both diagnostic probes "
                "before updating this gate. Refusing expensive acceptance workload.")

    for package in PACKAGES:
        if checksums[package] != VERIFIED_CHECKSUMS[package]:
            return f"Unverified {package} 3.1.6 Hex checksums; refusing index workload."
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
    print("Verified Concord/VSR/ExTurso 3.1.6; entering the unchanged A28 workload, not acceptance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
