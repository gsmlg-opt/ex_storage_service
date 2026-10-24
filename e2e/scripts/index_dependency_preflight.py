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
        "7a941b40ca14468b8b70a9d008bae778d5b0ecb18036e9fe9a98b19a9ae80059",
        "77e9956c2371b07fa0a5f2af9d6a60ea3e069c019b87b06bde0468006bb7d646"),
    "viewstamped_replication": (
        "d24aa27721c3ece5b87e8ff8bd3d15374748322cdcf7eca3439790796c4c7292",
        "6ee3a55639d88dcf72c6c14038f246164e48526f1078bd350e6106f264e08e63"),
    "ex_turso": (
        "e64494c4b8ab29a039c398361b5f529656530bcd143f546547a9493030f4241d",
        "8b29934f107e0dafa35dcb232fa60390cbdd64fce17afdd0ae01632a1d11896f"),
}
VERIFIED_SOURCES = {
    "viewstamped_replication/lib/viewstamped_replication/replica.ex":
        "bb62a681344538fee05a357082d0e87721deb5a3614c1344f07b4e2aa4e16541",
    "viewstamped_replication/lib/viewstamped_replication/protocol.ex":
        "5720e495bab714513f88b960550cd8f95ace7c80d28e2995d27bf1935f78bd17",
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
    "concord/lib/concord/engine/turso.ex":
        "8f53f2f400c17ba7b6da388b1aef83399f5edbe01940511ef275cd31b5bbaa5f",
    "concord/lib/concord/turso/migrations.ex":
        "5b94751a61879f439e5ed4f6f33376739c9a98daf46c015e142fac617c5eff71",
    "concord/lib/concord/turso/codec.ex":
        "de4a0dbc3983432d6a10d2f79db1ca11845d3bb1ac52326145d32e885fc1f34d",
    "concord/lib/concord/kv.ex":
        "1804cf3e650164acc83ac7b91f767f2eb5d430cde847baefc78a5213768bf9a4",
    "concord/lib/concord/turso.ex":
        "1a2c72daea3eabd779c13a5ba3171e32f8ec4c6636a65b276994ff3715160c30",
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
    if set(versions.values()) != {"3.1.8"}:
        return (f"Unvalidated index dependency versions {versions}; require released upstream fixes "
                "for gsmlg-dev/concord#101/#102/#104/#105/#106, source/API review and both diagnostic probes "
                "before updating this gate. Refusing expensive acceptance workload.")

    for package in PACKAGES:
        if checksums[package] != VERIFIED_CHECKSUMS[package]:
            return f"Unverified {package} 3.1.8 Hex checksums; refusing index workload."
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
    print("Verified Concord/VSR/ExTurso 3.1.8; entering the unchanged A28 workload, not acceptance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
