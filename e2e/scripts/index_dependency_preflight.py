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
        "7d87a8eea12407afacd45e1520592ab4b825dd5e137da1083603877ba0c391dd",
        "b9c5f88ba9d8c313d5c7785c66e26e3062154660008144c299c2456a22717e99"),
    "viewstamped_replication": (
        "2082c55fa038e0feb64b036bfe22a0b948cf933f140bf3f3ac603fc6935ac5b6",
        "dd02eadcea5cc54106725e57596f8e05ceab867122c02cee024bae21a4e1e431"),
    "ex_turso": (
        "038041734f78656edc114a5bed5dbac7922af608e4cc78454c7e6f19ee4ea324",
        "d6b06493b371ad25578c956ac44e4b89cce9c7d4580bab26d3c82f1e3fc934dd"),
}
VERIFIED_SOURCES = {
    "viewstamped_replication/lib/viewstamped_replication/replica.ex":
        "bb62a681344538fee05a357082d0e87721deb5a3614c1344f07b4e2aa4e16541",
    "viewstamped_replication/lib/viewstamped_replication/protocol.ex":
        "14b79d1bbbe8423398bb69a33cbd2d6a8d350457ab8cee69df18b995f4544f4e",
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
        "656271d2fa12a3141fbf706c6c110df5fad5c68f9651d2975381a51021b3771c",
    "concord/lib/concord/engine/vsr/state_machine.ex":
        "6f83999ab0283639fd8f8265b0d89f67b8898d9678e17d9af650674a2c823f78",
    "concord/lib/concord/state_machine.ex":
        "fb417d9970ded983b25fdd459e46d13b5c8c28a37f2d275dcb349164ce422699",
    "concord/lib/concord/state_machine/observer.ex":
        "f23fac22c4b17a4a8e66e60ad86da759455e671f93b396084c71026c965da668",
    "concord/lib/concord/engine/turso.ex":
        "6e0a7e752e4c8eb2891abec2ea6337158d4e35baebb88f3ffad8b6237d974d16",
    "concord/lib/concord/turso/migrations.ex":
        "82373ca14c7b4433a9eb1a052270472edd0287517c550cc3878fae4ffff5d8d2",
    "concord/lib/concord/turso/codec.ex":
        "de4a0dbc3983432d6a10d2f79db1ca11845d3bb1ac52326145d32e885fc1f34d",
    "concord/lib/concord/kv.ex":
        "69fc31f1d7f085a655643f7d608664c3e20d301691f2034a6b4a2a48c5c57383",
    "concord/lib/concord/turso.ex":
        "2488ef7e7f2e8c813306cd02d90c29aa6569bc3691af3f36cc9b4f5c4e0b9a67",
    "concord/lib/concord/turso/prefix_replacement.ex":
        "5a8a2fc416b6cf641929e56ed964508acbef9e0d798f06ca393d1a6fce4c7909",
    "concord/lib/concord/engine/vsr.ex":
        "9d19de390234847c8ee885a3ee64939dc111078a650af43e01e3d190d85095e6",
    "concord/lib/concord/deadline.ex":
        "e64b777fb4749016e8bacc4354728bbead16e31d0c7218d019b9b6b1a9d34e72",
    "concord/lib/concord/command_schema.ex":
        "15b17e48e67e0f1b9e216269f1a26b1b7bc8126d2b31fa9282b291ba2606664e",
    "concord/lib/concord/command_envelope.ex":
        "9b334d24522585a8467bde86ba77a7fba7116f8cd2ec0b7ac0fd8bf6254f5a1c",
    "concord/lib/concord/turso/list_queue.ex":
        "5f6f02131b70de1ecb81d658a361f097a7710cdba712f2ed0b24089257d53d68",
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
    if set(versions.values()) == {"3.1.11"}:
        return ("BLOCKED A28: gsmlg-dev/concord#112 (multi-voter read history rebuilding); "
                "coordinated 3.1.11 is not admissible.")
    if set(versions.values()) != {"3.1.13"}:
        return (f"Unvalidated index dependency versions {versions}; require released upstream fixes "
                "for gsmlg-dev/concord#101/#102/#104/#105/#106/#112, source/API review and both diagnostic probes "
                "before updating this gate. Refusing expensive acceptance workload.")

    for package in PACKAGES:
        if checksums[package] != VERIFIED_CHECKSUMS[package]:
            return f"Unverified {package} 3.1.13 Hex checksums; refusing index workload."
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
    print("Verified Concord/VSR/ExTurso 3.1.13; entering the unchanged A28 workload, not acceptance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
