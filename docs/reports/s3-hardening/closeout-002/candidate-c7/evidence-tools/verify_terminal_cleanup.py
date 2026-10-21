"""Prepared C7 read-only cleanup verifier; root reviews and executes serially.

Never reads samples, credentials, seed/control files, process argv or environments.
Never kills, removes, connects to service endpoints or changes a fixture.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys

import terminal_evidence as helper

REPO = Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
RUNNER = Path(__file__).resolve().parent
CANDIDATE = REPO / "docs/reports/s3-hardening/closeout-002/candidate-c7"
REPORTS = REPO / "docs/reports/s3-hardening"
SHA = "fcc02e148ae31f00022d563ebce2b039be5d32dd"
FINGERPRINT = "ee754b0345b3198e84abd78e898b121e966787c1faa563642097ca3151653913"
MAX_METADATA = 16 * 1024 * 1024
ROOT_PATTERN = re.compile(r"/tmp/(ess-(?:s3-hardening|hardening|c7-real-concord-stage)-[A-Za-z0-9_-]+)")
NODE_PATTERN = re.compile(r"\b((?:ess_(?:meta|data_a|data_b|gateway_[a-f0-9]{10}|restore_[a-f0-9]{12}_(?:source|recovered)_(?:meta|data_a|data_b|standalone))|gateway_ctl_[a-f0-9]{10}|resource_ctl_[a-f0-9]{10}))@127\.0\.0\.1\b")
PORT_PATTERN = re.compile(r"(?:127\.0\.0\.1|localhost|0\.0\.0\.0|\[::\]):([0-9]{1,5})\b")
CONTAINER_PATTERNS = (
    re.compile(r"ess-gateway-[a-f0-9]{12}"),
    re.compile(r"ess-hardening-resource-[a-f0-9]{12}"),
    re.compile(r"ess-hardening-fault-[a-f0-9]{12}"),
    re.compile(r"ess-release-[a-f0-9]{32}"),
)


def main():
    if sys.argv[1:] != ["--execute-after-parent-authorization"]:
        raise SystemExit("Prepared only; root authorization and terminal drivers required")
    output = CANDIDATE / "terminal-cleanup.json"
    if output.exists():
        raise RuntimeError("refusing to replace an existing cleanup receipt")
    provenance, failures = {}, []

    def read(path, allowed_roots=None):
        path = Path(path)
        raw = helper.bounded_read(path, allowed_roots or [CANDIDATE], max_bytes=MAX_METADATA)
        provenance[str(path)] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        return raw.decode("utf-8", errors="replace")

    def data(path, allowed_roots=None):
        return json.loads(read(path, allowed_roots))

    # No live probe starts until the actual driver and every lane are terminal.
    freeze = data(CANDIDATE / "freeze.json")
    helper.validate_freeze(freeze)
    drivers = {}
    record = data(CANDIDATE / "invocation.json")
    helper.validate_invocation(record, freeze)
    drivers[str(CANDIDATE)] = dict(status=record["status"], exit_status=record["exit_status"])
    acceptance = runpy.run_path(str(REPO / "e2e/hardening_acceptance.py"))
    current_sha = acceptance["sha"]()
    helper.require(acceptance["compatible_revision"](SHA, current_sha), "current source revision differs from C7")
    helper.require(acceptance["source_fingerprint"]() == FINGERPRINT,
                   "current executable inputs differ from C7")
    helper.require(tuple(acceptance["PROFILES"]) == helper.PROFILES, "all eleven lanes required")
    probes = []  # This candidate has no supplemental service invocation.

    roots, ports = {}, {9001: {"cluster_harness.sh:data-a"}, 9002: {"cluster_harness.sh:data-b"},
        9003: {"cluster_harness.sh:metadata role must not listen"},
        9101: {"cluster_harness.sh:data-a"}, 9102: {"cluster_harness.sh:data-b"},
        9103: {"cluster_harness.sh:metadata role must not listen"},
        19000: {"gateway_hardening.py:default ESS port"},
        19100: {"gateway_hardening.py:default RustFS port"}}
    names = {name: {"cluster_harness.sh:fixed isolated topology"}
             for name in ("ess_meta", "ess_data_a", "ess_data_b")}

    def references(text, source):
        for match in ROOT_PATTERN.finditer(text):
            root = "/tmp/" + match[1]
            if root not in ("/tmp/ess-s3-hardening-python", "/tmp/ess-s3-hardening-tools"):
                roots.setdefault(root, set()).add(source)
        for match in PORT_PATTERN.finditer(text):
            port = int(match[1])
            if 1 <= port <= 65535:
                ports.setdefault(port, set()).add(source)
        for match in NODE_PATTERN.finditer(text):
            names.setdefault(match[1], set()).add(source)

    lane_receipts, lane_validation = [], {}
    for name in helper.PROFILES:
        receipt = CANDIDATE / (name + ".json")
        record = data(receipt)
        lane_validation[name] = helper.validate_lane(record, name, freeze, acceptance, CANDIDATE)
        lane_receipts.append(str(receipt))
        for command in record["commands"]:
            references(json.dumps(command["argv"]), str(receipt))
            log = REPO / command["log"]
            references(read(log), str(log))

    fixture_inventory = []
    for virtual, sources in sorted(roots.items()):
        physical = RUNNER / "tmp" / Path(virtual).name
        present = physical.exists() or physical.is_symlink()
        info = physical.lstat() if present else None
        fixture_inventory.append(dict(virtual_root=virtual, physical_root=str(physical),
            referenced_by=sorted(sources), exists=present, symlink=physical.is_symlink(),
            device=info.st_dev if info else None, inode=info.st_ino if info else None,
            storage_payloads_not_read=True, runtime_logs_read_separately=True,
            deletion_authorized_by_this_receipt=False,
            credentials_policy="retain privately; root must review archives before removal"))
        # Runtime startup logs can disclose exact ephemeral endpoints/node names.
        # No seed/control/credential file is opened and no source text is emitted.
        helper.require(not physical.is_symlink(), "symlink fixture root refused")
        if present:
            helper.safe_directory_path(physical, [RUNNER / "tmp"])
            for pattern in ("beam-*.log", "server.log", "restarted-server.log", "logs/meta.log",
                            "logs/data-a.log", "logs/data-b.log"):
                for log in sorted(physical.glob(pattern)):
                    references(read(log, [physical]), str(log))

    pid_namespace = os.readlink("/proc/self/ns/pid")

    def identity(pid):
        try:
            fields = Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
            return dict(pid=int(pid), start_ticks=int(fields[19]), state=fields[0])
        except (FileNotFoundError, NotADirectoryError, ProcessLookupError):
            return None

    identities, observer_paths = [], {}
    for directory in REPORTS.glob("closeout002-c7-*"):
        helper.require(not directory.is_symlink(), "symlink evidence root refused")
        if not directory.is_dir():
            continue
        helper.safe_directory_path(directory, [REPORTS])
        for path in directory.rglob("observer-*.json"):
            observer_paths[path] = directory
    for item in fixture_inventory:
        physical = Path(item["physical_root"])
        if item["exists"]:
            for path in list(physical.glob("observer-*.json")) + list(physical.glob("resource-samples/*/observer-*.json")):
                observer_paths[path] = physical
    observer_count = 0
    for path in sorted(observer_paths):
        if path.name == "observer-stop-verification.json":
            continue
        observer_count += 1
        record = data(path, [observer_paths[path]])
        if record.get("namespaces", {}).get("pid") != pid_namespace:
            failures.append("observer identity PID namespace differs: " + str(path))
            continue
        for role in ("observer", "beam"):
            expected = record[role]
            actual = identity(expected["pid"])
            same = actual is not None and actual["start_ticks"] == int(expected["start_ticks"])
            running = same and actual["state"] != "Z"
            identities.append(dict(source=str(path), role=role, expected=expected, observed=actual,
                original_identity_absent=not same, original_identity_running=running,
                zombie=same and actual["state"] == "Z"))
            if same:
                failures.append("recorded C7 process identity remains (running or unreaped zombie): " + role)
    if not observer_count:
        failures.append("no C7 observer identity receipts discovered")
    for probe in probes:
        for mode in ("exercise", "recover"):
            path = probe / (mode + ".json")
            if path.is_file():
                record = data(path)
                expected = dict(pid=int(record["beam_os_pid"]), start_ticks=int(record["beam_proc_start_ticks"]))
                actual = identity(expected["pid"])
                same = actual is not None and actual["start_ticks"] == expected["start_ticks"]
                running = same and actual["state"] != "Z"
                identities.append(dict(source=str(path), role="supplemental_beam", expected=expected,
                    observed=actual, original_identity_absent=not same, original_identity_running=running))
                if same:
                    failures.append("supplemental BEAM identity remains (running or unreaped zombie)")

    # Query name-only metadata; never inspect full host process argv/environment.
    epmd = subprocess.run(["epmd", "-names"], capture_output=True, text=True, timeout=10)
    registered = set(re.findall(r"^name ([A-Za-z0-9_-]+) at port [0-9]+$", epmd.stdout, re.M))
    matching_names = sorted(registered.intersection(names))
    if epmd.returncode or matching_names:
        failures.append("known Erlang names cannot be certified absent")

    socket_checks = {port: dict(sources=sorted(sources), listeners=0, other_tcp_states={})
                     for port, sources in sorted(ports.items())}
    for network_file in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        for line in network_file.read_text().splitlines()[1:]:
            fields = line.split()
            port = int(fields[1].split(":")[1], 16)
            if port in socket_checks:
                row = socket_checks[port]
                if fields[3] == "0A":
                    row["listeners"] += 1
                else:
                    states = row["other_tcp_states"]
                    states[fields[3]] = states.get(fields[3], 0) + 1
    if any(row["listeners"] for row in socket_checks.values()):
        failures.append("a known fixture listener port remains occupied; attribution unproven")

    containers = []
    for pattern in CONTAINER_PATTERNS:
        prefix = pattern.pattern.split("[")[0]
        result = subprocess.run(["docker", "ps", "--all", "--filter", "name=" + prefix,
            "--format", "{{.Names}}"], capture_output=True, text=True, timeout=15)
        matches = sorted(name for name in result.stdout.splitlines() if pattern.fullmatch(name))
        containers.append(dict(fixture_name_pattern=pattern.pattern, query_exit_status=result.returncode,
            matching_names=matches, absent=result.returncode == 0 and not matches,
            returned_container_ownership="not inferred; no container touched"))
        if result.returncode or matches:
            failures.append("fixture container namespace cannot be certified absent: " + pattern.pattern)

    for source in (Path(__file__), RUNNER / "terminal_evidence.py", RUNNER / "namespace-command.json",
                   REPO / "e2e/scripts/resource_observer.py", REPO / "e2e/cluster_harness.sh",
                   REPO / "e2e/gateway_hardening.py", REPO / "e2e/s3_hardening_resources.py",
                   REPO / "e2e/s3_hardening_faults.py", REPO / "scripts/release/release.py",
                   REPO / "e2e/hardening_acceptance.py"):
        read(source, [source])
    report = dict(task="ESS-S3-HARDENING-CLOSEOUT-002", candidate_sha=SHA, source_sha256=FINGERPRINT,
        current_sha=current_sha, current_executable_fingerprint_verified=True,
        recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), acceptance=False,
        purpose="read-only terminal fixture observation; fixture removal never promotes acceptance",
        status="passed" if not failures else "failed", exit_status=0 if not failures else 1,
        terminal_drivers=drivers, lane_receipts=lane_receipts, lane_validation=lane_validation,
        process_identity_checks=identities,
        verifier_pid_namespace=pid_namespace, exact_known_erlang_names={key: sorted(value) for key, value in names.items()},
        epmd_query_exit_status=epmd.returncode, matching_known_erlang_names=matching_names,
        exact_known_listener_ports=socket_checks, fixture_container_namespaces=containers,
        disposable_root_inventory=fixture_inventory, provenance=provenance, failures=failures,
        limits=["No resource sample JSONL, credentials, process argv/environments, or private seed/control files read",
            "Known PID/start identities only; zombie status is recorded separately from absence/reaping",
            "No independent proof for unrecorded/reparented descendants, native helpers, or dynamic node names missing from outputs",
            "Exact referenced ports only; occupied ports are not attributed to ESS without identity evidence",
            "Other TCP states are counts at referenced local ports, not complete connection ownership or cleanup proof",
            "Container prefix absence proves fixture namespace empty; returned matches are not assumed C7-owned",
            "Only output-referenced disposable roots inventoried; selected runtime logs and observer metadata read; storage payloads, credentials, seed/control files, unreferenced roots and storage usage uninspected",
            "Restore child PID namespaces and unrecorded helper ports remain outside direct PID verification",
            "No fixture/container/process removed; root must privately archive and review credentials before later cleanup"])
    with output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps(dict(receipt=str(output), status=report["status"], acceptance=False,
        identities=len(identities), roots=len(fixture_inventory), failures=len(failures))))
    raise SystemExit(report["exit_status"])


if __name__ == "__main__":
    main()
