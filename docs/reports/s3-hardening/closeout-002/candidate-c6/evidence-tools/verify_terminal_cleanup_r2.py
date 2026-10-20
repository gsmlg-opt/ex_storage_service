"""Prepared C6 read-only cleanup verifier; root reviews and executes serially.

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

REPO = Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
RUNNER = Path(__file__).resolve().parent
CANDIDATE = REPO / "docs/reports/s3-hardening/closeout-002/candidate-c6"
REPORTS = REPO / "docs/reports/s3-hardening"
SHA = "69371bc0c2d00f63fd8f3ff0edc751c28a02d441"
FINGERPRINT = "97e56e9eb014454095aff5cc5501f3acc132dbbdcbd04d7d4555d46226031bcb"
MAX_METADATA = 16 * 1024 * 1024
ROOT_PATTERN = re.compile(r"/tmp/(ess-(?:s3-hardening|hardening|c6-real-concord-stage)-[A-Za-z0-9_-]+)")
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

    def read(path):
        path = Path(path)
        if path.name == "samples.jsonl" or path.suffix == ".jsonl":
            raise RuntimeError("resource sample reads forbidden")
        size = path.stat().st_size
        if size > MAX_METADATA:
            raise RuntimeError("bounded metadata/log limit exceeded: " + str(path))
        raw = path.read_bytes()
        provenance[str(path)] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        return raw.decode("utf-8", errors="replace")

    def data(path):
        return json.loads(read(path))

    # Nothing live is queried before both serial drivers are terminal.
    drivers = {}
    for directory in (CANDIDATE, CANDIDATE / "cache-prerequisite-retry"):
        record = data(directory / "invocation.json")
        if record.get("status") not in ("passed", "failed") or "exit_status" not in record:
            raise RuntimeError("original and cache retry must both be terminal")
        drivers[str(directory)] = dict(status=record["status"], exit_status=record["exit_status"])
    freeze = data(CANDIDATE / "freeze.json")
    if freeze["candidate_sha"] != SHA or freeze["source_sha256"] != FINGERPRINT:
        raise RuntimeError("unexpected C6 freeze")
    acceptance = runpy.run_path(str(REPO / "e2e/hardening_acceptance.py"))
    current_sha = acceptance["sha"]()
    if acceptance["source_fingerprint"]() != FINGERPRINT:
        raise RuntimeError("current executable inputs differ from C6")
    probes = [CANDIDATE / name for name in
              ("supplemental-real-concord-stage", "supplemental-real-concord-stage-r2")]
    for probe in probes:
        if probe.exists():
            record = data(probe / "invocation.json")
            if record.get("status") not in ("passed", "failed") or "exit_status" not in record:
                raise RuntimeError("supplemental probe must be terminal if present")

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

    lane_receipts = []
    for directory in (CANDIDATE, CANDIDATE / "cache-prerequisite-retry"):
        for name in ("correctness", "standalone", "gateway", "cluster", "index", "resources",
                     "faults", "crashes", "resilience", "restore", "release"):
            receipt = directory / (name + ".json")
            if not receipt.exists():
                continue
            record = data(receipt)
            if record.get("status") == "in_progress":
                raise RuntimeError("a C6 lane is still in progress")
            lane_receipts.append(str(receipt))
            # Only recorded lane commands/logs, never unrelated host logs.
            for command in record.get("commands", []):
                references(json.dumps(command.get("argv", [])), str(receipt))
                log = REPO / command["log"]
                if not log.is_relative_to(CANDIDATE) or not log.is_file():
                    raise RuntimeError("missing or out-of-scope lane log")
                references(read(log), str(log))

    # A29 was independently rerun outside the blocked original index entry.
    a29_log = CANDIDATE / "index-2.log"
    if a29_log.is_file() and str(a29_log) not in provenance:
        references(read(a29_log), str(a29_log))
    for probe in probes:
        for metadata in (probe / "invocation.json", probe / "exercise.json", probe / "recover.json"):
            if metadata.is_file():
                references(read(metadata), str(metadata))

    fixture_inventory = []
    for virtual, sources in sorted(roots.items()):
        physical = RUNNER / "tmp" / Path(virtual).name
        present = physical.exists() or physical.is_symlink()
        info = physical.lstat() if present else None
        fixture_inventory.append(dict(virtual_root=virtual, physical_root=str(physical),
            referenced_by=sorted(sources), exists=present, symlink=physical.is_symlink(),
            device=info.st_dev if info else None, inode=info.st_ino if info else None,
            contents_not_read=True, deletion_authorized_by_this_receipt=False,
            credentials_policy="retain privately; root must review archives before removal"))
        # Runtime startup logs can disclose exact ephemeral endpoints/node names.
        # No seed/control/credential file is opened and no source text is emitted.
        if present and not physical.is_symlink():
            for pattern in ("beam-*.log", "server.log", "restarted-server.log", "logs/meta.log",
                            "logs/data-a.log", "logs/data-b.log"):
                for log in sorted(physical.glob(pattern)):
                    if log.is_file() and not log.is_symlink():
                        references(read(log), str(log))

    pid_namespace = os.readlink("/proc/self/ns/pid")

    def identity(pid):
        try:
            fields = Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
            return dict(pid=int(pid), start_ticks=int(fields[19]), state=fields[0])
        except (FileNotFoundError, NotADirectoryError):
            return None

    identities, observer_paths = [], set()
    for directory in REPORTS.glob("closeout002-c6-*"):
        if directory.is_dir() and not directory.is_symlink():
            observer_paths.update(directory.rglob("observer-*.json"))
    for item in fixture_inventory:
        physical = Path(item["physical_root"])
        if item["exists"] and not item["symlink"]:
            observer_paths.update(physical.glob("observer-*.json"))
            observer_paths.update(physical.glob("resource-samples/*/observer-*.json"))
    observer_count = 0
    for path in sorted(observer_paths):
        if path.name == "observer-stop-verification.json":
            continue
        observer_count += 1
        record = data(path)
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
                failures.append("recorded C6 process identity remains (running or unreaped zombie): " + role)
    if not observer_count:
        failures.append("no C6 observer identity receipts discovered")
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

    for source in (Path(__file__), RUNNER / "namespace-command-r2.json",
                   REPO / "e2e/scripts/resource_observer.py", REPO / "e2e/cluster_harness.sh",
                   REPO / "e2e/gateway_hardening.py", REPO / "e2e/s3_hardening_resources.py",
                   REPO / "e2e/s3_hardening_faults.py", REPO / "scripts/release/release.py",
                   REPO / "e2e/hardening_acceptance.py"):
        read(source)
    report = dict(task="ESS-S3-HARDENING-CLOSEOUT-002", candidate_sha=SHA, source_sha256=FINGERPRINT,
        current_sha=current_sha, current_executable_fingerprint_verified=True,
        recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), acceptance=False,
        purpose="read-only terminal fixture observation; fixture removal never promotes acceptance",
        status="passed" if not failures else "failed", exit_status=0 if not failures else 1,
        terminal_drivers=drivers, lane_receipts=lane_receipts, process_identity_checks=identities,
        verifier_pid_namespace=pid_namespace, exact_known_erlang_names={key: sorted(value) for key, value in names.items()},
        epmd_query_exit_status=epmd.returncode, matching_known_erlang_names=matching_names,
        exact_known_listener_ports=socket_checks, fixture_container_namespaces=containers,
        disposable_root_inventory=fixture_inventory, provenance=provenance, failures=failures,
        limits=["No resource sample JSONL, credentials, process argv/environments, or private seed/control files read",
            "Known PID/start identities only; zombie status is recorded separately from absence/reaping",
            "No independent proof for unrecorded/reparented descendants, native helpers, or dynamic node names missing from outputs",
            "Exact referenced ports only; occupied ports are not attributed to ESS without identity evidence",
            "Other TCP states are counts at referenced local ports, not complete connection ownership or cleanup proof",
            "Container prefix absence proves fixture namespace empty; returned matches are not assumed C6-owned",
            "Only output-referenced disposable roots inventoried; contents, unreferenced roots and storage usage uninspected",
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
