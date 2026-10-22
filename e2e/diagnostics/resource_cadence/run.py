#!/usr/bin/env python3
"""One bounded diagnostic; never a full acceptance or publication receipt."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

from common import alive, diagnostic_mode, identity, write_json

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
TIMEOUT_SECONDS = 900
CLASSIFICATION = dict(acceptance_status="incomplete_diagnostic_subset",
                      acceptance_promotion=False, publication_eligible=False)


def write(path, value):
    write_json(path, dict(value, **CLASSIFICATION))


def initialize(label):
    mode = diagnostic_mode()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", label):
        raise ValueError("unique alphanumeric diagnostic label required")
    output = REPO / "docs/reports/s3-hardening" / label
    output.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(REPO / "e2e"))
    from hardening_acceptance import source_fingerprint
    write(output / "diagnostic.json", dict(status="prepared", label=label, mode=mode,
        selected_prefix_groups=6, observed_nodes=["data-a", "data-b"],
        traced_nodes=["data-a", "data-b"] if mode == "syscall-trace" else [],
        timeout_seconds=TIMEOUT_SECONDS, sampling_ms=25, coverage_limit_ms=100,
        source_head=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        source_fingerprint=source_fingerprint(),
        tool_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in sorted(HERE.iterdir()) if path.is_file()}))
    return output


def discover(output, owned):
    roots = set()
    for path in output.glob("data-*-launch.json"):
        launch = json.loads(path.read_text())
        roots.add(Path(launch["root"]))
        for key in ("observer", "beam"):
            expected = launch.get(key)
            if expected:
                owned[(expected["pid"], expected["start_ticks"])] = expected
        diagnostic = Path(launch["diagnostic"])
        ready = diagnostic / "monitor-ready.json"
        if ready.is_file():
            monitor = json.loads(ready.read_text())["monitor"]
            owned[(monitor["pid"], monitor["start_ticks"])] = monitor
        observer = launch["observer"]
        if launch["mode"] == "syscall-trace" and alive(observer):
            try:
                status = Path(f"/proc/{observer['pid']}/status").read_text()
                tracer_pid = next(int(line.split()[1]) for line in status.splitlines()
                                  if line.startswith("TracerPid:"))
                tracer = identity(tracer_pid) if tracer_pid else None
                if tracer:
                    owned[(tracer["pid"], tracer["start_ticks"])] = tracer
            except (OSError, StopIteration):
                pass
    for root in roots:
        for path in (root / "pids").glob("*"):
            try:
                expected = identity(int(path.read_text().strip()))
                if expected:
                    owned[(expected["pid"], expected["start_ticks"])] = expected
            except (OSError, ValueError):
                pass
    return roots


def signal_owned(expected, number):
    if alive(expected):
        try:
            os.kill(expected["pid"], number)
        except ProcessLookupError:
            pass


def group_members(pgid):
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) == pgid:
                found = identity(int(path.name))
                if found:
                    result.append(found)
        except (OSError, ValueError, IndexError):
            pass
    return result


def execute(python, label, output):
    preparation = json.loads((output / "diagnostic.json").read_text())
    mode = diagnostic_mode()
    if preparation["mode"] != mode:
        raise ValueError("diagnostic mode changed after initialization")
    launcher = HERE / "python-launcher"
    if (not Path(python).is_absolute() or not os.access(python, os.X_OK)
            or not os.access(launcher, os.X_OK)):
        raise ValueError("absolute executable pinned Python and executable launcher required")
    if (output / "started.json").exists() or (REPO / "docs/reports/s3-hardening" / (label + "-cluster")).exists():
        raise ValueError("diagnostic label already used")
    env = dict(os.environ, PAGER="cat", MIX_ENV="prod",
               ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2",
               ESS_DIAGNOSTIC_PINNED_PYTHON=python, ESS_DIAGNOSTIC_EVIDENCE=str(output),
               ESS_DIAGNOSTIC_MODE=mode)
    argv = ["bash", "e2e/run_hardening_cluster_resources.sh", str(launcher), label + "-cluster"]
    started = time.monotonic()
    owned = {}
    errors = []
    child = None
    roots = set()
    with (output / "run.log").open("xb") as log:
        try:
            child = subprocess.Popen(argv, cwd=REPO, env=env, stdout=log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            shell = identity(child.pid)
            owned[(shell["pid"], shell["start_ticks"])] = shell
            write(output / "started.json", dict(argv=argv, shell=shell, pgid=child.pid))
            progress = started
            while child.poll() is None:
                roots.update(discover(output, owned))
                now = time.monotonic()
                if now >= progress:
                    print(json.dumps(dict(status="running", elapsed_seconds=round(now - started, 1),
                                          **CLASSIFICATION)), flush=True)
                    progress = now + 30
                if now - started >= TIMEOUT_SECONDS:
                    raise TimeoutError("diagnostic 900-second bound exceeded")
                time.sleep(.25)
        except BaseException as error:
            errors.append(type(error).__name__)
            if child is not None and child.poll() is None:
                # The freshly created process group is owned until wait/reaping.
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
        finally:
            roots.update(discover(output, owned))
            # Include surviving children even if bootstrap failed before either
            # data observer could publish its launch receipt.
            for member in group_members(child.pid) if child else []:
                owned[(member["pid"], member["start_ticks"])] = member
            # Natural fixture shutdown should finish all observers and tracers.
            deadline = time.monotonic() + 15
            while any(alive(value) for value in owned.values()) and time.monotonic() < deadline:
                time.sleep(.05)
            remaining = [value for value in owned.values() if alive(value)]
            if remaining:
                errors.append("owned_identity_required_emergency_cleanup")
                for value in remaining:
                    signal_owned(value, signal.SIGTERM)
                deadline = time.monotonic() + 5
                while any(alive(value) for value in remaining) and time.monotonic() < deadline:
                    time.sleep(.05)
                for value in remaining:
                    signal_owned(value, signal.SIGKILL)
                deadline = time.monotonic() + 2
                while any(alive(value) for value in remaining) and time.monotonic() < deadline:
                    time.sleep(.025)
            terminals = {}
            for root in roots:
                for node in ("data-a", "data-b"):
                    diagnostic = root / ("observer-diagnostic-" + node)
                    if diagnostic.is_dir():
                        shutil.copytree(diagnostic, output / diagnostic.name, dirs_exist_ok=True,
                                        ignore=shutil.ignore_patterns("*.fifo", "*.sock"))
                        terminal = diagnostic / "monitor-terminal.json"
                        if terminal.is_file():
                            terminals[node] = json.loads(terminal.read_text())
            if set(terminals) != {"data-a", "data-b"} or any(
                    row["status"] != "complete" or row["mode"] != mode for row in terminals.values()):
                errors.append("both_observer_full_lifetime_receipts_required")
            group = group_members(child.pid) if child else []
            if any(row["state"] != "Z" for row in group):
                errors.append("process_group_still_running")
            identities = [dict(expected=value, observed=identity(value["pid"]), stopped=not alive(value))
                          for value in owned.values()]
            if any(not row["stopped"] for row in identities):
                errors.append("owned_identity_still_running")
            status = child.returncode if child else None
            write(output / "diagnostic.json", dict(preparation,
                status="diagnostic_complete" if status == 0 and not errors else "failed",
                returncode=status, errors=errors, elapsed_seconds=time.monotonic() - started,
                owned_identities=identities, remaining_group_members=group,
                roots=[str(root) for root in roots], observed_nodes=sorted(terminals),
                traced_nodes=sorted(terminals) if mode == "syscall-trace" else [],
                provenance=json.loads((output / "started.json").read_text()) if child else None))
    return 0 if status == 0 and not errors else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--python")
    parser.add_argument("--initialize", action="store_true")
    args = parser.parse_args()
    if args.initialize:
        initialize(args.label)
        return 0
    if not args.python or not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        parser.error("--python and a valid unique --label are required")
    output = REPO / "docs/reports/s3-hardening" / args.label
    if not (output / "diagnostic.json").is_file():
        parser.error("initialize the diagnostic artifact before prerequisites")
    return execute(args.python, args.label, output)


if __name__ == "__main__":
    sys.exit(main())
