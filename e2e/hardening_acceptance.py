#!/usr/bin/env python3
"""Run isolated acceptance lanes and audit all 38 mandatory scenario records.

Lane success is execution evidence, not an automatic scenario pass. The checked-in
scenario ledger must independently audit every assertion in each matrix row.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import time
from functools import lru_cache

REPO = Path(__file__).resolve().parents[1]
PROFILES = ("correctness", "standalone", "gateway", "cluster", "index", "resources", "faults", "crashes", "resilience", "restore", "release")


def sha():
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def executable_input(relative):
    return (relative.parts[0] in ("apps", "config", "e2e", "scripts", ".github")
        or (relative.parts[:3] == ("docs", "cli", "compatibility") and relative.suffix == ".json")
        or relative.name in ("mix.exs", "mix.lock", "Dockerfile", ".dockerignore", "package.json",
                             "package-lock.json", ".npmrc", ".formatter.exs"))


@lru_cache(maxsize=128)
def compatible_revision(candidate, current):
    """Reports may follow the frozen candidate without requiring their future SHA.

    Only report/document descendants qualify; the content fingerprint separately
    fences all working-tree executable inputs, including uncommitted files.
    """
    if candidate == current:
        return True
    if not isinstance(candidate, str) or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        return False
    if subprocess.run(["git", "merge-base", "--is-ancestor", candidate, current], cwd=REPO,
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        return False
    changes = subprocess.check_output(["git", "diff", "--name-only", "-z", candidate, current], cwd=REPO)
    return not any(executable_input(Path(os.fsdecode(name))) for name in changes.split(b"\0") if name)


def source_fingerprint():
    """Also fence uncommitted source changes; HEAD alone is not a build identity."""
    names = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=REPO)
    digest = hashlib.sha256()
    for name in sorted(set(names.split(b"\0")) - {b""}):
        relative = Path(os.fsdecode(name))
        if not executable_input(relative):
            continue
        path = REPO / relative
        digest.update(name + b"\0")
        if path.is_file():
            digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def stop_owned(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


def run_command(argv, environment, log, timeout):
    with log.open("wb") as output:
        process = subprocess.Popen(argv, cwd=REPO, env=environment, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_owned(process)
            return 124
        except BaseException:
            stop_owned(process)
            raise


def commands(profile, args, label):
    # Resolving a virtualenv executable symlink selects its base environment.
    python = str(Path(args.python).absolute())
    if profile == "correctness":
        return [("test", ["mix", "format", "--check-formatted"]),
                ("test", ["mix", "compile", "--force", "--warnings-as-errors"]),
                ("test", [python, "e2e/run_correctness_tests.py", "--extended", "--output",
                          str(args.output / "suite-selection")]),
                ("test", [python, "-m", "unittest", "discover", "-s", "e2e", "-p", "test_*.py"])]
    if profile == "standalone":
        return [("prod", ["bash", "e2e/run_hardening_phase2_standalone.sh", python, label]),
                ("prod", ["bash", "e2e/run_hardening_retained_standalone.sh", python, label + "-retained"])]
    if profile == "gateway":
        if not args.mc:
            raise ValueError("gateway requires --mc /absolute/verified/mc")
        # The gateway owner creates this fresh root and retains its result/logs.
        root = f"/tmp/ess-s3-hardening-{label}"
        return [("prod", [python, "e2e/gateway_hardening.py", "--root", root, "--mc", args.mc])]
    if profile == "cluster":
        return [("test", ["mix", "test", "apps/ex_storage_service/test/ex_storage_service/cluster/three_voter_cluster_test.exs",
                          "--include", "cluster"]),
                ("prod", ["bash", "e2e/run_hardening_phase1_cluster.sh", python, label])]
    if profile == "crashes":
        return [("prod", ["bash", "e2e/run_hardening_crashes.sh", python, label]),
                ("prod", ["bash", "e2e/run_hardening_replication_crash.sh", python, label + "-replication"])]
    if profile == "resources":
        return [("prod", ["bash", "e2e/run_hardening_resources.sh", python, label]),
                ("prod", ["bash", "e2e/run_hardening_cluster_resources.sh", python, label + "-cluster"]),
                ("test", ["mix", "test", "apps/ex_storage_service_cluster/test/ex_storage_service_cluster/large_stream_test.exs",
                          "--include", "large_stream", "--max-cases", "1", "--no-color"])]
    if profile == "resilience":
        return [("prod", ["bash", "e2e/run_hardening_resilience.sh", python, label]),
                ("prod", ["bash", "e2e/run_hardening_replication_backlog.sh", python, label + "-backlog"])]
    if profile == "index":
        return [("prod", ["bash", "e2e/run_hardening_index.sh", python, label]),
                ("prod", ["bash", "e2e/run_hardening_index_cluster.sh", python, label + "-cluster"])]
    if profile == "faults":
        return [("prod", ["bash", f"e2e/run_hardening_{profile}.sh", python, label])]
    if profile == "restore":
        return [("prod", ["bash", "e2e/run_hardening_restore.sh", python, label, "all"])]
    if profile == "release":
        return [("prod", ["bash", "e2e/run_hardening_release.sh", python, label])]
    raise ValueError("unknown profile")


def run_profile(profile, args):
    label = f"{args.label}-{profile}"
    result_path = args.output / f"{profile}.json"
    if result_path.exists():
        raise ValueError(f"refusing to overwrite previous lane result: {result_path}")
    record = {"profile": profile, "status": "in_progress", "starting_sha": sha(),
              "ending_sha": None, "source_sha256": source_fingerprint(), "commands": [],
              "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    write_json(result_path, record)
    environment = dict(os.environ, PAGER="cat", ESS_HARDENING_PHASE2="true", ESS_HARDENING_FAILURES="true")
    if args.mc:
        environment["ESS_HARDENING_MC"] = args.mc
    # Every test invocation uses a disposable root; mix's test alias removes it.
    environment["ESS_DATA_ROOT"] = f"/tmp/ess-s3-hardening-{label}-tests"
    try:
        steps = commands(profile, args, label)
        if profile not in ("correctness", "release"):
            steps.insert(0, ("prod", ["mix", "compile", "--force", "--warnings-as-errors"]))
        for index, (mode, argv) in enumerate(steps):
            log = args.output / f"{profile}-{index}.log"
            started = time.monotonic()
            code = run_command(argv, dict(environment, MIX_ENV=mode), log, args.timeout)
            record["commands"].append({"argv": argv, "mix_env": mode, "exit_status": code,
                                       "duration_seconds": round(time.monotonic() - started, 3),
                                       "log": str(log.relative_to(REPO))})
            write_json(result_path, record)
            if code:
                record.update(status="blocked", reason=f"command exited {code}; inspect retained log")
                # Scale and migration, and the two resource topologies, are
                # independent. Retain both real outcomes after a failed case;
                # a failed prerequisite compile still stops the whole lane.
                if profile not in ("index", "resources") or index == 0:
                    break
        else:
            if all(command["exit_status"] == 0 for command in record["commands"]):
                record["status"] = "passed"
    except (OSError, ValueError) as failure:
        record.update(status="blocked", reason=str(failure))
    except BaseException:
        record.update(status="blocked", reason="interrupted; owned command stopped")
        raise
    finally:
        if profile == "gateway":
            gateway_root = Path(f"/tmp/ess-s3-hardening-{label}")
            retained = args.output / "gateway-artifacts"
            retained.mkdir(exist_ok=True)
            # RPC input/output and credentials stay private in the disposable
            # root. Only sanitized diagnostics and the public result are copied.
            for artifact in [*gateway_root.glob("*.log"), gateway_root / "gateway-results.json"]:
                if artifact.is_file():
                    shutil.copyfile(artifact, retained / artifact.name)
        record["ending_sha"] = sha()
        if source_fingerprint() != record["source_sha256"]:
            record.update(status="blocked", reason="source changed during lane execution")
        write_json(result_path, record)
    print(f"{profile}: {record['status']} ({result_path.relative_to(REPO)})", flush=True)
    return record["status"] == "passed"


def audit(ledger, lane_directory):
    errors = []
    current_sha = sha()
    current_source = source_fingerprint()
    value = json.loads(ledger.read_text())
    rows = value.get("scenarios", [])
    expected = {f"A{number:02}" for number in range(1, 39)}
    actual = [row.get("id") for row in rows]
    if len(actual) != 38 or set(actual) != expected:
        errors.append("ledger must contain each of A01..A38 exactly once")
    for row in rows:
        name = row.get("id", "unknown")
        status = row.get("status", "pending")
        print(f"{name}: {status}")
        if status != "passed":
            errors.append(f"{name}: {status}; every mandatory row must pass")
            continue
        evidence = row.get("evidence", [])
        if not evidence:
            errors.append(f"{name}: passed row has no evidence")
        for item in evidence:
            path = (REPO / item.get("path", "")).resolve()
            if not path.is_relative_to(REPO) or not path.is_file() or path.stat().st_size == 0:
                errors.append(f"{name}: missing or unsafe evidence path")
        if value.get("schema", 1) >= 2:
            clauses = row.get("clauses", [])
            if not clauses:
                errors.append(f"{name}: missing clause-level acceptance audit")
            for clause in clauses:
                clause_name = clause.get("id", name + ".unknown")
                if clause.get("status") != "passed" or not clause.get("evidence"):
                    errors.append(f"{clause_name}: pending, failed or missing clause evidence")
                for item in clause.get("evidence", []):
                    path = (REPO / item.get("path", "")).resolve()
                    if not path.is_relative_to(REPO) or not path.is_file() or path.stat().st_size == 0:
                        errors.append(f"{clause_name}: missing or unsafe clause evidence path")
        # An explicit complete-row audit is required; log presence alone cannot
        # prove every assertion of a compound acceptance scenario.
        if not row.get("assertion_audit") or not isinstance(row.get("assertion_audit"), list) or not all(
                isinstance(item, str) and item for item in row["assertion_audit"]):
            errors.append(f"{name}: missing complete assertion audit")
        verification = row.get("verification", {})
        for field in ("command", "mode", "toolchain", "starting_sha", "ending_sha", "source_sha256"):
            if not verification.get(field):
                errors.append(f"{name}: missing verification.{field}")
        if not compatible_revision(verification.get("ending_sha"), current_sha):
            errors.append(f"{name}: verification belongs to another revision")
        if verification.get("source_sha256") != current_source:
            errors.append(f"{name}: verification belongs to different source contents")
        if verification.get("exit_status") != 0:
            errors.append(f"{name}: verification did not exit zero")
    for profile in PROFILES:
        path = lane_directory / f"{profile}.json"
        if not path.is_file():
            errors.append(f"{profile}: pending; no lane execution record")
            continue
        record = json.loads(path.read_text())
        print(f"profile {profile}: {record.get('status', 'pending')}")
        if (record.get("profile") != profile or record.get("status") != "passed"
                or not record.get("commands")
                or any(command.get("exit_status") != 0 for command in record["commands"])):
            errors.append(f"{profile}: blocked or incomplete execution")
        if not compatible_revision(record.get("ending_sha"), current_sha):
            errors.append(f"{profile}: execution belongs to another revision")
        if record.get("source_sha256") != current_source:
            errors.append(f"{profile}: execution belongs to different source contents")
        for command in record.get("commands", []):
            path = (REPO / command.get("log", "")).resolve()
            if not path.is_relative_to(REPO) or not path.is_file():
                errors.append(f"{profile}: missing command log")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "verify"))
    parser.add_argument("--profile", choices=(*PROFILES, "all"), default="all")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--mc")
    parser.add_argument("--label", default=datetime.datetime.now(datetime.timezone.utc).strftime("acceptance-%Y%m%dT%H%M%SZ"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=REPO / "docs/plans/s3-hardening-results.json")
    parser.add_argument("--timeout", type=int, default=10800)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.label) or args.timeout <= 0:
        parser.error("safe unique label and positive command timeout required")
    args.output = args.output.resolve()
    if not args.output.is_relative_to(REPO / "docs/reports/s3-hardening"):
        parser.error("output must be under docs/reports/s3-hardening")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.action == "run":
        selected = PROFILES if args.profile == "all" else (args.profile,)
        results = [run_profile(profile, args) for profile in selected]
        if args.profile != "all":
            return 0 if all(results) else 1
    errors = audit(args.ledger, args.output)
    write_json(args.output / "aggregate.json", {"goal": "ESS-S3-HARDENING-001",
               "status": "passed" if not errors else "blocked", "errors": errors})
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    # KeyboardInterrupt runs the owning harness cleanup before returning failure.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    sys.exit(main())
