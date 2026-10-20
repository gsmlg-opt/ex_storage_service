"""Prepared supplemental diagnostic. Execute only after explicit parent authorization.

No repository executable edits, no acceptance promotion, no fixture cleanup.
First unsuccessful BEAM invocation stops; original diagnostics are retained.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import signal
import subprocess
import sys
import time

if sys.argv[1:] != ["--execute-after-parent-authorization"]:
    raise SystemExit("Prepared only: explicit parent authorization is required before execution")

repo = Path("/home/gao/Workspace/gsmlg-opt/ex_storage_service")
runner = Path(__file__).resolve().parent
candidate = repo / "docs/reports/s3-hardening/closeout-002/candidate-c6"
for directory in (candidate, candidate / "cache-prerequisite-retry"):
    record = json.loads((directory / "invocation.json").read_text())
    if record.get("status") not in ("passed", "failed") or "exit_status" not in record:
        raise RuntimeError("original and prerequisite retry must both be terminal")
for receipt in candidate.glob("*.json"):
    data = json.loads(receipt.read_text())
    if isinstance(data, dict) and data.get("status") == "in_progress":
        raise RuntimeError("original lane still active")

freeze = json.loads((candidate / "freeze.json").read_text())
acceptance = runpy.run_path(str(repo / "e2e/hardening_acceptance.py"))
if acceptance["sha"]() != freeze["candidate_sha"] or acceptance["source_fingerprint"]() != freeze["source_sha256"]:
    raise RuntimeError("candidate inputs changed")

fixture = runner / "real_concord_stage_probe.exs"
namespace = json.loads((runner / "namespace-command-r2.json").read_text())
label = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
root = "/tmp/ess-c6-real-concord-stage-" + label
physical_root = runner / "tmp" / Path(root).name
output = candidate / "supplemental-real-concord-stage"
output.mkdir(exist_ok=False)
for source in (fixture, Path(__file__), runner / "namespace-command-r2.json"):
    shutil.copyfile(source, output / source.name)
environment = dict(os.environ, PAGER="cat", MIX_ENV="prod", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2",
    ESS_DATA_ROOT=root, ESS_BLOB_ROOT=root + "/blob", ESS_TMP_ROOT=root + "/tmp",
    ESS_METADATA_ROOT=root + "/metadata", ESS_RA_ROOT=root + "/ra", ESS_MODE="standalone",
    ESS_AUTO_START="false", ESS_WEB_ENABLED="false", ESS_PUBLIC_S3_ENABLED="false",
    ESS_NODE_ROLE="data", ESS_NODE_ID="c6-real-concord-probe", ESS_NODE_GENERATION="1",
    ESS_INSTANCE="c6-real-concord-probe", ESS_CONCORD_COMMAND_VERSION="0",
    ESS_MASTER_KEY="disposable-c6-real-concord-stage-probe", ESS_REPAIR_ENABLED="false",
    ESS_SCRUB_ENABLED="false", ESS_CLUSTER_BOOTSTRAP="false")
record = dict(acceptance=False, supplemental_only=True, candidate_sha=freeze["candidate_sha"],
    source_sha256=freeze["source_sha256"], fixture_sha256=hashlib.sha256(fixture.read_bytes()).hexdigest(),
    runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    fixture_root=root, physical_fixture_root=str(physical_root),
    status="in_progress", commands=[], original_and_retry_terminal=True)

def save():
    (output / "invocation.json").write_text(json.dumps(record, indent=2) + "\n")

save()
for mode in ("exercise", "recover"):
    argv = namespace + ["--", "mix", "run", "--no-compile", "--no-start", str(fixture), mode, root]
    started = time.monotonic()
    with (output / (mode + ".log")).open("xb") as log:
        process = subprocess.Popen(argv, cwd=repo, env=environment, stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=120)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            record.update(status="failed", reason="invocation interrupted or exceeded 120 second bound",
                exit_status=process.returncode)
            save()
            raise
    record["commands"].append(dict(mode=mode, argv=argv, exit_status=code,
        owning_wrapper_pid=process.pid, owning_wrapper_reaped=process.returncode is not None,
        duration_seconds=time.monotonic() - started))
    if code != 0:
        record.update(status="failed", exit_status=code)
        save()
        print(json.dumps(dict(output=str(output), status="failed", mode=mode, exit_status=code)))
        raise SystemExit(code)
    receipt = physical_root / (mode + ".json")
    data = json.loads(receipt.read_text())
    if data.get("acceptance") is not False or data.get("mode") != mode:
        raise RuntimeError("missing or unexpected supplemental receipt")
    stat = Path("/proc") / data["beam_os_pid"] / "stat"
    try:
        current_start = stat.read_text().split(") ", 1)[1].split()[19]
        same_beam_alive = current_start == data["beam_proc_start_ticks"]
    except FileNotFoundError:
        current_start = None
        same_beam_alive = False
    record["commands"][-1].update(beam_os_pid=data["beam_os_pid"],
        beam_proc_start_ticks=data["beam_proc_start_ticks"],
        beam_identity_stopped=not same_beam_alive,
        post_exit_current_start_ticks=current_start)
    shutil.copyfile(receipt, output / (mode + ".json"))
    record[mode + "_receipt_sha256"] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    if same_beam_alive:
        record.update(status="failed", exit_status=1, reason="owning BEAM OS identity still alive")
        save()
        raise SystemExit(1)
    save()

record.update(status="passed", exit_status=0,
    ending_sha=acceptance["sha"](), ending_source_sha256=acceptance["source_fingerprint"](),
    two_owning_invocations_exited_reaped=True,
    owning_beam_os_identities_verified_stopped=True,
    independent_os_descendant_cleanup="pending; no all-descendants shutdown claim",
    fixtures_retained=True)
if record["ending_source_sha256"] != record["source_sha256"]:
    record.update(status="failed", exit_status=1, reason="candidate inputs changed")
save()
print(json.dumps(dict(output=str(output), fixture_root=str(physical_root),
    status=record["status"], acceptance=False)))
raise SystemExit(record["exit_status"])
