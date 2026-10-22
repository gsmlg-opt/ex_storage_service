#!/usr/bin/env python3
"""A32 actual whole-BEAM failure/restart at storage publication boundaries.

Uses existing per-call fault injection; no production switches or dependency edits.
Replication faults remain in the cluster lane. Private RPC files stay in /tmp.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import boto3
import botocore
from gateway_hardening import Runtime, client
from hardening_acceptance import source_fingerprint
from hardening_ports import free_port


def main():
    def terminate(_signal, _frame):
        raise KeyboardInterrupt("owned crash lane termination requested")
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    if not str(root).startswith("/tmp/ess-hardening-crash-") or root.exists():
        raise ValueError("new exclusively disposable root required")
    root.mkdir(mode=0o700)
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    port = free_port()
    runtime = Runtime(root, port)
    results = {"status": "in_progress", "groups": [], "versions": {
        "boto3": boto3.__version__, "botocore": botocore.__version__},
        "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree": "uncommitted hardening implementation",
        "source_sha256": source_fingerprint()}

    def control(action, data):
        sequence = uuid.uuid4().hex
        request, response = root / f"rpc-{sequence}-request.json", root / f"rpc-{sequence}-response.json"
        request.write_text(json.dumps(data))
        request.chmod(0o600)
        with open(root / "control.log", "ab") as output:
            completed = subprocess.run([
                "elixir", "--name", f"crash_ctl_{sequence[:10]}@127.0.0.1",
                "--cookie", runtime.cookie, "e2e/scripts/crash_hardening_control.exs",
                action, str(request), str(response)], env=runtime.env,
                stdout=output, stderr=subprocess.STDOUT, timeout=45)
        if action == "crash":
            assert completed.returncode != 0, "crash RPC unexpectedly succeeded"
            return
        if completed.returncode:
            raise RuntimeError(f"crash control {action} failed; private diagnostic log retained")
        return json.loads(response.read_text())

    try:
        runtime.start()
        credentials = control("seed", {"bucket": "crash-boundaries"})
        s3 = client(f"http://127.0.0.1:{port}", credentials["access_key"], credentials["secret_key"])
        s3.put_bucket_versioning(Bucket="crash-boundaries", VersioningConfiguration={"Status": "Enabled"})
        acknowledged = b"acknowledged-data-before-faults" * 1000
        committed = s3.put_object(Bucket="crash-boundaries", Key="acknowledged", Body=acknowledged)["VersionId"]
        for boundary in ["stage", "after_stage", "protection_data_sync", "protection_rename", "protection_directory_sync", "sync", "rename", "directory_sync", "after_blob_commit", "metadata_commit", "response"]:
            body = (boundary.encode() + b"-crash-body-") * 1000
            expected_hash = hashlib.sha256(body).hexdigest()
            operation_id = "crash-" + boundary
            witness = root / f"{boundary}-witness.json"
            before_pid = runtime.process.pid
            control("crash", {"bucket": "crash-boundaries", "key": boundary,
                "operation_id": operation_id, "boundary": boundary, "witness": str(witness),
                "body_base64": base64.b64encode(body).decode()})
            crash_state = json.loads(witness.read_text())
            assert crash_state["boundary"] == boundary
            anonymous = [item["name"] for item in crash_state["staging"] if item["name"].startswith("ess1-a-")]
            protected = [item["name"] for item in crash_state["staging"] if item["name"].startswith(("ess1-h-", "ess1-g-"))]
            assert runtime.process.wait(timeout=15) == 137
            runtime.stop()
            runtime.start()
            assert runtime.process.pid != before_pid
            response = s3.get_object(Bucket="crash-boundaries", Key="acknowledged", VersionId=committed)
            assert response["Body"].read() == acknowledged
            deadline = time.monotonic() + 20
            while True:
                state = control("inspect", {"bucket": "crash-boundaries", "key": boundary, "operation_id": operation_id})
                if not any(name in state["staging"] for name in anonymous):
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError("dead-owner anonymous stage was not reclaimed after restart")
                time.sleep(0.1)
            assert all(name in state["staging"] for name in protected), "protected recovery stage was removed"
            if boundary in ["after_stage", "protection_data_sync", "protection_rename"]:
                assert anonymous, "crash did not leave a managed anonymous stage"
            if boundary == "protection_directory_sync":
                assert protected, "crash did not leave a protected stage"
            if boundary == "response":
                assert state["object"]["exists"] and state["object"]["hash"] == expected_hash
                assert state["intent"] == "committed"
                response = s3.get_object(Bucket="crash-boundaries", Key=boundary)
                assert hashlib.sha256(response["Body"].read()).hexdigest() == expected_hash
                version = response["VersionId"]
                versions = s3.list_object_versions(Bucket="crash-boundaries", Prefix=boundary)["Versions"]
                assert [item["VersionId"] for item in versions] == [version]
            else:
                assert not state["object"]["exists"], "uncommitted object became visible"
                assert state["intent"] == ("absent" if boundary in ["stage", "after_stage", "protection_data_sync", "protection_rename", "protection_directory_sync"] else "pending")
                try:
                    s3.get_object(Bucket="crash-boundaries", Key=boundary)
                except botocore.exceptions.ClientError as failure:
                    assert failure.response["Error"]["Code"] == "NoSuchKey"
                else:
                    raise AssertionError("uncommitted object served after restart")
            row = {"boundary": boundary, "status": "passed", "process_exit": 137,
                "new_process": True, "acknowledged_version_and_bytes_retained": True,
                "object": state["object"], "intent": state["intent"],
                "anonymous_stages_reclaimed": len(anonymous), "protected_stages_retained": len(protected)}
            results["groups"].append(row)
            print(json.dumps(row), flush=True)
            (evidence / "results.json").write_text(json.dumps(results, indent=2))
        assert source_fingerprint() == results["source_sha256"], "source changed during crash acceptance"
        results["status"] = "passed"
    except BaseException as failure:
        results["status"] = "failed"
        results["error_type"] = type(failure).__name__
        raise
    finally:
        runtime.close()
        (evidence / "results.json").write_text(json.dumps(results, indent=2))
        # Server logs and RPC remain private. Evidence contains no credentials.
        print(f"Disposable crash root retained: {root}", flush=True)


if __name__ == "__main__":
    main()
