#!/usr/bin/env python3
"""Real RF2/W2 remote-ack crash, isolated visibility, retained-root resume."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid

import boto3
import botocore
from gateway_hardening import client


def main():
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = Path(os.environ["ESS_CLUSTER_E2E_ROOT"]).resolve()
    assert re.fullmatch(r"/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}", str(root))
    assert root.is_dir() and not root.is_symlink()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    access, secret = os.environ["E2E_ACCESS_KEY_ID"], os.environ["E2E_SECRET_ACCESS_KEY"]
    peers = [client(f"http://127.0.0.1:{port}", access, secret, read_timeout=15) for port in [9001, 9002]]
    bucket = "replication-crash-" + uuid.uuid4().hex[:16]
    body = (b"real-replication-boundary-" + uuid.uuid4().hex.encode()) * 4096
    digest = hashlib.sha256(body).hexdigest()
    state = {"schema": 1, "root": str(root), "bucket": bucket, "hash": digest, "size": len(body),
             "operation_id": bucket + "-operation", "body_base64": base64.b64encode(body).decode()}
    state_path = root / "replication-crash-state.json"
    state_path.write_text(json.dumps(state))
    state_path.chmod(0o600)
    result = {"status": "in_progress", "checks": [], "source_sha": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True).strip(),
        "versions": {"boto3": boto3.__version__, "botocore": botocore.__version__}}

    def control(action):
        sequence = uuid.uuid4().hex
        output = root / f"replication-control-{sequence}.json"
        with (root / "replication-control.log").open("ab") as log:
            # This disposable fixture uses the existing documented test cookie.
            try:
                completed = subprocess.run(["elixir", "--name", f"replication_ctl_{sequence[:10]}@127.0.0.1",
                    "--cookie", "ess_cluster_e2e_cookie", "e2e/scripts/replication_crash_control.exs",
                    action, str(state_path), str(output)], stdout=log, stderr=subprocess.STDOUT, timeout=60)
            except subprocess.TimeoutExpired:
                raise RuntimeError("replication controller timed out; inspect retained private log") from None
        if completed.returncode:
            raise RuntimeError("replication controller failed; inspect retained private log")
        return json.loads(output.read_text())

    def passed(name, detail):
        result["checks"].append({"name": name, "passed": True, "detail": detail})
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print("PASS " + name, flush=True)

    def get_exact(s3, key, data, version=None):
        request = {"Bucket": bucket, "Key": key}
        if version:
            request["VersionId"] = version
        response = s3.get_object(**request)
        try:
            assert response["Body"].read(len(data) + 1) == data
        finally:
            response["Body"].close()
        if version:
            assert response["VersionId"] == version

    def absent(s3):
        try:
            s3.get_object(Bucket=bucket, Key="uncertain")
        except botocore.exceptions.ClientError as failure:
            assert failure.response["Error"]["Code"] == "NoSuchKey"
        else:
            raise AssertionError("uncertain operation became visible before metadata commit")
        versions = s3.list_object_versions(Bucket=bucket, Prefix="uncertain")
        assert not versions.get("Versions") and not versions.get("DeleteMarkers")

    try:
        peers[0].create_bucket(Bucket=bucket)
        peers[0].put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        acknowledged = b"acknowledged-before-replica-crash" * 1024
        version = peers[0].put_object(Bucket=bucket, Key="acknowledged", Body=acknowledged)["VersionId"]
        before_pid = int((root / "pids/data-b").read_text())
        assert control("crash")["crashed_during_replication"]
        witness = json.loads((root / "replication-crash-witness.json").read_text())
        assert witness["boundary"] == "remote_durable_ack" and witness["peer"] == "data-a"
        assert witness["hash"] == digest and witness["size"] == len(body) and witness["generation"] == 1
        assert int(witness["pid"]) == before_pid
        deadline = time.monotonic() + 15
        while Path(f"/proc/{before_pid}/stat").exists():
            if Path(f"/proc/{before_pid}/stat").read_text().split()[2] == "Z":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("replication origin BEAM remained alive after crash witness")
            time.sleep(.1)
        get_exact(peers[0], "acknowledged", acknowledged, version)
        absent(peers[0])
        pending = control("inspect-peer")
        assert pending["intent"] == "pending" and pending["intent_hash"] == digest
        assert not pending["object"]["exists"] and not pending["locations"]
        assert pending["blob"] == {"exists": True, "verified": True, "size": len(body)}
        assert pending["rf"] == 2 and pending["w"] == 2
        passed("remote-durable-ack-origin-crash-isolates-uncertain-operation", pending)
        subprocess.run(["bash", "e2e/cluster_harness.sh", "restart-data-b"], check=True, timeout=120)
        subprocess.run(["bash", "e2e/cluster_harness.sh", "wait-ready"], check=True, timeout=120)
        after_pid = int((root / "pids/data-b").read_text())
        assert after_pid != before_pid
        deadline = time.monotonic() + 45
        while True:
            rejoin = control("rejoined")
            if rejoin["ready"]:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("restarted origin did not converge/rejoin")
            time.sleep(.5)
        for s3 in peers:
            get_exact(s3, "acknowledged", acknowledged, version)
            absent(s3)
        origin = control("inspect-origin")
        assert origin["intent"] == "pending" and not origin["object"]["exists"]
        assert not origin["blob"]["exists"] and not origin["locations"]
        passed("retained-root-rejoin-preserves-data-and-uncertainty", rejoin)
        resumed = control("resume")
        assert resumed["hash"] == digest and resumed["size"] == len(body)
        for s3 in peers:
            get_exact(s3, "acknowledged", acknowledged, version)
            get_exact(s3, "uncertain", body, resumed["version_id"])
            versions = s3.list_object_versions(Bucket=bucket, Prefix="uncertain")
            assert [item["VersionId"] for item in versions["Versions"]] == [resumed["version_id"]]
            assert not versions.get("DeleteMarkers") and not versions.get("IsTruncated")
            assert s3.head_object(Bucket=bucket, Key="uncertain")["Metadata"] == {"fault": "replication"}
        for action in ["inspect-peer", "inspect-origin"]:
            final = control(action)
            assert final["intent"] == "committed" and final["intent_hash"] == digest
            assert final["object"]["hash"] == digest and final["object"]["size"] == len(body)
            assert final["blob"] == {"exists": True, "verified": True, "size": len(body)}
            assert sorted(final["locations"], key=lambda row: row["node"]) == [
                {"node": node, "generation": 1, "state": "ready", "size": len(body)} for node in ["data-a", "data-b"]]
            assert final["rf"] == 2 and final["w"] == 2
        passed("same-internal-operation-resumes-strict-quorum-exactly-one-version", final)
        result["status"] = "passed"
    except BaseException as failure:
        result.update(status="failed", error_type=type(failure).__name__)
        raise
    finally:
        (evidence / "results.json").write_text(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
