#!/usr/bin/env python3
"""Small real three-voter crash, strict-quorum, scrub and fencing profile.

Only the shell owner starts/stops services. State is private and contains no keys.
Every HTTP mutation has zero automatic retries; retries must not hide strict RF/W.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError


PAYLOADS = {
    "old": b"acknowledged-old-version" * 41,
    "current": b"acknowledged-current-version" * 43,
    "shared": b"shared-immutable-content" * 47,
    "repair": b"bounded-real-scrub-repair" * 53,
    "part": b"acknowledged-multipart-part" * 59,
    "reader": b"active-reader-body" * 49152,
}


def save(path, value):
    temporary = path.with_suffix(".new")
    with open(temporary, "x", encoding="utf-8") as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, indent=2, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def clients():
    endpoints = os.environ["E2E_S3_ENDPOINTS"].split(",")
    assert endpoints == ["http://127.0.0.1:9001", "http://127.0.0.1:9002"]
    return [boto3.client("s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"],
        region_name="us-east-1", config=Config(signature_version="s3v4",
            s3={"addressing_style": "path"}, retries={"total_max_attempts": 1},
            connect_timeout=2, read_timeout=10)) for endpoint in endpoints]


def expect_status(status, function, **kwargs):
    try:
        function(**kwargs)
    except ClientError as failure:
        assert failure.response["ResponseMetadata"]["HTTPStatusCode"] == status
        return failure.response["Error"]["Code"]
    raise AssertionError(f"expected HTTP {status}")


def assert_get(s3, state, key, payload, version=None):
    args = {"Bucket": state["bucket"], "Key": key}
    if version is not None:
        args["VersionId"] = version
    response = s3.get_object(**args)
    try:
        assert response["Body"].read(len(payload) + 1) == payload
    finally:
        response["Body"].close()
    if version is not None:
        assert response["VersionId"] == version


def assert_fixture(s3, state):
    for name in ("old", "current"):
        assert_get(s3, state, "versions", PAYLOADS[name], state["versions"][name])
    assert_get(s3, state, "versions", PAYLOADS["current"])
    for key in ("shared-a", "shared-b"):
        assert_get(s3, state, key, PAYLOADS["shared"])
    assert_get(s3, state, "repair", PAYLOADS["repair"])
    assert_get(s3, state, "reader", PAYLOADS["reader"])
    head = s3.head_object(Bucket=state["bucket"], Key="versions")
    assert head["Metadata"] == {"fixture": "retained"}
    assert head["ContentType"] == "application/octet-stream"
    tags = s3.get_object_tagging(Bucket=state["bucket"], Key="versions")
    assert tags["TagSet"] == [{"Key": "fault", "Value": "retained"}]
    versions = s3.list_object_versions(Bucket=state["bucket"], Prefix="versions")
    assert not versions.get("IsTruncated")
    assert {row["VersionId"] for row in versions["Versions"]} == set(state["versions"].values())
    assert not versions.get("DeleteMarkers")


def assert_failed_writes_absent(s3, state):
    for key in ("failed-new", "failed-generation"):
        expect_status(404, s3.head_object, Bucket=state["bucket"], Key=key)
        rows = s3.list_object_versions(Bucket=state["bucket"], Prefix=key)
        assert not rows.get("Versions") and not rows.get("DeleteMarkers")


def control(action, path, state):
    response = path.parent / f"resilience-control-{uuid.uuid4().hex}.json"
    with open(path.parent / "resilience-control.log", "ab") as output:
        try:
            completed = subprocess.run(["elixir", "--name", f"ess_resilience_ctl_{uuid.uuid4().hex[:10]}@127.0.0.1",
                "--cookie", "ess_cluster_e2e_cookie", "e2e/scripts/resilience_hardening_control.exs",
                action, str(path), str(response)], timeout=60,
                stdout=output, stderr=subprocess.STDOUT)
        except (subprocess.SubprocessError, OSError):
            raise RuntimeError(f"resilience control {action} failed; see private control log") from None
    if completed.returncode != 0:
        raise RuntimeError(f"resilience control {action} exited {completed.returncode}; see private control log")
    return json.loads(response.read_text())


def await_topology(action, path, state):
    # Each RPC is finite; at most 30 seconds of retries for convergence.
    deadline = time.monotonic() + 30
    while True:
        result = control(action, path, state)
        if result["ready"]:
            return result
        assert time.monotonic() < deadline, f"cluster convergence failed: {result}"
        time.sleep(0.25)


def run_read_repair(path, state, s3):
    bucket = state["bucket"]
    before = s3.head_object(Bucket=bucket, Key="reader")
    removed = control("remove-read-repair", path, state)
    response = s3.get_object(Bucket=bucket, Key="reader", Range="bytes=0-31")
    try:
        assert response["Body"].read(33) == PAYLOADS["reader"][:32]
    finally:
        response["Body"].close()
    partial = control("read-repair-state", path, state)
    assert not partial["local_exists"] and not partial["ready"]
    assert_get(s3, state, "reader", PAYLOADS["reader"])
    deadline = time.monotonic() + 30
    while True:
        repaired = control("read-repair-state", path, state)
        if repaired["ready"]:
            break
        assert time.monotonic() < deadline, "public GET did not restore local CAS and fenced location"
        time.sleep(0.25)
    after = s3.head_object(Bucket=bucket, Key="reader")
    assert after["VersionId"] == before["VersionId"]
    assert after["ETag"] == before["ETag"]
    return {"removal": removed, "range_did_not_publish": True,
        "full_public_get": True, "repair": repaired, "object_version_unchanged": True}


def run(action, path, state, s3s):
    s3 = s3s[0]
    bucket = state["bucket"]
    if action == "seed":
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        state["versions"] = {}
        for name in ("old", "current"):
            result = s3.put_object(Bucket=bucket, Key="versions", Body=PAYLOADS[name],
                Metadata={"fixture": "retained"}, ContentType="application/octet-stream",
                Tagging="fault=retained")
            assert result["VersionId"] != "null"
            state["versions"][name] = result["VersionId"]
        for key, name in (("shared-a", "shared"), ("shared-b", "shared"), ("repair", "repair"), ("reader", "reader")):
            s3.put_object(Bucket=bucket, Key=key, Body=PAYLOADS[name])
        state["upload"] = s3.create_multipart_upload(Bucket=bucket, Key="multipart")["UploadId"]
        state["part_etag"] = s3.upload_part(Bucket=bucket, Key="multipart", UploadId=state["upload"],
            PartNumber=1, Body=PAYLOADS["part"])["ETag"]
        state["hashes"] = {key: hashlib.sha256(value).hexdigest() for key, value in PAYLOADS.items()}
        state["sizes"] = {key: len(value) for key, value in PAYLOADS.items()}
        for peer in s3s:
            assert_fixture(peer, state)
        save(path, state)
        return {"acknowledged_versions": 2, "shared_keys": 2, "pending_multipart": True}
    if action == "read-repair":
        try:
            return run_read_repair(path, state, s3)
        finally:
            control("resume-read-repair", path, state)
    if action == "maintenance":
        result = control("maintenance", path, state)
        deadline = time.monotonic() + 30
        while True:
            repaired = control("repair-state", path, state)
            if repaired["ready"]:
                break
            assert time.monotonic() < deadline, "real CAS repair did not restore both replicas"
            time.sleep(0.25)
        for peer in s3s:
            assert_fixture(peer, state)
        gc = control("gc", path, state)
        for peer in s3s:
            assert_fixture(peer, state)
        return {**result, "repair": repaired, "gc": gc, "object_versions_unchanged": True}
    if action == "outage":
        try:
            urllib.request.urlopen("http://127.0.0.1:9001/health/ready", timeout=3)
        except urllib.error.HTTPError as failure:
            assert failure.code == 503
        else:
            raise AssertionError("strict cluster remained ready with only one data node")
        assert_fixture(s3, state)
        for key in ("failed-new", "versions"):
            expect_status(503, s3.put_object, Bucket=bucket, Key=key, Body=b"must-not-publish")
        expect_status(503, s3.upload_part, Bucket=bucket, Key="multipart", UploadId=state["upload"],
            PartNumber=1, Body=b"must-not-replace-part")
        expect_status(503, s3.complete_multipart_upload, Bucket=bucket, Key="multipart",
            UploadId=state["upload"], MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": state["part_etag"]}]})
        parts = s3.list_parts(Bucket=bucket, Key="multipart", UploadId=state["upload"])["Parts"]
        assert len(parts) == 1 and parts[0]["ETag"] == state["part_etag"]
        assert_fixture(s3, state)
        assert_failed_writes_absent(s3, state)
        expect_status(404, s3.head_object, Bucket=bucket, Key="multipart")
        return {"get_retained": True, "new_overwrite_part_complete_rejected": True,
            "failed_mutations_invisible": True, "original_part_retained": True}
    if action == "recovered":
        topology = await_topology("rejoined-data", path, state)
        for peer in s3s:
            assert_fixture(peer, state)
            assert_failed_writes_absent(peer, state)
        result = s3s[1].complete_multipart_upload(Bucket=bucket, Key="multipart", UploadId=state["upload"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": state["part_etag"]}]})
        state["multipart_version"] = result["VersionId"]
        s3.put_object(Bucket=bucket, Key="after-data-rejoin", Body=b"strict-write-after-rejoin")
        save(path, state)
        locations = control("locations", path, state)
        return {"topology": topology, "locations": locations, "multipart_completed": True}
    if action == "record-leader":
        result = await_topology("leader", path, state)
        state["before_loss"] = result
        save(path, state)
        return result
    if action == "elected":
        result = await_topology("elected", path, state)
        primary = state["before_loss"]["primary"]
        assert primary in ("meta", "data-a", "data-b")
        survivors = s3s if primary == "meta" else [s3s[1 if primary == "data-a" else 0]]
        if primary == "meta":
            survivors[0].put_object(Bucket=bucket, Key="after-election", Body=b"strict-write-after-election")
        else:
            expect_status(503, survivors[0].put_object, Bucket=bucket, Key="after-election",
                Body=b"must-not-publish")
        for peer in survivors:
            assert_fixture(peer, state)
            if primary == "meta":
                assert_get(peer, state, "after-election", b"strict-write-after-election")
            else:
                expect_status(404, peer.head_object, Bucket=bucket, Key="after-election")
            assert_get(peer, state, "multipart", PAYLOADS["part"], state["multipart_version"])
        state["election"] = result
        save(path, state)
        return {"topology": result, "primary_stopped": primary, "rf": 2, "w": 2,
            "strict_write_status": 200 if primary == "meta" else 503}
    if action == "verify":
        topology = await_topology("rejoined-all", path, state)
        stale = control("stale-generation", path, state)
        try:
            expect_status(503, s3.put_object, Bucket=bucket, Key="failed-generation", Body=b"stale-not-durable")
            assert_failed_writes_absent(s3, state)
        finally:
            control("restore-generation", path, state)
        s3.put_object(Bucket=bucket, Key="after-generation-restore", Body=b"strict-generation-restored")
        for peer in s3s:
            assert_fixture(peer, state)
            assert_failed_writes_absent(peer, state)
            assert_get(peer, state, "multipart", PAYLOADS["part"], state["multipart_version"])
            if state["before_loss"]["primary"] == "meta":
                assert_get(peer, state, "after-election", b"strict-write-after-election")
            else:
                expect_status(404, peer.head_object, Bucket=bucket, Key="after-election")
            assert_get(peer, state, "after-generation-restore", b"strict-generation-restored")
        locations = control("locations", path, state)
        return {"topology": topology, "stale_generation": stale,
            "stale_replica_write_rejected": True, "all_acknowledged_data_retained": True,
            "locations": locations}
    raise ValueError(action)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["seed", "read-repair", "maintenance", "outage", "recovered", "record-leader", "elected", "verify"])
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    root = Path(os.environ["ESS_CLUSTER_E2E_ROOT"]).resolve()
    assert re.fullmatch(r"/tmp/ess-s3-hardening-cluster-[a-zA-Z0-9]{8}", str(root))
    path = args.state.resolve()
    assert path == root / "resilience-state.json"
    state = ({"schema": 1, "root": str(root), "bucket": "hardening-resilience-" + uuid.uuid4().hex[:16]}
        if args.action == "seed" else json.loads(path.read_text()))
    assert state["root"] == str(root) and state["schema"] == 1
    assert re.fullmatch(r"hardening-resilience-[a-f0-9]{16}", state["bucket"])
    results_path = root / "resilience-result.json"
    results = (json.loads(results_path.read_text()) if results_path.exists() else {
        "schema": 1, "status": "in_progress", "groups": [],
        "boto3": boto3.__version__, "botocore": botocore.__version__,
        "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree": "uncommitted hardening implementation"})
    try:
        evidence = run(args.action, path, state, clients())
        results["groups"].append({"action": args.action, "status": "passed", "evidence": evidence})
        if args.action == "verify":
            assert [group["action"] for group in results["groups"]] == [
                "seed", "read-repair", "maintenance", "outage", "recovered", "record-leader", "elected", "verify"]
            results["status"] = "passed"
        print(json.dumps({"action": args.action, "status": "passed"}), flush=True)
    except BaseException as failure:
        results["status"] = "failed"
        results["failure"] = {"action": args.action, "type": type(failure).__name__}
        raise
    finally:
        save(results_path, results)


if __name__ == "__main__":
    main()
