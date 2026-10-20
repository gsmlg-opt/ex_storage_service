#!/usr/bin/env python3
"""A29 real native rebuild checkpoints, voter restart and HTTP revision barriers.

Only the shell owner operates node processes. Credentials and build owner tokens
never enter public evidence. No HTTP automatic retry or checksum override.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from hardening_acceptance import source_fingerprint


BODY = b"index-rebuild-acknowledged-payload" * 31


def save(path, value):
    temporary = path.with_suffix(".new")
    with temporary.open("x", encoding="utf-8") as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, indent=2)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


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
        return
    raise AssertionError(f"expected HTTP {status}")


def body(s3, state, key, version=None):
    args = {"Bucket": state["bucket"], "Key": key}
    if version is not None:
        args["VersionId"] = version
    result = s3.get_object(**args)
    try:
        assert result["Body"].read(len(BODY) + 1) == BODY
    finally:
        result["Body"].close()
    if version is not None:
        assert result["VersionId"] == version


def versions(s3, bucket):
    result = s3.list_object_versions(Bucket=bucket)
    assert not result.get("IsTruncated")
    return sorted([(row["Key"], row["VersionId"], row["IsLatest"], row["ETag"], row["Size"])
                   for row in result.get("Versions", [])]), sorted([
        (row["Key"], row["VersionId"], row["IsLatest"])
        for row in result.get("DeleteMarkers", [])])


def control(action, path):
    response = path.parent / f"index-control-{uuid.uuid4().hex}.json"
    with (path.parent / "index-control.log").open("ab") as log:
        result = subprocess.run(["elixir", "--name", f"ess_index_ctl_{uuid.uuid4().hex[:10]}@127.0.0.1",
            "--cookie", "ess_cluster_e2e_cookie", "e2e/scripts/index_cluster_control.exs",
            action, str(path), str(response)], stdout=log, stderr=subprocess.STDOUT, timeout=90)
    if result.returncode:
        raise RuntimeError(f"index {action} control failed; inspect private control log")
    return json.loads(response.read_text())


def await_file(path, future=None):
    deadline = time.monotonic() + 30
    while not path.exists():
        if future is not None and future.done():
            future.result()
            raise AssertionError("control finished without its synchronization barrier")
        assert time.monotonic() < deadline, "index control barrier deadline exceeded"
        time.sleep(0.025)


def gated(s3s, state):
    for s3 in s3s:
        expect_status(503, s3.list_objects_v2, Bucket=state["bucket"])
        expect_status(503, s3.put_object, Bucket=state["bucket"], Key="forbidden", Body=BODY)
        expect_status(503, s3.delete_object, Bucket=state["bucket"], Key="keep/0")
        expect_status(503, s3.create_multipart_upload, Bucket=state["bucket"], Key="forbidden-upload")
        # The immutable acknowledged body remains readable while its namespace is gated.
        body(s3, state, "versions", state["versions"][0])
        result = s3.put_object(Bucket=state["other_bucket"], Key=f"permitted-{uuid.uuid4().hex}", Body=BODY)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


def run(action, path, state, s3s):
    bucket = state["bucket"]
    if action == "seed":
        for name in (bucket, state["other_bucket"]):
            s3s[0].create_bucket(Bucket=name)
            s3s[0].put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        for index in range(8):
            s3s[0].put_object(Bucket=bucket, Key=f"keep/{index}", Body=BODY)
        state["versions"] = [s3s[0].put_object(Bucket=bucket, Key="versions", Body=BODY)["VersionId"]
                             for _ in range(2)]
        s3s[0].put_object(Bucket=bucket, Key="deleted", Body=BODY)
        state["marker"] = s3s[0].delete_object(Bucket=bucket, Key="deleted")["VersionId"]
        state["upload"] = s3s[0].create_multipart_upload(Bucket=bucket, Key="active-multipart")["UploadId"]
        state["part_etag"] = s3s[0].upload_part(Bucket=bucket, Key="active-multipart",
            UploadId=state["upload"], PartNumber=1, Body=BODY)["ETag"]
        state["inventory"] = versions(s3s[0], bucket)
        save(path, state)
        return {"acknowledged_versions": len(state["inventory"][0]), "marker": True, "active_part": True}
    if action == "prepare":
        return control("prepare", path)
    if action == "gated":
        gated(s3s, state)
        return {"both_endpoints_gated": True, "other_bucket_writes": True, "explicit_bytes_retained": True}
    if action == "rejoin":
        deadline = time.monotonic() + 30
        while True:
            topology = control("topology", path)
            if topology["ready"]:
                break
            assert time.monotonic() < deadline, "all three voters did not rejoin"
            time.sleep(0.25)
        checkpoint = control("takeover", path)
        gated(s3s, state)
        return {"topology": topology, "checkpoint": checkpoint, "gate_survives_restart": True}
    if action == "finish":
        with ThreadPoolExecutor(max_workers=2) as pool:
            replay = pool.submit(control, "resume", path)
            await_file(path.parent / "index-resume-active", replay)
            (path.parent / "index-writer-started").touch(exist_ok=False)
            written = []
            for index in range(8):
                key = f"concurrent/{index}"
                result = s3s[index % 2].put_object(Bucket=state["other_bucket"], Key=key, Body=BODY)
                written.append((key, result["VersionId"]))
                expect_status(503, s3s[0].list_objects_v2, Bucket=bucket)
            (path.parent / "index-writer-finished").touch(exist_ok=False)
            resumed = replay.result(timeout=90)
            # Hold an actual native page after its first descriptor read.
            page = pool.submit(control, "revision-barrier", path)
            await_file(path.parent / "index-page-paused", page)
            try:
                s3s[1].put_object(Bucket=bucket, Key="revision-after-snapshot", Body=BODY)
            finally:
                (path.parent / "index-page-release").touch(exist_ok=False)
            barrier = page.result(timeout=90)
        retained = control("retained", path)
        expected_versions = sorted(tuple(row) for row in state["inventory"][0])
        expected_markers = sorted(tuple(row) for row in state["inventory"][1])
        for s3 in s3s:
            actual_versions, actual_markers = versions(s3, bucket)
            assert [row for row in actual_versions if row[0] not in ("legacy:colon", "revision-after-snapshot")] == expected_versions
            assert actual_markers == expected_markers
            assert {row[1] for row in actual_versions if row[0] == "legacy:colon"} == {"null", "legacy-indexed", "legacy-unindexed"}
            for version in state["versions"]:
                body(s3, state, "versions", version)
            for version in ("null", "legacy-indexed", "legacy-unindexed"):
                body(s3, state, "legacy:colon", version)
            uploads = s3.list_multipart_uploads(Bucket=bucket)
            assert [(row["Key"], row["UploadId"]) for row in uploads["Uploads"]] == [("active-multipart", state["upload"])]
            parts = s3.list_parts(Bucket=bucket, Key="active-multipart", UploadId=state["upload"])
            assert [(row["PartNumber"], row["ETag"], row["Size"]) for row in parts["Parts"]] == [(1, state["part_etag"], len(BODY))]
            keys = {row["Key"] for row in s3.list_objects_v2(Bucket=bucket)["Contents"]}
            assert keys == {f"keep/{index}" for index in range(8)} | {"versions", "legacy:colon", "revision-after-snapshot"}
            for key, version in written:
                response = s3.get_object(Bucket=state["other_bucket"], Key=key, VersionId=version)
                try:
                    assert response["Body"].read(len(BODY) + 1) == BODY
                finally:
                    response["Body"].close()
        assert source_fingerprint() == state["source_fingerprint"], "source changed during A29 lane"
        return {"resumed": resumed, "revision_barrier": barrier, "retained": retained,
                "concurrent_other_bucket_writes": len(written), "both_endpoint_inventories": True,
                "raw_bytes_versions_and_multipart": True, "source_unchanged": True}
    raise AssertionError("unknown A29 action")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("seed", "prepare", "gated", "rejoin", "finish"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = Path(os.environ["ESS_CLUSTER_E2E_ROOT"]).resolve()
    assert re.fullmatch(r"/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}", str(root))
    assert args.state.resolve() == root / "index-state.json"
    if args.action == "seed":
        suffix = uuid.uuid4().hex[:16]
        state = {"schema": 1, "root": str(root), "bucket": f"hardening-index-{suffix}",
                 "other_bucket": f"hardening-index-other-{suffix}", "source_fingerprint": source_fingerprint()}
        evidence = {"scenario": "A29", "status": "in_progress", "checks": [],
                    "boto3": boto3.__version__, "botocore": botocore.__version__,
                    "source_fingerprint": state["source_fingerprint"], "payload_sha256": hashlib.sha256(BODY).hexdigest()}
    else:
        state = json.loads(args.state.read_text())
        evidence = json.loads(args.evidence.read_text())
    try:
        result = run(args.action, args.state, state, clients())
        evidence["checks"].append({"action": args.action, "result": "passed", "details": result})
        if args.action == "finish":
            evidence["status"] = "passed"
    except BaseException:
        evidence.update(status="failed", failed_action=args.action)
        save(args.evidence, evidence)
        raise
    save(args.evidence, evidence)
    print(json.dumps({"scenario": "A29", "action": args.action, "result": "passed"}), flush=True)


if __name__ == "__main__":
    main()
