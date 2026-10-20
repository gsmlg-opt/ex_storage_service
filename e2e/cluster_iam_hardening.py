#!/usr/bin/env python3
"""Phase 1 IAM and bucket races on the disposable three-voter harness.

Uses Boto3/Botocore signing independently of ESS. IAM mutations occur through
the existing distributed controller boundary; credentials stay in a private
temporary file and never appear in command arguments or result output.
"""
import argparse
import concurrent.futures
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def client(endpoint, key, secret):
    return boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=key,
        aws_secret_access_key=secret, region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"total_max_attempts": 1}, max_pool_connections=64,
                      connect_timeout=5, read_timeout=60))


def status(operation):
    try:
        response = operation()
        if "Body" in response:
            response["Body"].close()
        return response["ResponseMetadata"]["HTTPStatusCode"]
    except ClientError as error:
        return error.response["ResponseMetadata"]["HTTPStatusCode"]


def url_status(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def race(operations):
    barrier = threading.Barrier(len(operations), timeout=30)

    def run(operation):
        barrier.wait()
        return status(operation)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(operations)) as pool:
        return list(pool.map(run, operations))


def control(action, state_path, bucket):
    env = dict(os.environ, MIX_ENV="prod", ESS_PUBLIC_S3_ENABLED="false", ESS_WEB_ENABLED="false",
               ESS_S3_AUTH_ENABLED="true",
               ESS_MASTER_KEY=os.environ.get("ESS_CLUSTER_E2E_MASTER_KEY", "cluster-e2e-master-key"))
    name = f"ess_iam_hardening_{os.getpid()}@127.0.0.1"
    command = ["elixir", "--name", name, "--cookie",
               os.environ.get("ESS_CLUSTER_E2E_COOKIE", "ess_cluster_e2e_cookie"),
               "-S", "mix", "run", "--no-start", "--no-compile",
               "e2e/scripts/cluster_hardening_control.exs", action, str(state_path), bucket]
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
    log_path = state_path.with_name(f"controller-{action}.log")
    log_path.touch(mode=0o600)
    log_path.write_text(result.stdout + result.stderr)
    check(result.returncode == 0, f"cluster IAM control {action} failed; inspect private controller log")
    # A failure log may include terms from RPC. Keep it private, never print it.
    return result


def iam_checks(admins, endpoints, directory, scope="all"):
    bucket = "hardening-iam-" + uuid.uuid4().hex[:16]
    admins[0].create_bucket(Bucket=bucket)
    admins[0].put_object(Bucket=bucket, Key="probe", Body=b"current-policy")
    admins[1].put_object(Bucket=bucket, Key="source", Body=b"private-copy-source")
    state_path = pathlib.Path(directory) / "iam.json"
    state_path.touch(mode=0o600)
    control("seed", state_path, bucket)
    state = json.loads(state_path.read_text())
    users = [client(endpoint, state["access_key_id"], state["secret_access_key"]) for endpoint in endpoints]
    checks = []

    def probes(expected):
        actual = [status(lambda s3=s3: s3.get_object(Bucket=bucket, Key="probe")) for s3 in users]
        check(actual == [expected, expected], f"both-endpoint policy probes: expected {expected}, got {actual}")

    try:
        probes(200)
        urls = [s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "probe"}, ExpiresIn=300) for s3 in users]
        check([url_status(url) for url in urls] == [200, 200], "initial presign policy")
        for s3 in users:
            check(status(lambda: s3.copy_object(Bucket=bucket, Key="destination", CopySource={"Bucket": bucket, "Key": "source"})) == 403,
                  "destination grant authorized unreadable copy source")
        for s3 in admins:
            check(status(lambda: s3.head_object(Bucket=bucket, Key="destination")) == 404, "denied copy published destination")
        checks.append("copy-source-denial-both-endpoints")

        control("attach-deny", state_path, bucket)
        probes(403)
        check([url_status(url) for url in urls] == [403, 403], "changed policy did not revoke presign")
        checks.append("attached-deny-linearizable-both-endpoints")
        if scope == "all":
            control("remove-deny-record", state_path, bucket)
            probes(403)
            checks.append("missing-attached-deny-fails-closed-both-endpoints")
            control("corrupt-deny-record", state_path, bucket)
            probes(403)
            checks.append("malformed-attached-deny-fails-closed-both-endpoints")
        control("detach-deny", state_path, bucket)
        probes(200)
        control("suspend", state_path, bucket)
        probes(403)
        check([url_status(url) for url in urls] == [403, 403], "suspended user retained presign")
        checks.append("suspended-user-both-endpoints")
        control("activate", state_path, bucket)
        probes(200)
        control("revoke", state_path, bucket)
        probes(403)
        check([url_status(url) for url in urls] == [403, 403], "revoked key retained presign")
        checks.append("revoked-key-and-presign-both-endpoints")
    finally:
        control("cleanup", state_path, bucket)
    return checks, bucket


def bucket_races(admins):
    checks = []
    outcomes = []
    for index in range(8):
        bucket = "hardening-race-" + uuid.uuid4().hex[:16]
        admins[0].create_bucket(Bucket=bucket)
        put, delete = race([
            lambda: admins[index % 2].put_object(Bucket=bucket, Key="racer", Body=b"candidate"),
            lambda: admins[(index + 1) % 2].delete_bucket(Bucket=bucket)])
        check((put == 200 and delete == 409) or (put in (404, 409) and delete == 204),
              f"bucket PUT/delete race returned PUT={put} DELETE={delete}")
        outcomes.append([put, delete])
        if delete == 204:
            admins[0].create_bucket(Bucket=bucket)
            for s3 in admins:
                listing = s3.list_objects_v2(Bucket=bucket)
                check(listing.get("KeyCount", 0) == 0, "old write revived in recreated bucket")
        else:
            for s3 in admins:
                response = s3.get_object(Bucket=bucket, Key="racer")
                check(response["Body"].read() == b"candidate", "race winner body unavailable")
                response["Body"].close()

    bucket = "hardening-mpu-race-" + uuid.uuid4().hex[:16]
    admins[0].create_bucket(Bucket=bucket)
    upload = admins[0].create_multipart_upload(Bucket=bucket, Key="complete")["UploadId"]
    etag = admins[1].upload_part(Bucket=bucket, Key="complete", UploadId=upload, PartNumber=1, Body=b"complete-candidate")["ETag"]
    complete, delete = race([
        lambda: admins[0].complete_multipart_upload(Bucket=bucket, Key="complete", UploadId=upload,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]}),
        lambda: admins[1].delete_bucket(Bucket=bucket)])
    check(complete == 200 and delete == 409, f"multipart/bucket race: {complete}/{delete}")
    checks.extend(["8-cross-endpoint-bucket-write-races", "multipart-bucket-delete-race"])
    return checks, outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("all", "features"), default="all")
    args = parser.parse_args()
    endpoints = [value.rstrip("/") for value in os.environ["E2E_S3_ENDPOINTS"].split(",")]
    check(len(endpoints) == 2, "requires both disposable cluster endpoints")
    check(all(urllib.parse.urlsplit(endpoint).hostname in ("127.0.0.1", "localhost", "::1")
              for endpoint in endpoints), "cluster hardening accepts only local disposable endpoints")
    admins = [client(endpoint, os.environ["E2E_ACCESS_KEY_ID"], os.environ["E2E_SECRET_ACCESS_KEY"]) for endpoint in endpoints]
    directory = tempfile.mkdtemp(prefix="ess-cluster-iam-")
    try:
        checks, bucket = iam_checks(admins, endpoints, directory, args.scope)
        print(json.dumps({"profile": "cluster-phase1-iam", "result": "passed",
                          "scope": args.scope, "endpoints": endpoints, "bucket": bucket,
                          "checks": checks}, sort_keys=True), flush=True)
        races, outcomes = bucket_races(admins) if args.scope == "all" else ([], [])
    except Exception as error:
        raise RuntimeError(f"{error}; private controller diagnostics retained at {directory}") from None
    else:
        shutil.rmtree(directory)
    print(json.dumps({"profile": "cluster-phase1-iam-bucket-races" if args.scope == "all" else "cluster-iam-features",
                      "scope": args.scope, "result": "passed",
                      "endpoints": endpoints, "bucket": bucket, "checks": checks + races,
                      "bucket_race_statuses": outcomes}, sort_keys=True))


if __name__ == "__main__":
    main()
