#!/usr/bin/env python3
"""A30 real disk-backed 64MiB/1GiB transfers; fixed predeclared envelopes.

All services/data are owned and disposable. Full samples and failures are retained.
This lane covers local/gateway paths; cluster repair remains a separate lane.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import uuid

import boto3
import botocore
from resource_cases import manifest, runner_details, verify_coverage
from gateway_hardening import Runtime, client, wait_http
from gateway_fault_proxy import FaultProxy
from hardening_acceptance import source_fingerprint
from resource_cadence import execute_group, measure_samples

MIB = 1024 * 1024


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class PacedFile:
    def __init__(self, path, pace=False):
        self.file = open(path, "rb")
        self.pace = pace

    def read(self, size=-1):
        started = time.monotonic()
        block = self.file.read(min(size, MIB) if size >= 0 else MIB)
        if self.pace and block:
            time.sleep(max(0, len(block) / (32 * MIB) - (time.monotonic() - started)))
        return block

    def __getattr__(self, name):
        return getattr(self.file, name)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.file.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--profile", choices=["all", "upstream"], default="all")
    args = parser.parse_args()
    root = args.root.resolve()
    if not str(root).startswith("/tmp/ess-hardening-resource-") or root.exists():
        raise ValueError("new exclusively disposable resource root required")
    root.mkdir(mode=0o700)
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    result = {"status": "in_progress", "profile": args.profile,
        "starting_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_sha256": source_fingerprint(), "groups": [], "upload_verifications": [], "versions": {
        "boto3": boto3.__version__, "botocore": botocore.__version__},
        "runner": runner_details(root), "required_cases": [{"name": name, "size": 1073741824, "concurrency": 4} for name in ["gateway-cold-get", "gateway-copy-to-local"]], "acceptance": False,
        "envelope": "docs/plans/s3-hardening-resource-envelope.md"}
    runtime = Runtime(root, port())
    runtime.requires = ["e2e/scripts/resource_sampler.exs", "e2e/scripts/resource_diagnostics.exs", "/fast/build/ess-closeout-c5-copy-diagnostic/copy_trace.exs"]
    runtime.env["E2E_RESOURCE_ROOT"] = str(root)
    upstream_port = port()
    endpoint = f"http://127.0.0.1:{upstream_port}"
    container = "ess-hardening-resource-" + uuid.uuid4().hex[:12]
    samples_path = root / "samples.jsonl"
    proxy = None

    def terminate(_signal, _frame):
        # Executor shutdown joins active requests. Stop the owned peer first so
        # those requests wake immediately rather than waiting on socket limits.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        runtime.stop()
        if proxy:
            proxy.clear()
        raise KeyboardInterrupt("resource acceptance terminated")

    signal.signal(signal.SIGTERM, terminate)

    def samples():
        # Last append may be in progress; only accept complete lines.
        content = samples_path.read_bytes()
        return [json.loads(line) for line in content.split(b"\n")[:-1] if line]

    def measured(name, size, concurrency, operation):
        if name not in ["gateway-cold-get", "gateway-copy-to-local"]:
            return
        time.sleep(1.2)
        baseline = samples()[-10:]
        result["active_group"] = {"name": name, "size": size, "concurrency": concurrency}
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        # Repeat fast operations through at least350ms of active sampling. A
        # transfer completing between samples is not measured evidence.
        window, operation_failure = execute_group(operation, concurrency, repeat_ms=350)
        time.sleep(1.2)
        all_samples = samples()
        limits = {"binary": (64 + 32 * concurrency) * MIB,
                  "total": (128 + 48 * concurrency) * MIB,
                  "rss": (256 + 64 * concurrency) * MIB,
                  "queue_max": 128, "queue_total": 2048,
                  "temporary": 2 * size * concurrency + 64 * MIB}
        measurement = measure_samples(all_samples, baseline, window["start_ns"], window["end_ns"], limits)
        failures = measurement["failures"]
        if operation_failure is not None:
            failures.append("operation_failed")
        row = {"name": name, "size": size, "concurrency": concurrency,
               **window, **measurement}
        result["groups"].append(row)
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(row), flush=True)
        if operation_failure is not None:
            raise operation_failure
        if failures:
            raise AssertionError(f"predeclared resource envelope exceeded: {name}: {failures}")

    def verify_upstream_upload(name, keys, size, concurrency, expected_hash):
        # Independent RustFS readback is outside the measured PUT interval and
        # bypasses ESS and its cache. Each consumer retains one bounded chunk.
        verification = {"name": name, "status": "in_progress", "size": size,
                        "concurrency": concurrency, "expected_sha256": expected_hash,
                        "checked_objects": 0, "check_count": 0, "objects": []}
        result["upload_verifications"].append(verification)
        (evidence / "results.json").write_text(json.dumps(result, indent=2))

        def readback(index):
            key = keys[index]
            response = remote.get_object(Bucket="resource-upstream", Key=key)
            digest = hashlib.sha256()
            received = 0
            try:
                for chunk in response["Body"].iter_chunks(256 * 1024):
                    digest.update(chunk)
                    received += len(chunk)
            finally:
                response["Body"].close()
            actual_hash = digest.hexdigest()
            assert received == size, "upstream uploaded object length mismatch"
            assert actual_hash == expected_hash, "upstream uploaded object SHA256 mismatch"
            assert response["Metadata"]["sha256"] == expected_hash, "upstream uploaded metadata mismatch"
            return {"path": "/resource-upstream/" + key, "bytes": received,
                    "sha256": actual_hash, "metadata_sha256": response["Metadata"]["sha256"]}

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            objects = list(pool.map(readback, range(concurrency)))
        verification.update(status="passed", checked_objects=len(objects),
                            check_count=3 * len(objects), objects=objects)
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print(json.dumps({"upload_verification": verification}), flush=True)

    try:
        subprocess.run(["docker", "run", "-d", "--name", container,
                        "-p", f"127.0.0.1:{upstream_port}:9000",
                        "-e", "RUSTFS_ACCESS_KEY=rustfsadmin", "-e", "RUSTFS_SECRET_KEY=rustfsadmin",
                        "-e", "RUSTFS_CONSOLE_ENABLE=false", "rustfs/rustfs:1.0.1"],
                       check=True, stdout=subprocess.DEVNULL, timeout=45)
        result["versions"]["rustfs"] = subprocess.check_output([
            "docker", "image", "inspect", "rustfs/rustfs:1.0.1", "--format", "{{.Id}} {{json .RepoDigests}}"], text=True).strip()
        wait_http(endpoint + "/health")
        remote = client(endpoint, "rustfsadmin", "rustfsadmin", read_timeout=180)
        for _ in range(120):
            try:
                remote.list_buckets()
                break
            except botocore.exceptions.ClientError as failure:
                if failure.response["ResponseMetadata"]["HTTPStatusCode"] != 503:
                    raise
                time.sleep(.25)
        remote.create_bucket(Bucket="resource-upstream")
        proxy = FaultProxy(endpoint)
        runtime.start()
        credentials = runtime.control("seed", {"bucket": "resource-gateway", "remote_bucket": "resource-upstream",
            "proxy_endpoint": proxy.endpoint, "remote_access": "rustfsadmin", "remote_secret": "rustfsadmin"})
        runtime.control("resource-config", {"bucket": "resource-gateway", "local_bucket": "resource-local"})
        s3 = client(f"http://127.0.0.1:{runtime.port}", credentials["access_key"], credentials["secret_key"], read_timeout=180)
        # Warm transport, checksums and cache before baseline measurements.
        for bucket in ["resource-local", "resource-gateway"]:
            s3.put_object(Bucket=bucket, Key="warm", Body=b"warm")
            s3.get_object(Bucket=bucket, Key="warm")["Body"].read()
        for size in [1024 * MIB]:
            payload = root / "payload.bin"
            digest = hashlib.sha256()
            block = bytes(range(256)) * 4096
            with payload.open("wb") as output:
                for _ in range(size // len(block)):
                    output.write(block)
                    digest.update(block)
            expected_hash = digest.hexdigest()
            for concurrency in [4]:
                keys = [f"{size}-{concurrency}-{i}" for i in range(concurrency)]

                def upload(bucket, i, slow=False, upstream=False):
                    with PacedFile(payload, slow) as body:
                        (remote if upstream else s3).put_object(
                            Bucket=bucket, Key=keys[i], Body=body, ContentLength=size,
                            Metadata={"sha256": expected_hash})

                def download(bucket, i, slow=False, key_prefix=""):
                    response = s3.get_object(Bucket=bucket, Key=key_prefix + keys[i])
                    digest = hashlib.sha256()
                    received = 0
                    try:
                        for chunk in response["Body"].iter_chunks(256 * 1024):
                            started = time.monotonic()
                            digest.update(chunk)
                            received += len(chunk)
                            if slow:
                                time.sleep(max(0, len(chunk) / (32 * MIB) - (time.monotonic() - started)))
                    finally:
                        response["Body"].close()
                    assert received == size and digest.hexdigest() == expected_hash
                    assert response["Metadata"]["sha256"] == expected_hash

                def copy(i, source_bucket="resource-local", key_prefix="copy-"):
                    s3.copy_object(Bucket="resource-local", Key=key_prefix + keys[i],
                                   CopySource={"Bucket": source_bucket, "Key": keys[i]})
                    download("resource-local", i, key_prefix=key_prefix)

                def multipart(i, from_copy=False):
                    key_prefix = "part-copy-" if from_copy else "multipart-"
                    key = key_prefix + keys[i]
                    created = s3.create_multipart_upload(Bucket="resource-local", Key=key,
                        Metadata={"sha256": expected_hash}, ChecksumAlgorithm="CRC32")
                    upload_id = created["UploadId"]
                    try:
                        if from_copy:
                            response = s3.upload_part_copy(Bucket="resource-local", Key=key,
                                UploadId=upload_id, PartNumber=1,
                                CopySource={"Bucket": "resource-local", "Key": keys[i]})["CopyPartResult"]
                        else:
                            with PacedFile(payload) as body:
                                response = s3.upload_part(Bucket="resource-local", Key=key,
                                    UploadId=upload_id, PartNumber=1, Body=body,
                                    ContentLength=size, ChecksumAlgorithm="CRC32")
                        part = {"ETag": response["ETag"], "PartNumber": 1,
                                "ChecksumCRC32": response["ChecksumCRC32"]}
                        s3.complete_multipart_upload(Bucket="resource-local", Key=key,
                            UploadId=upload_id, MultipartUpload={"Parts": [part]})
                    except BaseException:
                        s3.abort_multipart_upload(Bucket="resource-local", Key=key, UploadId=upload_id)
                        raise
                    download("resource-local", i, key_prefix=key_prefix)

                if args.profile == "all":
                    measured("local-put", size, concurrency, lambda i: upload("resource-local", i))
                    measured("local-get", size, concurrency, lambda i: download("resource-local", i))
                    measured("local-copy", size, concurrency, copy)
                    measured("local-multipart", size, concurrency, multipart)
                    measured("local-upload-part-copy", size, concurrency, lambda i: multipart(i, True))
                    measured("local-slow-put", size, concurrency, lambda i: upload("resource-local", i, True))
                    measured("local-slow-get", size, concurrency, lambda i: download("resource-local", i, True))
                    measured("gateway-put", size, concurrency, lambda i: upload("resource-gateway", i))
                    # Upstream-only keys force a real cache miss before cache-hit verification.
                    keys = ["cold-" + key for key in keys]
                    with ThreadPoolExecutor(max_workers=concurrency) as pool:
                        list(pool.map(lambda i: upload("resource-upstream", i, upstream=True), range(concurrency)))
                    measured("gateway-cold-get", size, concurrency, lambda i: download("resource-gateway", i))
                    measured("gateway-copy-to-local", size, concurrency, lambda i: copy(i, "resource-gateway", "gateway-copy-"))
        if source_fingerprint() != result["source_sha256"]:
            raise AssertionError("source changed during resource profile")
        verify_coverage(result["groups"], result["required_cases"])
        result["active_group"] = None
        result["status"] = "passed"
    except BaseException as failure:
        result["status"] = "failed"
        result["error_type"] = type(failure).__name__
        raise
    finally:
        originally_passed = result["status"] == "passed"
        cleanup_failures = []

        def cleanup(name, action):
            try:
                action()
            except BaseException as failure:
                cleanup_failures.append(failure)
                result.update(status="failed")
                result.setdefault("cleanup_errors", []).append(
                    {"component": name, "error_type": type(failure).__name__})

        cleanup("runtime", runtime.close)
        if proxy:
            cleanup("proxy", proxy.close)
        cleanup("ending_sha", lambda: result.update(ending_sha=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True).strip()))
        def retain_diagnostics():
            if samples_path.exists():
                cleanup("samples", lambda: (evidence / "samples.jsonl").write_bytes(samples_path.read_bytes()))
            for pattern in ("observer-*.json", "resource-export-*.json"):
                for diagnostic in root.glob(pattern):
                    cleanup(diagnostic.name, lambda: (evidence / diagnostic.name).write_bytes(diagnostic.read_bytes()))
            diagnostic = root / "resource-errors.jsonl"
            if diagnostic.exists():
                cleanup(diagnostic.name, lambda: (evidence / diagnostic.name).write_bytes(diagnostic.read_bytes()))

        cleanup("diagnostic_discovery", retain_diagnostics)
        cleanup("container", lambda: subprocess.run(["docker", "rm", "-f", container],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True))
        cleanup("terminal_log", lambda: print(f"Disposable resource root retained: {root}", flush=True))
        cleanup("results", lambda: (evidence / "results.json").write_text(json.dumps(result, indent=2)))
        if cleanup_failures and originally_passed:
            raise cleanup_failures[0]


if __name__ == "__main__":
    main()
