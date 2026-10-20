#!/usr/bin/env python3
"""A36/A37: disposable logical backup and independently isolated recovery.

The host runs `source`; `recover` must run inside the runner's mount namespace.
All credentials live in a private fixture file, never the evidence JSON.
"""
import argparse
import base64
import gzip
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import threading
import time
import urllib.request
import uuid

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError


CONTROL = "e2e/scripts/restore_hardening_control.exs"
WORKERS = ("multipart_gc", "content_gc", "cas_gc", "packer", "lifecycle",
           "cross_cluster_replication", "repair", "scrub")
MASTER_KEY = "disposable-restore-master-key-kept-separately-from-bundle"
COOKIE = "ess_restore_disposable_cookie"
INTERNAL_SECRET = "disposable-restore-internal-secret-at-least-32-bytes"
ATTRS = {"ContentType": "application/ess-restore", "ContentEncoding": "gzip",
         "ContentLanguage": "zh", "CacheControl": "max-age=42",
         "ContentDisposition": "inline; filename=restore.bin", "Metadata": {"owner": "restore"}}
TAGS = [{"Key": "owner", "Value": "restore"}]


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())


def read(path):
    return json.loads(Path(path).read_text())


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def command(argv, *, env, log, expected=0, timeout=150):
    with open(log, "a") as output:
        result = subprocess.run(argv, env=env, stdout=output, stderr=subprocess.STDOUT,
                                timeout=timeout, check=False)
    check(result.returncode == expected, f"command failed ({result.returncode}): {argv[0]}; see {log}")


def safe_env():
    # No inherited ESS roots, bootstrap flags or credentials may select live data.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("ESS_", "E2E_", "AWS_"))}
    env.update(PAGER="cat", MIX_ENV="prod", ESS_MODE="standalone", ESS_AUTO_START="false",
               ESS_PUBLIC_S3_ENABLED="false", ESS_WEB_ENABLED="false",
               ESS_MASTER_KEY=MASTER_KEY,
               ERL_FLAGS=os.environ.get("ERL_FLAGS", "+S 4:4 +SDcpu 2 +SDio 2"))
    return env


class Nodes:
    def __init__(self, mode, generation, root, prefix, evidence, control, phase):
        self.mode, self.generation, self.root = mode, generation, Path(root)
        self.prefix, self.evidence, self.control, self.phase = prefix, Path(evidence), Path(control), phase
        self.ids = ["data-a"] if mode == "standalone" else ["meta", "data-a", "data-b"]
        self.names = {node: f"ess_restore_{prefix}_{node.replace('-', '_')}@127.0.0.1" for node in self.ids}
        self.ports = {node: free_port() for node in self.ids}
        self.internal_ports = {node: free_port() for node in self.ids}
        self.processes, self.handles = {}, []
        self.control.mkdir(parents=True, exist_ok=True)
        self.evidence.mkdir(parents=True, exist_ok=True)

    def env(self, node, boot, bootstrap=False):
        directory = self.root / node
        role = "metadata" if node == "meta" else "data"
        env = safe_env()
        public = boot != "quarantine" and role == "data"
        env.update(ESS_MODE=self.mode, ESS_INSTANCE=node, ESS_NODE_ID=node, ESS_NODE_ROLE=role,
            ESS_CONCORD_COMMAND_VERSION="1" if self.phase == "recovered" else "0",
            ESS_NODE_GENERATION=str(self.generation), ESS_AUTO_START=str(public).lower(),
            ESS_DATA_ROOT=str(directory), ESS_BLOB_ROOT=str(directory / "blob"), ESS_TMP_ROOT=str(directory / "tmp"),
            ESS_METADATA_ROOT=str(directory / "metadata"), ESS_RA_ROOT=str(directory / "ra"),
            ESS_CLUSTER_NAME="ess-restore-" + self.prefix, ESS_CLUSTER_TOPOLOGY="static" if self.mode == "cluster" else "none",
            ESS_CLUSTER_MEMBERS=",".join(f"{name}={self.names[name]}" for name in self.ids) if self.mode == "cluster" else "",
            ESS_CLUSTER_BOOTSTRAP=str(bootstrap).lower(), ESS_CLUSTER_DATA_PLANE_ENABLED=str(public and self.mode == "cluster").lower(),
            ESS_PUBLIC_S3_ENABLED=str(public).lower(), ESS_S3_AUTH_ENABLED="true", ESS_S3_PORT=str(self.ports[node]),
            ESS_INTERNAL_BIND="127.0.0.1", ESS_INTERNAL_PORT=str(self.internal_ports[node]),
            ESS_INTERNAL_ADVERTISED_URL=f"http://127.0.0.1:{self.internal_ports[node]}",
            ESS_INTERNAL_SECRET=INTERNAL_SECRET, ESS_REPLICATION_FACTOR="2" if self.mode == "cluster" else "1",
            ESS_WRITE_QUORUM="2" if self.mode == "cluster" else "1", ESS_ALLOW_DEGRADED_WRITES="false",
            ESS_REPAIR_ENABLED="false", ESS_SCRUB_ENABLED="false", ESS_RESTORE_BOOT=boot,
            ESS_RESTORE_READY_FILE=str(self.control / (node + ".ready.json")))
        return env

    def argv(self, node, *arguments):
        return ["elixir", "--name", self.names[node], "--cookie", COOKIE, "-S", "mix", "run",
                "--no-start", "--no-compile", *arguments]

    def start(self, boot, bootstrap=False):
        check(not self.processes, "existing owned nodes must stop before restart")
        for node in self.ids:
            ready = self.control / (node + ".ready.json")
            ready.unlink(missing_ok=True)
            output = open(self.evidence / f"{self.phase}-{node}-{boot}.log", "a")
            self.handles.append(output)
            process = subprocess.Popen(self.argv(node, "--no-halt", CONTROL, "boot"),
                env=self.env(node, boot, bootstrap), stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True)
            self.processes[node] = process
        deadline = time.monotonic() + 100
        for node, process in self.processes.items():
            ready = self.control / (node + ".ready.json")
            while not ready.exists():
                check(process.poll() is None, f"{node} exited during {boot}; see node log")
                check(time.monotonic() < deadline, f"{node} boot deadline exceeded")
                time.sleep(0.2)
        if boot != "quarantine":
            for endpoint in self.endpoints():
                while True:
                    try:
                        with urllib.request.urlopen(endpoint + "/health/ready", timeout=2) as result:
                            check(result.status == 200, "ready endpoint")
                        break
                    except (OSError, AssertionError):
                        check(time.monotonic() < deadline, "S3 ready deadline exceeded")
                        time.sleep(0.2)

    def stop(self):
        owned = list(self.processes.values())
        for process in owned:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in owned:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        self.processes.clear()
        for handle in self.handles:
            handle.close()
        self.handles.clear()

    def endpoints(self):
        return [f"http://127.0.0.1:{self.ports[node]}" for node in self.ids if node != "meta"]

    def rpc(self, operation, payload, node="data-a", private_output=None):
        tag = uuid.uuid4().hex
        source = self.control / (tag + "-input.json")
        output = Path(private_output) if private_output else self.control / (tag + "-output.json")
        save(source, payload)
        control_name = f"ess_restore_control_{tag}@127.0.0.1"
        argv = ["elixir", "--name", control_name, "--cookie", COOKIE, "-S", "mix", "run",
                "--no-start", "--no-compile", CONTROL, "rpc", operation, self.names[node], str(source), str(output)]
        command(argv, env=safe_env(), log=self.evidence / "control.log")
        return read(output)

    def prepare(self, bundle):
        for node in self.ids:
            command(self.argv(node, CONTROL, "prepare", str(bundle)), env=self.env(node, "quarantine"),
                    log=self.evidence / "prepare.log")


def client(endpoint, credentials):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=credentials["access_key"],
        aws_secret_access_key=credentials["secret_key"], region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 0},
                      connect_timeout=5, read_timeout=30, s3={"addressing_style": "path"}))


def object_record(s3, bucket, key, body, **attributes):
    result = s3.put_object(Bucket=bucket, Key=key, Body=body, Tagging="owner=restore",
                           ChecksumAlgorithm="SHA256", **attributes)
    return {"key": key, "version": result["VersionId"], "sha256": hashlib.sha256(body).hexdigest(),
            "size": len(body), "attributes": attributes}


def multipart(s3, bucket, key, complete):
    attributes = dict(ATTRS, ContentEncoding="identity")
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key, Tagging="owner=restore",
        ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE", **attributes)
    parts, digest, size = [], hashlib.sha256(), 0
    for number, body in enumerate((b"M" * (5 * 1024 * 1024), b"restore-final-part"), 1):
        checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
        result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload["UploadId"], PartNumber=number,
                               Body=body, ChecksumSHA256=checksum)
        parts.append({"PartNumber": number, "ETag": result["ETag"], "ChecksumSHA256": checksum})
        digest.update(body)
        size += len(body)
    record = {"key": key, "upload_id": upload["UploadId"], "parts": parts,
              "sha256": digest.hexdigest(), "size": size, "attributes": attributes}
    if complete:
        result = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=record["upload_id"],
                                              MultipartUpload={"Parts": parts})
        record["version"] = result["VersionId"]
    return record


def version_inventory(s3, bucket):
    entries = []
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        entries.extend((item["Key"], item["VersionId"], False) for item in page.get("Versions", []))
        entries.extend((item["Key"], item["VersionId"], True) for item in page.get("DeleteMarkers", []))
    return sorted(entries)


def seed_objects(s3, bucket):
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    cors = {"CORSRules": [{"AllowedOrigins": ["https://restore.example"], "AllowedMethods": ["GET"],
                           "AllowedHeaders": ["x-example"], "ExposeHeaders": ["ETag"], "MaxAgeSeconds": 60}]}
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors)
    cors = {"CORSRules": s3.get_bucket_cors(Bucket=bucket)["CORSRules"]}
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": TAGS})
    lifecycle = {"Rules": [{"ID": "restore-retained-config", "Status": "Disabled", "Filter": {"Prefix": "never/"},
                            "Expiration": {"Days": 30}}]}
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
    original = gzip.compress(b"restore-raw-object\x00\xff" * 128, mtime=0)
    records = [object_record(s3, bucket, "versioned", original, **ATTRS),
               object_record(s3, bucket, "versioned", b"later-original-version", **dict(ATTRS, ContentEncoding="identity")),
               object_record(s3, bucket, "shared", original, **ATTRS)]
    marker = s3.delete_object(Bucket=bucket, Key="versioned")["VersionId"]
    active = multipart(s3, bucket, "multipart-active", False)
    completed = multipart(s3, bucket, "multipart-completed", True)
    return {"schema": 1, "bucket": bucket, "objects": records, "marker": marker,
        "active": active, "completed": completed, "cors": cors, "lifecycle": lifecycle,
        "iam_policy_contract": "attached admin allow plus explicit denied-key deny; version/tag reader allow with write denied",
        "versions": version_inventory(s3, bucket), "source_hash": hashlib.sha256(original).hexdigest(),
        "source_size": len(original), "boto3": boto3.__version__, "botocore": botocore.__version__}


def verify_object(s3, bucket, record):
    result = s3.get_object(Bucket=bucket, Key=record["key"], VersionId=record["version"], ChecksumMode="ENABLED")
    digest, size = hashlib.sha256(), 0
    try:
        while chunk := result["Body"].read(65536):
            digest.update(chunk)
            size += len(chunk)
    finally:
        result["Body"].close()
    check(digest.hexdigest() == record["sha256"] and size == record["size"], "restored version bytes")
    check(all(result.get(name) == value for name, value in record["attributes"].items()), "restored attributes")
    check(s3.get_object_tagging(Bucket=bucket, Key=record["key"], VersionId=record["version"])["TagSet"] == TAGS,
          "restored version tags")
    head = s3.head_object(Bucket=bucket, Key=record["key"], VersionId=record["version"], ChecksumMode="ENABLED")
    check(head["ContentLength"] == record["size"], "restored HEAD size")
    check(bool(head.get("ChecksumSHA256")), "restored checksum attribute")


def source_phase(args):
    root, evidence = Path(args.root), Path(args.evidence)
    check(root.parent == Path("/tmp") and root.name.startswith("ess-hardening-restore-"), "disposable root required")
    fixture = root / "fixture"
    fixture.mkdir()
    evidence.mkdir(parents=True, exist_ok=True)
    prefix = uuid.uuid4().hex[:12]
    nodes = Nodes(args.mode, 1, root / "source", prefix + "_source", evidence, root / "control", "source")
    canary_file = evidence / "canary-requests.json"

    class Canary(http.server.BaseHTTPRequestHandler):
        count = 0
        def log_message(self, *unused):
            pass
        def do_DELETE(self):
            type(self).count += 1
            save(canary_file, {"requests": type(self).count})
            self.send_response(204)
            self.end_headers()

    canary = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Canary)
    threading.Thread(target=canary.serve_forever, daemon=True).start()
    save(canary_file, {"requests": 0})
    try:
        nodes.start("source", bootstrap=args.mode == "cluster")
        bucket = "hardening-restore-" + prefix
        nodes.rpc("seed", {"bucket": bucket}, private_output=fixture / "credentials.json")
        credentials = read(fixture / "credentials.json")
        s3 = client(nodes.endpoints()[0], credentials["admin"])
        expected = seed_objects(s3, bucket)
        if args.mode == "standalone":
            nodes.rpc("pack", {"hash": expected["source_hash"]})
        for node in nodes.ids:
            nodes.rpc("quiesce", {}, node=node)
        boundary = nodes.rpc("boundary", {"bucket": bucket, "hash": expected["source_hash"],
            "size": expected["source_size"], "canary": f"http://127.0.0.1:{canary.server_port}"})
        report = nodes.rpc("backup", {"sources": list(nodes.names.values()), "bundle": str(root / "bundle")})
        nodes.stop()
        nodes.start("source", bootstrap=False)
        s3.delete_object(Bucket=bucket, Key="shared")
        s3.delete_object(Bucket=bucket, Key="versioned", VersionId=expected["objects"][0]["version"])
        check(version_inventory(s3, bucket) != [tuple(item) for item in expected["versions"]], "post-backup originals changed")
        s3.close()
        nodes.stop()
        expected.update(mode=args.mode, boundary=boundary, backup=report, source_roots=[str(nodes.root)],
                        source_nodes=list(nodes.names.values()), prefix=prefix, canary_port=canary.server_port,
                        source_pid_namespace=os.readlink("/proc/self/ns/pid"),
                        source_mount_namespace=os.readlink("/proc/self/ns/mnt"))
        save(fixture / "expected.json", expected)
        save(evidence / "backup-boundary.json", {"mode": args.mode, "backup": report,
             "post_backup_original_changes_proved": True, "source_processes_stopped": True})
        recover_command = ["bwrap", "--unshare-user", "--uid", "0", "--gid", "0", "--unshare-pid",
            "--die-with-parent", "--new-session", "--ro-bind", "/", "/", "--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev",
            "--bind", str(root / "recovery"), "/tmp/recovery", "--ro-bind", str(root / "bundle"), "/tmp/backup",
            "--ro-bind", str(fixture), "/tmp/fixture", "--bind", str(evidence.resolve()), "/tmp/evidence",
            "--chdir", str(Path.cwd()), "--", args.python, str(Path(__file__).resolve()), "recover",
            "--mode", args.mode, "--root", "/tmp/recovery", "--evidence", "/tmp/evidence", "--python", args.python]
        # A pinned venv may itself live in /tmp, which is otherwise deliberately
        # hidden. Expose only that venv, never its parent or the original roots.
        python_home = Path(args.python).parent.parent
        if str(python_home).startswith("/tmp/"):
            check(not any(Path(source).is_relative_to(python_home) for source in expected["source_roots"]),
                  "Python environment must not expose source roots")
            insertion = recover_command.index("--chdir")
            recover_command[insertion:insertion] = ["--ro-bind", str(python_home), str(python_home)]
        (root / "recovery").mkdir()
        command(recover_command, env=safe_env(), log=evidence / "isolated-recovery.log", timeout=900)
        check(Canary.count == 0, "outbound work must stay disabled under the explicitly selected worker policy")
        save(evidence / "canary-requests.json", {"requests": Canary.count, "worker_policy": "all false, explicitly activated"})
        print(json.dumps({"scenario": "A36" if args.mode == "standalone" else "A37", "status": "passed",
                          "backup_id": report["backup_id"], "evidence": str(evidence)}))
    finally:
        nodes.stop()
        canary.shutdown()
        canary.server_close()


def negative_cases(evidence, control):
    cases = ["wrong-key", "missing-key", "metadata-missing", "metadata-corrupt", "body-missing", "body-corrupt"]
    manifest = read("/tmp/backup/manifest.json")
    if any(item["kind"] == "pack" for item in manifest["artifacts"]):
        cases.extend(["pack-missing", "pack-corrupt", "sidecar-missing"])
    outcomes = []
    for case in cases:
        target = Path("/tmp/recovery/negative") / case
        shutil.copytree("/tmp/backup", target)
        env = safe_env()
        if case == "wrong-key":
            env["ESS_MASTER_KEY"] = "wrong-disposable-key"
        elif case == "missing-key":
            # runtime.exs requires a prod key, so absence is exercised directly
            # by the contract test; an empty provisioned key must also fail here.
            env["ESS_MASTER_KEY"] = ""
        else:
            kind = "metadata" if case.startswith("metadata") else "pack" if case.startswith("pack") else "pack-index" if case.startswith("sidecar") else "blob"
            item = next(entry for entry in manifest["artifacts"] if entry["kind"] == kind)
            path = target / item["path"]
            if case.endswith("missing"):
                path.unlink()
            else:
                with path.open("r+b") as handle:
                    handle.truncate(max(0, item["size"] - 1))
        command(["mix", "run", "--no-start", "--no-compile", CONTROL, "verify-bundle", str(target), "invalid"],
                env=env, log=evidence / "negative-cases.log")
        outcomes.append({"case": case, "rejected": True})
    save(evidence / "negative-cases.json", outcomes)


def recover_phase(args):
    check(Path(args.root) == Path("/tmp/recovery"), "restore must run in the isolated mount namespace")
    expected = read("/tmp/fixture/expected.json")
    evidence, root = Path(args.evidence), Path(args.root)
    for source in expected["source_roots"]:
        check(not Path(source).exists(), "original data roots must be inaccessible")
    mounts = Path("/proc/self/mountinfo").read_text()
    mount_rows = [line.split() for line in mounts.splitlines()]
    backup_mount = next(row for row in mount_rows if row[4] == "/tmp/backup")
    check("ro" in backup_mount[5].split(","), "backup mount must be read-only")
    check(any(row[4] == "/tmp" for row in mount_rows) and any(row[4] == "/tmp/recovery" for row in mount_rows),
          "isolation mounts required")
    check(os.readlink("/proc/self/ns/pid") != expected["source_pid_namespace"], "independent PID namespace")
    check(os.readlink("/proc/self/ns/mnt") != expected["source_mount_namespace"], "independent mount namespace")
    (evidence / "mount-isolation.txt").write_text(mounts)
    save(evidence / "original-root-isolation.json", {"source_roots": expected["source_roots"],
         "all_inaccessible": True, "backup_read_only": True, "pid_namespace": True})
    control = root / "control"
    control.mkdir()
    negative_cases(evidence, control)
    nodes = Nodes(args.mode, 2, root / "targets", expected["prefix"] + "_recovered", evidence, control, "recovered")
    started = time.monotonic()
    try:
        nodes.prepare("/tmp/backup")
        nodes.start("quarantine", bootstrap=args.mode == "cluster")
        # Bootstrap is used only for the first empty VSR start; every restart is false.
        nodes.stop()
        nodes.start("quarantine", bootstrap=False)
        nodes.rpc("restore", {"bundle": "/tmp/backup"})
        for node in nodes.ids:
            if node != "data-a":
                nodes.rpc("install", {"bundle": "/tmp/backup"}, node=node)
        nodes.stop()
        nodes.start("quarantine", bootstrap=False)
        reports = [nodes.rpc("verify", {"bundle": "/tmp/backup"}, node=node) for node in nodes.ids]
        fences = nodes.rpc("fences", {"old_owner": expected["boundary"]["old_owner"], "hash": expected["source_hash"]})
        nodes.rpc("reject-workers", {"bundle": "/tmp/backup"})
        activations = [nodes.rpc("activate", {"bundle": "/tmp/backup"}, node=node) for node in nodes.ids]
        nodes.stop()
        nodes.start("active", bootstrap=False)
        credentials = read("/tmp/fixture/credentials.json")
        for endpoint in nodes.endpoints():
            s3 = client(endpoint, credentials["admin"])
            check(version_inventory(s3, expected["bucket"]) == [tuple(item) for item in expected["versions"]], "exact restored versions and markers")
            for record in [*expected["objects"], expected["completed"]]:
                verify_object(s3, expected["bucket"], record)
            check(s3.get_bucket_versioning(Bucket=expected["bucket"])["Status"] == "Enabled", "versioning configuration")
            check(s3.get_bucket_cors(Bucket=expected["bucket"])["CORSRules"] == expected["cors"]["CORSRules"], "CORS configuration")
            check(s3.get_bucket_tagging(Bucket=expected["bucket"])["TagSet"] == TAGS, "bucket tags")
            check(s3.get_bucket_lifecycle_configuration(Bucket=expected["bucket"])["Rules"] == expected["lifecycle"]["Rules"], "lifecycle configuration")
            check(s3.get_bucket_location(Bucket=expected["bucket"]).get("LocationConstraint") in (None, "", "us-east-1"), "bucket location")
            active = expected["active"]
            parts = s3.list_parts(Bucket=expected["bucket"], Key=active["key"], UploadId=active["upload_id"])["Parts"]
            check([{name: part[name] for name in ("PartNumber", "ETag", "ChecksumSHA256")} for part in parts] == active["parts"], "retained multipart parts")
            reader = client(endpoint, credentials["reader"])
            verify_object(reader, expected["bucket"], expected["objects"][0])
            for denied_client, key in [(reader, "reader-denied"), (s3, "denied")]:
                try:
                    denied_client.put_object(Bucket=expected["bucket"], Key=key, Body=b"denied")
                    raise AssertionError("restored IAM/policy deny was bypassed")
                except ClientError as error:
                    check(error.response["ResponseMetadata"]["HTTPStatusCode"] == 403, "restored policy denies")
            reader.close()
            s3.close()
        s3 = client(nodes.endpoints()[0], credentials["admin"])
        active = expected["active"]
        result = s3.complete_multipart_upload(Bucket=expected["bucket"], Key=active["key"], UploadId=active["upload_id"],
                                              MultipartUpload={"Parts": active["parts"]})
        verify_object(s3, expected["bucket"], dict(active, version=result["VersionId"]))
        post_activation = s3.put_object(Bucket=expected["bucket"], Key="post-activation", Body=b"activated", ChecksumAlgorithm="SHA256")
        check(bool(post_activation["VersionId"]), "ordinary writes only after validated activation")
        s3.close()
        save(evidence / "restore-verification.json", {"scenario": "A36" if args.mode == "standalone" else "A37",
            "backup_id": expected["backup"]["backup_id"], "consistency_point": expected["backup"]["consistency_point"],
            "mode": args.mode, "recovery_seconds": round(time.monotonic() - started, 3), "original_roots_inaccessible": True,
            "quarantine_restart_verified": True, "nodes": reports, "fences": fences, "activations": activations,
            "versions_bytes_tags_attributes_checksums_iam_config_multipart": True,
            "worker_policy": {worker: False for worker in WORKERS},
            "boto3": boto3.__version__, "botocore": botocore.__version__})
    finally:
        nodes.stop()


def main():
    def terminate(_signal, _frame):
        raise KeyboardInterrupt("restore runner terminated")
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["source", "recover"])
    parser.add_argument("--mode", choices=["standalone", "cluster"], required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--python", required=True)
    args = parser.parse_args()
    source_phase(args) if args.phase == "source" else recover_phase(args)


if __name__ == "__main__":
    main()
