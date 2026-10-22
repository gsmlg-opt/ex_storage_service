#!/usr/bin/env python3
"""Real durable dispatcher retries through peer outage and whole-BEAM restart."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import threading
import time
import uuid

import boto3
import botocore

from gateway_hardening import Runtime, client


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Replica:
    def __init__(self):
        self.token = secrets.token_hex(24)
        self.healthy = False
        self.objects = {}
        self.failures = 0
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def reply(self, status, headers=()):
                self.send_response(status)
                for key, value in headers:
                    self.send_header(key, value)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def dispatch(self):
                if self.headers.get("Authorization") != "Bearer " + owner.token:
                    self.close_connection = True
                    self.reply(403)
                    return
                size = int(self.headers.get("Content-Length", "0"))
                assert 0 <= size <= 1024 * 1024
                digest, md5, count = hashlib.sha256(), hashlib.md5(), 0
                while count < size:
                    chunk = self.rfile.read(min(65536, size - count))
                    assert chunk
                    digest.update(chunk)
                    md5.update(chunk)
                    count += len(chunk)
                with owner.lock:
                    if not owner.healthy:
                        owner.failures += 1
                        self.reply(503)
                    elif self.command == "HEAD":
                        item = owner.objects.get(self.path)
                        if item is None:
                            self.reply(404)
                        else:
                            self.send_response(200)
                            self.send_header("Content-Length", str(item["size"]))
                            self.send_header("ETag", '"' + item["etag"] + '"')
                            self.end_headers()
                    elif self.command == "PUT":
                        owner.objects[self.path] = {"size": count,
                            "hash": digest.hexdigest(), "etag": md5.hexdigest()}
                        self.reply(200)
                    elif self.command == "DELETE":
                        owner.objects.pop(self.path, None)
                        self.reply(204)

            do_HEAD = dispatch
            do_PUT = dispatch
            do_DELETE = dispatch

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


class BacklogRuntime(Runtime):
    def backlog_control(self, action, **payload):
        sequence = uuid.uuid4().hex
        request = self.root / f"backlog-{sequence}-request.json"
        response = self.root / f"backlog-{sequence}-result.json"
        request.write_text(json.dumps(dict(payload, root=str(self.root))))
        request.chmod(0o600)
        with (self.root / "backlog-control.log").open("ab") as output:
            try:
                subprocess.run(["elixir", "--name", f"backlog_ctl_{sequence[:10]}@127.0.0.1",
                    "--cookie", self.cookie, "e2e/scripts/replication_backlog_control.exs",
                    action, str(request), str(response)], env=self.env, stdout=output,
                    stderr=subprocess.STDOUT, check=True, timeout=45)
            except (subprocess.SubprocessError, OSError):
                raise RuntimeError("backlog control failed; inspect private backlog-control.log") from None
        return json.loads(response.read_text())


def main():
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    assert root.parent == Path("/tmp") and root.name.startswith("ess-hardening-backlog-")
    assert not root.exists()
    root.mkdir(mode=0o700)
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    runtime, replica = BacklogRuntime(root, free_port()), Replica()
    result = {"status": "in_progress", "checks": [],
        "versions": {"boto3": boto3.__version__, "botocore": botocore.__version__},
        "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree": "uncommitted hardening implementation"}
    bucket = "backlog-acceptance"
    old, current = b"superseded-before-recovery" * 257, b"current-after-recovery" * 271

    def passed(name, detail):
        result["checks"].append({"name": name, "detail": detail, "passed": True})
        print("PASS " + name, flush=True)

    try:
        runtime.start()
        credentials = runtime.backlog_control("seed")
        s3 = client(f"http://127.0.0.1:{runtime.port}", credentials["access_key"], credentials["secret_key"])
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        runtime.backlog_control("configure", bucket=bucket, endpoint=replica.endpoint, token=replica.token)
        s3.put_object(Bucket=bucket, Key="retained", Body=old)
        version = s3.put_object(Bucket=bucket, Key="retained", Body=current)["VersionId"]
        s3.put_object(Bucket=bucket, Key="deleted", Body=old)
        s3.delete_object(Bucket=bucket, Key="deleted")
        deadline = time.monotonic() + 15
        while True:
            before = runtime.backlog_control("status")
            jobs = before["jobs"]
            assert not any(job["state"] == "failed" for job in jobs), "default retry budget exhausted before restart"
            attempted = [job for job in jobs if job["state"] == "pending" and job["attempts"] >= 1]
            if len(jobs) == 4 and attempted and replica.failures:
                break
            assert time.monotonic() < deadline, "normal dispatcher did not record durable peer-outage backlog"
            time.sleep(0.05)
        assert before["backlog"]["pending"] + before["backlog"]["running"] >= 1
        assert before["backlog"]["oldest_age_ms"] > 0
        assert "replication_backlog_scan_complete 1" in before["metrics"]
        passed("peer-outage-records-durable-backlog-and-age", before["backlog"])

        old_pid = runtime.process.pid
        runtime.stop(crash=True)
        with replica.lock:
            replica.healthy = True
        runtime.start()
        assert runtime.process.pid != old_pid
        deadline = time.monotonic() + 60
        while True:
            after = runtime.backlog_control("status")
            assert not any(job["state"] == "failed" for job in after["jobs"])
            if len(after["jobs"]) == 4 and all(job["state"] == "completed" for job in after["jobs"]):
                break
            assert time.monotonic() < deadline, "persisted jobs did not drain through the normal dispatcher"
            time.sleep(0.1)
        by_id = {job["id"]: job for job in after["jobs"]}
        assert set(by_id) == {job["id"] for job in jobs}
        for job in attempted:
            assert by_id[job["id"]]["attempts"] > job["attempts"]
            assert by_id[job["id"]]["fencing_token"] > job["fencing_token"]
            assert by_id[job["id"]]["created_at_ms"] == job["created_at_ms"]
        assert after["backlog"]["pending"] == after["backlog"]["running"] == after["backlog"]["failed"] == 0
        assert after["backlog"]["oldest_age_ms"] == 0
        with replica.lock:
            assert replica.objects["/remote/retained"]["hash"] == hashlib.sha256(current).hexdigest()
            assert replica.objects["/remote/retained"]["size"] == len(current)
            assert "/remote/deleted" not in replica.objects
        response = s3.get_object(Bucket=bucket, Key="retained", VersionId=version)
        try:
            assert response["Body"].read(len(current) + 1) == current
        finally:
            response["Body"].close()
        passed("whole-beam-restart-retries-fenced-jobs-without-stale-remote-overwrite",
            {"job_count": 4, "reclaimed_jobs": len(attempted), "backlog": after["backlog"],
             "current_remote_hash_verified": True, "deleted_remote_absent": True})
        result["status"] = "passed"
    except BaseException as failure:
        result.update(status="failed", error_type=type(failure).__name__)
        raise
    finally:
        cleanup_failures = []
        for name, action in (("runtime", runtime.close),
                             ("service_logs", lambda: runtime.retain_logs(evidence)),
                             ("replica", replica.close)):
            try:
                action()
            except BaseException as failure:
                result.update(status="failed")
                result.setdefault("cleanup_errors", []).append(
                    {"component": name, "error_type": type(failure).__name__})
                cleanup_failures.append(failure)
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print(f"Disposable replication backlog root retained: {root}", flush=True)
        if cleanup_failures:
            raise cleanup_failures[0]


if __name__ == "__main__":
    main()
