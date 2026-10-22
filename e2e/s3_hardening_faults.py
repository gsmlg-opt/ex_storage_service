#!/usr/bin/env python3
"""A31 real connection faults and isolated filesystem ENOSPC acceptance.

Requires a compiled production checkout and pinned hardening Python environment.
All roots are fresh /tmp/ess-hardening-fault-*; no production inputs are accepted.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from gateway_fault_proxy import FaultProxy
from gateway_hardening import Runtime, client, wait_http
from resource_cadence import measure_online_windows
from hardening_ports import free_port as port

MIB = 1024 * 1024


def signed_get_error(endpoint_port, signing, path):
    request = AWSRequest(method="GET", url=f"http://127.0.0.1:{endpoint_port}{path}",
        headers={"Host": f"127.0.0.1:{endpoint_port}",
                 "x-amz-content-sha256": hashlib.sha256(b"").hexdigest(),
                 "x-amz-checksum-mode": "ENABLED"})
    SigV4Auth(signing, "s3", "us-east-1").add_auth(request)
    connection = http.client.HTTPConnection("127.0.0.1", endpoint_port, timeout=15)
    try:
        connection.request("GET", path, headers=dict(request.headers))
        response = connection.getresponse()
        body = response.read(65_537)
        assert response.status == 503, "interrupted upstream GET must return ServiceUnavailable503"
        assert len(body) <= 65_536, "unbounded error body"
        assert response.getheader("Content-Length") == str(len(body)), "stale object framing on error"
        assert response.getheader("Content-Type").split(";", 1)[0] == "application/xml"
        assert not any(name.lower().startswith("x-amz-checksum-") for name, _ in response.getheaders()), \
            "object checksum described an XML error body"
        assert response.getheader("Content-Encoding") is None, "object encoding leaked onto XML"
        assert ET.fromstring(body).findtext("Code") == "ServiceUnavailable"
        return {"status": response.status, "code": "ServiceUnavailable", "body_bytes": len(body),
                "object_checksum_headers": False}
    finally:
        connection.close()


class FaultRuntime(Runtime):
    def __init__(self, root, port):
        super().__init__(root, port)
        self.online_windows = []
        self.requires = ["e2e/scripts/resource_sampler.exs"]
        self.env["E2E_RESOURCE_ROOT"] = str(root)

    def start(self):
        self.boot_started_ns = time.time_ns()
        self.boot_started_monotonic_ns = time.monotonic_ns()
        super().start()
        self.online_started()

    def online_started(self):
        self.online_windows.append({"generation": self.generation,
            "boot_started_ns": self.boot_started_ns,
            "boot_started_monotonic_ns": self.boot_started_monotonic_ns,
            "start_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns()})

    def stop(self, crash=False):
        window = self.online_windows[-1] if self.online_windows else None
        closing = window is not None and "end_ns" not in window
        if closing:
            window.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns(),
                          stop_kind="SIGKILL" if crash else "SIGTERM",
                          unexpectedly_exited=self.process is None or self.process.poll() is not None)
        super().stop(crash=crash)
        if closing:
            window.update(stop_completed_ns=time.time_ns(), stop_completed_monotonic_ns=time.monotonic_ns())

    def fault_control(self, action):
        sequence = uuid.uuid4().hex
        request = self.root / f"fault-{sequence}-request.json"
        response = self.root / f"fault-{sequence}-result.json"
        request.write_text("{}")
        request.chmod(0o600)
        with open(self.root / "fault-control.log", "ab") as output:
            try:
                subprocess.run(["elixir", "--name", f"fault_ctl_{sequence[:10]}@127.0.0.1",
                                "--cookie", self.cookie, "e2e/scripts/fault_hardening_control.exs",
                                action, str(request), str(response)], env=self.env, stdout=output,
                               stderr=subprocess.STDOUT, check=True, timeout=45)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                raise RuntimeError("fault controller failed; inspect private fault-control.log") from None
        return json.loads(response.read_text())


class FullDiskRuntime(FaultRuntime):
    """Only the CAS and its staging root occupy the bounded private tmpfs."""
    def start(self):
        self.boot_started_ns = time.time_ns()
        self.boot_started_monotonic_ns = time.monotonic_ns()
        self.generation += 1
        blob = self.root / "data/blob"
        blob.mkdir(parents=True, exist_ok=True)
        self.env["ESS_TMP_ROOT"] = str(blob / "tmp")
        runtime_tmp = self.root / "runtime-tmp"
        runtime_tmp.mkdir(mode=0o700, exist_ok=True)
        self.env["TMPDIR"] = str(runtime_tmp)
        output = open(self.root / f"beam-{self.generation}.log", "wb")
        self.handles.append(output)
        requires = [argument for path in self.requires for argument in ["-r", path]]
        self.process = subprocess.Popen([
            "bwrap", "--die-with-parent", "--ro-bind", "/", "/",
            "--dev", "/dev", "--proc", "/proc",
            "--bind", str(self.root), str(self.root),
            "--size", str(8 * MIB), "--tmpfs", str(blob),
            "elixir", "--name", self.name, "--cookie", self.cookie,
            *requires,
            "-S", "mix", "phx.server", "--no-compile"],
            env=self.env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        wait_http(f"http://127.0.0.1:{self.port}/health", self.process)
        self.online_started()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    if root.parent != Path("/tmp") or not root.name.startswith("ess-hardening-fault-") or root.exists():
        raise ValueError("fresh /tmp/ess-hardening-fault-* root required")
    root.mkdir(mode=0o700)
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    runtime = FaultRuntime(root, port())
    full_root = root / "diskfull"
    full_root.mkdir(mode=0o700)
    disk = FullDiskRuntime(full_root, port())
    container = "ess-hardening-fault-" + uuid.uuid4().hex[:12]
    upstream_port = port()
    remote_endpoint = f"http://127.0.0.1:{upstream_port}"
    proxy = None
    result = {"status": "in_progress", "checks": [], "snapshots": [],
              "limits": {"queue_max": 128, "queue_total": 2048, "process_delta": 32},
              "scope": "standalone local/gateway faults; cold range and open-reader eviction use focused HTTP contracts"}

    def terminate(_signal, _frame):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        runtime.stop()
        disk.stop()
        if proxy:
            proxy.clear()
        raise KeyboardInterrupt("fault acceptance terminated")

    signal.signal(signal.SIGTERM, terminate)

    def passed(name, details=None):
        result["checks"].append({"name": name, "passed": True, "details": details})
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print("PASS " + name, flush=True)

    def clean(service, baseline=None):
        deadline = time.monotonic() + 20
        while True:
            snapshot = service.fault_control("snapshot")
            result["snapshots"].append(snapshot)
            queues_ok = snapshot["queue_max"] <= 128 and snapshot["queue_total"] <= 2048
            processes_ok = baseline is None or snapshot["process_count"] <= baseline["process_count"] + 32
            if snapshot["staging_files"] == 0 and queues_ok and processes_ok:
                return snapshot
            if time.monotonic() >= deadline:
                raise AssertionError("fault leaked staging/tasks or exceeded queue envelope: " + json.dumps(snapshot))
            time.sleep(.5)

    try:
        subprocess.run(["docker", "run", "-d", "--name", container,
                        "-p", f"127.0.0.1:{upstream_port}:9000",
                        "-e", "RUSTFS_ACCESS_KEY=rustfsadmin", "-e", "RUSTFS_SECRET_KEY=rustfsadmin",
                        "-e", "RUSTFS_CONSOLE_ENABLE=false", "rustfs/rustfs:1.0.1"],
                       check=True, stdout=subprocess.DEVNULL, timeout=45)
        wait_http(remote_endpoint + "/health")
        remote = client(remote_endpoint, "rustfsadmin", "rustfsadmin")
        deadline = time.monotonic() + 30
        while True:
            try:
                remote.create_bucket(Bucket="fault-upstream")
                break
            except ClientError as failure:
                if failure.response["ResponseMetadata"]["HTTPStatusCode"] != 503 or time.monotonic() >= deadline:
                    raise
                time.sleep(.25)
        proxy = FaultProxy(remote_endpoint)
        runtime.start()
        credentials = runtime.control("seed", {"bucket": "fault-gateway", "remote_bucket": "fault-upstream",
            "proxy_endpoint": proxy.endpoint, "remote_access": "rustfsadmin", "remote_secret": "rustfsadmin"})
        runtime.control("resource-config", {"bucket": "fault-gateway", "local_bucket": "fault-local"})
        endpoint = f"http://127.0.0.1:{runtime.port}"
        s3 = client(endpoint, credentials["access_key"], credentials["secret_key"], read_timeout=15)
        signing = Credentials(credentials["access_key"], credentials["secret_key"])
        original = b"original-complete-body"
        s3.put_object(Bucket="fault-local", Key="interrupted", Body=original)
        baseline = clean(runtime)
        for bucket in ["fault-local", "fault-gateway"]:
            # A real signed request declares8MiB, transmits64KiB, then loses its
            # owner socket. No SDK retry can turn it into a completed request.
            request = AWSRequest(method="PUT", url=f"{endpoint}/{bucket}/interrupted", data=b"",
                headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD", "Content-Length": str(8 * MIB),
                         "Host": f"127.0.0.1:{runtime.port}"})
            SigV4Auth(signing, "s3", "us-east-1").add_auth(request)
            connection = http.client.HTTPConnection("127.0.0.1", runtime.port, timeout=5)
            connection.putrequest("PUT", f"/{bucket}/interrupted", skip_host=True, skip_accept_encoding=True)
            for name, value in request.headers.items():
                connection.putheader(name, value)
            connection.endheaders()
            connection.send(b"x" * 65_536)
            deadline = time.monotonic() + 10
            while runtime.fault_control("snapshot")["staging_files"] == 0:
                if time.monotonic() >= deadline:
                    raise AssertionError("signed upload never reached the staging reader")
                time.sleep(.1)
            connection.close()
            time.sleep(.5)
            clean(runtime, baseline)
            if bucket == "fault-local":
                assert s3.get_object(Bucket=bucket, Key="interrupted")["Body"].read() == original
            else:
                state = runtime.control("inspect", {"bucket": bucket, "key": "interrupted"})
                assert not state["object"]["exists"] and not state["cache"]["indexed"]
                assert proxy.count("PUT", "/fault-upstream/interrupted") == 0
            passed("signed-upload-disconnect-" + bucket)

        payload = bytes(range(256)) * (8 * MIB // 256)
        remote.put_object(Bucket="fault-upstream", Key="truncated", Body=payload)
        rule = proxy.fault("interrupt-download", "GET", "/fault-upstream/truncated")
        framing = signed_get_error(runtime.port, signing, "/fault-gateway/truncated")
        try:
            response = s3.get_object(Bucket="fault-gateway", Key="truncated")
            try:
                response["Body"].read()
            finally:
                response["Body"].close()
        except ClientError as failure:
            assert failure.response["ResponseMetadata"]["HTTPStatusCode"] == 503
            assert failure.response["Error"]["Code"] == "ServiceUnavailable"
        else:
            raise AssertionError("interrupted upstream response was reported complete")
        assert rule["observed"].wait(5), "fault did not cut the upstream body"
        proxy.clear()
        clean(runtime, baseline)
        state = runtime.control("inspect", {"bucket": "fault-gateway", "key": "truncated"})
        assert not state["cache"]["indexed"], "partial cache became visible"
        passed("upstream-response-interruption", {"raw_framing": framing, "default_sdk_error_parsed": True})

        # Cancel a real downstream response after its first bounded read.
        for bucket in ["fault-local", "fault-gateway"]:
            (s3 if bucket == "fault-local" else remote).put_object(
                Bucket=bucket if bucket == "fault-local" else "fault-upstream", Key="reader-cancel", Body=payload)
            response = s3.get_object(Bucket=bucket, Key="reader-cancel")
            assert len(response["Body"].read(65_536)) == 65_536
            response["Body"].close()
            clean(runtime, baseline)
            body = s3.get_object(Bucket=bucket, Key="reader-cancel")["Body"]
            try:
                assert hashlib.sha256(body.read()).digest() == hashlib.sha256(payload).digest()
            finally:
                body.close()
            passed("downstream-reader-cancellation-" + bucket)
        runtime.stop(crash=True)
        runtime.start()
        clean(runtime)
        assert s3.get_object(Bucket="fault-local", Key="interrupted")["Body"].read() == original
        assert s3.get_object(Bucket="fault-gateway", Key="truncated")["Body"].read() == payload
        passed("restart-after-connection-faults")

        disk.start()
        disk_credentials = disk.control("seed", {"bucket": "disk-gateway", "remote_bucket": "fault-upstream",
            "proxy_endpoint": proxy.endpoint, "remote_access": "rustfsadmin", "remote_secret": "rustfsadmin"})
        disk.control("resource-config", {"bucket": "disk-gateway", "local_bucket": "disk-local"})
        full_s3 = client(f"http://127.0.0.1:{disk.port}", disk_credentials["access_key"], disk_credentials["secret_key"], read_timeout=15)
        full_s3.put_object(Bucket="disk-local", Key="preserved", Body=original)
        fill = disk.fault_control("fill-disk")
        assert fill["enospc"], "test filesystem did not actually return ENOSPC"
        for bucket in ["disk-local", "disk-gateway"]:
            try:
                full_s3.put_object(Bucket=bucket, Key="preserved", Body=payload)
            except ClientError as failure:
                assert failure.response["ResponseMetadata"]["HTTPStatusCode"] >= 400
            else:
                raise AssertionError("disk-full upload unexpectedly succeeded")
        disk.fault_control("free-disk")
        clean(disk)
        assert full_s3.get_object(Bucket="disk-local", Key="preserved")["Body"].read() == original
        assert proxy.count("PUT", "/fault-upstream/preserved") == 0
        full_s3.put_object(Bucket="disk-local", Key="after-free", Body=original)
        assert full_s3.get_object(Bucket="disk-local", Key="after-free")["Body"].read() == original
        passed("actual-isolated-enospc-and-recovery", fill)
        # The sampler may be appending its final line during this read.
        samples = [json.loads(line) for line in (root / "samples.jsonl").read_bytes().split(b"\n")[:-1] if line]
        assert samples, "fault workload has no live telemetry samples"
        peak = {key: max(sample[key] for sample in samples) for key in ["queue_max", "queue_total"]}
        assert peak["queue_max"] <= 128 and peak["queue_total"] <= 2048, peak
        passed("continuous-mailbox-envelope", {"samples": len(samples), "peak": peak})
        result["status"] = "passed"
    except BaseException as failure:
        result["status"] = "failed"
        result["error_type"] = type(failure).__name__
        raise
    finally:
        runtime.close()
        disk.close()
        if proxy:
            proxy.close()
        subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=30)
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        for index, service_root in enumerate([root, full_root]):
            for path in service_root.glob("beam-*.log"):
                (evidence / f"service-{index}-{path.name}").write_bytes(path.read_bytes())
        if (root / "samples.jsonl").exists():
            (evidence / "samples.jsonl").write_bytes((root / "samples.jsonl").read_bytes())
        online_coverage = {}
        coverage_failures = []
        for name, service in [("local-gateway", runtime), ("isolated-enospc", disk)]:
            path = service.root / "samples.jsonl"
            samples = [json.loads(line) for line in path.read_bytes().split(b"\n")[:-1] if line] if path.exists() else []
            if path.exists():
                (evidence / f"{name}-samples.jsonl").write_bytes(path.read_bytes())
            coverage = measure_online_windows(samples, service.online_windows,
                                             {"queue_max": 128, "queue_total": 2048})
            for window in service.online_windows:
                if window.get("unexpectedly_exited"):
                    coverage["failures"].append(f"generation{window['generation']}:unexpected_runtime_exit")
            online_coverage[name] = coverage
            coverage_failures.extend(f"{name}:{failure}" for failure in coverage["failures"])
        result["online_coverage"] = online_coverage
        result["coverage_failures"] = coverage_failures
        originally_passed = result["status"] == "passed"
        if coverage_failures:
            result["status"] = "failed"
            for check in result["checks"]:
                if check["name"] == "continuous-mailbox-envelope":
                    check["passed"] = False
                    check["details"]["online_coverage_failed"] = True
        (evidence / "results.json").write_text(json.dumps(result, indent=2))
        print(f"Private disposable fault root retained: {root}", flush=True)
        if coverage_failures and originally_passed:
            raise AssertionError("fault online telemetry coverage failed; inspect retained results.json")


if __name__ == "__main__":
    main()
