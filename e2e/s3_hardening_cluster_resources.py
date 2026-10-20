#!/usr/bin/env python3
"""Real large RF2/W2 replica, fenced repair and external-worker resource proof."""
import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid

import boto3
import botocore
from gateway_fault_proxy import FaultProxy
from gateway_hardening import client
from hardening_acceptance import source_fingerprint
from s3_hardening_resources import MIB, PacedFile
from resource_cadence import execute_group, measure_samples
from resource_cases import manifest, runner_details, verify_coverage


class ReplicaSink:
    """Independent bounded consumer for the supported legacy bearer protocol."""
    def __init__(self):
        self.token = uuid.uuid4().hex
        self.rows = {}
        self.lock = threading.Lock()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def reply(self, code):
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True

            def authorized(self):
                return self.headers.get("Authorization") == "Bearer " + owner.token

            def do_HEAD(self):
                # Always miss: every measured job must transfer actual bytes.
                self.reply(404 if self.authorized() else 403)

            def do_PUT(self):
                if not self.authorized() or self.headers.get("Transfer-Encoding"):
                    return self.reply(403)
                self.connection.settimeout(180)
                remaining = int(self.headers["Content-Length"])
                received = 0
                digest = hashlib.sha256()
                while remaining:
                    started = time.monotonic()
                    block = self.rfile.read(min(65_536, remaining))
                    if not block:
                        return self.reply(400)
                    digest.update(block)
                    received += len(block)
                    remaining -= len(block)
                    time.sleep(max(0, len(block) / (32 * MIB) - (time.monotonic() - started)))
                with owner.lock:
                    owner.rows[self.path] = {"size": received, "hash": digest.hexdigest(),
                                             "authenticated": True}
                self.reply(200)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def page_cache():
    wanted = {"Cached", "SReclaimable", "Dirty", "Writeback"}
    return {row.split(":")[0]: int(row.split()[1]) * 1024
            for row in Path("/proc/meminfo").read_text().splitlines()
            if row.split(":")[0] in wanted}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    root = Path(os.environ["ESS_CLUSTER_E2E_ROOT"]).resolve()
    assert re.fullmatch(r"/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}", str(root))
    assert root.is_dir() and not root.is_symlink()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    bucket = "hardening-resources-" + uuid.uuid4().hex[:16]
    state = {"schema": 1, "root": str(root), "bucket": bucket, "fixtures": []}
    state_path = root / "cluster-resource-state.json"
    proxy = FaultProxy("http://127.0.0.1:9102")
    sink = ReplicaSink()
    state.update(proxy=proxy.endpoint, sink=sink.endpoint, sink_token=sink.token)
    result = {"status": "in_progress", "groups": [], "source_sha256": source_fingerprint(),
              "starting_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "versions": {"boto3": boto3.__version__, "botocore": botocore.__version__},
              "envelope": "docs/plans/s3-hardening-resource-envelope.md",
              "sampling_ms": 25, "peer_rate_bytes_per_second": 32 * MIB,
              "runner": runner_details(root), "required_cases": manifest("cluster"),
              "scope": "strict replica PUT; authenticated peer GET; fenced repair pull/push; real external handler/worker"}

    def save():
        (evidence / "results.json").write_text(json.dumps(result, indent=2))

    def write_state():
        state_path.write_text(json.dumps(state))
        state_path.chmod(0o600)

    def control(action, index=0):
        sequence = uuid.uuid4().hex
        output = root / f"cluster-resource-control-{sequence}.json"
        with (root / "cluster-resource-control.log").open("ab") as log:
            try:
                completed = subprocess.run(["elixir", "--name", f"resource_ctl_{sequence[:10]}@127.0.0.1",
                    "--cookie", "ess_cluster_e2e_cookie", "e2e/scripts/cluster_resource_control.exs",
                    action, str(state_path), str(index), str(output)],
                    stdout=log, stderr=subprocess.STDOUT, timeout=250)
            except subprocess.TimeoutExpired:
                raise RuntimeError("cluster resource controller timed out; inspect retained private log") from None
        if completed.returncode:
            raise RuntimeError("cluster resource controller failed; inspect retained private log")
        return json.loads(output.read_text())

    def samples(node):
        content = (root / "resource-samples" / node / "samples.jsonl").read_bytes()
        return [json.loads(row) for row in content.split(b"\n")[:-1] if row]

    def retained_disk():
        rows = {}
        for node in ["data-a", "data-b"]:
            files = [path for path in (root / node / "blob").rglob("*") if path.is_file()]
            rows[node] = {"files": len(files), "bytes": sum(path.stat().st_size for path in files)}
        return rows

    def staging_snapshot():
        rows = {}
        for node in ["meta", "data-a", "data-b"]:
            files = [path for path in (root / node / "tmp").rglob("*") if path.is_file()]
            rows[node] = {"files": len(files), "bytes": sum(path.stat().st_size for path in files)}
        return rows

    def measured(name, size, concurrency, operation):
        state["group"] = uuid.uuid4().hex
        write_state()
        result["active_group"] = {"name": name, "size": size, "concurrency": concurrency}
        save()
        time.sleep(1.2)
        baselines = {node: samples(node)[-10:] for node in ["meta", "data-a", "data-b"]}
        cache_before = page_cache()
        disk_before = retained_disk()
        staging_before = staging_snapshot()
        assert all(row["files"] == 0 for row in staging_before.values()), "preexisting fixture staging junk"
        window, operation_failure = execute_group(operation, concurrency)
        time.sleep(1.2)
        limits = {"binary": (64 + 32 * concurrency) * MIB,
                  "total": (128 + 48 * concurrency) * MIB,
                  "rss": (256 + 64 * concurrency) * MIB,
                  "queue_max": 128, "queue_total": 2048,
                  "temporary": 2 * size * concurrency + 64 * MIB}
        nodes = {}
        failures = []
        for node, baseline in baselines.items():
            all_rows = samples(node)
            nodes[node] = measure_samples(all_rows, baseline, window["start_ns"], window["end_ns"], limits)
            failures.extend(f"{node}:{key}" for key in nodes[node]["failures"])
        staging_after = staging_snapshot()
        failures.extend(f"{node}:leaked_staging_files" for node, value in staging_after.items()
                        if value["files"] != 0)
        if operation_failure is not None:
            failures.append("operation_failed")
        row = {"name": name, "size": size, "concurrency": concurrency,
               **window, "nodes": nodes, "limits": limits,
               "page_cache_host_before": cache_before, "page_cache_host_after": page_cache(),
               "retained_cas_before": disk_before, "retained_cas_after": retained_disk(),
               "staging_before": staging_before, "staging_after": staging_after,
               "failures": failures}
        result["groups"].append(row)
        save()
        print(json.dumps(row), flush=True)
        if operation_failure is not None:
            raise operation_failure
        assert not failures, f"predeclared cluster resource envelope exceeded: {failures}"

    def terminate(*_):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        # Wake active controller/HTTP calls before executor shutdown joins them.
        subprocess.run(["bash", "e2e/cluster_harness.sh", "stop-all"], timeout=45, check=True)
        proxy.clear()
        raise KeyboardInterrupt("cluster resources terminated")

    signal.signal(signal.SIGTERM, terminate)
    try:
        write_state()
        result["fixture"] = control("setup")
        s3 = client("http://127.0.0.1:9001", os.environ["E2E_ACCESS_KEY_ID"],
                    os.environ["E2E_SECRET_ACCESS_KEY"], read_timeout=180)
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="warm", Body=b"warm")
        response = s3.get_object(Bucket=bucket, Key="warm")
        try:
            assert response["Body"].read() == b"warm"
        finally:
            response["Body"].close()
        for size in [64 * MIB, 1024 * MIB]:
            fixtures = []
            for index in range(4):
                payload = root / f"payload-{index}.bin"
                block = bytes([index]) + bytes(range(256)) * 4095 + bytes(255)
                assert len(block) == MIB
                digest = hashlib.sha256()
                with payload.open("wb") as output:
                    for _ in range(size // MIB):
                        output.write(block)
                        digest.update(block)
                fixtures.append({"path": str(payload), "hash": digest.hexdigest(), "size": size})
            assert len({row["hash"] for row in fixtures}) == 4
            for concurrency in [1, 4]:
                state["fixtures"] = [{**row, "key": f"{size}-{concurrency}-{i}"}
                                     for i, row in enumerate(fixtures[:concurrency])]
                write_state()

                def upload(index):
                    fixture = state["fixtures"][index]
                    with PacedFile(fixture["path"], True) as body:
                        s3.put_object(Bucket=bucket, Key=fixture["key"], Body=body, ContentLength=size,
                                      Metadata={"sha256": fixture["hash"]})
                    return control("verify", index)

                measured("strict-quorum-replica-put", size, concurrency, upload)
                paths = ["/internal/v1/blobs/" + row["hash"] for row in state["fixtures"]]
                proxy.fault("pace-download", "GET", paths, rate=32 * MIB)
                measured("authenticated-peer-get", size, concurrency, lambda i: control("peer-get", i))
                assert all(proxy.count("GET", path) >= 1 for path in paths)
                for target, source, mode, method in [("data-a", "data-b", "pull", "GET"),
                                                       ("data-b", "data-a", "push", "PUT")]:
                    state.update(target=target, source=source)
                    write_state()
                    for index in range(concurrency):
                        control("verify", index)
                        control("prepare-repair", index)
                    proxy.fault("pace-download" if method == "GET" else "pace-upload",
                                method, paths, rate=32 * MIB)
                    before = [proxy.count(method, path) for path in paths]

                    def repair(index):
                        completed = control("repair", index)
                        return {"job": completed, "cas": control("verify", index)}

                    measured("fenced-repair-" + mode, size, concurrency, repair)
                    assert all(proxy.count(method, path) > count for path, count in zip(paths, before))
                proxy.clear()

                def external(index):
                    fixture = state["fixtures"][index]
                    completed = control("external", index)
                    with sink.lock:
                        actual = sink.rows["/" + bucket + "/" + fixture["key"]]
                    assert actual == {"size": size, "hash": fixture["hash"], "authenticated": True}
                    return {"job": completed, "independent_sink": actual}

                measured("cross-cluster-worker-paced-sink", size, concurrency, external)
                save()
        verify_coverage(result["groups"], result["required_cases"])
        result["final_staging"] = staging_snapshot()
        assert all(row["files"] == 0 for row in result["final_staging"].values()), "final leaked staging files"
        result.pop("active_group", None)
        result["status"] = "passed"
    except BaseException as failure:
        result.update(status="failed", error_type=type(failure).__name__)
        raise
    finally:
        proxy.close()
        sink.close()
        if (root / "resource-samples").exists():
            shutil.copytree(root / "resource-samples", evidence / "samples", dirs_exist_ok=True)
        diagnostic_failures = []
        for node in ["meta", "data-a", "data-b"]:
            path = root / node / "sanitized-upload-errors.jsonl"
            if path.exists():
                content = path.read_bytes()
                (evidence / f"sanitized-upload-errors-{node}.jsonl").write_bytes(content)
                markers = [json.loads(line).get("coverage") for line in content.splitlines() if line]
                diagnostic_failures.extend(f"{node}:{marker}" for marker in markers
                                           if marker in ["event_limit_reached", "time_limit_reached"])
        originally_passed = result["status"] == "passed"
        if diagnostic_failures:
            result.update(status="failed", diagnostic_coverage_failures=diagnostic_failures)
        save()
        if diagnostic_failures and originally_passed:
            raise AssertionError("cluster error diagnostic coverage exhausted; inspect retained results.json")


if __name__ == "__main__":
    main()
