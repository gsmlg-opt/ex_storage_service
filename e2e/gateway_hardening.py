#!/usr/bin/env python3
"""Real RustFS gateway/fault/restart acceptance; exclusively disposable roots.

Run after production compilation, with pinned hardening-requirements installed:
  python e2e/gateway_hardening.py --root /tmp/ess-s3-hardening-gateway-RUN \
    --mc /tmp/ess-s3-hardening-tools/mc
No production endpoints are accepted. Logs/results survive service cleanup.
"""
import argparse
import base64
import concurrent.futures
import gzip
import hashlib
import hmac
import importlib.metadata
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError

from gateway_fault_proxy import FaultProxy


def check(value, message):
    if not value:
        raise AssertionError(message)


def wait_http(url, process=None):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process and process.poll() is not None:
            raise RuntimeError("owned service exited; inspect retained log")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise TimeoutError(f"service readiness failed: {url}")


def client(endpoint, access, secret, read_timeout=75):
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                        aws_secret_access_key=secret, region_name="us-east-1",
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      retries={"total_max_attempts": 1},
                                      connect_timeout=5, read_timeout=read_timeout))


def error(operation, expected_status, expected_code=None):
    try:
        operation()
    except ClientError as failure:
        check(failure.response["ResponseMetadata"]["HTTPStatusCode"] == expected_status,
              f"unexpected status: {failure.response['ResponseMetadata']['HTTPStatusCode']}")
        if expected_code:
            check(failure.response["Error"]["Code"] == expected_code,
                  f"unexpected error code: {failure.response['Error']['Code']}")
        return
    raise AssertionError("operation unexpectedly succeeded")


def raw(endpoint, credentials, bucket, key, body, headers=None, signed_body=None, method="PUT"):
    path = f"/{bucket}/{key}"
    headers = dict(headers or {})
    headers.setdefault("x-amz-content-sha256", hashlib.sha256(
        body if signed_body is None else signed_body).hexdigest())
    headers["content-length"] = str(len(body))
    request = AWSRequest(method=method, url=endpoint + path, data=body, headers=headers)
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
    wire = urllib.request.Request(request.url, data=body, method=method, headers=dict(request.headers))
    try:
        with urllib.request.urlopen(wire, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as failure:
        return failure.code, failure.read()


def signed_chunk_put(endpoint, credentials, bucket, key, corrupt=False, damage=None):
    payload = b"123456789"
    placeholder = b"9;chunk-signature=" + b"0" * 64 + b"\r\n" + payload + b"\r\n0;chunk-signature=" + b"0" * 64 + b"\r\n\r\n"
    wire_length = len(placeholder) - (2 if damage == "truncated" else 0)
    request = AWSRequest(method="PUT", url=f"{endpoint}/{bucket}/{key}", data=placeholder,
                         headers={"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                                  "content-encoding": "aws-chunked",
                                  "x-amz-decoded-content-length": "8" if damage == "decoded-length" else "9",
                                  "content-length": str(wire_length)})
    SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
    authorization = request.headers["Authorization"]
    scope = authorization.split("Credential=", 1)[1].split(",", 1)[0].split("/", 1)[1]
    previous = authorization.split("Signature=", 1)[1]
    date = request.headers["X-Amz-Date"]
    signing_key = ("AWS4" + credentials.secret_key).encode()
    for item in scope.split("/"):
        signing_key = hmac.new(signing_key, item.encode(), hashlib.sha256).digest()
    chunks = []
    for chunk in [payload, b""]:
        to_sign = "\n".join(["AWS4-HMAC-SHA256-PAYLOAD", date, scope, previous,
                              hashlib.sha256(b"").hexdigest(), hashlib.sha256(chunk).hexdigest()])
        signature = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()
        chunks.append(f"{len(chunk):x};chunk-signature={signature}\r\n".encode() + chunk + b"\r\n")
        previous = signature
    body = b"".join(chunks)
    if corrupt:
        body = body.replace(payload, b"923456789", 1)
    if damage == "terminal-signature":
        body = body.replace(previous.encode(), b"0" * 64)
    if damage == "truncated":
        body = body[:-2]
    check(len(body) == wire_length, "signed fixture framing length changed")
    wire = urllib.request.Request(request.url, data=body, method="PUT", headers=dict(request.headers))
    try:
        with urllib.request.urlopen(wire, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as failure:
        return failure.code


class Runtime:
    def __init__(self, root, port):
        self.root = root
        self.port = port
        self.name = f"ess_gateway_{uuid.uuid4().hex[:10]}@127.0.0.1"
        self.cookie = secrets.token_hex(24)
        self.process = None
        self.generation = 0
        self.handles = []
        self.requires = []
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("ESS_")}
        self.env.update(MIX_ENV="prod", PAGER="cat", ESS_MODE="standalone",
                        E2E_RESOURCE_PYTHON=sys.executable,
                        ESS_DATA_ROOT=str(root / "data"), ESS_BLOB_ROOT=str(root / "data/blob"),
                        ESS_TMP_ROOT=str(root / "data/tmp"), ESS_METADATA_ROOT=str(root / "data/metadata"),
                        ESS_WEB_ENABLED="false", ESS_S3_AUTH_ENABLED="true", ESS_PUBLIC_S3_ENABLED="true",
                        ESS_S3_PORT=str(port), ESS_MASTER_KEY=secrets.token_hex(32),
                        E2E_GATEWAY_CONTROL_TARGET=self.name)

    def start(self):
        self.generation += 1
        output = open(self.root / f"beam-{self.generation}.log", "wb")
        self.handles.append(output)
        requires = [argument for path in self.requires for argument in ["-r", path]]
        self.process = subprocess.Popen(["elixir", "--name", self.name, "--cookie", self.cookie,
                                         *requires, "-S", "mix", "phx.server", "--no-compile"],
                                        env=self.env, stdout=output, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        wait_http(f"http://127.0.0.1:{self.port}/health", self.process)

    def stop(self, crash=False):
        if self.process and self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL if crash else signal.SIGTERM)
            except ProcessLookupError:
                pass  # Reap an already exited owned child below.
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.process = None
        if "E2E_RESOURCE_ROOT" in self.env:
            subprocess.run([self.env["E2E_RESOURCE_PYTHON"], "-B",
                            "e2e/scripts/resource_observer.py", "verify-stopped",
                            "--root", self.env["E2E_RESOURCE_ROOT"]],
                           env=self.env, check=True, timeout=10)

    def control(self, action, payload):
        sequence = uuid.uuid4().hex
        request = self.root / f"rpc-{sequence}-request.json"
        response = self.root / f"rpc-{sequence}-result.json"
        request.write_text(json.dumps(payload))
        request.chmod(0o600)
        with open(self.root / "control.log", "ab") as output:
            try:
                subprocess.run(["elixir", "--name", f"gateway_ctl_{sequence[:10]}@127.0.0.1",
                                "--cookie", self.cookie, "e2e/scripts/gateway_hardening_control.exs",
                                action, str(request), str(response)], env=self.env, stdout=output,
                               stderr=subprocess.STDOUT, check=True, timeout=45)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                # subprocess exceptions include argv, which contains the cookie.
                raise RuntimeError("gateway controller failed; inspect private control.log") from None
        return json.loads(response.read_text())

    def close(self):
        self.stop()
        for handle in self.handles:
            handle.close()

    def retain_logs(self, evidence):
        """Export owned service logs; credentials and RPC files stay private."""
        sensitive = re.compile(r"access|secret|master_key|cookie|password|token|credential|authorization", re.I)
        secrets = {self.cookie}

        def collect(value, private=False):
            if isinstance(value, dict):
                for key, item in value.items():
                    collect(item, private or bool(sensitive.search(key)))
            elif isinstance(value, list):
                for item in value:
                    collect(item, private)
            elif private and isinstance(value, str) and value:
                secrets.add(value)

        collect(self.env)
        for pattern in ("*-request.json", "*-result.json"):
            for private in self.root.glob(pattern):
                collect(json.loads(private.read_text()))
        variants = {encoded for value in secrets for encoded in
                    (value, urllib.parse.quote(value, safe=""), json.dumps(value)[1:-1]) if encoded}
        variants = sorted(variants, key=len, reverse=True)
        for source in self.root.glob("beam-*.log"):
            if not re.fullmatch(r"beam-[0-9]+\.log", source.name) or source.is_symlink():
                continue
            with source.open(encoding="utf-8", errors="replace") as incoming, \
                    (Path(evidence) / source.name).open("w") as outgoing:
                for line in incoming:
                    if "::add-mask::" in line:
                        outgoing.write("[redacted credential mask]\n")
                        continue
                    for value in variants:
                        line = line.replace(value, "[redacted]")
                    line = re.sub(r"(https?://)[^\s/@]+:[^\s/@]+@", r"\1[redacted]@", line)
                    outgoing.write(line)


def main():
    def terminate(_signal, _frame):
        raise KeyboardInterrupt("gateway acceptance terminated")

    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--ess-port", type=int, default=19000)
    parser.add_argument("--rustfs-port", type=int, default=19100)
    parser.add_argument("--mc", required=True)
    parser.add_argument("--scope", choices=("all", "features"), default="all")
    args = parser.parse_args()
    root = args.root.resolve()
    check(root.parent == Path("/tmp") and root.name.startswith("ess-s3-hardening-") and not root.exists(),
          "root must be a fresh /tmp/ess-s3-hardening-* directory")
    root.mkdir(mode=0o700)
    container = "ess-gateway-" + uuid.uuid4().hex[:12]
    remote_endpoint = f"http://127.0.0.1:{args.rustfs_port}"
    endpoint = f"http://127.0.0.1:{args.ess_port}"
    remote_access, remote_secret = "rustfsadmin", "rustfsadmin"
    bucket, remote_bucket = "gateway-hardening", "upstream-hardening"
    runtime, proxy = Runtime(root, args.ess_port), None
    evidence = {"scope": args.scope, "checks": [], "versions": {name: importlib.metadata.version(name)
                for name in ["boto3", "botocore"]}, "object_sizes": {}, "complete": False}
    result_path = root / "gateway-results.json"

    def passed(name, details=None):
        evidence["checks"].append({"name": name, "passed": True, "details": details})
        result_path.write_text(json.dumps(evidence, indent=2))
        print(f"PASS {name}", flush=True)

    def mc_compatibility():
        mc_env = dict(os.environ, MC_CONFIG_DIR=str(root / "mc-config"), ESS_S3_ENDPOINT=endpoint,
                      RUSTFS_ENDPOINT=remote_endpoint, RUSTFS_ACCESS_KEY=remote_access,
                      RUSTFS_SECRET_KEY=remote_secret, RUSTFS_BUCKET=remote_bucket, LOCAL_BUCKET=bucket,
                      E2E_ACCESS_KEY_ID=credentials["access_key"], E2E_SECRET_ACCESS_KEY=credentials["secret_key"],
                      PATH=str(Path(args.mc).absolute().parent) + os.pathsep + os.environ["PATH"])
        with open(root / "mc-cloud-cache.log", "wb") as output:
            subprocess.run(["bash", "e2e/cloud_cache_e2e.sh"], env=mc_env, stdout=output,
                           stderr=subprocess.STDOUT, check=True, timeout=180)
        passed("retained-independent-mc-cloud-cache-compatibility")

    try:
        with open(root / "rustfs-start.log", "wb") as output:
            subprocess.run(["docker", "run", "-d", "--name", container,
                            "-p", f"127.0.0.1:{args.rustfs_port}:9000",
                            "-e", f"RUSTFS_ACCESS_KEY={remote_access}",
                            "-e", f"RUSTFS_SECRET_KEY={remote_secret}",
                            "-e", "RUSTFS_CONSOLE_ENABLE=false", "rustfs/rustfs:1.0.1"],
                           stdout=output, stderr=subprocess.STDOUT, check=True, timeout=45)
        evidence["versions"]["rustfs_image"] = subprocess.check_output(
            ["docker", "image", "inspect", "rustfs/rustfs:1.0.1", "--format", "{{.Id}} {{json .RepoDigests}}"],
            text=True).strip()
        evidence["versions"]["mc"] = subprocess.check_output([args.mc, "--version"], text=True).strip()
        wait_http(remote_endpoint + "/health")
        upstream = client(remote_endpoint, remote_access, remote_secret)
        # RustFS /health can become live before the authenticated S3 service is ready.
        deadline = time.monotonic() + 30
        while True:
            try:
                upstream.list_buckets()
                break
            except ClientError as failure:
                if failure.response["ResponseMetadata"]["HTTPStatusCode"] != 503 or time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        upstream.create_bucket(Bucket=remote_bucket)
        upstream.put_bucket_versioning(Bucket=remote_bucket, VersioningConfiguration={"Status": "Enabled"})
        proxy = FaultProxy(remote_endpoint)
        runtime.start()
        credentials = runtime.control("seed", {"bucket": bucket, "remote_bucket": remote_bucket,
            "proxy_endpoint": proxy.endpoint, "remote_access": remote_access, "remote_secret": remote_secret})
        s3 = client(endpoint, credentials["access_key"], credentials["secret_key"])
        signing = Credentials(credentials["access_key"], credentials["secret_key"])
        payload = b"verified-original"
        s3.put_object(Bucket=bucket, Key="protected", Body=payload)
        path = f"/{remote_bucket}/protected"
        put_count = proxy.count("PUT", path)
        delete_count = proxy.count("DELETE", path)
        original_versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="protected")
        original_ids = sorted(item["VersionId"] for item in original_versions.get("Versions", []))
        all_counts = proxy.snapshot_counts()
        for selector in ["acl", "retention", "legal-hold"]:
            for method in ["PUT", "DELETE"]:
                status, body = raw(endpoint, signing, bucket, f"protected?{selector}",
                                   b"prohibited-overwrite" if method == "PUT" else b"", method=method)
                check(status == 501 and b"NotImplemented" in body,
                      f"unsupported {method} {selector} did not fail deliberately")
        check(proxy.snapshot_counts() == all_counts,
              "unsupported selector reached upstream (including HEAD/GET)")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="protected")
        check(sorted(item["VersionId"] for item in versions.get("Versions", [])) == original_ids
              and not versions.get("DeleteMarkers"), "unsupported selector changed upstream versions")
        check(upstream.get_object(Bucket=remote_bucket, Key="protected")["Body"].read() == payload,
              "unsupported selector changed upstream bytes")
        passed("unsupported-selectors-zero-upstream-requests-preserve-body-and-versions")
        all_counts = proxy.snapshot_counts()
        for method, target, body in [
                ("PUT", "?versioning", b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
                ("GET", "?versions", b""),
                ("GET", "/protected?versionId=unavailable", b""),
                ("POST", "/protected?uploads", b""),
                ("PUT", "/protected?partNumber=1&uploadId=unavailable", b"part"),
                ("POST", "/protected?uploadId=unavailable", b"<CompleteMultipartUpload/>"),
                ("DELETE", "/protected?uploadId=unavailable", b""),
                ("PUT", "/protected?tagging", b"<Tagging><TagSet/></Tagging>"),
                ("GET", "/protected?tagging", b""),
                ("DELETE", "/protected?tagging", b"")]:
            # raw() joins bucket/key; an empty key followed by ? targets the bucket.
            target = target[1:] if target.startswith("/") else target
            status, response_body = raw(endpoint, signing, bucket, target, body, method=method)
            check(status == 501 and b"NotImplemented" in response_body,
                  f"gateway-specific {method} {target} did not fail deliberately")
        check(proxy.snapshot_counts() == all_counts, "gateway-specific exclusion made an upstream request")
        check(upstream.get_object(Bucket=remote_bucket, Key="protected")["Body"].read() == payload,
              "gateway-specific exclusion changed upstream bytes")
        passed("gateway-versions-multipart-tagging-guards-zero-upstream-requests")
        for header, value in [("content-md5", base64.b64encode(b"x" * 16).decode()),
                              ("x-amz-checksum-crc32", "AAAAAA==")]:
            status, body = raw(endpoint, signing, bucket, "protected", b"different-content", {header: value})
            check(status == 400 and b"BadDigest" in body, "invalid checksum was accepted")
        status, body = raw(endpoint, signing, bucket, "protected", b"changed-payload", signed_body=b"intended-bytes!")
        check(status == 400 and b"XAmzContentSHA256Mismatch" in body, "payload hash alteration was accepted")
        check(proxy.count("PUT", path) == put_count, "invalid upload reached upstream")
        check(upstream.get_object(Bucket=remote_bucket, Key="protected")["Body"].read() == payload,
              "checksum failure mutated real RustFS content")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="protected")
        check(sorted(item["VersionId"] for item in versions.get("Versions", [])) == original_ids
              and not versions.get("DeleteMarkers"), "checksum failure changed upstream versions")
        passed("checksum-and-signed-payload-rejection-before-upstream")

        trailer_headers = {"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                           "content-encoding": "aws-chunked", "x-amz-decoded-content-length": "9",
                           "x-amz-trailer": "x-amz-checksum-crc32"}
        encoded = b"9\r\n123456789\r\n0\r\nx-amz-checksum-crc32: y/Q5Jg==\r\n\r\n"
        for bad in [encoded.replace(b"y/Q5Jg==", b"AAAAAA=="), encoded[:-3], encoded + b"unconsumed"]:
            status, _ = raw(endpoint, signing, bucket, "protected", bad, trailer_headers)
            check(status == 400, "malformed trailer upload accepted")
        for body, headers in [
                (encoded.replace(b"0\r\n", b"invalid\r\n"), trailer_headers),
                (encoded, dict(trailer_headers, **{"x-amz-decoded-content-length": "8"})),
                (encoded.replace(b"\r\n\r\n", b"\r\nx-amz-checksum-crc32: y/Q5Jg==\r\n\r\n"), trailer_headers)]:
            status, _ = raw(endpoint, signing, bucket, "protected", body, headers)
            check(status == 400, "invalid trailer termination, decoded length or duplicate trailer accepted")
        check(proxy.count("PUT", path) == put_count, "invalid trailer reached upstream")
        status, _ = raw(endpoint, signing, bucket, "valid-trailer", encoded, trailer_headers)
        check(status == 200 and upstream.get_object(Bucket=remote_bucket, Key="valid-trailer")["Body"].read() == b"123456789",
              "valid checksum trailer did not reach real RustFS correctly")
        check(signed_chunk_put(endpoint, signing, bucket, "valid-signed-chunks") == 200,
              "independent signed chunk chain rejected")
        check(upstream.get_object(Bucket=remote_bucket, Key="valid-signed-chunks")["Body"].read() == b"123456789",
              "signed chunks changed decoded upstream bytes")
        check(signed_chunk_put(endpoint, signing, bucket, "protected", corrupt=True) == 403,
              "altered signed chunk accepted")
        for damage, expected in [("terminal-signature", 403), ("truncated", 400), ("decoded-length", 400)]:
            check(signed_chunk_put(endpoint, signing, bucket, "protected", damage=damage) == expected,
                  f"signed chunk {damage} accepted")
        check(proxy.count("PUT", path) == put_count, "altered signed chunk reached upstream")
        check(upstream.get_object(Bucket=remote_bucket, Key="protected")["Body"].read() == payload,
              "invalid streaming request changed upstream bytes")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="protected")
        check(sorted(item["VersionId"] for item in versions.get("Versions", [])) == original_ids
              and not versions.get("DeleteMarkers"), "invalid streaming request changed upstream versions")
        passed("signed-chunks-and-trailers-valid-and-corrupt-truncated-trailing-rejection")

        raw_bytes = gzip.compress(b"raw-compressed-gateway-body" * 512, mtime=0)
        upstream.put_object(Bucket=remote_bucket, Key="external", Body=raw_bytes,
                            ContentEncoding="gzip", ContentType="application/octet-stream",
                            CacheControl="private", Metadata={"owner": "external"})
        ranged = s3.get_object(Bucket=bucket, Key="external", Range="bytes=3-12")
        check(ranged["Body"].read() == raw_bytes[3:13] and ranged["ContentRange"] == f"bytes 3-12/{len(raw_bytes)}",
              "cold range mismatch")
        check(ranged["ContentEncoding"] == "gzip" and ranged["Metadata"]["owner"] == "external",
              "cold range attribute mismatch")
        first = s3.get_object(Bucket=bucket, Key="external")
        check(first["Body"].read() == raw_bytes, "gzip bytes rewritten")
        replacement = b"replacement-with-another-length"
        replaced = upstream.put_object(Bucket=remote_bucket, Key="external", Body=replacement,
                                       ContentType="text/plain", CacheControl="no-store",
                                       Metadata={"owner": "replacement"})
        current = s3.get_object(Bucket=bucket, Key="external")
        check(current["Body"].read() == replacement and current["ETag"] == replaced["ETag"]
              and current["ContentLength"] == len(replacement) and current["Metadata"]["owner"] == "replacement"
              and current["CacheControl"] == "no-store", "stale cache/body attributes after replacement")
        upstream.delete_object(Bucket=remote_bucket, Key="external")
        error(lambda: s3.get_object(Bucket=bucket, Key="external"), 404, "NoSuchKey")
        passed("external-replacement-deletion-freshness-cold-range-raw-attributes")

        # Real RustFS checks the forwarded validated checksum and stores raw attrs.
        checksum_bytes = gzip.compress(b"checksum-attributes-gateway" * 32, mtime=0)
        checksum = base64.b64encode(hashlib.sha256(checksum_bytes).digest()).decode()
        s3.put_object(Bucket=bucket, Key="checksum-attributes", Body=checksum_bytes,
                      ContentType="application/x-gateway", ContentEncoding="gzip",
                      CacheControl="private", ContentDisposition="attachment", ContentLanguage="fr",
                      Metadata={"owner": "checksum-profile"}, ChecksumAlgorithm="SHA256", ChecksumSHA256=checksum)
        remote_attrs = upstream.head_object(Bucket=remote_bucket, Key="checksum-attributes", ChecksumMode="ENABLED")
        for target in [s3.head_object(Bucket=bucket, Key="checksum-attributes", ChecksumMode="ENABLED"), remote_attrs]:
            check(target["ChecksumSHA256"] == checksum and target["ContentEncoding"] == "gzip"
                  and target["ContentType"] == "application/x-gateway" and target["CacheControl"] == "private"
                  and target["ContentDisposition"] == "attachment" and target["ContentLanguage"] == "fr"
                  and target["Metadata"]["owner"] == "checksum-profile", "gateway checksum/attributes were not retained")
        check(upstream.get_object(Bucket=remote_bucket, Key="checksum-attributes")["Body"].read() == checksum_bytes,
              "verified checksum forwarding changed raw gzip bytes")
        copied = s3.copy_object(Bucket=bucket, Key="checksum-copy", CopySource={"Bucket": bucket, "Key": "checksum-attributes"})
        check("CopyObjectResult" in copied, "gateway copy failed")
        copied_head = upstream.head_object(Bucket=remote_bucket, Key="checksum-copy", ChecksumMode="ENABLED")
        check(copied_head["ChecksumSHA256"] == checksum and copied_head["ContentEncoding"] == "gzip"
              and copied_head["Metadata"]["owner"] == "checksum-profile", "gateway COPY lost recomputed checksum or attrs")
        check(upstream.get_object(Bucket=remote_bucket, Key="checksum-copy")["Body"].read() == checksum_bytes,
              "gateway checksum COPY changed actual bytes")
        passed("current-rustfs-gateway-checksum-attributes-and-copy-roundtrip")

        listed = s3.list_objects_v2(Bucket=bucket)
        check("checksum-copy" in [item["Key"] for item in listed.get("Contents", [])],
              "gateway listing omitted the copied object")
        s3.delete_object(Bucket=bucket, Key="checksum-copy")
        error(lambda: s3.get_object(Bucket=bucket, Key="checksum-copy"), 404, "NoSuchKey")
        error(lambda: upstream.get_object(Bucket=remote_bucket, Key="checksum-copy"), 404, "NoSuchKey")
        for key in ["normal-batch-one", "normal-batch-two"]:
            s3.put_object(Bucket=bucket, Key=key, Body=payload)
        deleted = s3.delete_objects(Bucket=bucket, Delete={"Objects": [
            {"Key": "normal-batch-one"}, {"Key": "normal-batch-two"}]})
        check(not deleted.get("Errors") and sorted(item["Key"] for item in deleted["Deleted"])
              == ["normal-batch-one", "normal-batch-two"], "normal gateway batch delete failed")
        for key in ["normal-batch-one", "normal-batch-two"]:
            error(lambda: upstream.get_object(Bucket=remote_bucket, Key=key), 404, "NoSuchKey")
        passed("normal-gateway-list-delete-and-batch-delete")

        if args.scope == "features":
            mc_compatibility()
            evidence["complete"] = True
            return

        # Populate cache before every injected upstream failure; never allow stale-on-error.
        check(s3.get_object(Bucket=bucket, Key="protected")["Body"].read() == payload, "cache warm failed")
        for upstream_status, expected, code in [(403, 403, "AccessDenied"), (503, 503, "ServiceUnavailable"),
                                                 (429, 503, "SlowDown")]:
            before_read_failure = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
            check(before_read_failure["cache"]["indexed"] and before_read_failure["cache"]["file"]["exists"], "read-failure cache precondition missing")
            proxy.fault("status", "HEAD", path, upstream_status)
            error(lambda: s3.get_object(Bucket=bucket, Key="protected"), expected, code)
            error(lambda: s3.head_object(Bucket=bucket, Key="protected"), expected)
            after_read_failure = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
            check(after_read_failure["object"] == before_read_failure["object"]
                  and after_read_failure["cache"] == before_read_failure["cache"],
                  "failed GET/HEAD changed cache file or index identity before later read")
        proxy.clear()
        before_denial = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
        proxy.fault("status", "PUT", path, 403)
        error(lambda: s3.put_object(Bucket=bucket, Key="protected", Body=b"denied-write"), 403, "AccessDenied")
        proxy.fault("status", "DELETE", path, 403)
        error(lambda: s3.delete_object(Bucket=bucket, Key="protected"), 403, "AccessDenied")
        batch = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "protected"}]})
        check(batch.get("Errors", [{}])[0].get("Code") == "AccessDenied" and not batch.get("Deleted"),
              "batch DELETE lost truthful per-entry upstream denial")
        after_denial = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
        check(after_denial == before_denial, "failed mutation changed local metadata/journal state")
        proxy.clear()
        check(s3.get_object(Bucket=bucket, Key="protected")["Body"].read() == payload, "failed DELETE invalidated object")
        passed("truthful-denied-unavailable-throttled-get-head-put-delete-and-batch")

        for upstream_status, code in [(503, "ServiceUnavailable"), (429, "SlowDown")]:
            for method in ["PUT", "DELETE"]:
                key = f"failed-{method.lower()}-{upstream_status}"
                s3.put_object(Bucket=bucket, Key=key, Body=payload)
                before_failure = runtime.control("inspect", {"bucket": bucket, "key": key})
                proxy.fault("status", method, f"/{remote_bucket}/{key}", upstream_status)
                operation = (lambda: s3.put_object(Bucket=bucket, Key=key, Body=b"unavailable-write")) \
                    if method == "PUT" else (lambda: s3.delete_object(Bucket=bucket, Key=key))
                error(operation, 503, code)
                proxy.clear()
                after_failure = runtime.control("inspect", {"bucket": bucket, "key": key})
                check(after_failure["object"] == before_failure["object"]
                      and after_failure["cache"] == before_failure["cache"],
                      f"failed {method} {upstream_status} changed object/cache/index identity")
                check(upstream.get_object(Bucket=remote_bucket, Key=key)["Body"].read() == payload,
                      f"failed {method} {upstream_status} changed actual upstream content")
        passed("truthful-unavailable-throttled-put-delete-preserve-local-and-upstream")

        for key in ["batch-success", "batch-denied"]:
            s3.put_object(Bucket=bucket, Key=key, Body=payload)
        denied_before_batch = runtime.control("inspect", {"bucket": bucket, "key": "batch-denied"})
        proxy.fault("status", "DELETE", f"/{remote_bucket}/batch-denied", 403)
        batch = s3.delete_objects(Bucket=bucket, Delete={"Objects": [
            {"Key": "batch-success"}, {"Key": "batch-denied"}]})
        check([item["Key"] for item in batch.get("Deleted", [])] == ["batch-success"]
              and [(item["Key"], item["Code"]) for item in batch.get("Errors", [])]
              == [("batch-denied", "AccessDenied")], "mixed batch lost per-entry outcomes")
        proxy.clear()
        check(not runtime.control("inspect", {"bucket": bucket, "key": "batch-success"})["object"]["exists"],
              "successful batch deletion retained local metadata")
        denied_after_batch = runtime.control("inspect", {"bucket": bucket, "key": "batch-denied"})
        check(denied_after_batch["object"] == denied_before_batch["object"], "denied batch lost metadata")
        # Whole-bucket index changes for the successful sibling; the denied key entry and file must not.
        for field in ["entry_identity", "file"]:
            check(denied_after_batch["cache"][field] == denied_before_batch["cache"][field],
                  "denied batch changed cache entry/file identity before any later read")
        error(lambda: upstream.get_object(Bucket=remote_bucket, Key="batch-success"), 404, "NoSuchKey")
        check(upstream.get_object(Bucket=remote_bucket, Key="batch-denied")["Body"].read() == payload,
              "denied batch deletion changed upstream")
        passed("mixed-gateway-batch-delete-real-success-and-path-specific-denial")

        before_read_timeout = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
        proxy.fault("timeout", "HEAD", path)
        started = time.monotonic()
        error(lambda: s3.get_object(Bucket=bucket, Key="protected"), 503, "ServiceUnavailable")
        elapsed = time.monotonic() - started
        check(55 <= elapsed <= 75, f"socket timeout not exercised: {elapsed}")
        proxy.clear()
        after_read_timeout = runtime.control("inspect", {"bucket": bucket, "key": "protected"})
        check(after_read_timeout["cache"] == before_read_timeout["cache"]
              and after_read_timeout["object"] == before_read_timeout["object"],
              "read timeout changed cache/index identity before later read")
        passed("real-upstream-socket-inactivity-timeout-no-stale-cache", {"elapsed_seconds": round(elapsed, 3)})

        s3.put_object(Bucket=bucket, Key="timeout-delete", Body=payload)
        before_timeout = runtime.control("inspect", {"bucket": bucket, "key": "timeout-delete"})
        proxy.fault("timeout", "DELETE", f"/{remote_bucket}/timeout-delete")
        started = time.monotonic()
        error(lambda: s3.delete_object(Bucket=bucket, Key="timeout-delete"), 503, "ServiceUnavailable")
        elapsed = time.monotonic() - started
        check(55 <= elapsed <= 75, f"DELETE socket timeout not exercised: {elapsed}")
        proxy.clear()
        after_timeout = runtime.control("inspect", {"bucket": bucket, "key": "timeout-delete"})
        check(after_timeout["object"] == before_timeout["object"]
              and after_timeout["cache"] == before_timeout["cache"]
              and any(item["key"] == "timeout-delete" for item in after_timeout["journals"]),
              "unknown DELETE timeout lost local state or durable intent")
        check(upstream.get_object(Bucket=remote_bucket, Key="timeout-delete")["Body"].read() == payload,
              "timed-out unforwarded DELETE changed upstream")
        passed("real-delete-socket-inactivity-timeout-preserves-object-and-intent",
               {"elapsed_seconds": round(elapsed, 3)})

        def restart_unknown(method, key, operation):
            remote_path = f"/{remote_bucket}/{key}"
            before = proxy.count(method, remote_path)
            held = proxy.fault("hold-success", method, remote_path)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(operation)
                check(held["observed"].wait(30), f"upstream {method} did not reach durable hold")
                state = runtime.control("inspect", {"bucket": bucket, "key": key})
                check(any(record["key"] == key for record in state["journals"]), "intent not durable before crash")
                if method == "PUT":
                    check(not state["object"]["exists"], "PUT committed locally before held response")
                    check(any(record["key"] == key and record["path_exists"] for record in state["journals"]),
                          "PUT stage missing before crash")
                runtime.stop(crash=True)
                proxy.clear()
                try:
                    pending.result(timeout=10)
                except (BotoCoreError, OSError):
                    pass
                else:
                    raise AssertionError("request survived full BEAM termination")
            runtime.start()
            after_restart = runtime.control("inspect", {"bucket": bucket, "key": key})
            check(after_restart["runtime_pid"] != state["runtime_pid"], "recovery did not replace the full BEAM process")
            check(any(record["key"] == key for record in after_restart["journals"]), "journal lost across BEAM restart")
            runtime.control("reconcile", {})
            recovered = runtime.control("inspect", {"bucket": bucket, "key": key})
            check(not any(record["key"] == key for record in recovered["journals"]), "journal not reconciled")
            check(proxy.count(method, remote_path) == before + 1, "recovery repeated an upstream mutation")
            return recovered

        recovery_bytes = b"real-rustfs-put-before-full-beam-crash"
        state = restart_unknown("PUT", "recover-put", lambda: s3.put_object(
            Bucket=bucket, Key="recover-put", Body=recovery_bytes, Metadata={"recovery": "full-beam"}))
        check(state["object"]["exists"] and state["object"]["hash"] == hashlib.sha256(recovery_bytes).hexdigest(),
              "recovered local metadata hash mismatch")
        check(s3.get_object(Bucket=bucket, Key="recover-put")["Body"].read() == recovery_bytes,
              "recovered gateway body mismatch")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="recover-put")
        check(len(versions.get("Versions", [])) == 1 and not versions.get("DeleteMarkers"),
              "PUT recovery created additional remote versions")
        passed("durable-put-journal-full-beam-restart-local-recovery-no-upstream-retry")
        state = restart_unknown("DELETE", "protected", lambda: s3.delete_object(Bucket=bucket, Key="protected"))
        check(not state["object"]["exists"], "delete recovery retained local metadata")
        error(lambda: s3.get_object(Bucket=bucket, Key="protected"), 404, "NoSuchKey")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="protected")
        check(len(versions.get("Versions", [])) == 1 and len(versions.get("DeleteMarkers", [])) == 1,
              "DELETE recovery repeated mutation")
        passed("durable-delete-journal-full-beam-restart-local-recovery-no-upstream-retry")

        def restart_commit_failure(method, key, body):
            remote_path = f"/{remote_bucket}/{key}"
            before = proxy.count(method, remote_path)
            result = runtime.control("fail-local-commit", {"bucket": bucket, "key": key,
                "method": method, "body_base64": base64.b64encode(body).decode()})
            check(result["failed_before_local_commit"], "local commit failure was not reached")
            state = runtime.control("inspect", {"bucket": bucket, "key": key})
            journal = [item for item in state["journals"] if item["key"] == key]
            check(len(journal) == 1 and journal[0]["state"] == "prepared",
                  "explicit local failure did not retain durable intent")
            check(state["object"]["exists"] == (method == "DELETE"),
                  "injected local commit failure changed local visibility")
            if method == "PUT":
                check(journal[0]["path_exists"], "explicit failed PUT lost its staged bytes")
                check(upstream.get_object(Bucket=remote_bucket, Key=key)["Body"].read() == body,
                      "explicit failure occurred before real upstream PUT success")
            else:
                error(lambda: upstream.get_object(Bucket=remote_bucket, Key=key), 404, "NoSuchKey")
            runtime.stop(crash=True)
            runtime.start()
            after_restart = runtime.control("inspect", {"bucket": bucket, "key": key})
            check(after_restart["runtime_pid"] != state["runtime_pid"]
                  and any(item["key"] == key for item in after_restart["journals"]),
                  "explicit local failure journal did not survive a full BEAM replacement")
            runtime.control("reconcile", {})
            recovered = runtime.control("inspect", {"bucket": bucket, "key": key})
            check(not any(item["key"] == key for item in recovered["journals"]),
                  "normal production reconciliation did not resolve local commit failure")
            check(proxy.count(method, remote_path) == before + 1,
                  "explicit local failure recovery repeated upstream mutation")
            return recovered

        local_failure_bytes = b"real-upstream-success-before-injected-local-commit-failure"
        state = restart_commit_failure("PUT", "commit-failure-put", local_failure_bytes)
        check(state["object"]["exists"] and state["object"]["hash"] == hashlib.sha256(local_failure_bytes).hexdigest(),
              "local commit failure recovered wrong PUT metadata")
        check(s3.get_object(Bucket=bucket, Key="commit-failure-put")["Body"].read() == local_failure_bytes,
              "local commit failure recovered wrong PUT body")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="commit-failure-put")
        check(len(versions.get("Versions", [])) == 1 and not versions.get("DeleteMarkers"),
              "explicit PUT commit failure created another upstream version")
        passed("explicit-put-local-commit-failure-full-beam-restart-normal-reconciliation")
        s3.put_object(Bucket=bucket, Key="commit-failure-delete", Body=local_failure_bytes)
        state = restart_commit_failure("DELETE", "commit-failure-delete", b"")
        check(not state["object"]["exists"], "local commit failure recovery retained deleted metadata")
        error(lambda: s3.get_object(Bucket=bucket, Key="commit-failure-delete"), 404, "NoSuchKey")
        versions = upstream.list_object_versions(Bucket=remote_bucket, Prefix="commit-failure-delete")
        check(len(versions.get("Versions", [])) == 1 and len(versions.get("DeleteMarkers", [])) == 1,
              "explicit DELETE commit failure repeated upstream mutation")
        passed("explicit-delete-local-commit-failure-full-beam-restart-normal-reconciliation")

        mc_compatibility()
        evidence["complete"] = True
    except BaseException as failure:
        evidence["failure"] = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        runtime.close()
        if proxy:
            proxy.close()
        with open(root / "rustfs.log", "wb") as output:
            subprocess.run(["docker", "logs", container], stdout=output, stderr=subprocess.STDOUT)
        subprocess.run(["docker", "rm", "-fv", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result_path.write_text(json.dumps(evidence, indent=2))
        print(f"Evidence: {result_path}", flush=True)


if __name__ == "__main__":
    main()
