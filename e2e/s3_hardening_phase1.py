#!/usr/bin/env python3
"""Independent signed Phase 1 checks against disposable ESS endpoints only."""
import concurrent.futures
import importlib.metadata
import http.client
import json
import os
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid

import boto3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError


class Admission:
    """Fixture budget below production's 100-token/20-per-second limiter.

    Charge setup requests and reserve an entire 32-request race before releasing
    its barrier. Racing sends do not acquire this lock or sleep after release.
    One conservative budget covers every endpoint sharing the fixture key.
    """
    def __init__(self):
        self.tokens = 32.0
        self.updated = time.monotonic()
        self.lock = threading.Lock()
        self.local = threading.local()

    def acquire(self, count=1, **_):
        if getattr(self.local, "reserved", False):
            return
        if not 1 <= count <= 32:
            raise ValueError("fixture admission reservation must contain 1..32 requests")
        with self.lock:
            while True:
                now = time.monotonic()
                self.tokens = min(32.0, self.tokens + (now - self.updated) * 10)
                self.updated = now
                if self.tokens >= count:
                    self.tokens -= count
                    return
                time.sleep(max(0.001, (count - self.tokens) / 10))

    def reserved(self, operation):
        self.local.reserved = True
        try:
            return status(operation)
        finally:
            self.local.reserved = False


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def status(operation):
    try:
        return operation()["ResponseMetadata"]["HTTPStatusCode"]
    except ClientError as error:
        return error.response["ResponseMetadata"]["HTTPStatusCode"]


def raw(endpoint, credentials, method, path, body=b"", headers=None, admission=None):
    request = AWSRequest(method=method, url=endpoint + path, data=body, headers=headers or {})
    S3SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
    wire = urllib.request.Request(request.url, data=body, method=method, headers=dict(request.headers))
    if admission:
        admission.acquire()
    try:
        with urllib.request.urlopen(wire, timeout=30) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as error:
        return error.code, error.read(), dict(error.headers)


def signed_wire(endpoint, credentials, method, path, body=b"", headers=(), service="s3", admission=None):
    """Botocore signs; raw HTTP preserves duplicate headers and S3 path segments."""
    request = AWSRequest(method=method, url=endpoint + path, data=body)
    for name, value in headers:
        request.headers.add_header(name, value)
    S3SigV4Auth(credentials, service, "us-east-1").add_auth(request)
    target = urllib.parse.urlsplit(endpoint)
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=30)
    try:
        if admission:
            admission.acquire()
        connection.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
        connection.putheader("Host", target.netloc)
        connection.putheader("Content-Length", str(len(body)))
        for name, value in request.headers.items():
            if name.lower() not in ("host", "content-length"):
                connection.putheader(name, value)
        connection.endheaders(body)
        response = connection.getresponse()
        return response.status, response.read(), dict(response.getheaders())
    finally:
        connection.close()


def signature_profile(endpoints, credentials, bucket, admission):
    keys = ["space key", "literal+plus", "literal%2Fescape", "snow-雪",
            "slashes//kept", "dot/./kept", "parent/../kept"]
    query = "probe=&probe=two&space=a%20b&literal=%2B&percent=%252F"
    for endpoint in endpoints:
        for key in keys:
            path = f"/{bucket}/" + urllib.parse.quote(key, safe="/")
            body = ("signed:" + key).encode()
            code, _, _ = signed_wire(endpoint, credentials, "PUT", path + "?" + query, body,
                                      [("x-amz-meta-duplicate", "first"),
                                       ("x-amz-meta-duplicate", "second")], admission=admission)
            check(code == 200, f"independent special path/query/header PUT {key!r}: {code}")
            code, received, _ = signed_wire(endpoint, credentials, "GET", path + "?" + query,
                                            admission=admission)
            check(code == 200 and received == body, f"special key roundtrip {key!r}: {code}")
        code, _, _ = signed_wire(endpoint, credentials, "PUT", f"/{bucket}/bad-scope", b"bad",
                                  service="sts", admission=admission)
        check(code == 403, f"invalid credential service accepted: {code}")


def synchronized(operations, admission):
    admission.acquire(len(operations))
    barrier = threading.Barrier(len(operations), timeout=30)

    def run(operation):
        barrier.wait()
        return admission.reserved(operation)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(operations)) as pool:
        return list(pool.map(run, operations))


def main():
    admission = Admission()
    endpoints = [value.rstrip("/") for value in os.environ["E2E_S3_ENDPOINTS"].split(",")]
    credentials = Credentials(os.environ["E2E_ACCESS_KEY_ID"], os.environ["E2E_SECRET_ACCESS_KEY"])
    clients = [boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key, region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"total_max_attempts": 1}, max_pool_connections=64,
                      connect_timeout=5, read_timeout=60)) for endpoint in endpoints]
    for client in clients:
        client.meta.events.register("before-call.s3", admission.acquire)
    bucket = "hardening-p1-" + uuid.uuid4().hex[:18]
    clients[0].create_bucket(Bucket=bucket)
    checks = []

    clients[0].put_object(Bucket=bucket, Key="protected", Body=b"original")
    for endpoint in endpoints:
        for query in ["acl", "retention", "legal-hold", "object-lock", "website"]:
            for method in ["PUT", "DELETE"]:
                code, _, _ = raw(endpoint, credentials, method, f"/{bucket}/protected?{query}", b"bad",
                                  admission=admission)
                check(code == 501, f"unsupported selector {query} {method}: {code}")
        for query in ["uploadId=a&uploadId=b", "versionId=", "partNumber=0&uploadId=x", "uploads&uploadId=x"]:
            code, _, _ = raw(endpoint, credentials, "PUT", f"/{bucket}/protected?{query}", b"bad",
                              admission=admission)
            check(code == 400, f"ambiguous query {query}: {code}")
    check(clients[-1].get_object(Bucket=bucket, Key="protected")["Body"].read() == b"original", "selector mutation")
    checks.extend(["unsupported-selectors", "ambiguous-identities"])
    signature_profile(endpoints, credentials, bucket, admission)
    checks.append("botocore-special-path-query-duplicate-header-signatures")

    results = synchronized([
        lambda n=n: clients[n % len(clients)].put_object(
            Bucket=bucket, Key="create-only", Body=f"writer-{n}".encode(), IfNoneMatch="*")
        for n in range(32)], admission)
    check(results.count(200) == 1 and results.count(412) == 31, f"PUT race statuses {results}")
    old = clients[0].put_object(Bucket=bucket, Key="stale", Body=b"old")["ETag"]
    clients[-1].put_object(Bucket=bucket, Key="stale", Body=b"new", IfMatch=old)
    check(status(lambda: clients[0].put_object(Bucket=bucket, Key="stale", Body=b"bad", IfMatch=old)) == 412, "stale conditional write")
    checks.extend(["32-create-only-writers", "stale-if-match"])

    uploads = []
    for n in range(32):
        s3 = clients[n % len(clients)]
        upload_id = s3.create_multipart_upload(Bucket=bucket, Key="complete-only")["UploadId"]
        etag = s3.upload_part(Bucket=bucket, Key="complete-only", UploadId=upload_id,
                              PartNumber=1, Body=b"multipart-candidate")["ETag"]
        uploads.append((upload_id, etag))
    results = synchronized([
        lambda n=n, upload_id=upload_id, etag=etag: clients[n % len(clients)].complete_multipart_upload(
            Bucket=bucket, Key="complete-only", UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]}, IfNoneMatch="*")
        for n, (upload_id, etag) in enumerate(uploads)], admission)
    check(results.count(200) == 1 and results.count(412) == 31, f"multipart race statuses {results}")
    check(clients[-1].get_object(Bucket=bucket, Key="complete-only")["Body"].read() == b"multipart-candidate", "completion body mismatch")
    checks.append("32-conditional-completions")

    upload_id = clients[0].create_multipart_upload(Bucket=bucket, Key="stale-completion")["UploadId"]
    part_etag = clients[-1].upload_part(
        Bucket=bucket, Key="stale-completion", UploadId=upload_id,
        PartNumber=1, Body=b"conditional-completion")["ETag"]
    old = clients[0].put_object(Bucket=bucket, Key="stale-completion", Body=b"old")["ETag"]
    current = clients[-1].put_object(Bucket=bucket, Key="stale-completion", Body=b"new")["ETag"]
    completion = {"Parts": [{"PartNumber": 1, "ETag": part_etag}]}
    for s3 in clients:
        check(status(lambda: s3.complete_multipart_upload(
            Bucket=bucket, Key="stale-completion", UploadId=upload_id,
            MultipartUpload=completion, IfMatch=old)) == 412,
            "stale multipart If-Match accepted")
        check(s3.get_object(Bucket=bucket, Key="stale-completion")["Body"].read() == b"new",
              "stale multipart completion changed existing bytes")
    clients[0].complete_multipart_upload(
        Bucket=bucket, Key="stale-completion", UploadId=upload_id,
        MultipartUpload=completion, IfMatch=current)
    check(clients[-1].get_object(Bucket=bucket, Key="stale-completion")["Body"].read()
          == b"conditional-completion", "valid multipart If-Match did not publish staged parts")
    checks.append("stale-and-current-multipart-if-match-across-endpoints")

    upload_id = clients[0].create_multipart_upload(Bucket=bucket, Key="binding")["UploadId"]
    for s3 in clients:
        check(status(lambda: s3.upload_part(Bucket=bucket, Key="wrong", UploadId=upload_id,
                                            PartNumber=1, Body=b"bad")) == 404, "multipart upload key binding")
        check(status(lambda: s3.list_parts(Bucket=bucket, Key="wrong", UploadId=upload_id)) == 404, "multipart list key binding")
        check(status(lambda: s3.abort_multipart_upload(Bucket=bucket, Key="wrong", UploadId=upload_id)) == 404, "multipart abort key binding")
    clients[0].abort_multipart_upload(Bucket=bucket, Key="binding", UploadId=upload_id)
    checks.append("multipart-key-binding")

    for endpoint in endpoints:
        code, _, _ = raw(endpoint, credentials, "PUT", f"/{bucket}/protected", b"bad",
                          {"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}, admission=admission)
        check(code == 400, f"bad md5 returned {code}")
        # Raw independent signer hashes this actual body; a separate declared checksum is wrong.
        code, _, _ = raw(endpoint, credentials, "PUT", f"/{bucket}/protected", b"bad",
                          {"x-amz-checksum-sha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
                          admission=admission)
        check(code == 400, f"bad sha256 checksum returned {code}")
    check(clients[0].get_object(Bucket=bucket, Key="protected")["Body"].read() == b"original", "checksum failure published")
    checks.append("independent-bad-checksums")

    history = bucket + "-history"
    clients[0].create_bucket(Bucket=history)
    clients[0].put_bucket_versioning(Bucket=history, VersioningConfiguration={"Status": "Enabled"})
    clients[-1].put_object(Bucket=history, Key="key", Body=b"historical")
    clients[0].delete_object(Bucket=history, Key="key")
    check(status(lambda: clients[-1].delete_bucket(Bucket=history)) == 409, "historical bucket deletion succeeded")
    checks.append("historical-bucket-delete")

    print(json.dumps({"profile": "phase1-independent-http", "endpoints": endpoints,
                      "bucket": bucket, "checks": checks, "result": "passed",
                      "boto3": importlib.metadata.version("boto3"),
                      "botocore": importlib.metadata.version("botocore")}, sort_keys=True))


if __name__ == "__main__":
    main()
