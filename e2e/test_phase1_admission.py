"""Actual Phase1 fixture against an independent production-rate-limit contract.

Only network delivery is replaced. SDK request preparation, independent signing,
fixture calls, conditional outcomes and both real thread barriers still execute.
"""
from contextlib import redirect_stdout
import hashlib
import io
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.parse

import boto3
from botocore.exceptions import ClientError

import s3_hardening_phase1 as fixture


class VirtualClock:
    def __init__(self):
        self.now = 0.0
        self.lock = threading.Lock()

    def monotonic(self):
        with self.lock:
            return self.now

    def sleep(self, seconds):
        if seconds <= 0:
            raise AssertionError("admission must advance the clock")
        with self.lock:
            if self.now + seconds == self.now:
                raise AssertionError("admission delay is below clock resolution")
            self.now += seconds


class Response:
    def __init__(self, status, body=b""):
        self.status = status
        self.status_code = status
        self.body = body
        self.headers = {}

    def read(self):
        return self.body

    def getheaders(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class ProductionContract:
    """Serialized 100-burst/20-per-second contract, independent of admission.

    This is the published limiter contract, not a copy of its ETS algorithm.
    Conditional failures consume requests just as the actual router does.
    """
    def __init__(self, clock):
        self.clock = clock
        self.lock = threading.Lock()
        self.allowance = 100.0
        self.updated = 0.0
        self.objects = {}
        self.uploads = {}
        self.requests = []
        self.slowdown_operations = []
        self.bursts = {}
        self.clients = []
        self.raw_requests = 0
        self.admissions = {}
        self.reservations = []

    def dispatched(self):
        thread = threading.get_ident()
        if self.admissions.get(thread, 0) < 1:
            raise AssertionError("signed setup request bypassed fixture admission")
        self.admissions[thread] -= 1

    def consume(self, operation):
        now = self.clock.monotonic()
        self.allowance = min(100.0, self.allowance + (now - self.updated) * 20.0)
        self.updated = now
        self.requests.append((operation, now))
        if self.allowance < 1:
            self.slowdown_operations.append(operation)
            raise ClientError({"Error": {"Code": "SlowDown", "Message": "Please reduce your request rate."},
                               "ResponseMetadata": {"HTTPStatusCode": 503}}, operation)
        self.allowance -= 1

    def api(self, model, context, **_):
        operation, args = model.name, context["fixture_arguments"]
        with self.lock:
            self.dispatched()
            self.consume(operation)
            key = (args.get("Bucket"), args.get("Key"))
            current = self.objects.get(key)
            upload = self.uploads.get(args.get("UploadId"))
            code, result = 200, {}
            if operation == "PutObject":
                if args.get("IfNoneMatch") == "*" and current is not None:
                    code = 412
                elif args.get("IfMatch") and args["IfMatch"] != self.etag(current):
                    code = 412
                else:
                    self.objects[key] = args["Body"]
                    result["ETag"] = self.etag(args["Body"])
                if key[1] == "create-only":
                    self.bursts.setdefault("put", []).append((threading.get_ident(), self.clock.monotonic()))
            elif operation == "GetObject":
                result.update(Body=io.BytesIO(current), ETag=self.etag(current))
            elif operation == "CreateMultipartUpload":
                upload_id = str(len(self.requests))
                self.uploads[upload_id] = dict(key=key)
                result["UploadId"] = upload_id
            elif operation in ("UploadPart", "ListParts", "AbortMultipartUpload", "CompleteMultipartUpload"):
                if upload is None or upload["key"] != key:
                    code = 404
                elif operation == "UploadPart":
                    upload["body"] = args["Body"]
                    result["ETag"] = self.etag(args["Body"])
                elif operation == "AbortMultipartUpload":
                    del self.uploads[args["UploadId"]]
                elif operation == "CompleteMultipartUpload":
                    if args.get("IfNoneMatch") == "*" and current is not None:
                        code = 412
                    elif args.get("IfMatch") and args["IfMatch"] != self.etag(current):
                        code = 412
                    else:
                        self.objects[key] = upload["body"]
                        del self.uploads[args["UploadId"]]
                    if args.get("IfNoneMatch") == "*":
                        self.bursts.setdefault("complete", []).append((threading.get_ident(), self.clock.monotonic()))
            elif operation == "DeleteObject":
                self.objects.pop(key, None)
            elif operation == "DeleteBucket":
                code = 409
            elif operation not in ("CreateBucket", "PutBucketVersioning"):
                raise AssertionError("unexpected fixture operation: " + operation)
            result["ResponseMetadata"] = {"HTTPStatusCode": code}
            if code >= 400:
                result["Error"] = {"Code": str(code), "Message": "expected fixture rejection"}
            return Response(code), result

    @staticmethod
    def etag(body):
        return '"' + hashlib.md5(body).hexdigest() + '"' if body is not None else None

    def client(self, *_args, **kwargs):
        client = self.real_client("s3", **kwargs)
        self.clients.append(client)

        def arguments(params, context, **_):
            context["fixture_arguments"] = dict(params)

        client.meta.events.register("provide-client-params.s3", arguments)
        client.meta.events.register_last("before-call.s3", self.api)
        return client

    def wire(self, method, path, body, headers):
        with self.lock:
            self.dispatched()
            if "/sts/" in headers.get("Authorization", ""):
                return Response(403)  # SigV4 rejects before the production limiter.
            self.consume("wire:" + method)
            self.raw_requests += 1
            url = urllib.parse.urlsplit(path)
            key = tuple(urllib.parse.unquote(url.path).lstrip("/").split("/", 1))
            query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            if any(name in query for name in ("acl", "retention", "legal-hold", "object-lock", "website")):
                return Response(501)
            if any(name in query for name in ("uploadId", "versionId", "partNumber")):
                return Response(400)
            lowered = {name.lower(): value for name, value in headers.items()}
            if "content-md5" in lowered or "x-amz-checksum-sha256" in lowered:
                return Response(400)
            if method == "PUT":
                self.objects[key] = body
                return Response(200)
            return Response(200, self.objects[key])

    def urlopen(self, request, **_):
        return self.wire(request.method, request.full_url, request.data or b"", dict(request.headers))

    def connection(self, *_args, **_kwargs):
        server = self

        class Connection:
            def putrequest(self, method, path, **_):
                self.method, self.path, self.headers = method, path, {}

            def putheader(self, name, value):
                self.headers[name] = value

            def endheaders(self, body):
                self.body = body

            def getresponse(self):
                return server.wire(self.method, self.path, self.body, self.headers)

            def close(self):
                pass

        return Connection()


class Phase1AdmissionTest(unittest.TestCase):
    def run_fixture(self, endpoints=1):
        clock = VirtualClock()
        server = ProductionContract(clock)
        server.real_client = boto3.client
        output = io.StringIO()
        acquire = fixture.Admission.acquire

        def record_admission(admission, count=1, **kwargs):
            acquire(admission, count=count, **kwargs)
            with server.lock:
                if count == 1:
                    thread = threading.get_ident()
                    server.admissions[thread] = server.admissions.get(thread, 0) + 1
                else:
                    server.reservations.append(count)

        urls = ",".join(f"http://127.0.0.1:{19000 + offset}" for offset in range(endpoints))
        with patch.dict("os.environ", {"E2E_S3_ENDPOINTS": urls,
                "E2E_ACCESS_KEY_ID": "fixture-access", "E2E_SECRET_ACCESS_KEY": "fixture-secret"}), \
             patch.object(fixture.boto3, "client", server.client), \
             patch.object(fixture.urllib.request, "urlopen", server.urlopen), \
             patch.object(fixture.http.client, "HTTPConnection", server.connection), \
             patch.object(fixture.Admission, "acquire", record_admission), \
             patch("time.monotonic", clock.monotonic), patch("time.sleep", clock.sleep), \
             redirect_stdout(output):
            fixture.main()
        return clock, server, json.loads(output.getvalue())

    def test_unpaced_actual_fixture_reproduces_observed_multipart_slowdown(self):
        with patch.object(fixture.Admission, "acquire", return_value=None):
            with self.assertRaises(ClientError) as error:
                self.run_fixture()
        self.assertEqual("CreateMultipartUpload", error.exception.operation_name)
        self.assertEqual("SlowDown", error.exception.response["Error"]["Code"])

    def test_actual_fixture_completes_under_production_limiter_with_32_way_bursts(self):
        for endpoints in (1, 2):
            with self.subTest(endpoints=endpoints):
                clock, server, receipt = self.run_fixture(endpoints)
                self.assertEqual("passed", receipt["result"])
                self.assertEqual(10, len(receipt["checks"]))
                self.assertEqual([], server.slowdown_operations)
                self.assertGreater(clock.monotonic(), 0)
                self.assertGreater(server.raw_requests, 20)
                self.assertEqual([32, 32], server.reservations)
                self.assertTrue(all(remaining == 0 for remaining in server.admissions.values()))
                for name in ("put", "complete"):
                    group = server.bursts[name]
                    self.assertEqual(32, len(group))
                    self.assertEqual(32, len({thread for thread, _ in group}))
                    self.assertEqual(1, len({when for _, when in group}), "burst was paced after its barrier")
                for client in server.clients:
                    self.assertEqual(1, client.meta.config.retries["total_max_attempts"])
                    self.assertEqual("s3v4", client.meta.config.signature_version)


if __name__ == "__main__":
    unittest.main()
