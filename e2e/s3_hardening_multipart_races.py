#!/usr/bin/env python3
"""Independent signed multipart response-loss and synchronized HTTP race probes."""
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
import threading
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from s3_hardening_multipart_restart import clients, check


def outcome(operation):
    try:
        return {"ok": True, "result": operation()}
    except ClientError as error:
        return {"ok": False, "code": error.response["Error"]["Code"],
                "status": error.response["ResponseMetadata"]["HTTPStatusCode"]}


def synchronized(operations):
    barrier = threading.Barrier(len(operations), timeout=60)

    def run(operation):
        barrier.wait()
        return outcome(operation)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(operations)) as pool:
        return list(pool.map(run, operations))


def allowed(result, codes):
    check(result["ok"] or (result["code"] in codes and result["status"] in (400, 404, 409)),
          "unexpected multipart race error: " + str({k: v for k, v in result.items() if k != "result"}))


def digest(data):
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


def composite(data):
    return base64.b64encode(hashlib.sha256(hashlib.sha256(data).digest()).digest()).decode() + "-1"


def part(s3, bucket, key, upload, data, number=1):
    result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                          PartNumber=number, Body=data, ChecksumSHA256=digest(data))
    check(result["ChecksumSHA256"] == digest(data), "part response checksum")
    return {"PartNumber": number, "ETag": result["ETag"], "ChecksumSHA256": result["ChecksumSHA256"]}


def complete(s3, bucket, key, upload, manifest):
    return s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                        MultipartUpload={"Parts": manifest})


def versions(s3, bucket, key):
    found = []
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=key):
        check(not page.get("DeleteMarkers"), "race unexpectedly created a delete marker")
        found.extend(item for item in page.get("Versions", []) if item["Key"] == key)
    check(len(found) <= 1, "race duplicated an internal completion")
    return found


def verify_object(servers, bucket, key, upload, manifest, data, version):
    for s3 in servers:
        response = s3.get_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        received = hashlib.sha256()
        size = 0
        try:
            while chunk := response["Body"].read(65536):
                received.update(chunk)
                size += len(chunk)
        finally:
            response["Body"].close()
        check(size == len(data) and received.digest() == hashlib.sha256(data).digest(), "race final raw bytes")
        check(response["ChecksumSHA256"] == composite(data) and response["ChecksumType"] == "COMPOSITE",
              "race final stored checksum")
        check(response["VersionId"] == version, "race stable current version")
        inventory = versions(s3, bucket, key)
        check(len(inventory) == 1 and inventory[0]["VersionId"] == version, "race exact version inventory")
        retry = complete(s3, bucket, key, upload, manifest)
        check(retry["VersionId"] == version, "race recovery retry stable VersionId")
        late = outcome(lambda: part(s3, bucket, key, upload, b"prohibited-late-write", 2))
        allowed(late, {"NoSuchUpload"})
        check(not late["ok"], "late write accepted after completed state")
        check(len(versions(s3, bucket, key)) == 1, "late write/retry created another version")


def discard_completion_body(endpoint, bucket, key, upload, manifest):
    root = ET.Element("CompleteMultipartUpload")
    for value in manifest:
        node = ET.SubElement(root, "Part")
        for name in ("PartNumber", "ETag", "ChecksumSHA256"):
            ET.SubElement(node, name).text = str(value[name])
    body = ET.tostring(root)
    path = f"/{bucket}/{key}?uploadId=" + urllib.parse.quote(upload, safe="")
    request = AWSRequest(method="POST", url=endpoint + path, data=body,
                         headers={"Content-Type": "application/xml", "Content-Length": str(len(body))})
    credentials = Credentials(os.environ["E2E_ACCESS_KEY_ID"], os.environ["E2E_SECRET_ACCESS_KEY"])
    S3SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
    target = urllib.parse.urlsplit(endpoint)
    connection_type = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
    connection = connection_type(target.hostname, target.port, timeout=60)
    try:
        connection.request("POST", path, body=body, headers=dict(request.headers))
        response = connection.getresponse()
        status = response.status
        # Deliberately never call read(): no completion XML or VersionId is delivered
        # to this application. Header parsing may buffer socket bytes internally.
        response.close()
        check(status == 200, "discarded completion HTTP status")
    finally:
        connection.close()


def initiate(s3, bucket, key, data):
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key,
        ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE")["UploadId"]
    return upload, [part(s3, bucket, key, upload, data)]


def response_loss(servers, endpoints, bucket):
    key, data = "response-loss", b"response-body-loss" * 1024
    upload, manifest = initiate(servers[0], bucket, key, data)
    discard_completion_body(endpoints[0], bucket, key, upload, manifest)
    # Independent HEAD establishes the committed identity before any retry.
    version = servers[-1].head_object(Bucket=bucket, Key=key)["VersionId"]
    verify_object(servers, bucket, key, upload, manifest, data, version)
    return {"case": key, "discarded": "entire completion XML unread; response and socket closed after headers",
            "version_id": version, "result": "passed"}


def overwrite_race(servers, bucket, index):
    key = f"overwrite-{index}"
    original, replacement = b"o" * (5 * 1024 * 1024), b"r" * (5 * 1024 * 1024)
    a, b = servers[index % len(servers)], servers[(index + 1) % len(servers)]
    upload, manifest = initiate(a, bucket, key, original)
    completed, overwritten = synchronized([
        lambda: complete(a, bucket, key, upload, manifest),
        lambda: part(b, bucket, key, upload, replacement)])
    allowed(completed, {"InvalidPart", "ConditionalRequestConflict"})
    allowed(overwritten, {"NoSuchUpload", "ConditionalRequestConflict"})
    inventory = versions(a, bucket, key)
    check(not (completed["ok"] and overwritten["ok"]), "old manifest completed after successful part overwrite")
    if inventory:
        check(not overwritten["ok"], "successful overwrite lost behind old completion")
        expected, version = original, inventory[0]["VersionId"]
    else:
        # An error does not imply rollback: inspect the authoritative parts first.
        listing = a.list_parts(Bucket=bucket, Key=key, UploadId=upload)
        current = [{name: value[name] for name in ("PartNumber", "ETag", "ChecksumSHA256")}
                   for value in listing["Parts"]]
        check(len(current) == 1 and current[0]["ChecksumSHA256"] in (digest(original), digest(replacement)),
              "race left an unexpected part identity")
        expected = replacement if current[0]["ChecksumSHA256"] == digest(replacement) else original
        if overwritten["ok"]:
            check(expected == replacement, "acknowledged overwrite missing from durable parts")
        manifest = current
        recovered = complete(b, bucket, key, upload, manifest)
        version = recovered["VersionId"]
    verify_object(servers, bucket, key, upload, manifest, expected, version)
    return {"case": key, "complete_ok": completed["ok"], "overwrite_ok": overwritten["ok"],
            "final_part": "replacement" if expected == replacement else "original", "result": "passed"}


def abort_race(servers, bucket, index):
    key, data = f"abort-{index}", b"a" * (5 * 1024 * 1024)
    a, b = servers[index % len(servers)], servers[(index + 1) % len(servers)]
    upload, manifest = initiate(a, bucket, key, data)
    completed, aborted, late = synchronized([
        lambda: complete(a, bucket, key, upload, manifest),
        lambda: b.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload),
        lambda: part(b, bucket, key, upload, b"concurrent-extra-part", 2)])
    allowed(completed, {"NoSuchUpload", "ConditionalRequestConflict"})
    allowed(aborted, {"NoSuchUpload", "ConditionalRequestConflict"})
    allowed(late, {"NoSuchUpload", "ConditionalRequestConflict"})
    inventory = versions(a, bucket, key)
    check(not (aborted["ok"] and (completed["ok"] or inventory)), "abort success coexists with published completion")
    if not inventory:
        current = outcome(lambda: a.list_parts(Bucket=bucket, Key=key, UploadId=upload))
        allowed(current, {"NoSuchUpload"})
        if current["ok"]:
            check(not aborted["ok"], "aborted upload remains writable/active")
            # Resolve a revision conflict without assuming either request rolled back.
            recovered = complete(a, bucket, key, upload, manifest)
            inventory = [{"VersionId": recovered["VersionId"]}]
    if inventory:
        verify_object(servers, bucket, key, upload, manifest, data, inventory[0]["VersionId"])
        terminal = "completed"
    else:
        terminal = "aborted"
        for s3 in servers:
            check(not versions(s3, bucket, key), "aborted race published an object")
            for operation in (lambda: part(s3, bucket, key, upload, b"late-after-abort", 2),
                              lambda: complete(s3, bucket, key, upload, manifest),
                              lambda: s3.list_parts(Bucket=bucket, Key=key, UploadId=upload)):
                result = outcome(operation)
                allowed(result, {"NoSuchUpload"})
                check(not result["ok"], "aborted upload revived")
            check(not versions(s3, bucket, key), "late write revived aborted object")
    return {"case": key, "complete_ok": completed["ok"], "abort_ok": aborted["ok"],
            "concurrent_part_ok": late["ok"], "terminal": terminal, "result": "passed"}


def main():
    endpoints, servers = clients()
    bucket = "hardening-multipart-races-" + uuid.uuid4().hex[:16]
    s3 = servers[0]
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    print(json.dumps({"profile": "multipart-races", "event": "started", "bucket": bucket,
                      "endpoints": endpoints}), flush=True)
    # Retain disposable state on failure for inspection; cleanup follows passing invariants only.
    results = [response_loss(servers, endpoints, bucket)]
    print(json.dumps(results[-1]), flush=True)
    for index in range(4):
        results.append(overwrite_race(servers, bucket, index))
        print(json.dumps(results[-1]), flush=True)
        results.append(abort_race(servers, bucket, index))
        print(json.dumps(results[-1]), flush=True)
    check(not s3.list_multipart_uploads(Bucket=bucket).get("Uploads"), "race left an active upload")
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
            s3.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
    s3.delete_bucket(Bucket=bucket)
    print(json.dumps({"profile": "multipart-races", "result": "passed", "endpoints": endpoints,
                      "checks": results, "synchronization": "thread barriers; no sleeps"}))


if __name__ == "__main__":
    main()
