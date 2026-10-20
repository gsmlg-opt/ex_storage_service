#!/usr/bin/env python3
"""Two-stage default-SDK multipart acceptance across a disposable BEAM restart."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import urllib.parse
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


ATTRIBUTES = {"ContentType": "application/ess-restart", "ContentLanguage": "zh",
              "CacheControl": "max-age=42", "ContentDisposition": "inline; filename=restart.bin",
              "ContentEncoding": "identity", "Metadata": {"owner": "multipart-restart"}}
TAGS = [{"Key": "owner", "Value": "multipart-restart"}]


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def clients():
    endpoints = os.environ["E2E_S3_ENDPOINTS"].split(",")
    for endpoint in endpoints:
        url = urllib.parse.urlsplit(endpoint)
        check(url.scheme in ("http", "https") and url.hostname in ("localhost", "127.0.0.1", "::1")
              and not url.username and not url.password and url.path in ("", "/")
              and not url.query and not url.fragment, "disposable loopback endpoint required")
    return endpoints, [boto3.client("s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"], region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 0},
                      s3={"addressing_style": "path"})) for endpoint in endpoints]


def prepare(s3, endpoints):
    bucket = "hardening-multipart-restart-" + uuid.uuid4().hex[:16]
    s3.create_bucket(Bucket=bucket)
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    records = {}
    for key in ("active", "completed"):
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key, **ATTRIBUTES,
            Tagging="owner=multipart-restart", ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE")
        parts, digests, content_hash = [], [], hashlib.sha256()
        size = 0
        for number, data in enumerate((key.encode()[:1] * (5 * 1024 * 1024),
                                       (key + "-final-part").encode()), 1):
            digest = hashlib.sha256(data).digest()
            encoded = base64.b64encode(digest).decode()
            result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload["UploadId"],
                PartNumber=number, Body=data, ChecksumSHA256=encoded)
            check(result["ChecksumSHA256"] == encoded, "prepared part checksum response")
            parts.append({"PartNumber": number, "ETag": result["ETag"], "ChecksumSHA256": encoded})
            digests.append(digest)
            content_hash.update(data)
            size += len(data)
        record = {"upload_id": upload["UploadId"], "parts": parts, "size": size,
                  "sha256": content_hash.hexdigest(), "checksum":
                  base64.b64encode(hashlib.sha256(b"".join(digests)).digest()).decode() + "-2"}
        if key == "completed":
            result = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=record["upload_id"],
                                                  MultipartUpload={"Parts": parts})
            check(result["ChecksumSHA256"] == record["checksum"], "prepared completion checksum")
            record["version_id"] = result["VersionId"]
        records[key] = record
    return {"schema": 1, "stage": "prepared", "bucket": bucket,
            "endpoints": endpoints, "objects": records}


def check_parts(s3, bucket, record):
    listing = s3.list_parts(Bucket=bucket, Key="active", UploadId=record["upload_id"], MaxParts=1)
    check(listing["IsTruncated"], "active parts first page survives restart")
    parts = list(listing["Parts"])
    listing = s3.list_parts(Bucket=bucket, Key="active", UploadId=record["upload_id"],
                            PartNumberMarker=listing["NextPartNumberMarker"], MaxParts=1)
    check(not listing["IsTruncated"], "active parts final page survives restart")
    parts.extend(listing["Parts"])
    check([{field: part[field] for field in ("PartNumber", "ETag", "ChecksumSHA256")}
           for part in parts] == record["parts"], "durable exact part identities and checksums")


def check_object(s3, bucket, key, record):
    result = s3.get_object(Bucket=bucket, Key=key, VersionId=record["version_id"], ChecksumMode="ENABLED")
    digest, count = hashlib.sha256(), 0
    try:
        while chunk := result["Body"].read(65536):
            digest.update(chunk)
            count += len(chunk)
    finally:
        result["Body"].close()
    check(count == record["size"] and digest.hexdigest() == record["sha256"], "durable raw object bytes")
    check(result["ChecksumSHA256"] == record["checksum"] and result["ChecksumType"] == "COMPOSITE",
          "durable composite checksum identity")
    check(all(result.get(name) == value for name, value in ATTRIBUTES.items()), "durable initiation attributes")
    check(s3.get_object_tagging(Bucket=bucket, Key=key, VersionId=record["version_id"])["TagSet"] == TAGS,
          "durable initiation tags")
    versions, markers = [], []
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=key):
        versions.extend(item for item in page.get("Versions", []) if item["Key"] == key)
        markers.extend(item for item in page.get("DeleteMarkers", []) if item["Key"] == key)
    check(len(versions) == 1 and versions[0]["VersionId"] == record["version_id"],
          "completion retry publishes no duplicate version")
    check(versions[0]["IsLatest"] and not markers, "durable current version without delete markers")
    try:
        s3.list_parts(Bucket=bucket, Key=key, UploadId=record["upload_id"])
        raise AssertionError("completed upload exposed as active after restart")
    except ClientError as error:
        check(error.response["Error"]["Code"] == "NoSuchUpload", "completed upload remains hidden")


def verify(servers, state):
    bucket, records = state["bucket"], state["objects"]
    for s3 in servers:
        active = s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        check([(item["Key"], item["UploadId"]) for item in active] == [("active", records["active"]["upload_id"])],
              "initiated upload survives and completed upload stays hidden")
        check_parts(s3, bucket, records["active"])
    for key, record in records.items():
        result = servers[-1].complete_multipart_upload(Bucket=bucket, Key=key,
            UploadId=record["upload_id"], MultipartUpload={"Parts": record["parts"]})
        check(result["ChecksumSHA256"] == record["checksum"], "completion checksum after restart")
        if key == "completed":
            check(result["VersionId"] == record["version_id"], "completed retry retains pre-restart VersionId")
        else:
            record["version_id"] = result["VersionId"]
        for s3 in servers:
            check_object(s3, bucket, key, record)
    for key, record in records.items():
        servers[0].delete_object(Bucket=bucket, Key=key, VersionId=record["version_id"])
    servers[0].delete_bucket(Bucket=bucket)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("prepare", "verify"))
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    root = args.state.parent.resolve()
    check(root.parent == Path("/tmp") and root.name.startswith("ess-s3-hardening-")
          and root.is_dir() and not args.state.is_symlink(), "fresh disposable state root required")
    endpoints, servers = clients()
    if args.stage == "prepare":
        check(not args.state.exists(), "refusing to overwrite restart state")
        state = prepare(servers[0], endpoints)
        with open(args.state, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as target:
            json.dump(state, target, indent=2)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
    else:
        state = json.loads(args.state.read_text())
        check(set(state) == {"schema", "stage", "bucket", "endpoints", "objects"}
              and state["schema"] == 1 and state["stage"] == "prepared"
              and state["endpoints"] == endpoints
              and state["bucket"].startswith("hardening-multipart-restart-")
              and set(state["objects"]) == {"active", "completed"}, "restart state identity mismatch")
        verify(servers, state)
    print(json.dumps({"profile": "multipart-restart", "stage": args.stage, "result": "passed",
                      "endpoints": endpoints, "boto3": boto3.__version__}))


if __name__ == "__main__":
    main()
