#!/usr/bin/env python3
"""Default Boto3 multipart/checksum/attribute acceptance on disposable loopback."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.parse
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cluster_iam_hardening import control


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def check_error(operation, status, code):
    try:
        operation()
    except ClientError as failure:
        check(failure.response["ResponseMetadata"]["HTTPStatusCode"] == status
              and failure.response["Error"]["Code"] == code, "part-copy error identity")
        return
    raise AssertionError("invalid part copy succeeded")


def remove_local_source(bucket, stage, content, **identity):
    root = Path(os.environ["ESS_CLUSTER_E2E_ROOT"]).resolve()
    check(root.parent == Path("/tmp") and
          re.fullmatch(r"ess-s3-hardening-cluster-[A-Za-z0-9]{8}", root.name)
          and root.is_dir(), "generated disposable cluster root required")
    digest = hashlib.sha256(content).hexdigest()
    state = {"schema": 1, "root": str(root), "bucket": bucket, "stage": stage,
             "hash": digest, "size": len(content), **identity}
    state_path = root / "multipart-source-control.json"
    descriptor = os.open(state_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as sink:
        json.dump(state, sink)
    control("remove-multipart-source-local", state_path, bucket)
    result_path = Path(str(state_path) + ".result.json")
    result = json.loads(result_path.read_text())
    check(result == {"schema": 1, "stage": stage, "hash": digest, "size": len(content),
                     "removed_node": "data-a", "verified_peer": "data-b", "local_absent": True,
                     "locations_unchanged": True, "replication_factor": 2, "write_quorum": 2},
          "guarded remote-source fixture evidence mismatch")
    local_path = root / "data-a" / "blob" / "objects" / "sha256" / digest[:2] / digest[2:]
    check(not local_path.exists() and not local_path.is_symlink(),
          "local source must be absent immediately before HTTP operation")
    result_path.rename(root / f"multipart-remote-{stage}-evidence.json")
    state_path.with_name("controller-remove-multipart-source-local.log").rename(
        root / f"multipart-remote-{stage}-controller.log")
    return result


def check_part_copy(clients, bucket, uploads, cluster):
    # A unique hash prevents this file-loss fixture from touching a shared blob.
    source_bytes = bucket.rsplit("-", 1)[-1].encode() + b":historical-part-copy-body"
    historical = clients[0].put_object(Bucket=bucket, Key="copy-source", Body=source_bytes)
    clients[-1].put_object(Bucket=bucket, Key="copy-source", Body=b"different-current-source")
    selected = source_bytes[3:13]
    part_digest = hashlib.sha256(selected).digest()
    part_checksum = base64.b64encode(part_digest).decode()
    object_checksum = base64.b64encode(hashlib.sha256(part_digest).digest()).decode() + "-1"
    source = {"Bucket": bucket, "Key": "copy-source", "VersionId": historical["VersionId"]}
    remote_evidence = []
    for index, s3 in enumerate(clients):
        key = f"copied-part-{index}"
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key,
            ContentType="application/ess-part-copy", Metadata={"owner": "part-copy"},
            Tagging="owner=part-copy", ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE")["UploadId"]
        uploads.append((key, upload))
        params = {"Bucket": bucket, "Key": key, "UploadId": upload, "PartNumber": 1,
                  "CopySource": source, "CopySourceRange": "bytes=3-12"}
        check_error(lambda: s3.upload_part_copy(**params, CopySourceIfMatch='"stale"'),
                    412, "PreconditionFailed")
        invalid_range = dict(params, CopySourceRange="bytes=999-1000")
        check_error(lambda: s3.upload_part_copy(**invalid_range), 416, "InvalidRange")
        check(not s3.list_parts(Bucket=bucket, Key=key, UploadId=upload).get("Parts"),
              "rejected copy leaves no part")
        if cluster and index == 0:
            remote_evidence.append(remove_local_source(bucket, "object", source_bytes,
                key="copy-source", version_id=historical["VersionId"]))
        copied = s3.upload_part_copy(**params, CopySourceIfMatch=historical["ETag"])
        check(copied["CopySourceVersionId"] == historical["VersionId"],
              "part copy identifies selected source version")
        result = copied["CopyPartResult"]
        check(result["ChecksumSHA256"] == part_checksum, "selected-range part checksum")
        parts = [{"PartNumber": 1, "ETag": result["ETag"], "ChecksumSHA256": part_checksum}]
        for peer in clients:
            listing = peer.list_parts(Bucket=bucket, Key=key, UploadId=upload)
            check(len(listing["Parts"]) == 1 and listing["Parts"][0]["Size"] == len(selected)
                  and listing["Parts"][0]["ETag"] == result["ETag"]
                  and listing["Parts"][0]["ChecksumSHA256"] == part_checksum,
                  "part-copy identity and checksum on every endpoint")
        if cluster and index == 0:
            remote_evidence.append(remove_local_source(bucket, "part", selected,
                key=key, upload_id=upload, part_number=1))
        completed = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                                MultipartUpload={"Parts": parts})
        uploads.remove((key, upload))
        check(completed["ChecksumSHA256"] == object_checksum, "part-copy composite checksum")
        for peer in clients:
            read = peer.get_object(Bucket=bucket, Key=key, VersionId=completed["VersionId"],
                                   ChecksumMode="ENABLED")
            with read["Body"] as body:
                check(body.read() == selected, "part copy reads historical range, not current source")
            check(read["ChecksumSHA256"] == object_checksum
                  and read["ContentType"] == "application/ess-part-copy"
                  and read["Metadata"] == {"owner": "part-copy"}, "part-copy initiation attributes")
            check(peer.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] ==
                  [{"Key": "owner", "Value": "part-copy"}], "part-copy initiation tags")
    return remote_evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("all", "features"), default="all")
    args = parser.parse_args()
    endpoints = os.environ["E2E_S3_ENDPOINTS"].split(",")
    cluster = len(endpoints) == 2
    check(len(endpoints) in (1, 2), "one standalone or both cluster endpoints required")
    if cluster:
        check([urllib.parse.urlsplit(endpoint).port for endpoint in endpoints] == [9001, 9002],
              "remote-source fixture requires harness data-a then data-b endpoints")
        check(bool(os.environ.get("ESS_CLUSTER_E2E_ROOT")), "disposable cluster root required")
    for endpoint in endpoints:
        url = urllib.parse.urlsplit(endpoint)
        check(url.scheme in ("http", "https") and url.hostname in ("localhost", "127.0.0.1", "::1")
              and not url.username and not url.password and url.path in ("", "/") and not url.query,
              "disposable loopback endpoint required")
    clients = [boto3.client("s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"], region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 0},
                      s3={"addressing_style": "path"})) for endpoint in endpoints]
    a, b = clients[0], clients[-1]
    bucket = "hardening-multipart-" + uuid.uuid4().hex[:16]
    a.create_bucket(Bucket=bucket)
    uploads = []
    try:
        a.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        remote_evidence = check_part_copy(clients, bucket, uploads, cluster and args.scope == "all")
        for key in ["a/one", "a/two", "b/one"]:
            upload = a.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
            uploads.append((key, upload))
        for s3 in clients:
            listing = s3.list_multipart_uploads(Bucket=bucket, MaxUploads=1)
            check(listing["IsTruncated"] and len(listing["Uploads"]) == 1, "upload first page")
            seen = list(listing["Uploads"])
            while listing["IsTruncated"]:
                listing = s3.list_multipart_uploads(Bucket=bucket, MaxUploads=1,
                    KeyMarker=listing["NextKeyMarker"], UploadIdMarker=listing["NextUploadIdMarker"])
                seen.extend(listing.get("Uploads", []))
            check({(u["Key"], u["UploadId"]) for u in seen} == set(uploads), "upload pages no loss/repeat")
            grouped = s3.list_multipart_uploads(Bucket=bucket, Delimiter="/", MaxUploads=1)
            check(grouped["IsTruncated"] and grouped["CommonPrefixes"] == [{"Prefix": "a/"}],
                  "upload prefix-only page")
        for key, upload in uploads:
            a.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        uploads.clear()
        initiated = a.create_multipart_upload(Bucket=bucket, Key="composed", ContentType="application/ess-test",
            ContentLanguage="zh", CacheControl="max-age=42", Metadata={"owner": "multipart"},
            Tagging="owner=multipart", ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE")
        upload = initiated["UploadId"]
        uploads.append(("composed", upload))
        payloads = [b"a" * (5 * 1024 * 1024), b"final-part"]
        parts = []
        digests = []
        for number, data in enumerate(payloads, 1):
            digest = hashlib.sha256(data).digest()
            digests.append(digest)
            encoded = base64.b64encode(digest).decode()
            result = clients[number % len(clients)].upload_part(Bucket=bucket, Key="composed",
                UploadId=upload, PartNumber=number, Body=data, ChecksumSHA256=encoded)
            check(result["ChecksumSHA256"] == encoded, "part checksum response")
            parts.append({"PartNumber": number, "ETag": result["ETag"], "ChecksumSHA256": encoded})
        for s3 in clients:
            first = s3.list_parts(Bucket=bucket, Key="composed", UploadId=upload, MaxParts=1)
            check(first["IsTruncated"] and first["Parts"][0]["PartNumber"] == 1, "parts first page")
            final = s3.list_parts(Bucket=bucket, Key="composed", UploadId=upload,
                                  PartNumberMarker=first["NextPartNumberMarker"], MaxParts=1)
            check(not final["IsTruncated"] and final["Parts"][0]["PartNumber"] == 2, "parts final page")
        result = a.complete_multipart_upload(Bucket=bucket, Key="composed", UploadId=upload,
                                            MultipartUpload={"Parts": parts})
        expected = base64.b64encode(hashlib.sha256(b"".join(digests)).digest()).decode() + "-2"
        check(result["ChecksumSHA256"] == expected, "composite checksum response")
        retry = b.complete_multipart_upload(Bucket=bucket, Key="composed", UploadId=upload,
                                           MultipartUpload={"Parts": parts})
        check(retry["VersionId"] == result["VersionId"], "acknowledged completion retry stable identity")
        uploads.clear()
        for s3 in clients:
            response = s3.get_object(Bucket=bucket, Key="composed", ChecksumMode="ENABLED")
            digest = hashlib.sha256()
            while chunk := response["Body"].read(65536):
                digest.update(chunk)
            response["Body"].close()
            check(digest.digest() == hashlib.sha256(b"".join(payloads)).digest(), "composed raw bytes")
            check(response["ChecksumSHA256"] == expected and response["ContentType"] == "application/ess-test"
                  and response["ContentLanguage"] == "zh" and response["CacheControl"] == "max-age=42"
                  and response["Metadata"] == {"owner": "multipart"}, "initiation attrs survive completion")
            versions = s3.list_object_versions(Bucket=bucket, Prefix="composed")
            check(len(versions["Versions"]) == 1, "retry does not publish another version")
            try:
                s3.list_parts(Bucket=bucket, Key="composed", UploadId=upload)
                raise AssertionError("completed tombstone exposed as active upload")
            except ClientError as error:
                check(error.response["Error"]["Code"] == "NoSuchUpload", "completed parts hidden")
        print(json.dumps({"profile": "multipart", "result": "passed", "endpoints": endpoints,
                          "scope": args.scope, "boto3": boto3.__version__,
                          "remote_source_evidence": remote_evidence}))
    finally:
        for key, upload in uploads:
            a.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        for page in a.get_paginator("list_object_versions").paginate(Bucket=bucket):
            for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                a.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
        a.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
