#!/usr/bin/env python3
"""P2.5 independent Boto3 tag/CORS/location checks on disposable local ESS.

Provide E2E_S3_ENDPOINTS (comma-separated localhost endpoints), E2E_ACCESS_KEY_ID,
E2E_SECRET_ACCESS_KEY and optionally E2E_S3_REGION (hosted region, us-east-1).
Normal Boto3 signing/checksum defaults remain enabled. This probe creates and
cleans up one uniquely named bucket; it never launches servers or changes IAM.
"""
import importlib.metadata
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def check(value, message):
    if not value:
        raise AssertionError(message)


def error(operation, status, code):
    try:
        operation()
    except ClientError as failure:
        check(failure.response["ResponseMetadata"]["HTTPStatusCode"] == status,
              f"expected status {status}: {failure.response['ResponseMetadata']}")
        check(failure.response["Error"]["Code"] == code,
              f"expected code {code}: {failure.response['Error']}")
        return
    raise AssertionError(f"expected {code}, operation succeeded")


def tag_set(**tags):
    return {"TagSet": [{"Key": key, "Value": value} for key, value in sorted(tags.items())]}


def tags(response):
    return {entry["Key"]: entry["Value"] for entry in response["TagSet"]}


def unsigned(endpoint, method, path, headers):
    request = urllib.request.Request(endpoint + path, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as failure:
        return failure.code, failure.read(), failure.headers


def cleanup(s3, bucket):
    listing = s3.list_object_versions(Bucket=bucket)
    check(not listing.get("IsTruncated"), "unexpected cleanup pagination")
    for entry in listing.get("Versions", []) + listing.get("DeleteMarkers", []):
        s3.delete_object(Bucket=bucket, Key=entry["Key"], VersionId=entry["VersionId"])
    s3.delete_bucket(Bucket=bucket)


def main():
    endpoints = [entry.rstrip("/") for entry in os.environ["E2E_S3_ENDPOINTS"].split(",")]
    for endpoint in endpoints:
        parsed = urllib.parse.urlsplit(endpoint)
        check(parsed.scheme in ("http", "https") and parsed.hostname in
              ("localhost", "127.0.0.1", "::1") and not parsed.username and
              not parsed.password and parsed.path in ("", "/") and not parsed.query,
              "only disposable loopback ESS endpoints are accepted")
    region = os.environ.get("E2E_S3_REGION", "us-east-1")
    clients = [boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"],
        region_name="us-east-1", config=Config(signature_version="s3v4",
            s3={"addressing_style": "path"}, retries={"total_max_attempts": 1},
            connect_timeout=5, read_timeout=30)) for endpoint in endpoints]
    primary, peer = clients[0], clients[-1]
    bucket = "hardening-tags-" + uuid.uuid4().hex[:18]
    primary.create_bucket(Bucket=bucket)
    checks = []
    pending_uploads = []
    try:
        for s3 in clients:
            location = s3.get_bucket_location(Bucket=bucket)
            check(location.get("LocationConstraint") == (None if region == "us-east-1" else region),
                  "hosted region LocationConstraint mismatch")
            check(s3.head_bucket(Bucket=bucket)["ResponseMetadata"]["HTTPHeaders"].get(
                  "x-amz-bucket-region") == region, "HEAD bucket region mismatch")
        checks.append("location-and-region-all-endpoints")
        primary.put_bucket_tagging(Bucket=bucket, Tagging=tag_set(owner="研发", purpose="acceptance"))
        check(tags(peer.get_bucket_tagging(Bucket=bucket)) == {"owner": "研发", "purpose": "acceptance"},
              "bucket tags missing on peer")
        primary.delete_bucket_tagging(Bucket=bucket)
        error(lambda: peer.get_bucket_tagging(Bucket=bucket), 404, "NoSuchTagSet")
        checks.append("bucket-tagging-cross-endpoint-crud")

        primary.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        old = primary.put_object(Bucket=bucket, Key="key", Body=b"historical", Tagging="stage=old")
        current = peer.put_object(Bucket=bucket, Key="key", Body=b"current", Tagging="stage=new")
        primary.put_object_tagging(Bucket=bucket, Key="key", VersionId=old["VersionId"],
                                  Tagging=tag_set(stage="historical"))
        primary.put_object_tagging(Bucket=bucket, Key="key", Tagging=tag_set(stage="current"))
        check(tags(peer.get_object_tagging(Bucket=bucket, Key="key")) == {"stage": "current"},
              "current tags mismatch")
        check(tags(peer.get_object_tagging(Bucket=bucket, Key="key", VersionId=old["VersionId"])) ==
              {"stage": "historical"}, "historical tags mismatch")
        primary.delete_object_tagging(Bucket=bucket, Key="key", VersionId=old["VersionId"])
        check(tags(peer.get_object_tagging(Bucket=bucket, Key="key", VersionId=old["VersionId"])) == {},
              "historical tag deletion failed")
        historical = peer.get_object(Bucket=bucket, Key="key", VersionId=old["VersionId"])
        check(historical["Body"].read() == b"historical" and historical["VersionId"] == old["VersionId"],
              "tag deletion changed historical bytes or version")
        current_read = primary.get_object(Bucket=bucket, Key="key")
        check(current_read["Body"].read() == b"current" and current_read["VersionId"] == current["VersionId"],
              "tag mutation changed current bytes or version")
        check(current_read.get("TagCount") == 1, "object tag count header missing")
        check(len(peer.list_object_versions(Bucket=bucket, Prefix="key").get("Versions", [])) == 2,
              "tagging created extra versions")
        checks.append("version-tagging-retains-bytes-version-and-count")
        for s3 in clients:
            first = s3.list_object_versions(Bucket=bucket, Prefix="key", MaxKeys=1)
            check(first["IsTruncated"] and first["NextKeyMarker"] == "key" and
                  first["NextVersionIdMarker"] == current["VersionId"] and
                  first["Versions"][0]["IsLatest"], "version first-page marker contract")
            last = s3.list_object_versions(Bucket=bucket, Prefix="key", MaxKeys=1,
                KeyMarker=first["NextKeyMarker"], VersionIdMarker=first["NextVersionIdMarker"])
            check(not last["IsTruncated"] and last["Versions"][0]["VersionId"] == old["VersionId"] and
                  not last["Versions"][0]["IsLatest"], "version second-page marker contract")
            zero = s3.list_object_versions(Bucket=bucket, MaxKeys=0)
            check(not zero["IsTruncated"] and not zero.get("Versions") and not zero.get("DeleteMarkers"),
                  "zero version page must be empty and terminal")
        for key in ("dir/a/one", "dir/a/two", "dir/b/one"):
            primary.put_object(Bucket=bucket, Key=key, Body=b"prefix")
        first = peer.list_object_versions(Bucket=bucket, Prefix="dir/", Delimiter="/", MaxKeys=1)
        check(first["CommonPrefixes"] == [{"Prefix": "dir/a/"}] and first["IsTruncated"] and
              not first.get("Versions") and first["NextKeyMarker"] == "dir/a/",
              "common-prefix-only version page contract")
        last = primary.list_object_versions(Bucket=bucket, Prefix="dir/", Delimiter="/", MaxKeys=1,
                                           KeyMarker=first["NextKeyMarker"])
        check(last["CommonPrefixes"] == [{"Prefix": "dir/b/"}] and not last["IsTruncated"],
              "common-prefix version continuation contract")
        checks.append("version-and-common-prefix-pagination")

        for key, directive, expected in [("copy", "COPY", {"stage": "current"}),
                                         ("replace", "REPLACE", {"copy": "replacement"})]:
            args = {"Bucket": bucket, "Key": key, "CopySource": {"Bucket": bucket, "Key": "key"},
                    "TaggingDirective": directive}
            if directive == "REPLACE":
                args["Tagging"] = "copy=replacement"
            primary.copy_object(**args)
            check(tags(peer.get_object_tagging(Bucket=bucket, Key=key)) == expected,
                  f"{directive} copy tags mismatch")
        upload = primary.create_multipart_upload(Bucket=bucket, Key="multipart", Tagging="upload=retained")
        pending_uploads.append(("multipart", upload["UploadId"]))
        part = peer.upload_part(Bucket=bucket, Key="multipart", UploadId=upload["UploadId"],
                                PartNumber=1, Body=b"multipart")
        primary.complete_multipart_upload(Bucket=bucket, Key="multipart", UploadId=upload["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]})
        pending_uploads.remove(("multipart", upload["UploadId"]))
        check(tags(peer.get_object_tagging(Bucket=bucket, Key="multipart")) == {"upload": "retained"},
              "multipart initialization tags lost")
        checks.append("copy-replace-multipart-tags")
        marker = primary.delete_object(Bucket=bucket, Key="key")["VersionId"]
        error(lambda: peer.get_object_tagging(Bucket=bucket, Key="key"), 404, "NoSuchKey")
        error(lambda: peer.delete_object_tagging(Bucket=bucket, Key="key", VersionId=marker),
              405, "MethodNotAllowed")
        check(any(entry["VersionId"] == marker for entry in peer.list_object_versions(
              Bucket=bucket, Prefix="key").get("DeleteMarkers", [])), "tag DELETE removed marker")
        checks.append("delete-marker-tagging-preserves-marker")

        rules = {"CORSRules": [{"ID": "browser", "AllowedOrigins": ["https://*.example.test"],
                 "AllowedMethods": ["GET", "PUT"], "AllowedHeaders": ["x-amz-*", "content-type"],
                 "ExposeHeaders": ["etag", "x-amz-version-id"], "MaxAgeSeconds": 60}]}
        primary.put_bucket_cors(Bucket=bucket, CORSConfiguration=rules)
        check(peer.get_bucket_cors(Bucket=bucket)["CORSRules"] == rules["CORSRules"],
              "persisted CORS differs on peer")
        headers = {"Origin": "https://app.example.test", "Access-Control-Request-Method": "PUT",
                   "Access-Control-Request-Headers": "x-amz-meta-owner, content-type"}
        for endpoint in endpoints:
            code, _, response_headers = unsigned(endpoint, "OPTIONS", f"/{bucket}/multipart", headers)
            check(code == 200 and response_headers.get("Access-Control-Allow-Origin") == headers["Origin"],
                  "allowed preflight failed")
            vary = ",".join(response_headers.get_all("Vary", []))
            check(all(value in vary.lower() for value in
                      ("origin", "access-control-request-method", "access-control-request-headers")),
                  "preflight cache variance incomplete")
            denied = dict(headers, Origin="https://attacker.invalid")
            code, _, response_headers = unsigned(endpoint, "OPTIONS", f"/{bucket}/multipart", denied)
            check(code == 403 and response_headers.get("Access-Control-Allow-Origin") is None,
                  "disallowed preflight accepted")
            code, _, response_headers = unsigned(endpoint, "GET", f"/{bucket}/multipart",
                                                  {"Origin": headers["Origin"]})
            check(code == 403 and response_headers.get("Access-Control-Allow-Origin") == headers["Origin"],
                  "actual CORS bypassed authentication or omitted error headers")
        primary.delete_bucket_cors(Bucket=bucket)
        error(lambda: peer.get_bucket_cors(Bucket=bucket), 404, "NoSuchCORSConfiguration")
        checks.append("cors-persistence-preflight-cache-and-actual-auth")
    finally:
        for key, upload_id in pending_uploads:
            primary.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        cleanup(primary, bucket)
    print(json.dumps({"status": "passed", "checks": checks, "endpoints": len(endpoints),
        "boto3": importlib.metadata.version("boto3"), "botocore": importlib.metadata.version("botocore")}, indent=2))


if __name__ == "__main__":
    main()
