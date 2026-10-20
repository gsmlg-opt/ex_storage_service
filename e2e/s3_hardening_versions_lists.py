#!/usr/bin/env python3
"""Independent default-Boto3 P2 versions/listing profile, disposable loopback only."""
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


def wire_head(s3, bucket, key, version=None):
    params = {"Bucket": bucket, "Key": key}
    if version is not None:
        params["VersionId"] = version
    url = s3.generate_presigned_url("head_object", Params=params, ExpiresIn=60,
                                    HttpMethod="HEAD")
    request = urllib.request.Request(url, method="HEAD")
    try:
        response = urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as failure:
        response = failure
    with response:
        return response.status, response.headers, response.read()


def main():
    endpoints = os.environ["E2E_S3_ENDPOINTS"].split(",")
    for endpoint in endpoints:
        parsed = urllib.parse.urlsplit(endpoint)
        check(parsed.hostname in ("127.0.0.1", "localhost", "::1") and
              parsed.scheme in ("http", "https") and not parsed.username and
              not parsed.password and parsed.path in ("", "/") and not parsed.query,
              "disposable loopback endpoints required")
    clients = [boto3.client("s3", endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"], region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 0},
                      s3={"addressing_style": "path"})) for endpoint in endpoints]
    first, last = clients[0], clients[-1]
    bucket = "hardening-versions-" + uuid.uuid4().hex[:16]
    first.create_bucket(Bucket=bucket)
    try:
        for body in (b"null-original", b"null-replacement"):
            first.put_object(Bucket=bucket, Key="versioned", Body=body)
        check(last.get_object(Bucket=bucket, Key="versioned", VersionId="null")["Body"].read()
              == b"null-replacement", "one readable null slot")
        first.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        numbered = first.put_object(Bucket=bucket, Key="versioned", Body=b"numbered")["VersionId"]
        check(numbered != "null", "enabled numbered identity")
        first.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
        for body in (b"suspended-one", b"suspended-two"):
            check(first.put_object(Bucket=bucket, Key="versioned", Body=body)["VersionId"] == "null",
                  "suspended response null identity")
        for s3 in clients:
            versions = s3.list_object_versions(Bucket=bucket)
            check({v["VersionId"] for v in versions["Versions"]} == {"null", numbered},
                  "suspended replacement preserves numbered history")
            historical = s3.get_object(Bucket=bucket, Key="versioned", VersionId=numbered)
            with historical["Body"] as body:
                check(body.read() == b"numbered", "explicit historical bytes")
            check(historical["VersionId"] == numbered, "historical GET version header")
            for version, expected in ((None, b"suspended-two"), (numbered, b"numbered")):
                status, headers, body = wire_head(s3, bucket, "versioned", version)
                check(status == 200 and body == b"", "successful HEAD has no body")
                check(headers["x-amz-version-id"] == (version or "null")
                      and int(headers["Content-Length"]) == len(expected),
                      "HEAD identifies the version and length actually served")
            copied = s3.copy_object(Bucket=bucket, Key="historical-copy",
                CopySource={"Bucket": bucket, "Key": "versioned", "VersionId": numbered},
                CopySourceIfMatch=historical["ETag"])
            check(copied["CopySourceVersionId"] == numbered and copied["VersionId"] == "null",
                  "COPY identifies selected historical source and suspended destination")
            copied_object = s3.get_object(Bucket=bucket, Key="historical-copy", VersionId="null")
            with copied_object["Body"] as body:
                check(body.read() == b"numbered", "version COPY pins historical bytes")
            check(copied_object["VersionId"] == "null", "copied GET version header")
            s3.delete_object(Bucket=bucket, Key="historical-copy", VersionId="null")
            try:
                s3.get_object(Bucket=bucket, Key="versioned", VersionId="missing-version")
                raise AssertionError("missing explicit version accepted")
            except ClientError as failure:
                check(failure.response["Error"]["Code"] == "NoSuchVersion"
                      and failure.response["ResponseMetadata"]["HTTPStatusCode"] == 404,
                      "missing explicit GET version error")
            status, _, body = wire_head(s3, bucket, "versioned", "missing-version")
            check(status == 404 and body == b"", "missing explicit HEAD version is bodyless")
        marker = first.delete_object(Bucket=bucket, Key="versioned")
        check(marker["DeleteMarker"] and marker["VersionId"] == "null", "null delete marker")
        for s3 in clients:
            for version, expected_status in ((None, 404), ("null", 405)):
                status, headers, body = wire_head(s3, bucket, "versioned", version)
                check(status == expected_status and body == b"", "marker HEAD error is bodyless")
                check(headers["x-amz-delete-marker"] == "true"
                      and headers["x-amz-version-id"] == "null", "marker HEAD identity headers")
                if version is not None:
                    check(bool(headers["Last-Modified"]), "explicit marker modification header")
            try:
                s3.get_object(Bucket=bucket, Key="versioned", VersionId="null")
                raise AssertionError("explicit marker accepted")
            except ClientError as failure:
                check(failure.response["ResponseMetadata"]["HTTPStatusCode"] == 405,
                      "explicit marker 405")
                headers = failure.response["ResponseMetadata"]["HTTPHeaders"]
                check(headers["x-amz-delete-marker"] == "true"
                      and headers["x-amz-version-id"] == "null" and headers["last-modified"],
                      "explicit marker GET identity headers")
        first.delete_object(Bucket=bucket, Key="versioned", VersionId="null")
        check(last.get_object(Bucket=bucket, Key="versioned")["Body"].read() == b"numbered",
              "deleting null marker reveals numbered history")
        first.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        keys = ["a/1", "a/2", "b/1", "m +雪", "z&<"]
        for key in keys:
            first.put_object(Bucket=bucket, Key=key, Body=key.encode())
        for s3 in clients:
            for operation in ("list_objects", "list_objects_v2"):
                result = getattr(s3, operation)(Bucket=bucket, MaxKeys=0)
                check(not result.get("Contents") and not result["IsTruncated"], "zero page")
                result = getattr(s3, operation)(Bucket=bucket, Delimiter="/", MaxKeys=2)
                check([p["Prefix"] for p in result["CommonPrefixes"]] == ["a/", "b/"],
                      "common-prefix-only page")
                if operation.endswith("v2"):
                    check(result["KeyCount"] == 2, "prefixes counted")
                    continuation = {"ContinuationToken": result["NextContinuationToken"]}
                else:
                    continuation = {"Marker": result["NextMarker"]}
                result = getattr(s3, operation)(Bucket=bucket, Delimiter="/", MaxKeys=10,
                                               **continuation)
                check([p["Key"] for p in result["Contents"]] == ["m +雪", "versioned", "z&<"],
                      "continuation key order and XML special keys")
            pages = list(s3.get_paginator("list_object_versions").paginate(
                Bucket=bucket, PaginationConfig={"PageSize": 1}))
            entries = [v for page in pages for v in page.get("Versions", [])]
            check(len(entries) == len(keys) + 1 and all(v["IsLatest"] for v in entries),
                  "one-entry version pagination without repeats")
            response = s3.list_objects_v2(Bucket=bucket, StartAfter="b/", EncodingType="url")
            check([item["Key"] for item in response["Contents"]] ==
                  ["b%2F1", "m%20%2B%E9%9B%AA", "versioned", "z%26%3C"], "encoded key fields")
        print(json.dumps({"profile": "versions-lists", "endpoints": endpoints,
                          "boto3": boto3.__version__, "result": "passed"}))
    finally:
        for page in first.get_paginator("list_object_versions").paginate(Bucket=bucket):
            for entry in page.get("Versions", []) + page.get("DeleteMarkers", []):
                first.delete_object(Bucket=bucket, Key=entry["Key"], VersionId=entry["VersionId"])
        first.delete_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
