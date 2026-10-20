#!/usr/bin/env python3
"""Sequential daily-use checks with small fixtures, normal signing and checksums.

Owns one loopback-only standalone server and disposable roots. This is a separate
scope from the historical all-38, resource, concurrency and recovery acceptance.
"""
import argparse
import base64
from contextlib import contextmanager
import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from hardening_acceptance import REPO, sha, source_fingerprint


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def read_object(s3, bucket, key, **options):
    response = s3.get_object(Bucket=bucket, Key=key, **options)
    with response["Body"] as body:
        return response, body.read()


@contextmanager
def group(report, name):
    result = {"name": name, "status": "in_progress"}
    report["checks"].append(result)
    started = time.monotonic()
    try:
        yield
        result["status"] = "passed"
        print(f"PASS {name}", flush=True)
    except Exception:
        result["status"] = "failed"
        raise
    finally:
        result["seconds"] = round(time.monotonic() - started, 3)


def business_checks(s3, endpoint, report):
    bucket = "daily-" + secrets.token_hex(8)
    key = "notes/hello +雪.txt"
    data = "daily storage round trip\n".encode() * 64
    with group(report, "bucket-create-list-head-location-tags"):
        s3.create_bucket(Bucket=bucket)
        check(bucket in [b["Name"] for b in s3.list_buckets()["Buckets"]], "bucket missing")
        check(s3.head_bucket(Bucket=bucket)["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"]
              == "us-east-1", "bucket region")
        check(s3.get_bucket_location(Bucket=bucket).get("LocationConstraint") is None, "location")
        tags = {"TagSet": [{"Key": "purpose", "Value": "daily"}]}
        s3.put_bucket_tagging(Bucket=bucket, Tagging=tags)
        check(s3.get_bucket_tagging(Bucket=bucket)["TagSet"] == tags["TagSet"], "bucket tags")
        s3.delete_bucket_tagging(Bucket=bucket)

    with group(report, "put-head-get-range-checksum-attributes"):
        digest = base64.b64encode(hashlib.sha256(data).digest()).decode()
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain",
                      CacheControl="max-age=60", Metadata={"purpose": "daily"},
                      ChecksumSHA256=digest)
        head = s3.head_object(Bucket=bucket, Key=key)
        check(head["ContentLength"] == len(data) and head["Metadata"] == {"purpose": "daily"},
              "HEAD length/metadata")
        response, content = read_object(s3, bucket, key, ChecksumMode="ENABLED")
        check(content == data and response["ChecksumSHA256"] == digest, "bytes/checksum")
        check(response["ContentType"] == "text/plain" and response["CacheControl"] == "max-age=60",
              "object attributes")
        response, content = read_object(s3, bucket, key, Range="bytes=2-17")
        check(content == data[2:18] and response["ResponseMetadata"]["HTTPStatusCode"] == 206,
              "range content/status")

    with group(report, "default-sdk-checksum-gzip-bytes"):
        compressed = gzip.compress(data, mtime=0)
        # No checksum config override: retain the selected SDK's normal defaults.
        result = s3.put_object(Bucket=bucket, Key="compressed.gz", Body=compressed,
                               ContentType="application/gzip", ContentEncoding="gzip")
        response, content = read_object(s3, bucket, "compressed.gz", ChecksumMode="ENABLED")
        check(content == compressed and response["ContentEncoding"] == "gzip", "raw gzip bytes")
        check(bool(result.get("ChecksumCRC32")) and response.get("ChecksumCRC32")
              == result["ChecksumCRC32"], "default SDK checksum round trip")

    with group(report, "list-prefix-delimiter-pagination"):
        for name in ("docs/a.txt", "docs/b.txt", "docs/sub/c.txt"):
            s3.put_object(Bucket=bucket, Key=name, Body=name.encode())
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="docs/", PaginationConfig={"PageSize": 1})
        names = [item["Key"] for page in pages for item in page.get("Contents", [])]
        check(names == ["docs/a.txt", "docs/b.txt", "docs/sub/c.txt"], "list v2 continuation")
        listing = s3.list_objects(Bucket=bucket, Prefix="docs/", Delimiter="/")
        check([item["Key"] for item in listing["Contents"]] == ["docs/a.txt", "docs/b.txt"]
              and listing["CommonPrefixes"] == [{"Prefix": "docs/sub/"}], "list v1 delimiter")

    with group(report, "object-tags-copy-replace-delete-tags"):
        tags = {"TagSet": [{"Key": "stage", "Value": "daily"}]}
        s3.put_object_tagging(Bucket=bucket, Key=key, Tagging=tags)
        check(s3.get_object_tagging(Bucket=bucket, Key=key)["TagSet"] == tags["TagSet"], "object tags")
        s3.copy_object(Bucket=bucket, Key="copied", CopySource={"Bucket": bucket, "Key": key})
        response, content = read_object(s3, bucket, "copied")
        check(content == data and response["Metadata"] == {"purpose": "daily"}, "copy attributes")
        check(s3.get_object_tagging(Bucket=bucket, Key="copied")["TagSet"] == tags["TagSet"],
              "copy tags")
        s3.copy_object(Bucket=bucket, Key="replaced", CopySource={"Bucket": bucket, "Key": key},
                       MetadataDirective="REPLACE", Metadata={"purpose": "replaced"},
                       ContentType="application/octet-stream", TaggingDirective="REPLACE",
                       Tagging="stage=replaced")
        response, content = read_object(s3, bucket, "replaced")
        check(content == data and response["Metadata"] == {"purpose": "replaced"}, "replace metadata")
        check(s3.get_object_tagging(Bucket=bucket, Key="replaced")["TagSet"]
              == [{"Key": "stage", "Value": "replaced"}], "replace tags")
        s3.delete_object_tagging(Bucket=bucket, Key=key)
        check(not s3.get_object_tagging(Bucket=bucket, Key=key)["TagSet"], "delete tags")
        check(read_object(s3, bucket, key)[1] == data, "tag deletion preserves bytes")

    with group(report, "presigned-get-put-and-authentication"):
        url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60)
        with urllib.request.urlopen(url, timeout=15) as response:
            check(response.read() == data, "presigned GET")
        url = s3.generate_presigned_url("put_object", Params={"Bucket": bucket, "Key": "presigned"},
                                        ExpiresIn=60)
        with urllib.request.urlopen(urllib.request.Request(url, data=b"presigned daily", method="PUT"),
                                    timeout=15) as response:
            check(response.status == 200, "presigned PUT")
        check(read_object(s3, bucket, "presigned")[1] == b"presigned daily", "presigned PUT bytes")
        try:
            urllib.request.urlopen(endpoint + "/" + bucket + "/presigned", timeout=15)
        except urllib.error.HTTPError as error:
            with error:
                check(error.code == 403, "anonymous access must be denied")
        else:
            raise AssertionError("anonymous object read accepted")

    with group(report, "cors-configuration-preflight-authenticated-read"):
        rules = [{"ID": "daily", "AllowedOrigins": ["https://daily.example"], "AllowedMethods": ["GET", "HEAD"],
                  "AllowedHeaders": ["x-daily"], "ExposeHeaders": ["etag"], "MaxAgeSeconds": 60}]
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration={"CORSRules": rules})
        check(s3.get_bucket_cors(Bucket=bucket)["CORSRules"] == rules, "CORS configuration")
        request = urllib.request.Request(endpoint + "/" + bucket + "/presigned", method="OPTIONS",
            headers={"Origin": "https://daily.example", "Access-Control-Request-Method": "GET",
                     "Access-Control-Request-Headers": "x-daily"})
        with urllib.request.urlopen(request, timeout=15) as response:
            check(response.headers["Access-Control-Allow-Origin"] == "https://daily.example", "preflight")
        url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "presigned"},
                                        ExpiresIn=60)
        request = urllib.request.Request(url, headers={"Origin": "https://daily.example"})
        with urllib.request.urlopen(request, timeout=15) as response:
            check(response.read() == b"presigned daily" and response.headers["Access-Control-Allow-Origin"]
                  == "https://daily.example", "authenticated CORS read")
        s3.delete_bucket_cors(Bucket=bucket)

    with group(report, "multipart-upload-list-parts-complete-and-abort"):
        upload = s3.create_multipart_upload(Bucket=bucket, Key="multipart", ContentType="text/plain",
            Metadata={"purpose": "multipart"}, Tagging="stage=multipart",
            ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE")["UploadId"]
        payloads = [b"m" * (5 * 1024 * 1024), b"daily final part"]
        parts = []
        for number, payload in enumerate(payloads, 1):
            checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode()
            result = s3.upload_part(Bucket=bucket, Key="multipart", UploadId=upload,
                PartNumber=number, Body=payload, ChecksumSHA256=checksum)
            check(result["ChecksumSHA256"] == checksum, "part checksum")
            parts.append({"PartNumber": number, "ETag": result["ETag"], "ChecksumSHA256": checksum})
        check([p["PartNumber"] for p in s3.list_parts(Bucket=bucket, Key="multipart", UploadId=upload)["Parts"]]
              == [1, 2], "parts listing")
        s3.complete_multipart_upload(Bucket=bucket, Key="multipart", UploadId=upload,
                                     MultipartUpload={"Parts": parts})
        response, content = read_object(s3, bucket, "multipart", ChecksumMode="ENABLED")
        check(content == b"".join(payloads) and response["Metadata"] == {"purpose": "multipart"},
              "multipart bytes/attributes")
        check(s3.get_object_tagging(Bucket=bucket, Key="multipart")["TagSet"]
              == [{"Key": "stage", "Value": "multipart"}], "multipart initiation tags")
        upload = s3.create_multipart_upload(Bucket=bucket, Key="aborted")["UploadId"]
        check(any(u["UploadId"] == upload for u in s3.list_multipart_uploads(Bucket=bucket)["Uploads"]),
              "active uploads listing")
        s3.abort_multipart_upload(Bucket=bucket, Key="aborted", UploadId=upload)
        check(not s3.list_multipart_uploads(Bucket=bucket).get("Uploads"), "abort removes active upload")

    with group(report, "multipart-part-copy"):
        upload = s3.create_multipart_upload(Bucket=bucket, Key="part-copy")["UploadId"]
        result = s3.upload_part_copy(Bucket=bucket, Key="part-copy", UploadId=upload, PartNumber=1,
                                    CopySource={"Bucket": bucket, "Key": key})
        s3.complete_multipart_upload(Bucket=bucket, Key="part-copy", UploadId=upload,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": result["CopyPartResult"]["ETag"]}]})
        check(read_object(s3, bucket, "part-copy")[1] == data, "part-copy bytes")

    with group(report, "batch-delete-and-single-delete"):
        names = ["copied", "replaced"]
        result = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": name} for name in names]})
        check(not result.get("Errors") and {item["Key"] for item in result["Deleted"]} == set(names),
              "batch delete result")
        s3.delete_object(Bucket=bucket, Key="presigned")
        check(not s3.list_objects_v2(Bucket=bucket, Prefix="presigned").get("Contents"), "single delete")

    with group(report, "versions-explicit-read-copy-delete-marker"):
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        check(s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled", "enable versioning")
        first = s3.put_object(Bucket=bucket, Key="versioned", Body=b"first")["VersionId"]
        second = s3.put_object(Bucket=bucket, Key="versioned", Body=b"second")["VersionId"]
        check(first != second and read_object(s3, bucket, "versioned", VersionId=first)[1] == b"first",
              "historical read")
        check(s3.head_object(Bucket=bucket, Key="versioned", VersionId=first)["VersionId"] == first,
              "historical HEAD")
        tags = {"TagSet": [{"Key": "version", "Value": "first"}]}
        s3.put_object_tagging(Bucket=bucket, Key="versioned", VersionId=first, Tagging=tags)
        check(s3.get_object_tagging(Bucket=bucket, Key="versioned", VersionId=first)["TagSet"]
              == tags["TagSet"], "version tags")
        s3.copy_object(Bucket=bucket, Key="version-copy",
                       CopySource={"Bucket": bucket, "Key": "versioned", "VersionId": first})
        check(read_object(s3, bucket, "version-copy")[1] == b"first", "historical COPY")
        marker = s3.delete_object(Bucket=bucket, Key="versioned")
        check(marker["DeleteMarker"], "normal versioned delete creates marker")
        listing = s3.list_object_versions(Bucket=bucket, Prefix="versioned")
        check({v["VersionId"] for v in listing["Versions"]} == {first, second}
              and listing["DeleteMarkers"][0]["VersionId"] == marker["VersionId"], "version listing")
        s3.delete_object(Bucket=bucket, Key="versioned", VersionId=marker["VersionId"])
        check(read_object(s3, bucket, "versioned")[1] == b"second", "delete marker reveals latest")

    with group(report, "remove-created-versions-and-bucket"):
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
            for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
        s3.delete_bucket(Bucket=bucket)
        check(bucket not in [b["Name"] for b in s3.list_buckets()["Buckets"]], "bucket deletion")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    check(re.fullmatch(r"[a-zA-Z0-9_-]+", args.label), "invalid evidence label")
    check(Path.cwd() == REPO, "run from repository root")
    output = REPO / "docs/reports/s3-hardening" / ("daily-" + args.label)
    output.mkdir(parents=True, exist_ok=False)
    root = Path(tempfile.mkdtemp(prefix="ess-s3-daily-"))
    report = {"profile": "daily", "status": "in_progress", "starting_sha": sha(),
              "source_sha256": source_fingerprint(), "started_at": datetime.datetime.now(
                  datetime.timezone.utc).isoformat(), "checks": [], "commands": [],
              "scope": {"mode": "standalone", "client_concurrency": 1,
                        "largest_part_bytes": 5 * 1024 * 1024, "multipart_parts": 2,
                        "authentication": True, "sdk_checksum_defaults": True,
                        "extended_acceptance_executed": False},
              "versions": {"python": sys.version.split()[0], "boto3": boto3.__version__,
                           "botocore": botocore.__version__}, "disposable_root": str(root)}
    environment = {k: v for k, v in os.environ.items() if not k.startswith(("ESS_", "E2E_"))}
    with socket.socket() as port:
        port.bind(("127.0.0.1", 0))
        number = port.getsockname()[1]
    endpoint = f"http://127.0.0.1:{number}"
    credential_file = root / "credentials.env"
    environment.update({"MIX_ENV": "prod", "PAGER": "cat", "ESS_MODE": "standalone",
        "ESS_AUTO_START": "true", "ESS_WEB_ENABLED": "false", "ESS_S3_AUTH_ENABLED": "true",
        "ESS_NODE_ROLE": "data", "ESS_METADATA_SCHEMA": "v2", "ESS_CONCORD_COMMAND_VERSION": "0",
        "ESS_DATA_ROOT": str(root), "ESS_BLOB_ROOT": str(root / "cas"),
        "ESS_TMP_ROOT": str(root / "tmp"), "ESS_METADATA_ROOT": str(root / "metadata"),
        "ESS_RA_ROOT": str(root / "ra"), "ESS_S3_PORT": str(number), "ESS_S3_REGION": "us-east-1",
        "ESS_MASTER_KEY": base64.b64encode(secrets.token_bytes(32)).decode(),
        "ESS_ADMIN_PASSWORD_HASH": hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
        "E2E_GITHUB_ENV": str(credential_file)})
    server = None
    client = None
    started = time.monotonic()
    try:
        report["versions"]["beam"] = subprocess.check_output(
            ["elixir", "--version"], env=environment, text=True).strip()
        lock = (REPO / "mix.lock").read_text()
        for dependency in ("concord", "viewstamped_replication", "ex_turso", "http_fetch"):
            report["versions"][dependency] = re.search(
                rf'"{dependency}": \{{:hex, :\w+, "([^"]+)"', lock).group(1)
        command = ["mix", "compile", "--warnings-as-errors"]
        with group(report, "production-compile"):
            with (output / "compile.log").open("w") as log:
                result = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=600, check=False)
            report["commands"].append({"argv": command, "exit_status": result.returncode})
            check(result.returncode == 0, "production compilation failed; see compile.log")
        with (output / "server.log").open("w") as log:
            command = ["mix", "run", "--no-start", "--no-compile", "--no-halt",
                       "e2e/scripts/daily_hardening_server.exs"]
            report["commands"].append({"argv": command})
            server = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
            report["server_pid"] = server.pid
            report["endpoint"] = endpoint
            with group(report, "server-health-and-readiness"):
                for _ in range(150):
                    check(server.poll() is None, "server stopped during startup; see server.log")
                    if credential_file.exists():
                        try:
                            with urllib.request.urlopen(endpoint + "/health/ready", timeout=1) as response:
                                report["readiness_http_status"] = response.status
                                if response.status == 200:
                                    break
                        except urllib.error.HTTPError as error:
                            report["readiness_http_status"] = error.code
                            error.close()
                        except (OSError, urllib.error.URLError) as error:
                            report["readiness_connection_error"] = type(error).__name__
                    time.sleep(0.2)
                else:
                    raise AssertionError("daily server did not become ready; see server.log")
            credentials = dict(line.split("=", 1) for line in credential_file.read_text().splitlines())
            credential_file.unlink()
            client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                aws_access_key_id=credentials["E2E_ACCESS_KEY_ID"],
                aws_secret_access_key=credentials["E2E_SECRET_ACCESS_KEY"],
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                              connect_timeout=5, read_timeout=30, retries={"total_max_attempts": 1}))
            report["sdk_checksum_policy"] = {
                "request": client.meta.config.request_checksum_calculation,
                "response": client.meta.config.response_checksum_validation}
            check(set(report["sdk_checksum_policy"].values()) == {"when_supported"},
                  "normal SDK checksum policies must remain enabled")
            business_checks(client, endpoint, report)
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error_type"] = type(error).__name__
        if isinstance(error, ClientError):
            report["error"] = {"operation": error.operation_name, "code": error.response["Error"]["Code"],
                               "http_status": error.response["ResponseMetadata"]["HTTPStatusCode"]}
        elif isinstance(error, AssertionError):
            report["error"] = str(error)
        # Do not retain request URLs or exception strings containing signed credentials.
        print(f"FAIL daily ({type(error).__name__}); see {output / 'daily.json'}", flush=True)
    finally:
        if client is not None:
            client.close()
        if server is not None and server.poll() is None:
            os.killpg(server.pid, signal.SIGTERM)
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                report["status"] = "failed"
        report["server_stopped"] = server is None or server.poll() is not None
        with socket.socket() as probe:
            probe.settimeout(1)
            report["listener_closed"] = probe.connect_ex(("127.0.0.1", number)) != 0
        if report["server_stopped"] and report["listener_closed"]:
            shutil.rmtree(root)
        report["disposable_root_removed"] = not root.exists()
        if not all(report[name] for name in ("server_stopped", "listener_closed", "disposable_root_removed")):
            report["status"] = "failed"
        report["ending_sha"] = sha()
        report["ending_source_sha256"] = source_fingerprint()
        if report["source_sha256"] != report["ending_source_sha256"]:
            report["status"] = "failed"
            report["source_changed_during_run"] = True
        report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["seconds"] = round(time.monotonic() - started, 3)
        (output / "daily.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"profile": "daily", "status": report["status"],
                      "checks": len(report["checks"]), "receipt": str(output / "daily.json")}), flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
