#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def endpoint_url():
    return os.environ.get("E2E_S3_ENDPOINT", "http://localhost:9000").rstrip("/")


def admin_url():
    return os.environ.get("E2E_ADMIN_ENDPOINT", "http://localhost:4000").rstrip("/")


def s3_client(access_key_id=None, secret_access_key=None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url(),
        aws_access_key_id=access_key_id or require_env("E2E_ACCESS_KEY_ID"),
        aws_secret_access_key=secret_access_key or require_env("E2E_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    )


def wait_for_http(url, label, expected_status=200, timeout=60):
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status == expected_status:
                    return
                last_error = f"HTTP {response.status}"
        except Exception as exc:  # noqa: BLE001 - e2e readiness loop reports the last failure.
            last_error = repr(exc)

        time.sleep(1)

    raise AssertionError(f"{label} was not ready at {url}: {last_error}")


def expect_client_error(expected_status, action):
    try:
        action()
    except ClientError as exc:
        actual_status = exc.response["ResponseMetadata"]["HTTPStatusCode"]
        if actual_status != expected_status:
            raise AssertionError(f"expected HTTP {expected_status}, got {actual_status}") from exc
        return exc

    raise AssertionError(f"expected ClientError HTTP {expected_status}")


def expect_http_error(expected_status, url):
    try:
        urllib.request.urlopen(url, timeout=10)
    except urllib.error.HTTPError as exc:
        if exc.code != expected_status:
            raise AssertionError(f"expected HTTP {expected_status}, got {exc.code}") from exc
        return

    raise AssertionError(f"expected HTTP {expected_status} for {url}")


def read_body(response):
    return response["Body"].read()


def assert_status(response, expected_status):
    actual_status = response["ResponseMetadata"]["HTTPStatusCode"]
    if actual_status != expected_status:
        raise AssertionError(f"expected HTTP {expected_status}, got {actual_status}")


def cleanup_bucket(client, bucket, keep_keys=None):
    keep = set(keep_keys or [])

    try:
        listed = client.list_objects_v2(Bucket=bucket)
    except ClientError:
        return

    objects = listed.get("Contents", [])
    delete = [{"Key": obj["Key"]} for obj in objects if obj["Key"] not in keep]

    if delete:
        client.delete_objects(Bucket=bucket, Delete={"Objects": delete})

    if not keep:
        try:
            client.delete_bucket(Bucket=bucket)
        except ClientError:
            pass


def assert_status_in(response, expected_statuses):
    actual_status = response["ResponseMetadata"]["HTTPStatusCode"]
    if actual_status not in expected_statuses:
        expected = ", ".join(str(status) for status in expected_statuses)
        raise AssertionError(f"expected HTTP {expected}, got {actual_status}")


def exercise(args):
    wait_for_http(f"{endpoint_url()}/health", "S3 API")
    wait_for_http(f"{admin_url()}/", "admin UI")
    wait_for_http(f"{admin_url()}/metrics", "metrics endpoint")

    client = s3_client()
    readonly = s3_client(
        require_env("E2E_READONLY_ACCESS_KEY_ID"),
        require_env("E2E_READONLY_SECRET_ACCESS_KEY"),
    )

    bucket = f"e2e-{int(time.time())}-{uuid.uuid4().hex[:10]}"
    object_key = "folder/test-object-metadata.txt"
    copy_key = "folder/copied-object.txt"
    zero_key = "folder/zero-byte.txt"
    multipart_key = "multipart/combined.bin"
    abort_key = "multipart/aborted.bin"
    persist_key = "persist/restart-check.txt"

    payload = b"hello from ex_storage_service e2e\n"
    persist_payload = b"this object must survive an application restart\n"
    part_one = b"a" * 1024
    part_two = b"b" * 2048

    try:
        create_bucket = client.create_bucket(Bucket=bucket)
        assert_status_in(create_bucket, {200, 201})

        assert_status(client.head_bucket(Bucket=bucket), 200)
        buckets = client.list_buckets()["Buckets"]
        if bucket not in [item["Name"] for item in buckets]:
            raise AssertionError(f"{bucket} was not returned by ListBuckets")

        put = client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=payload,
            ContentType="text/plain",
            Metadata={"purpose": "e2e", "case": "metadata"},
        )
        assert_status(put, 200)
        expected_etag = hashlib.md5(payload).hexdigest()
        if put["ETag"].strip('"') != expected_etag:
            raise AssertionError("PutObject ETag did not match payload MD5")

        head = client.head_object(Bucket=bucket, Key=object_key)
        assert_status(head, 200)
        if head["ContentLength"] != len(payload):
            raise AssertionError("HeadObject returned the wrong content length")
        if head["Metadata"].get("purpose") != "e2e":
            raise AssertionError("HeadObject did not return custom metadata")

        got = client.get_object(Bucket=bucket, Key=object_key)
        assert_status(got, 200)
        if read_body(got) != payload:
            raise AssertionError("GetObject body did not match uploaded payload")

        ranged = client.get_object(Bucket=bucket, Key=object_key, Range="bytes=0-4")
        assert_status(ranged, 206)
        if read_body(ranged) != payload[:5]:
            raise AssertionError("Range GetObject returned the wrong bytes")

        listed = client.list_objects_v2(Bucket=bucket, Prefix="folder/")
        assert_status(listed, 200)
        if object_key not in [item["Key"] for item in listed.get("Contents", [])]:
            raise AssertionError("ListObjectsV2 did not include the uploaded object")

        delimited = client.list_objects_v2(Bucket=bucket, Delimiter="/")
        prefixes = [item["Prefix"] for item in delimited.get("CommonPrefixes", [])]
        if "folder/" not in prefixes:
            raise AssertionError("ListObjectsV2 delimiter did not return the folder common prefix")

        copied = client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": object_key},
            Key=copy_key,
        )
        assert_status(copied, 200)
        if read_body(client.get_object(Bucket=bucket, Key=copy_key)) != payload:
            raise AssertionError("CopyObject copy did not match the source body")

        assert_status(client.put_object(Bucket=bucket, Key=zero_key, Body=b""), 200)
        zero_head = client.head_object(Bucket=bucket, Key=zero_key)
        if zero_head["ContentLength"] != 0:
            raise AssertionError("zero-byte object did not keep length 0")

        upload = client.create_multipart_upload(Bucket=bucket, Key=multipart_key)
        upload_id = upload["UploadId"]
        uploaded_one = client.upload_part(
            Bucket=bucket,
            Key=multipart_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_one,
        )
        uploaded_two = client.upload_part(
            Bucket=bucket,
            Key=multipart_key,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_two,
        )
        parts = client.list_parts(Bucket=bucket, Key=multipart_key, UploadId=upload_id)
        if len(parts.get("Parts", [])) != 2:
            raise AssertionError("ListParts did not return both uploaded parts")
        complete = client.complete_multipart_upload(
            Bucket=bucket,
            Key=multipart_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": uploaded_one["ETag"]},
                    {"PartNumber": 2, "ETag": uploaded_two["ETag"]},
                ]
            },
        )
        assert_status(complete, 200)
        if read_body(client.get_object(Bucket=bucket, Key=multipart_key)) != part_one + part_two:
            raise AssertionError("completed multipart object body was wrong")

        abort = client.create_multipart_upload(Bucket=bucket, Key=abort_key)
        abort_id = abort["UploadId"]
        client.upload_part(
            Bucket=bucket,
            Key=abort_key,
            UploadId=abort_id,
            PartNumber=1,
            Body=b"discard me",
        )
        assert_status(client.abort_multipart_upload(Bucket=bucket, Key=abort_key, UploadId=abort_id), 204)
        expect_client_error(404, lambda: client.list_parts(Bucket=bucket, Key=abort_key, UploadId=abort_id))

        delete_response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": copy_key}, {"Key": zero_key}]},
        )
        assert_status(delete_response, 200)
        expect_client_error(404, lambda: client.head_object(Bucket=bucket, Key=copy_key))

        presigned_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=300,
        )
        with urllib.request.urlopen(presigned_url, timeout=10) as response:
            if response.status != 200 or response.read() != payload:
                raise AssertionError("presigned GET did not return the object body")

        unauthenticated_path = urllib.parse.quote(object_key)
        expect_http_error(403, f"{endpoint_url()}/{bucket}/{unauthenticated_path}")
        expect_client_error(403, lambda: s3_client("AKIAINVALID", "invalid-secret").list_buckets())
        expect_client_error(
            403,
            lambda: readonly.put_object(Bucket=bucket, Key="readonly-denied.txt", Body=b"denied"),
        )

        persist_put = client.put_object(Bucket=bucket, Key=persist_key, Body=persist_payload)
        assert_status(persist_put, 200)

        cleanup_bucket(client, bucket, keep_keys={persist_key})
        write_state(
            args.state_file,
            {
                "bucket": bucket,
                "key": persist_key,
                "sha256": hashlib.sha256(persist_payload).hexdigest(),
            },
        )
    except Exception:
        cleanup_bucket(client, bucket)
        raise

    print(f"e2e exercise passed; persistence object left at s3://{bucket}/{persist_key}")


def verify_persistence(args):
    wait_for_http(f"{endpoint_url()}/health", "S3 API")

    state = read_state(args.state_file)
    client = s3_client()
    bucket = state["bucket"]
    key = state["key"]

    assert_status(client.head_bucket(Bucket=bucket), 200)
    head = client.head_object(Bucket=bucket, Key=key)
    assert_status(head, 200)

    got = client.get_object(Bucket=bucket, Key=key)
    payload = read_body(got)
    sha256 = hashlib.sha256(payload).hexdigest()
    if sha256 != state["sha256"]:
        raise AssertionError("persisted object checksum changed after restart")

    listed = client.list_objects_v2(Bucket=bucket, Prefix="persist/")
    keys = [item["Key"] for item in listed.get("Contents", [])]
    if key not in keys:
        raise AssertionError("persisted object was not listed after restart")

    cleanup_bucket(client, bucket)
    print(f"e2e persistence passed; cleaned up s3://{bucket}")


def write_state(path, state):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(path, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, sort_keys=True)


def read_state(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Run ExStorageService S3 e2e checks")
    parser.add_argument("--phase", choices=["exercise", "verify-persistence"], required=True)
    parser.add_argument("--state-file", required=True)
    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    if args.phase == "exercise":
        exercise(args)
    else:
        verify_persistence(args)


if __name__ == "__main__":
    main(sys.argv[1:])
