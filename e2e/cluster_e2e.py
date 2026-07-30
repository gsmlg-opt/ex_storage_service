#!/usr/bin/env python3
import argparse
import concurrent.futures
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def endpoints():
    value = os.environ.get(
        "E2E_S3_ENDPOINTS", "http://127.0.0.1:9001,http://127.0.0.1:9002"
    )
    result = [item.strip().rstrip("/") for item in value.split(",") if item.strip()]
    if len(result) != 2:
        raise RuntimeError("E2E_S3_ENDPOINTS must contain exactly two endpoints")
    return result


def client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["E2E_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["E2E_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
            connect_timeout=2,
            read_timeout=10,
        ),
    )


def body(response):
    return response["Body"].read()


def save(path, state):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, sort_keys=True)


def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def assert_get(s3, bucket, key, expected, version_id=None):
    args = {"Bucket": bucket, "Key": key}
    if version_id:
        args["VersionId"] = version_id
    actual = body(s3.get_object(**args))
    if actual != expected:
        raise AssertionError(f"unexpected body for {key} version {version_id}")


def wait_http_status(url, expected, timeout=30):
    deadline = time.time() + timeout
    last_status = None

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                last_status = response.status
        except urllib.error.HTTPError as error:
            last_status = error.code
        except urllib.error.URLError:
            last_status = None

        if last_status == expected:
            return
        time.sleep(0.5)

    raise AssertionError(
        f"{url} did not return HTTP {expected}; last status was {last_status}"
    )


def assert_version_head(s3, bucket, key, expected_payloads):
    current = body(s3.get_object(Bucket=bucket, Key=key))
    if current not in expected_payloads:
        raise AssertionError("current head does not reference a concurrent version")


def exercise(path):
    endpoint_a, endpoint_b = endpoints()
    clients = [client(endpoint_a), client(endpoint_b)]
    bucket = f"cluster-e2e-{uuid.uuid4().hex[:16]}"
    version_key = "versions/concurrent.bin"
    persistent_key = "persist/cluster.bin"
    multipart_key = "multipart/cross-node.bin"

    clients[0].create_bucket(Bucket=bucket)
    clients[1].put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
    )

    payloads = [f"concurrent-version-{index}".encode() for index in range(32)]

    def put(index):
        response = clients[index % 2].put_object(
            Bucket=bucket, Key=version_key, Body=payloads[index]
        )
        version_id = response.get("VersionId")
        if not version_id or version_id == "null":
            raise AssertionError("versioned PUT did not return a version id")
        return version_id

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        version_ids = list(pool.map(put, range(len(payloads))))

    if len(set(version_ids)) != len(payloads):
        raise AssertionError("concurrent PUTs did not create unique versions")

    assert_version_head(clients[0], bucket, version_key, payloads)

    for index, (version_id, expected) in enumerate(zip(version_ids, payloads)):
        assert_get(clients[(index + 1) % 2], bucket, version_key, expected, version_id)

    part_one = b"a" * (5 * 1024 * 1024)
    part_two = b"b" * 4096
    upload = clients[0].create_multipart_upload(Bucket=bucket, Key=multipart_key)
    upload_id = upload["UploadId"]
    uploaded_one = clients[1].upload_part(
        Bucket=bucket,
        Key=multipart_key,
        UploadId=upload_id,
        PartNumber=1,
        Body=part_one,
    )
    uploaded_two = clients[0].upload_part(
        Bucket=bucket,
        Key=multipart_key,
        UploadId=upload_id,
        PartNumber=2,
        Body=part_two,
    )
    complete = clients[1].complete_multipart_upload(
        Bucket=bucket,
        Key=multipart_key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": uploaded_one["ETag"], "PartNumber": 1},
                {"ETag": uploaded_two["ETag"], "PartNumber": 2},
            ]
        },
    )
    multipart_version_id = complete.get("VersionId")
    expected_multipart = part_one + part_two
    assert_get(clients[0], bucket, multipart_key, expected_multipart)
    assert_get(clients[1], bucket, multipart_key, expected_multipart)

    persistent = b"survives voter and data-node restarts"
    persistent_put = clients[0].put_object(
        Bucket=bucket, Key=persistent_key, Body=persistent
    )
    persistent_version_id = persistent_put.get("VersionId")

    save(
        path,
        {
            "bucket": bucket,
            "version_key": version_key,
            "version_ids": version_ids,
            "payloads": [payload.decode() for payload in payloads],
            "persistent_key": persistent_key,
            "persistent_sha256": hashlib.sha256(persistent).hexdigest(),
            "persistent_version_id": persistent_version_id,
            "multipart_key": multipart_key,
            "multipart_sha256": hashlib.sha256(expected_multipart).hexdigest(),
            "multipart_version_id": multipart_version_id,
        },
    )


def data_node_failure(path):
    state = load(path)
    endpoint = endpoints()[0]
    s3 = client(endpoint)
    wait_http_status(f"{endpoint}/health/ready", 503)
    persistent = body(
        s3.get_object(Bucket=state["bucket"], Key=state["persistent_key"])
    )
    if hashlib.sha256(persistent).hexdigest() != state["persistent_sha256"]:
        raise AssertionError("strict-W failure read returned wrong bytes")

    try:
        s3.put_object(
            Bucket=state["bucket"], Key="failure/strict-w.bin", Body=b"must fail"
        )
    except ClientError as error:
        status = error.response["ResponseMetadata"]["HTTPStatusCode"]
        if status != 503:
            raise AssertionError(f"strict W failure returned HTTP {status}") from error
    else:
        raise AssertionError("strict W=2 write succeeded with one data node")


def recovered(path, key):
    state = load(path)
    clients = [client(endpoint) for endpoint in endpoints()]
    for s3 in clients:
        persistent = body(
            s3.get_object(Bucket=state["bucket"], Key=state["persistent_key"])
        )
        if hashlib.sha256(persistent).hexdigest() != state["persistent_sha256"]:
            raise AssertionError("recovered node returned wrong persistent bytes")
    clients[0].put_object(Bucket=state["bucket"], Key=key, Body=b"quorum restored")


def verify_persistence(path):
    state = load(path)
    clients = [client(endpoint) for endpoint in endpoints()]
    for index, s3 in enumerate(clients):
        persistent = body(
            s3.get_object(Bucket=state["bucket"], Key=state["persistent_key"])
        )
        multipart = body(
            s3.get_object(Bucket=state["bucket"], Key=state["multipart_key"])
        )
        if hashlib.sha256(persistent).hexdigest() != state["persistent_sha256"]:
            raise AssertionError(f"endpoint {index} lost the persistent object")
        if hashlib.sha256(multipart).hexdigest() != state["multipart_sha256"]:
            raise AssertionError(f"endpoint {index} lost the multipart object")

    for index, (version_id, expected) in enumerate(
        zip(state["version_ids"], state["payloads"])
    ):
        assert_get(
            clients[index % 2],
            state["bucket"],
            state["version_key"],
            expected.encode(),
            version_id,
        )

    assert_version_head(
        clients[1],
        state["bucket"],
        state["version_key"],
        [payload.encode() for payload in state["payloads"]],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        required=True,
        choices=[
            "exercise",
            "data-node-failure",
            "data-node-recovered",
            "metadata-leader-failure",
            "verify-persistence",
        ],
    )
    parser.add_argument("--state-file", required=True)
    args = parser.parse_args()

    if args.phase == "exercise":
        exercise(args.state_file)
    elif args.phase == "data-node-failure":
        data_node_failure(args.state_file)
    elif args.phase == "data-node-recovered":
        recovered(args.state_file, "recovery/data-node.bin")
    elif args.phase == "metadata-leader-failure":
        recovered(args.state_file, "recovery/metadata-leader.bin")
    else:
        verify_persistence(args.state_file)


if __name__ == "__main__":
    main()
