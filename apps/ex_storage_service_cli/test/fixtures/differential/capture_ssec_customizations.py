#!/usr/bin/env python3
"""Capture pinned raw API SSE-C and metadata validation on owned loopback peers."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from capture_operations import AWS_SHA256, ROOT, run


def variants():
    cases = []
    operations = ["HeadObject", "GetObject", "PutObject", "CopyObject",
                  "CreateMultipartUpload", "UploadPart", "UploadPartCopy",
                  "CompleteMultipartUpload"]

    def add(operation, scenario, fields, extra=(), reject=False):
        command = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", operation).lower()
        document = {"Bucket": "bucket", "Key": "key", **fields}
        if operation in ["UploadPart", "UploadPartCopy", "CompleteMultipartUpload"]:
            document["UploadId"] = "upload"
        if operation in ["UploadPart", "UploadPartCopy"]:
            document["PartNumber"] = 1
        if operation in ["CopyObject", "UploadPartCopy"]:
            document["CopySource"] = "source/key"
        args = ["s3api", command, "--cli-input-json", json.dumps(document, ensure_ascii=False), *extra]
        if operation == "GetObject":
            args = ["s3api", command]
            for key, value in document.items():
                flag = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", key).lower()
                args += ["--" + flag, value]
            args.append("@TMP@/result.bin")
        body = {"CopyObject": "<CopyObjectResult><ETag>\"copy\"</ETag></CopyObjectResult>",
                "UploadPartCopy": "<CopyPartResult><ETag>\"part\"</ETag></CopyPartResult>",
                "CompleteMultipartUpload": "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>",
                "CreateMultipartUpload": "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>"}.get(operation, "")
        cases.append({"operation": operation, "scenario": scenario, "args": args,
                      "reject": reject, "response": {"status": 200, "headers": {"etag": '"fixture"'},
                      "body_base64": base64.b64encode(body.encode()).decode()}})

    for operation in operations:
        fields = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": "0123456789abcdefghijklmnopqrstuv"}
        if operation in ["CopyObject", "UploadPartCopy"]:
            fields.update(CopySourceSSECustomerAlgorithm="AES256", CopySourceSSECustomerKey="source-key-0123456789abcdefghijkl")
        add(operation, "raw-key-auto-md5", fields)
    for operation in ["HeadObject", "CopyObject", "UploadPartCopy"]:
        fields = {"SSECustomerKey": "caller-encoded", "SSECustomerKeyMD5": "caller-md5"}
        if operation != "HeadObject":
            fields.update(CopySourceSSECustomerKey="source-encoded", CopySourceSSECustomerKeyMD5="source-md5")
        add(operation, "explicit-md5-preserves-key", fields)
    add("HeadObject", "file-text-key", {}, ["--sse-customer-key", "file://@TMP@/key.txt"])
    add("CopyObject", "file-binary-source-key", {}, ["--copy-source-sse-customer-key", "fileb://@TMP@/key.bin"])
    for operation in ["PutObject", "CopyObject", "CreateMultipartUpload"]:
        add(operation, "ascii-metadata", {"Metadata": {"owner": "fixture", "empty": ""}})
        add(operation, "non-ascii-metadata-key", {"Metadata": {"雪": "fixture"}}, reject=True)
        add(operation, "non-ascii-metadata-value", {"Metadata": {"owner": "雪"}}, reject=True)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    assert hashlib.sha256(args.aws.read_bytes()).hexdigest() == AWS_SHA256
    report = {"schema_version": 1, "aws_sha256": AWS_SHA256,
              "model_sha256": hashlib.sha256((ROOT / "priv/compatibility/aws/service-2.json").read_bytes()).hexdigest(),
              "scope": "Eight pinned SSE-C operation hooks, explicit MD5 bypass, text/binary file input and three ASCII metadata hooks", "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-ssec-oracle-") as temporary:
        directory = Path(temporary)
        (directory / "key.txt").write_text("raw text 雪", encoding="utf-8")
        (directory / "key.bin").write_bytes(bytes(range(32)) + b"\xff")
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
                   AWS_CLI_AUTO_PROMPT="off", AWS_RESPONSE_CHECKSUM_VALIDATION="when_required",
                   NO_PROXY="*", PAGER="cat")
        for case in variants():
            case["aws"] = run(str(args.aws), case["args"], case["response"], env, directory)
            assert not case["aws"]["peer_errors"], case
            assert len(case["aws"]["requests"]) == (0 if case["reject"] else 1), case
            assert (case["aws"]["status"] != 0) == case["reject"], case
            report["cases"].append(case)
            print(case["operation"], case["scenario"], case["aws"]["status"], flush=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
