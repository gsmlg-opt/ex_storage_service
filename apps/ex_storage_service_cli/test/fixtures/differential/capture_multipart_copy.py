#!/usr/bin/env python3
"""Pinned AWS high-level multipart-copy oracle, using loopback control responses."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile

from capture_operations import AWS_SHA256, run


def response(request, _index):
    target, method = request["target"], request["method"]
    headers, body = {}, b""
    if method == "HEAD":
        headers = {"content-length": "9437184", "etag": '"source-etag"',
                   "last-modified": "Fri, 02 Jan 2026 03:04:05 GMT", "content-type": "application/octet-stream",
                   "x-amz-tagging-count": "1", "x-amz-version-id": "source-version"}
    elif method == "GET" and "tagging" in target:
        body = b"<Tagging><TagSet><Tag><Key>owner</Key><Value>fixture</Value></Tag></TagSet></Tagging>"
    elif method == "POST" and "uploads" in target:
        body = b"<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>target</Key><UploadId>fixture-upload</UploadId></InitiateMultipartUploadResult>"
    elif method == "PUT" and "partNumber=" in target:
        checksums = {"CRC32": 4, "CRC32C": 4, "CRC64NVME": 8, "SHA1": 20, "SHA256": 32}
        fields = "".join("<Checksum" + name + ">" + base64.b64encode(bytes(size)).decode() + "</Checksum" + name + ">" for name, size in checksums.items())
        body = ('<CopyPartResult><LastModified>2026-01-02T03:04:05Z</LastModified><ETag>"part-etag"</ETag>' + fields + "</CopyPartResult>").encode()
    elif method == "POST" and "uploadId=" in target:
        body = b'<CompleteMultipartUploadResult><Location>http://fixture/bucket/target</Location><Bucket>bucket</Bucket><Key>target</Key><ETag>"copied-2"</ETag></CompleteMultipartUploadResult>'
    elif method == "DELETE" and "uploadId=" in target:
        pass
    else:
        raise RuntimeError("Unexpected object DATA request: " + method + " " + target)
    return {"status": 200, "headers": headers, "body_base64": base64.b64encode(body).decode()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256
    report = {"aws_sha256": AWS_SHA256, "source_bytes": 9437184,
              "response_part_checksums": "synthetic valid Base64 zero digests for each algorithm; no object entity GET is needed", "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-multipart-copy-oracle-") as temporary:
        env = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "ESS_", "MC_"))}
        env.update(AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret", AWS_CONFIG_FILE="/dev/null",
                   AWS_SHARED_CREDENTIALS_FILE="/dev/null", AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
                   AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off", NO_PROXY="*", PAGER="cat")
        for name, options in [("default", []), ("copy-props-none", ["--copy-props", "none"]), ("explicit-sha256", ["--checksum-algorithm", "SHA256"])]:
            command = ["s3", "cp", "s3://source/key", "s3://bucket/target", "--no-progress", *options]
            result = run(args.aws, command, response, env, Path(temporary))
            report["cases"].append({"name": name, "args": command, "result": result})
            print(name, result.get("status"), [(item["method"], item["target"]) for item in result["requests"]])
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return int(any(case["result"].get("status") != 0 or case["result"]["peer_errors"] for case in report["cases"]))


if __name__ == "__main__":
    raise SystemExit(main())
