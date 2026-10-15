#!/usr/bin/env python3
"""Capture all nine pinned AWS high-level commands against disposable loopback peers."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlsplit

from capture_operations import AWS_SHA256, NAMESPACE, Peer

RAW = b"fixture\x00\xff"
ETAG = '"' + hashlib.md5(RAW).hexdigest() + '"'


def response(status=200, body=b"", **headers):
    return {"status": status, "headers": headers,
            "body_base64": base64.b64encode(body).decode()}


def listing(contents=""):
    return response(body=('<ListBucketResult xmlns="' + NAMESPACE + '">'
        '<Name>bucket</Name><Prefix>p/</Prefix><IsTruncated>false</IsTruncated>'
        + contents + '</ListBucketResult>').encode(), **{"content-type": "application/xml"})


def cases():
    objects = "".join('<Contents><Key>p/' + name + '</Key><LastModified>2026-01-02T03:04:05Z</LastModified>'
               '<ETag>' + ETAG + '</ETag><Size>' + str(size) + '</Size><StorageClass>STANDARD</StorageClass></Contents>'
               for name, size in [("zero", 0), ("one", 1), ("seven", 7), ("kib", 1024), ("threshold", 1048064)])
    return [
        {"name": "ls-buckets", "command": "ls", "args": ["s3", "ls", "--bucket-name-prefix", "fi", "--bucket-region", "us-east-1", "--page-size", "2"]},
        {"name": "ls-human-summary", "command": "ls", "args": ["s3", "ls", "s3://bucket/p/", "--recursive", "--human-readable", "--summarize", "--page-size", "2", "--request-payer", "requester"], "listing": listing(objects)},
        {"name": "mb-regional", "command": "mb", "args": ["s3", "mb", "s3://bucket", "--region", "us-west-2"]},
        {"name": "rb-empty", "command": "rb", "args": ["s3", "rb", "s3://bucket"]},
        {"name": "rb-force-empty", "command": "rb", "args": ["s3", "rb", "s3://bucket", "--force"], "listing": listing()},
        {"name": "rm-requester", "command": "rm", "args": ["s3", "rm", "s3://bucket/p/% +?#/雪", "--request-payer", "requester"]},
        {"name": "website-documents", "command": "website", "args": ["s3", "website", "s3://bucket", "--index-document", "index.html", "--error-document", "error.html"]},
        {"name": "presign-unsigned", "command": "presign", "args": ["s3", "presign", "s3://bucket/p/% +?#/雪", "--expires-in", "90", "--no-sign-request"]},
        {"name": "cp-upload-fields", "command": "cp", "args": ["s3", "cp", "@TMP@/source.bin", "s3://bucket/p/% +?#/雪", "--no-progress", "--content-type", "application/x-fixture", "--content-encoding", "gzip", "--content-language", "en", "--content-disposition", "attachment; filename=fixture.bin", "--cache-control", "max-age=60", "--expires", "2030-01-02T03:04:05Z", "--metadata", "owner=fixture,note=two words", "--acl", "private", "--storage-class", "STANDARD_IA", "--sse", "aws:kms", "--sse-kms-key-id", "fixture-kms-key", "--checksum-algorithm", "SHA256", "--website-redirect", "/new", "--grants", "read=id=first", "read=uri=http://acs.amazonaws.com/groups/global/AllUsers", 'full=id="owner"']},
        {"name": "cp-download-raw", "command": "cp", "args": ["s3", "cp", "s3://bucket/p/% +?#/雪", "@TMP@/result.bin", "--no-progress", "--checksum-mode", "ENABLED", "--request-payer", "requester"]},
        {"name": "mv-server-copy", "command": "mv", "args": ["s3", "mv", "s3://bucket/p/source", "s3://bucket/p/destination", "--copy-props", "none", "--no-progress", "--source-region", "us-west-2"]},
        *[{"name": "cp-server-copy-" + name, "command": "cp",
           "args": ["s3", "cp", "s3://bucket/p/source", "s3://bucket/p/destination", "--no-progress"] + flags}
          for name, flags in [
              ("none", ["--copy-props", "none"]),
              ("metadata-only", ["--copy-props", "metadata-directive", "--content-type", "application/x-replaced"]),
              ("default-metadata", ["--metadata", "new=two"]),
              ("default", []),
              ("directive-copy", ["--metadata-directive", "COPY", "--copy-props", "none"]),
              ("directive-replace", ["--metadata-directive", "REPLACE", "--metadata", "new=two"]),
          ]],
        {"name": "sync-upload", "command": "sync", "args": ["s3", "sync", "@TMP@/local/", "s3://bucket/p/", "--no-progress", "--exclude", "*", "--include", "*.bin", "--size-only"], "listing": listing()},
    ]


def handler(case, responses):
    def reply(request, _number):
        method, target = request["method"], urlsplit(request["target"])
        if method == "HEAD":
            headers = {"content-length": str(len(RAW)), "etag": ETAG,
                       "last-modified": "Fri, 02 Jan 2026 03:04:05 GMT"}
            if case["name"].startswith("cp-server-copy-"):
                headers.update({"content-type": "application/x-original", "cache-control": "private",
                                "content-language": "en", "x-amz-meta-preserved": "source"})
            result = response(**headers)
        elif method == "GET" and target.path == "/":
            result = response(body=('<ListAllMyBucketsResult xmlns="' + NAMESPACE + '"><Buckets>'
                '<Bucket><Name>fixture-雪</Name><CreationDate>2026-01-02T03:04:05Z</CreationDate>'
                '</Bucket></Buckets></ListAllMyBucketsResult>').encode())
        elif method == "GET" and "list-type=" in target.query:
            result = case.get("listing", listing())
        elif method == "GET":
            result = response(body=RAW, etag=ETAG)
        elif method == "PUT" and "x-amz-copy-source" in request["headers"]:
            result = response(body=('<CopyObjectResult xmlns="' + NAMESPACE + '"><ETag>' + ETAG + '</ETag>'
                '<LastModified>2026-01-02T03:04:05Z</LastModified></CopyObjectResult>').encode())
        elif method == "DELETE":
            result = response(204)
        else:
            result = response(etag=ETAG)
        responses.append(result)
        return result
    return reply


def run(program, case, directory, env):
    responses = []
    peer = Peer(handler(case, responses))
    args = [value.replace("@TMP@", str(directory)) for value in case["args"]]
    argv = [program, "--endpoint-url", peer.endpoint, "--region", "us-east-1", "--no-cli-pager",
            "--cli-connect-timeout", "1", "--cli-read-timeout", "2"] + args
    # Pinned ClientFactory clears endpoint_url for an explicit source region.
    # Its environment endpoint fallback must also stay on the owned loopback peer.
    env = dict(env, AWS_ENDPOINT_URL_S3=peer.endpoint, AWS_ENDPOINT_URL=peer.endpoint)
    try:
        result = subprocess.run(argv, env=env, cwd=directory, capture_output=True, timeout=30)
    finally:
        peer.close()
    def clean(value):
        return value.decode(errors="replace").replace(str(directory), "@TMP@").replace(peer.endpoint, "@ENDPOINT@")
    record = {"status": result.returncode, "stdout": clean(result.stdout), "stderr": clean(result.stderr),
              "requests": peer.requests, "peer_errors": peer.errors}
    outfile = directory / "result.bin"
    if outfile.exists():
        record["outfile_base64"] = base64.b64encode(outfile.read_bytes()).decode()
        outfile.unlink()
    return responses, record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256, "Unpinned AWS executable"
    report = {"schema_version": 1, "aws_sha256": AWS_SHA256, "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-highlevel-oracle-") as temporary:
        directory = Path(temporary)
        (directory / "source.bin").write_bytes(RAW)
        (directory / "local").mkdir()
        (directory / "local/file.bin").write_bytes(RAW)
        (directory / "local/ignored.txt").write_bytes(b"excluded")
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and key.lower() not in
               {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off",
                   AWS_RESPONSE_CHECKSUM_VALIDATION="when_required", NO_PROXY="*", PAGER="cat", TZ="UTC")
        for case in cases():
            responses, actual = run(args.aws, case, directory, env)
            record = {key: value for key, value in case.items() if key != "listing"}
            record.update(responses=responses, aws=actual)
            assert actual["status"] == 0, (case["name"], actual)
            assert not actual["peer_errors"], actual
            report["cases"].append(record)
            print(case["name"], "requests=" + str(len(actual["requests"])), flush=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
