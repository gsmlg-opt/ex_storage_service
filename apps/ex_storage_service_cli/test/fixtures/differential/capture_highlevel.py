#!/usr/bin/env python3
"""Capture all nine pinned AWS high-level commands against disposable loopback peers."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from urllib.parse import parse_qs, urlsplit

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
    ] + archive_cases() + ssec_cases() + sync_cases()


def archive_cases():
    result = []
    for restored in [False, True]:
        for command, direction in [("cp", "download"), ("cp", "server-copy"), ("mv", "server-copy")]:
            destination = "@TMP@/result.bin" if direction == "download" else "s3://bucket/p/destination"
            args = ["s3", command, "s3://bucket/p/source", destination, "--no-progress"]
            if direction == "server-copy":
                args += ["--copy-props", "none"]
            result.append({"name": command + "-" + direction + "-archive-" + ("restored" if restored else "unrestored"),
                           "command": command, "args": args, "archive": "GLACIER",
                           "restored": restored, "expected_status": 0 if restored else 2})
    for policy in ["ignore-glacier-warnings", "force-glacier-transfer"]:
        result.append({"name": "cp-download-archive-" + policy, "command": "cp",
                       "args": ["s3", "cp", "s3://bucket/p/source", "@TMP@/result.bin", "--no-progress", "--" + policy],
                       "archive": "GLACIER", "restored": False, "expected_status": 0})
    result.append({"name": "cp-download-deep-archive-restored", "command": "cp",
                   "args": ["s3", "cp", "s3://bucket/p/source", "@TMP@/result.bin", "--no-progress"],
                   "archive": "DEEP_ARCHIVE", "restored": True, "expected_status": 0})
    return result


def ssec_cases():
    destination = ["--sse-c", "AES256", "--sse-c-key", "fileb://@TMP@/destination.key"]
    source = ["--sse-c-copy-source", "AES256", "--sse-c-copy-source-key", "fileb://@TMP@/source.key"]
    return [
        {"name": "cp-upload-ssec", "command": "cp", "args": ["s3", "cp", "@TMP@/source.bin", "s3://bucket/p/destination", "--no-progress"] + destination},
        {"name": "cp-download-ssec", "command": "cp", "args": ["s3", "cp", "s3://bucket/p/source", "@TMP@/result.bin", "--no-progress", "--sse-c", "AES256", "--sse-c-key", "fileb://@TMP@/source.key"]},
        {"name": "cp-server-copy-ssec", "command": "cp", "args": ["s3", "cp", "s3://bucket/p/source", "s3://bucket/p/destination", "--no-progress", "--copy-props", "none"] + destination + source},
    ]


def sync_cases():
    result = []
    old, middle, new = 1767321245, 1767323045, 1767324845
    for direction in ["upload", "download", "server-copy"]:
        for age, source_time in [("older", old), ("newer", new)]:
            result.append(sync_case(direction, age, source_time, middle))
    result.append(sync_case("download", "newer-exact", new, middle, ["--exact-timestamps"]))
    result.append(sync_case("upload", "newer-size-only", new, middle, ["--size-only"]))
    for direction in ["upload", "download"]:
        for dryrun in [False, True]:
            result.append(sync_case(direction, "protected-delete" + ("-dryrun" if dryrun else ""),
                                    middle, middle, ["--delete", "--exclude", "*.txt"] + (["--dryrun"] if dryrun else []), delete=True))
    return result


def sync_case(direction, scenario, source_time, destination_time, flags=None, delete=False):
    local = "@TMP@/sync-local/"
    source = local if direction == "upload" else "s3://bucket/source/"
    destination = local if direction == "download" else "s3://bucket/destination/"
    args = ["s3", "sync", source, destination, "--no-progress"] + (flags or [])
    if direction == "server-copy":
        args += ["--copy-props", "none"]
    files = {"sync-local/file.bin": {"body_base64": base64.b64encode(RAW if direction == "upload" else b"old-data!").decode(),
                                     "mtime": source_time if direction == "upload" else destination_time}}
    remote = {}
    heads = {}
    for role, timestamp in [("source", source_time), ("destination", destination_time)]:
        if (direction == "upload" and role == "source") or (direction == "download" and role == "destination"):
            continue
        remote[role + "/"] = [{"name": "file.bin", "mtime": timestamp}]
        heads["/bucket/" + role + "/file.bin"] = object_head(timestamp)
    if delete:
        if direction == "upload":
            remote["destination/"] += [{"name": "stale.bin", "mtime": destination_time},
                                        {"name": "protected.txt", "mtime": destination_time}]
            heads["/bucket/destination/stale.bin"] = object_head(destination_time)
        else:
            for name in ["stale.bin", "protected.txt"]:
                files["sync-local/" + name] = {"body_base64": base64.b64encode(RAW).decode(), "mtime": destination_time}
            heads["/bucket/source/stale.bin"] = response(404)
    return {"name": "sync-" + direction + "-" + scenario, "command": "sync", "args": args,
            "sync_direction": direction, "local_files": files, "remote_listings": remote,
            "head_responses": heads, "inspect_files": list(files)}


def object_head(timestamp):
    import email.utils
    return response(**{"content-length": str(len(RAW)), "etag": ETAG,
                       "last-modified": email.utils.formatdate(timestamp, usegmt=True)})


def sync_listing(prefix, entries):
    from datetime import datetime, timezone
    objects = "".join('<Contents><Key>' + prefix + item["name"] + '</Key><LastModified>'
                      + datetime.fromtimestamp(item["mtime"], timezone.utc).isoformat().replace("+00:00", "Z")
                      + '</LastModified><ETag>' + ETAG + '</ETag><Size>' + str(len(RAW))
                      + '</Size><StorageClass>STANDARD</StorageClass></Contents>' for item in entries)
    return response(body=('<ListBucketResult xmlns="' + NAMESPACE + '"><Name>bucket</Name><Prefix>'
                          + prefix + '</Prefix><IsTruncated>false</IsTruncated>' + objects + '</ListBucketResult>').encode(),
                    **{"content-type": "application/xml"})


def prepare(directory, case):
    (directory / "destination.key").write_bytes(bytes(range(32)))
    (directory / "source.key").write_bytes(bytes(range(32, 64)))
    shutil.rmtree(directory / "sync-local", ignore_errors=True)
    for name, contents in case.get("local_files", {}).items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(contents["body_base64"]))
        os.utime(path, (contents["mtime"], contents["mtime"]))


def handler(case, responses):
    def reply(request, _number):
        method, target = request["method"], urlsplit(request["target"])
        if method == "HEAD" and target.path in case.get("head_responses", {}):
            result = case["head_responses"][target.path]
        elif method == "HEAD":
            headers = {"content-length": str(len(RAW)), "etag": ETAG,
                       "last-modified": "Fri, 02 Jan 2026 03:04:05 GMT"}
            if case["name"].startswith("cp-server-copy-"):
                headers.update({"content-type": "application/x-original", "cache-control": "private",
                                "content-language": "en", "x-amz-meta-preserved": "source"})
            if case.get("archive") and target.path == "/bucket/p/source":
                headers["x-amz-storage-class"] = case["archive"]
                headers["x-amz-restore"] = 'ongoing-request="false", expiry-date="Wed, 02 Jan 2030 03:04:05 GMT"' if case["restored"] else 'ongoing-request="true"'
            result = response(**headers)
        elif method == "GET" and target.path == "/":
            result = response(body=('<ListAllMyBucketsResult xmlns="' + NAMESPACE + '"><Buckets>'
                '<Bucket><Name>fixture-雪</Name><CreationDate>2026-01-02T03:04:05Z</CreationDate>'
                '</Bucket></Buckets></ListAllMyBucketsResult>').encode())
        elif method == "GET" and "list-type=" in target.query:
            prefix = parse_qs(target.query).get("prefix", [""])[0]
            if "remote_listings" in case:
                result = sync_listing(prefix, case["remote_listings"][prefix])
            else:
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
    prepare(directory, case)
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
    if case.get("inspect_files"):
        record["local_after"] = {}
        for name in case["inspect_files"]:
            path = directory / name
            record["local_after"][name] = {"body_base64": base64.b64encode(path.read_bytes()).decode(),
                                            "mtime": int(path.stat().st_mtime)} if path.exists() else None
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
            record = {key: value for key, value in case.items() if key not in {"listing", "remote_listings"}}
            record.update(responses=responses, aws=actual)
            assert actual["status"] == case.get("expected_status", 0), (case["name"], actual)
            assert not actual["peer_errors"], actual
            report["cases"].append(record)
            print(case["name"], "requests=" + str(len(actual["requests"])), flush=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
