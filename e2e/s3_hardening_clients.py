#!/usr/bin/env python3
"""Selected AWS CLI v2 and mc profile, disposable loopback endpoints only."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.parse
import uuid

import boto3
from botocore.config import Config


def main():
    endpoints = os.environ["E2E_S3_ENDPOINTS"].split(",")
    credentials = [os.environ["E2E_ACCESS_KEY_ID"], os.environ["E2E_SECRET_ACCESS_KEY"]]
    mc = os.environ.get("ESS_HARDENING_MC", "/tmp/ess-s3-hardening-tools/mc")
    tool_versions = {}
    with tempfile.TemporaryDirectory(prefix="ess-hardening-clients-") as work:
        root = Path(work)
        env = dict(os.environ, AWS_ACCESS_KEY_ID=credentials[0], AWS_SECRET_ACCESS_KEY=credentials[1],
                   AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
                   AWS_PAGER="", AWS_CONFIG_FILE=str(root / "aws-config"),
                   AWS_SHARED_CREDENTIALS_FILE=str(root / "aws-credentials"))
        def run(argv, binary=False):
            result = subprocess.run(argv, env=env, capture_output=True, timeout=120)
            if result.returncode:
                detail = result.stderr.decode(errors="replace")
                for secret in credentials:
                    detail = detail.replace(secret, "[redacted]").replace(urllib.parse.quote(secret, safe=""), "[redacted]")
                raise AssertionError(f"{Path(argv[0]).name} command failed {result.returncode}: {detail[:2000]}")
            return result.stdout if binary else result.stdout.decode()
        tool_versions["aws"] = run(["aws", "--version"]).strip()
        tool_versions["mc"] = run([mc, "--version"]).strip()
        for endpoint in endpoints:
            parsed = urllib.parse.urlsplit(endpoint)
            assert parsed.scheme in ("http", "https") and parsed.hostname in ("localhost", "127.0.0.1", "::1")
            assert not parsed.username and not parsed.password and parsed.path in ("", "/") and not parsed.query
            bucket = "hardening-clients-" + uuid.uuid4().hex[:16]
            client = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=credentials[0],
                aws_secret_access_key=credentials[1], region_name="us-east-1",
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
            client.create_bucket(Bucket=bucket)
            # This mc release parses MC_HOST credentials verbatim before URL parsing.
            # Generated ESS Base64 keys contain no credential delimiters.
            assert all(not any(char in value for char in ":@\r\n") for value in credentials)
            auth = ":".join(credentials)
            env["MC_HOST_hardening"] = f"{parsed.scheme}://{auth}@{parsed.netloc}"
            aws = ["aws", "--endpoint-url", endpoint, "--no-cli-pager"]
            mc_args = [mc, "--config-dir", str(root / "mc")]
            source = root / "source.bin"
            with source.open("wb") as sink:
                for _ in range(160):
                    sink.write(bytes(range(256)) * 256)
            with source.open("rb") as data:
                digest = hashlib.file_digest(data, "sha256").hexdigest()
            try:
                run(aws + ["s3", "cp", str(source), f"s3://{bucket}/aws-upload", "--metadata", "owner=aws"])
                downloaded = root / "aws-download.bin"
                run(aws + ["s3", "cp", f"s3://{bucket}/aws-upload", str(downloaded)])
                with downloaded.open("rb") as data:
                    assert hashlib.file_digest(data, "sha256").hexdigest() == digest
                run(aws + ["s3", "cp", f"s3://{bucket}/aws-upload", f"s3://{bucket}/aws-copy"])
                head = json.loads(run(aws + ["s3api", "head-object", "--bucket", bucket, "--key", "aws-copy"]))
                assert head["Metadata"] == {"owner": "aws"}
                run(aws + ["s3", "ls", f"s3://{bucket}/"])
                run(mc_args + ["cp", str(source), f"hardening/{bucket}/mc-upload"])
                run(mc_args + ["stat", f"hardening/{bucket}/mc-upload"])
                run(mc_args + ["ls", f"hardening/{bucket}/"])
                assert hashlib.sha256(run(mc_args + ["cat", f"hardening/{bucket}/mc-upload"], binary=True)).hexdigest() == digest
                run(mc_args + ["cp", f"hardening/{bucket}/mc-upload", f"hardening/{bucket}/mc-copy"])
                run(mc_args + ["rm", f"hardening/{bucket}/mc-upload"])
                assert client.get_object(Bucket=bucket, Key="mc-copy")["ContentLength"] == source.stat().st_size
            finally:
                for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket):
                    for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                        client.delete_object(Bucket=bucket, Key=item["Key"], VersionId=item["VersionId"])
                client.delete_bucket(Bucket=bucket)
    print(json.dumps({"profile": "selected-clients", "result": "passed", "versions": tool_versions,
                      "endpoints": endpoints}))


if __name__ == "__main__":
    main()
