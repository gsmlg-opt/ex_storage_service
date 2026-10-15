#!/usr/bin/env python3
"""Capture pinned AWS mv path resolution through an answering loopback proxy."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import urlsplit

import capture_operations as common

AP = "s3://arn:aws:s3:us-west-2:123456789012:accesspoint/example-ap/key"
OUTPOST = "s3://arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-123/accesspoint/example-ap/key"
MRAP = "s3://arn:aws:s3::123456789012:accesspoint/example.mrap/key"


def xml(value):
    return base64.b64encode(value.encode()).decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == common.AWS_SHA256
    cases = [
        ("access-point-destination", "s3://bucket/key", AP, ["--validate-same-s3-paths"], {}, "same"),
        ("access-point-source-region", AP, "s3://bucket/key", ["--validate-same-s3-paths", "--source-region", "eu-west-1"], {}, "same"),
        ("access-point-alias", "s3://bucket/key", "s3://example-ap-s3alias/key", ["--validate-same-s3-paths"], {}, "same"),
        ("sts-access-denied", "s3://bucket/key", "s3://example-ap-s3alias/key", ["--validate-same-s3-paths"], {}, "sts-denied"),
        ("sts-invalid-token", "s3://bucket/key", "s3://example-ap-s3alias/key", ["--validate-same-s3-paths"], {}, "sts-invalid-token"),
        ("mrap-checksum-when-required", "s3://bucket/key", MRAP, ["--validate-same-s3-paths"], {"AWS_REQUEST_CHECKSUM_CALCULATION": "when_required"}, "mrap-pages"),
        ("outposts-arn", "s3://bucket/key", OUTPOST.replace("us-west-2", "us-east-1"), ["--validate-same-s3-paths"], {}, "same"),
        ("outposts-region-mismatch", "s3://bucket/key", OUTPOST, ["--validate-same-s3-paths"], {}, "same"),
        ("outposts-alias", "s3://bucket/key", "s3://example-ap--op-s3/key", ["--validate-same-s3-paths"], {}, "same"),
        ("mrap-second-page", "s3://bucket/key", MRAP, ["--validate-same-s3-paths"], {}, "mrap-pages"),
        ("mrap-missing", "s3://bucket/key", MRAP, ["--validate-same-s3-paths"], {}, "mrap-missing"),
        ("environment-true", "s3://bucket/key", AP, [], {"AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS": "TrUe"}, "same"),
        ("environment-space-false", "s3://bucket/key", AP, ["--dryrun"], {"AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS": " true "}, "same"),
        ("different-key", "s3://bucket/key", AP + "2", ["--validate-same-s3-paths", "--dryrun"], {}, "same"),
        ("different-bucket", "s3://bucket/key", AP, ["--validate-same-s3-paths", "--dryrun"], {}, "different"),
        ("access-denied", "s3://bucket/key", AP, ["--validate-same-s3-paths"], {}, "denied"),
        ("fips-custom-endpoint", "s3://bucket/key", AP, ["--validate-same-s3-paths"], {"AWS_USE_FIPS_ENDPOINT": "true"}, "same"),
        ("dualstack-custom-endpoint", "s3://bucket/key", AP, ["--validate-same-s3-paths"], {"AWS_USE_DUALSTACK_ENDPOINT": "true"}, "same"),
        ("profile-fips", "s3://bucket/key", AP, ["--validate-same-s3-paths", "--profile", "selected"], {}, "profile-fips"),
        ("profile-dualstack", "s3://bucket/key", AP, ["--validate-same-s3-paths", "--profile", "selected"], {}, "profile-dualstack"),
        ("profile-service-endpoint", "s3://bucket/key", AP, ["--validate-same-s3-paths", "--profile", "selected"], {}, "profile"),
    ]
    report = {"aws_sha256": common.AWS_SHA256, "cases": []}
    common.VOLATILE_HEADERS -= {"authorization", "host", "x-amz-date"}
    with tempfile.TemporaryDirectory(prefix="ess-same-path-oracle-") as temporary:
        directory = Path(temporary)
        for name, source, destination, flags, extra_env, scenario in cases:
            responses = []

            def response(request, _index):
                path = urlsplit(request["target"]).path
                status = 200
                if "/accesspoint/" in path:
                    if scenario == "denied":
                        status, body = 403, "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>"
                    else:
                        bucket = "different" if scenario == "different" else "bucket"
                        body = f"<GetAccessPointResult><Bucket>{bucket}</Bucket></GetAccessPointResult>"
                elif "/mrap/instances" in path:
                    if scenario == "mrap-pages" and "nextToken=" not in request["target"]:
                        body = "<ListMultiRegionAccessPointsResult><AccessPoints/><NextToken>page+/=2</NextToken></ListMultiRegionAccessPointsResult>"
                    else:
                        alias = "other.mrap" if scenario == "mrap-missing" else "example.mrap"
                        body = f"<ListMultiRegionAccessPointsResult><AccessPoints><AccessPoint><Alias>{alias}</Alias><Regions><Region><Bucket>different</Bucket></Region><Region><Bucket>bucket</Bucket></Region></Regions></AccessPoint></AccessPoints></ListMultiRegionAccessPointsResult>"
                elif request["method"] == "POST" and scenario.startswith("sts-"):
                    code, message = ("AccessDenied", "STS denied caller") if scenario == "sts-denied" else ("InvalidClientTokenId", "The security token included in the request is invalid")
                    status, body = 403, f'<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error><RequestId>fixture-sts-request</RequestId></ErrorResponse>'
                elif request["method"] == "POST":
                    body = "<GetCallerIdentityResponse><GetCallerIdentityResult><Account>123456789012</Account><Arn>arn:aws:iam::123456789012:user/fixture</Arn><UserId>fixture</UserId></GetCallerIdentityResult></GetCallerIdentityResponse>"
                else:
                    body = ""
                result = {"status": status, "headers": {"content-length": "0", "last-modified": "Fri, 02 Jan 2026 03:04:05 GMT", "etag": '"fixture"'} if request["method"] == "HEAD" else {}, "body_base64": xml(body)}
                responses.append(result)
                return result

            peer = common.Peer(response)
            port = urlsplit(peer.endpoint).port
            control = f"http://control.invalid:{port}"
            sts = f"http://sts.invalid:{port}"
            config = directory / "config"
            extra_profile = "use_fips_endpoint = true\n" if scenario == "profile-fips" else "use_dualstack_endpoint = true\n" if scenario == "profile-dualstack" else ""
            config.write_text(f"[profile selected]\nregion = us-east-1\n{extra_profile}services = fixture\n[services fixture]\ns3_control =\n  endpoint_url = {control}\n")
            credentials = directory / "credentials"
            credentials.write_text("[selected]\naws_access_key_id = fixture-access\naws_secret_access_key = fixture-secret\n")
            env = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
            env.update(AWS_CONFIG_FILE=str(config), AWS_SHARED_CREDENTIALS_FILE=str(credentials), AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret", AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off", HTTP_PROXY=peer.endpoint, HTTPS_PROXY=peer.endpoint, AWS_ENDPOINT_URL_STS=sts, PAGER="cat")
            if not scenario.startswith("profile"):
                env["AWS_ENDPOINT_URL_S3_CONTROL"] = control
            env.update(extra_env)
            command = ["--endpoint-url", f"http://data.invalid:{port}", "--region", "us-east-1", "--no-cli-pager", "--cli-connect-timeout", "1", "--cli-read-timeout", "2", "s3", "mv", source, destination] + flags
            try:
                result = subprocess.run([args.aws] + command, env=env, capture_output=True, text=True, timeout=15)
            finally:
                peer.close()
            assert peer.errors == [], peer.errors
            for request in peer.requests:
                headers = request["headers"]
                authorization = headers.pop("authorization", "")
                match = re.search(r"Credential=[^/]+/[^/]+/([^,]+), SignedHeaders=([^,]+)", authorization)
                request["signing_scope"] = match.group(1) if match else None
                request["signed_headers"] = match.group(2) if match else None
                headers.pop("x-amz-date", None)
                request["target"] = request["target"].replace(f":{port}", ":@PORT@")
                headers["host"] = headers.get("host", "").replace(f":{port}", ":@PORT@")
            record = {"name": name, "source": source, "destination": destination, "flags": flags, "env": extra_env, "scenario": scenario, "aws": {"status": result.returncode, "stdout": result.stdout, "stderr": result.stderr, "requests": peer.requests}, "responses": responses}
            report["cases"].append(record)
            print(name, result.returncode, len(peer.requests), flush=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
