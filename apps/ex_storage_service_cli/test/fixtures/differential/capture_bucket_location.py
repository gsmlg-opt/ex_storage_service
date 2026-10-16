#!/usr/bin/env python3
"""Capture root-text/null location responses from the locked AWS CLI."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile

from capture_operations import AWS_SHA256, run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(args.aws.read_bytes()).hexdigest() == AWS_SHA256
    cases = []
    with tempfile.TemporaryDirectory(prefix="ess-location-oracle-") as temporary:
        directory = Path(temporary)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
                   AWS_CLI_AUTO_PROMPT="off", NO_PROXY="*", PAGER="cat")
        for name, body in [
            ("empty-element", '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>'),
            ("empty-paired-tags", '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>'),
            ("named-region", '<LocationConstraint>eu-west-1</LocationConstraint>'),
            ("legacy-eu", '<LocationConstraint>EU</LocationConstraint>'),
        ]:
            response = {"status": 200, "headers": {"content-type": "application/xml"},
                        "body_base64": base64.b64encode(body.encode()).decode()}
            result = run(str(args.aws), ["s3api", "get-bucket-location", "--bucket", "bucket"], response, env, directory)
            assert result["status"] == 0 and len(result["requests"]) == 1 and not result["peer_errors"], result
            cases.append({"name": name, "body": body, "aws": result})
    args.output.write_text(json.dumps({"aws_sha256": AWS_SHA256, "cases": cases}, indent=2) + "\n")


if __name__ == "__main__":
    main()
