#!/usr/bin/env python3
"""Capture all four actual pinned AWS waiters, including one retry and failure."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from capture_operations import AWS_SHA256, ROOT, run


def response(status):
    headers = {"x-amz-request-id": "fixture-request"}
    if status == 301:
        # Without a region header botocore recursively probes HeadBucket rather
        # than returning the response to its waiter. Keep this fixture local.
        headers["x-amz-bucket-region"] = "us-east-1"
    return {"status": status, "headers": headers, "body_base64": ""}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--ess", help="optional historical diagnostics")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256
    waiter_path = ROOT / "priv/compatibility/aws/waiters-2.json"
    waiters = json.loads(waiter_path.read_text())["waiters"]
    report = {"aws_sha256": AWS_SHA256, "waiters_sha256": hashlib.sha256(waiter_path.read_bytes()).hexdigest(), "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-waiter-oracle-") as temporary:
        directory = Path(temporary)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
        env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
                   AWS_CLI_AUTO_PROMPT="off", AWS_RESPONSE_CHECKSUM_VALIDATION="when_required",
                   ESS_CONFIG_DIR=str(directory / "config"), ESS_STATE_DIR=str(directory / "state"),
                   ERL_FLAGS="+S 2:2", NO_PROXY="*", PAGER="cat")
        for name, waiter in waiters.items():
            cli = re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()
            command = ["s3api", "wait", cli, "--bucket", "bucket"]
            if waiter["operation"] == "HeadObject":
                command += ["--key", "key%2F+ 雪"]
            success = [acceptor["expected"] for acceptor in waiter["acceptors"] if acceptor["state"] == "success"]
            primary = success[0]
            retry = 404 if name.endswith("Exists") and not name.endswith("NotExists") else 200
            scenarios = [("success-" + str(status), [status]) for status in success]
            scenarios += [("retry-then-success", [retry, primary]), ("terminal-error", [401 if name == "BucketExists" else 403])]
            for scenario, statuses in scenarios:
                responses = [response(status) for status in statuses]

                def select(_request, index):
                    return responses[min(index - 1, len(responses) - 1)]

                observed = run(args.aws, command, select, env, directory)
                assert observed.get("status") == (255 if scenario == "terminal-error" else 0), observed
                expected_requests = 2 if statuses == [301] else len(responses)
                assert observed["peer_errors"] == [] and len(observed["requests"]) == expected_requests, observed
                case = {"waiter": name, "scenario": scenario, "args": command,
                        "responses": [select(request, index) for index, request in enumerate(observed["requests"], 1)], "aws": observed}
                if args.ess:
                    case["initial_ess"] = run(args.ess, command, select, env, directory, ess=True)
                report["cases"].append(case)
                print(name, scenario, "aws=" + str(observed["status"]), "requests=" + str(len(observed["requests"])), flush=True)
                args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
