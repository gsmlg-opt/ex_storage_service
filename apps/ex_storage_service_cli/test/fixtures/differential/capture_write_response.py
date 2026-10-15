#!/usr/bin/env python3
"""Pinned Object Lambda oracle through a private loopback HTTP proxy."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import urlsplit

import capture_operations as common


def check_signature(request):
    headers = request["headers"]
    match = re.fullmatch(r"AWS4-HMAC-SHA256 Credential=([^/]+)/([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)", headers["authorization"])
    assert match
    access, scope, names, signature = match.groups()
    assert access == "fixture-access"
    date, region, service, suffix = scope.split("/")
    assert (region, service, suffix) == ("us-east-1", "s3-object-lambda", "aws4_request")
    body = base64.b64decode(request["body_base64"])
    assert headers["x-amz-content-sha256"] == hashlib.sha256(body).hexdigest()
    canonical_headers = "".join(name + ":" + " ".join(headers[name].split()) + "\n" for name in names.split(";"))
    canonical = "\n".join([request["method"], urlsplit(request["target"]).path, "", canonical_headers, names, headers["x-amz-content-sha256"]])
    string = "\n".join(["AWS4-HMAC-SHA256", headers["x-amz-date"], scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = b"AWS4fixture-secret"
    for part in (date, region, service, suffix):
        key = hmac.digest(key, part.encode(), "sha256")
    assert hmac.new(key, string.encode(), "sha256").hexdigest() == signature


def run(program, arguments, response, directory, ess=False):
    # Keep the signed host/date/authentication for independent signature checks.
    common.VOLATILE_HEADERS -= {"host", "x-amz-date", "authorization"}
    peer = common.Peer(response)
    endpoint = peer.endpoint.replace("127.0.0.1", "loopback.invalid")
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
    env.update(AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
               AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
               AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
               AWS_CLI_AUTO_PROMPT="off", AWS_RESPONSE_CHECKSUM_VALIDATION="when_required",
               ERL_FLAGS="+S 2:2", ESS_CONFIG_DIR=str(directory / "config"),
               ESS_STATE_DIR=str(directory / "state"), PAGER="cat")
    if ess:
        inetrc = directory / "inetrc"
        inetrc.write_text('{host,{127,0,0,1},["fixture.loopback.invalid"]}.\n{lookup,[file]}.\n')
        env["ERL_INETRC"] = str(inetrc)
    else:
        env.update(HTTP_PROXY=peer.endpoint, HTTPS_PROXY=peer.endpoint)
    command = (["escript", program] if ess else [program]) + [
        "--endpoint-url", endpoint, "--region", "us-east-1", "--no-cli-pager",
        "--cli-connect-timeout", "1", "--cli-read-timeout", "2", "--no-paginate"]
    command += [arg.replace("@TMP@", str(directory)) for arg in arguments]
    try:
        result = subprocess.run(command, env=env, capture_output=True, timeout=15)
        record = {"status": result.returncode, "stdout": result.stdout.decode(), "stderr": result.stderr.decode()}
    finally:
        peer.close()
    record.update(requests=peer.requests, peer_errors=peer.errors)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--ess", help="optional diagnostics, excluded from oracle fixtures")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == common.AWS_SHA256
    model_path = common.ROOT / "priv/compatibility/aws/service-2.json"
    report = {"aws_sha256": common.AWS_SHA256, "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(), "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-object-lambda-oracle-") as temporary:
        directory = Path(temporary)
        (directory / "body.bin").write_bytes(bytes(range(256)))
        base = ["s3api", "write-get-object-response", "--request-route", "fixture", "--request-token", "opaque%2F+=="]
        binary = ["--body", "@TMP@/body.bin", "--content-length", "256", "--status-code", "206",
                  "--content-type", "application/octet-stream", "--content-encoding", "gzip",
                  "--content-range", "bytes 0-255/512", "--metadata", '{"literal":"percent%2F+value"}']
        forwarded_error = ["--content-length", "0", "--status-code", "403", "--error-code", "AccessDenied", "--error-message", "Transformed access denied"]
        success = {"status": 200, "headers": {"x-amz-request-id": "fixture-request"}, "body_base64": ""}
        denied = {"status": 403, "headers": {"content-type": "application/xml", "x-amz-request-id": "fixture-request"},
                  "body_base64": base64.b64encode(b"<Error><Code>AccessDenied</Code><Message>Denied by fixture</Message><RequestId>fixture-request</RequestId></Error>").decode()}
        for scenario, options, response in [("binary-success", binary, success), ("forwarded-error-success", forwarded_error, success), ("service-access-denied", binary, denied)]:
            command = base + options
            observed = run(args.aws, command, response, directory)
            assert observed["status"] == (254 if response["status"] == 403 else 0), observed
            assert observed["peer_errors"] == [] and len(observed["requests"]) == 1, observed
            check_signature(observed["requests"][0])
            case = {"operation": "WriteGetObjectResponse", "scenario": scenario, "args": command,
                    "response": response, "aws": observed}
            if args.ess:
                case["initial_ess"] = run(args.ess, command, response, directory, ess=True)
            report["cases"].append(case)
            print(scenario, "aws=" + str(observed["status"]), "signature=verified", flush=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
