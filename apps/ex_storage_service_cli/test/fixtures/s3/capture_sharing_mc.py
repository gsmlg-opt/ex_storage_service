#!/usr/bin/env python3
"""Capture three native share contracts from the locked mc binary using only loopback.

python3 capture_sharing_mc.py --mc /path/to/locked/mc --output /tmp/share-capture.json
The caller must choose an output path; the checked-in fixture is never implicit.
"""
import argparse
import base64
import datetime
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import urllib.parse

REVISION = "77f82e18b5401a65958f1619df6ebb994634bd88"
BINARY_SHA256 = "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"
TARGET = "fixture/bucket/folder/a"
CASES = [
    ("download", ["--expire", "1s", "--expire", "2s"]),
    ("download", ["--version-id", "v +/雪", "--expire", "2s"]),
    ("upload", ["--recursive", "--expire", "2s"]),
]


class Fixture(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        body = b"<LocationConstraint>us-east-1</LocationConstraint>"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "1")
        self.send_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
        self.send_header("ETag", '\"fixture\"')
        self.end_headers()


def capture(binary):
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if digest != BINARY_SHA256:
        raise SystemExit("mc executable hash does not match the locked reference")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    captures = []
    try:
        with tempfile.TemporaryDirectory(prefix="ess-share-mc-") as config:
            environment = {
                key: value for key, value in os.environ.items()
                if not key.startswith(("MC_", "AWS_"))
            }
            environment["MC_HOST_fixture"] = (
                f"http://fixture-access:fixture-secret@127.0.0.1:{server.server_port}"
            )
            for action, options in CASES:
                result = subprocess.run(
                    [str(binary), "--config-dir", config, "--json", "share", action,
                     *options, TARGET],
                    env=environment, capture_output=True, text=True, timeout=15,
                )
                if result.returncode:
                    raise SystemExit(f"mc {action} capture failed; capability output omitted")
                observed = json.loads(result.stdout)
                if observed.get("status") != "success":
                    raise SystemExit(f"mc {action} did not return success")
                case = {"action": action, "options": options, "target": TARGET,
                        "exit_code": result.returncode, "status": observed["status"]}
                if action == "download":
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(observed["share"]).query)
                    case["query"] = {"X-Amz-Expires": query["X-Amz-Expires"][0]}
                    if "versionId" in query:
                        case["query"]["versionId"] = query["versionId"][0]
                else:
                    encoded = observed["share"].split("policy=", 1)[1].split(" ", 1)[0]
                    conditions = json.loads(base64.b64decode(encoded))["conditions"]
                    case["policy_condition"] = next(
                        condition for condition in conditions
                        if condition[:2] == ["starts-with", "$key"]
                    )
                captures.append(case)
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
    return {
        "reference": "mc", "source_revision": REVISION, "executable_sha256": digest,
        "captured_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": "Linux amd64; Python stdlib loopback HTTP fixture; private temporary config; MC_HOST_fixture uses fixture-access/fixture-secret only",
        "normalization": "Extracted semantic fields from actual JSON success output; removed loopback port, signing timestamp and signature. No real credentials or provider used.",
        "cases": captures,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mc", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    result = capture(arguments.mc.resolve(strict=True))
    arguments.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"Captured {len(result['cases'])} locked mc share cases")


if __name__ == "__main__":
    main()
