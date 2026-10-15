#!/usr/bin/env python3
"""Capture the pinned mc address-pinning contract using a loopback-only peer."""

import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading


def capture(binary):
    requests = []

    class Peer(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def respond(self):
            requests.append({"method": self.command, "target": self.path,
                             "host": self.headers["Host"].replace(str(self.server.server_port), "PORT")})
            body = (
                b'<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'
                if "location" in self.path else
                b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b'<Name>bucket</Name><Prefix>key</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>'
                b'<IsTruncated>false</IsTruncated><Contents><Key>key</Key>'
                b'<LastModified>2025-10-14T00:00:00Z</LastModified><ETag>"fixture-etag"</ETag>'
                b'<Size>3</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>'
            )
            self.send_response(200)
            self.send_header("Content-Length", "3" if self.command == "HEAD" else str(len(body)))
            self.send_header("ETag", '"fixture-etag"')
            self.send_header("Last-Modified", "Tue, 14 Oct 2025 00:00:00 GMT")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_GET = respond
        do_HEAD = respond

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_port
    cases = []

    try:
        with tempfile.TemporaryDirectory(prefix="ess-mc-resolve-") as directory:
            env = dict(os.environ, MC_HOST_fixture=f"http://fixture-access:fixture-secret@pin.invalid:{port}",
                       NO_PROXY="*", no_proxy="*")
            for label, flags in [
                ("exact-authority", [f"pin.invalid:{port}=127.0.0.1"]),
                ("last-entry-wins", [f"pin.invalid:{port}=127.0.0.2", f"pin.invalid:{port}=127.0.0.1"]),
            ]:
                requests.clear()
                argv = [str(binary), "--config-dir", directory, "--json", "--no-color"]
                for flag in flags:
                    argv.extend(["--resolve", flag])
                argv.extend(["stat", "fixture/bucket/key"])
                result = subprocess.run(argv, env=env, capture_output=True, timeout=15)
                assert result.returncode == 0, ((result.stdout + result.stderr).decode("utf-8", "replace"), requests)
                assert requests and all(row["host"] == "pin.invalid:PORT" for row in requests)
                assert any(row["method"] == "GET" and "prefix=key" in row["target"] for row in requests)
                cases.append({"id": label, "resolve": [flag.replace(str(port), "PORT") for flag in flags],
                              "exit_status": result.returncode, "wire": list(requests)})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    return {"reference": "minio/mc@77f82e18b5401a65958f1619df6ebb994634bd88",
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mc", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(args.mc.read_bytes()).hexdigest() == "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"
    args.output.write_text(json.dumps(capture(args.mc), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
