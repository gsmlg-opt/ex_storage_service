#!/usr/bin/env python3
"""Record pinned mc global-header behavior using only a loopback S3 peer."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading

MC_SHA256 = "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mc", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.mc).read_bytes()).hexdigest() == MC_SHA256
    requests = []

    class Peer(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            authorization = self.headers.get("Authorization", "")
            requests.append({"method": self.command, "target": self.path,
                             "x_trace": self.headers.get_all("X-Trace", []),
                             "signed_headers": authorization.split("SignedHeaders=")[-1].split(",")[0] if "SignedHeaders=" in authorization else None})
            body = (b'<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'
                    if "location" in self.path else
                    b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix/><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>')
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="ess-native-globals-") as directory:
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("MC_", "AWS_", "ESS_")) and "proxy" not in key.lower()}
            env.update(MC_HOST_fixture=f"http://fixture-access:fixture-secret@127.0.0.1:{server.server_port}",
                       MC_DISABLE_PAGER="true", NO_PROXY="*", PAGER="cat")
            common = ["-C", directory, "--dp", "--insecure"]
            command = common + ["--custom-header", "X-Trace:first:part", "--custom-header", "x-trace:second", "ls", "fixture/bucket"]
            result = subprocess.run([args.mc] + command, env=env, capture_output=True, text=True, timeout=20)
            assert result.returncode == 0 and requests, (result.stdout, result.stderr)
            assert all(request["x_trace"] == ["first:part", "second"] for request in requests)
            assert all("x-trace" not in request["signed_headers"] for request in requests)
            cases = [{"name": "repeated-header", "args": command[2:], "status": result.returncode,
                      "stdout": result.stdout, "stderr": result.stderr, "requests": list(requests)}]
            for value in ["missing-colon", "bad name:value", "x-test:a\r\ninjected:yes"]:
                requests.clear()
                result = subprocess.run([args.mc] + common + ["-H", value, "ls", "fixture/bucket"],
                                        env=env, capture_output=True, text=True, timeout=20)
                # Record the executable outcome, including any requests. The
                # reference ignores setGlobalsFromContext's returned error.
                cases.append({"name": "invalid-header", "header": value, "status": result.returncode,
                              "stdout": result.stdout, "stderr": result.stderr,
                              "requests": list(requests)})
            args.output.write_text(json.dumps({"mc_sha256": MC_SHA256, "cases": cases}, indent=2) + "\n")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


if __name__ == "__main__":
    main()
