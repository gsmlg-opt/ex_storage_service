"""Capture the pinned AWS per-file recursive-move deletion behavior on loopback."""

import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading


EXPECTED_SHA256 = "87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a"


def capture(binary):
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if digest != EXPECTED_SHA256:
        raise SystemExit("Expected the locked AWS CLI 2.31.18 Linux executable")

    with tempfile.TemporaryDirectory(prefix="ess-recursive-move-aws-") as temporary:
        root = Path(temporary)
        source = root / "src"
        source.mkdir()
        for name in ["a", "b"]:
            (source / name).write_bytes(b"raw")
        config = root / "config"
        config.write_text(
            "[default]\nregion = us-east-1\ns3 =\n    max_concurrent_requests = 1\n"
        )
        operations = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_PUT(self):
                self.rfile.read(int(self.headers.get("content-length", 0)))
                operations.append(
                    {
                        "method": "PUT",
                        "path": self.path,
                        "sources_present": [(source / name).exists() for name in ["a", "b"]],
                    }
                )
                failed = self.path.endswith("/b")
                body = (
                    b"<Error><Code>AccessDenied</Code><Message>fixture denial</Message></Error>"
                    if failed else b""
                )
                self.send_response(403 if failed else 200)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("ETag", '"uploaded"')
                self.end_headers()
                self.wfile.write(body)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        env = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
        env.update(
            {
                "AWS_CONFIG_FILE": str(config),
                "AWS_SHARED_CREDENTIALS_FILE": str(root / "empty"),
                "AWS_ACCESS_KEY_ID": "fixture",
                "AWS_SECRET_ACCESS_KEY": "fixture-secret",
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_MAX_ATTEMPTS": "1",
                "AWS_PAGER": "",
                "PAGER": "cat",
            }
        )
        try:
            result = subprocess.run(
                [str(binary), "s3", "mv", str(source), "s3://bucket/dest/", "--recursive",
                 "--endpoint-url", f"http://127.0.0.1:{server.server_port}", "--no-progress"],
                env=env, capture_output=True, timeout=30,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        observed = {
            "reference": "AWS CLI 2.31.18",
            "binary_sha256": digest,
            "options": ["s3", "mv", "SOURCE", "s3://bucket/dest/", "--recursive", "--no-progress"],
            "max_concurrent_requests": 1,
            "exit_code": result.returncode,
            "operations": operations,
            "sources_remaining": [(source / name).exists() for name in ["a", "b"]],
        }
        if result.returncode != 1 or observed["sources_remaining"] != [False, True]:
            raise SystemExit("Pinned AWS recursive move did not produce the expected fixture")
        return observed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(capture(args.aws.resolve()), indent=2) + "\n")
