#!/usr/bin/env python3
"""Build and execute an isolated consumer of the actual CLI Hex source package."""

import argparse
import base64
import hashlib
import http.server
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import select
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import zipfile

from hex_package import PACKAGE_LIMIT, read_package


ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "apps/ex_storage_service_cli"
ESS_LICENSE = (CLI / "LICENSE").read_bytes()
REQUIRED = (
    "mix.exs", "mix/compile.ess_signals.exs", "c_src/ess_signal.c", "c_src/ess_pager.c", "LICENSE",
    "priv/compatibility/NOTICE.txt", "priv/compatibility/NOTICE-YAML.txt",
    "priv/compatibility/NOTICE-QUERY.txt",
    "priv/compatibility/LICENSE-ESS.txt",
    "priv/compatibility/aws/service-2.json",
    "priv/compatibility/aws/endpoint-rule-set-1.json",
    "priv/compatibility/aws/paginators-1.json",
    "priv/compatibility/aws/waiters-2.json",
)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(argv, cwd, env, logs, timeout=900):
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, timeout=timeout)
    logs.append({"argv": [str(item) for item in argv], "status": result.returncode,
                 "stdout": result.stdout.decode("utf-8", "replace"),
                 "stderr": result.stderr.decode("utf-8", "replace")})
    if result.returncode:
        raise RuntimeError(f"{argv[0]} {argv[1]} failed: " + logs[-1]["stdout"] + logs[-1]["stderr"])
    return result.stdout


def run_pty(argv, cwd, env, logs, timeout=20, max_output=1_048_576):
    import fcntl
    import pty
    import struct
    import termios

    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    # Establish the controlling terminal after exec, rather than using a
    # preexec_fn while the HTTP fixture's Python thread is running.
    wrapper = ("import fcntl,os,sys,termios; "
               "fcntl.ioctl(0,termios.TIOCSCTTY,0); "
               "os.execvpe(sys.argv[1],sys.argv[1:],os.environ)")
    child = None
    output = bytearray()
    baseline = termios.tcgetattr(slave)
    try:
        child = subprocess.Popen([sys.executable, "-c", wrapper, *map(str, argv)],
                                 cwd=cwd, env=env, stdin=slave, stdout=slave, stderr=slave,
                                 start_new_session=True)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready = select.select([master], [], [], 0.05)[0]
            if ready:
                chunk = os.read(master, min(65_536, max_output + 1 - len(output)))
                output.extend(chunk)
                if len(output) > max_output:
                    raise RuntimeError("Installed pager PTY output exceeded its budget")
            elif child.poll() is not None:
                if termios.tcgetattr(slave) != baseline:
                    raise RuntimeError("Installed pager did not restore terminal settings")
                if child.returncode:
                    raise RuntimeError("Installed pager command failed: " + output.decode("utf-8", "replace"))
                return bytes(output).replace(b"\r\n", b"\n")
        raise RuntimeError("Installed pager PTY command exceeded its deadline")
    finally:
        try:
            if child is not None:
                if child.poll() is None:
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        child.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait(timeout=3)
                logs.append({"argv": list(map(str, argv)), "pty": True,
                             "status": child.returncode, "stdout": output.decode("utf-8", "replace")})
        finally:
            os.close(master)
            os.close(slave)


def check_installed_pager(installed, cwd, env, logs, output, endpoint):
    pager = output / "fixture pager.py"
    record = output / "pager arguments.json"
    temporary = output / "pager-tmp"
    temporary.mkdir(mode=0o700)
    pager.write_text('''import base64,json,os,sys,termios
from pathlib import Path
with open('/dev/tty', 'r+b', buffering=0) as terminal:
    assert os.tcgetpgrp(terminal.fileno()) == os.getpgrp()
    attributes = termios.tcgetattr(terminal.fileno())
    attributes[3] &= ~termios.ECHO
    termios.tcsetattr(terminal.fileno(), termios.TCSANOW, attributes)
data = sys.stdin.buffer.read(1048577)
assert len(data) <= 1048576
Path(sys.argv[1]).write_text(json.dumps({'argv': sys.argv[2:], 'stdin': base64.b64encode(data).decode()}))
sys.stdout.buffer.write(data)
''')
    literal = "literal $(not-a-command); *"
    pager_env = dict(env, AWS_PAGER=shlex.join([sys.executable, str(pager), str(record), literal]),
                     TMPDIR=str(temporary), ERL_FLAGS="+S 2:2", TERM="xterm")
    stdout = run_pty([str(installed), "--no-sign-request", "--region", "us-east-1",
                      "--endpoint-url", endpoint, "s3api", "list-buckets", "--output", "json"],
                     cwd, pager_env, logs)
    captured = json.loads(record.read_text())
    if captured["argv"] != [literal]:
        raise RuntimeError("Installed pager did not preserve literal arguments")
    expected = base64.b64decode(captured["stdin"], validate=True)
    if stdout != expected or json.loads(stdout)["Buckets"][0]["Name"] != "fixture-雪":
        raise RuntimeError("Installed pager changed the modeled output bytes")
    if list(temporary.iterdir()):
        raise RuntimeError("Installed pager left native extraction or spool files")


def check_installed_credential_process(installed, cwd, env, logs):
    stdout = run([
        "elixir", str(ROOT / "scripts/cli_compatibility/credential_process_probe.exs"),
        str(installed), str(CLI / "test/ex_storage_service_cli/credentials/process_test.exs"),
    ], cwd, env, logs, timeout=120)
    prefix = "ESS_CREDENTIAL_PROCESS_PROBE="
    records = [line[len(prefix):] for line in stdout.decode().splitlines() if line.startswith(prefix)]
    if len(records) != 1:
        raise RuntimeError("Installed credential process probe omitted its evidence")
    evidence = json.loads(records[0])
    required = {"literal-argv", "stderr-discard", "bounded-output", "child-group-isolation",
                "timeout-descendant-cleanup", "owner-death-descendant-cleanup",
                "overflow-descendant-cleanup"}
    if (evidence.get("module_source") != "installed-escript" or
            evidence.get("os") != platform.system().lower() or
            evidence.get("total", 0) < 10 or evidence.get("failures") != 0 or
            evidence.get("skipped") != 0 or evidence.get("excluded") != 0 or
            set(evidence.get("checks", [])) != required):
        raise RuntimeError("Installed credential process checks did not all pass")
    diagnostics = stdout.decode() + logs[-1]["stderr"]
    if any(secret in diagnostics for secret in ("stderr-secret", "private diagnostic")):
        raise RuntimeError("Installed credential process leaked fixture diagnostics")
    return evidence


def extract_package(archive, destination):
    checksum, contents = read_package(archive)
    total = 0
    with tarfile.open(fileobj=io.BytesIO(contents), mode="r:gz") as inner:
        for member in inner:
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts or member.issym() or member.islnk():
                raise RuntimeError("Unsafe Hex source archive entry")
            target = destination.joinpath(*name.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                total += member.size
                if total > PACKAGE_LIMIT:
                    raise RuntimeError("Expanded source package exceeds the fixture budget")
                target.parent.mkdir(parents=True, exist_ok=True)
                with inner.extractfile(member) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output)
            else:
                raise RuntimeError("Unexpected Hex source archive entry")
    for name in REQUIRED:
        if not (destination / name).is_file():
            raise RuntimeError(f"Missing packaged asset: {name}")
    source_license = (destination / "LICENSE").read_bytes()
    if source_license != ESS_LICENSE or (destination / "priv/compatibility/LICENSE-ESS.txt").read_bytes() != source_license:
        raise RuntimeError("Packaged ESS license notice differs from the source license")
    catalog = json.loads((destination / "priv/compatibility/aws/service-2.json").read_text())
    if len(catalog["operations"]) != 108:
        raise RuntimeError("Packaged operation catalog does not match the pinned reference")
    return checksum


def inspect_escript(path):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        licenses = [name for name in names if name.endswith("/priv/compatibility/LICENSE-ESS.txt")]
        if len(licenses) != 1 or archive.read(licenses[0]) != ESS_LICENSE:
            raise RuntimeError("Missing or incorrect ESS license notice in escript")
        for suffix in (
            "/priv/compatibility/aws/service-2.json", "/priv/compatibility/NOTICE-YAML.txt",
            "/priv/compatibility/NOTICE-QUERY.txt", "/priv/native/ess_signals.so",
            "/priv/native/ess_signals.json",
            "/priv/native/ess_pager", "/priv/native/ess_pager.json",
        ):
            if not any(name.endswith(suffix) for name in names):
                raise RuntimeError(f"Missing escript asset: {suffix}")
        forbidden = ("ex_storage_service", "ex_storage_service_cluster", "ex_storage_service_s3",
                     "ex_storage_service_web", "phoenix", "concord", "ecto")
        roots = {name.split("/", 1)[0] for name in names}
        if any(root == app or root.startswith(app + "-")
               for root in roots for app in forbidden):
            raise RuntimeError("Server application leaked into the standalone escript")
    return len(names)


class WireFixture(http.server.BaseHTTPRequestHandler):
    requests = []
    raw = bytes(range(256)) * 257

    def do_GET(self):
        self.requests.append(self.path)
        if self.path == "/bucket/raw":
            self.send_response(200)
            self.send_header("Content-Length", str(len(self.raw)))
            self.end_headers()
            self.wfile.write(self.raw)
            return
        body = ("<ListAllMyBucketsResult><Buckets><Bucket><Name>fixture-雪</Name>"
                "<CreationDate>2026-01-01T00:00:00Z</CreationDate></Bucket></Buckets>"
                "</ListAllMyBucketsResult>").encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--package", type=Path, help="Use a previously built/downloaded Hex tar")
    parser.add_argument("--package-url", help="Verify the actual public release tar instead")
    parser.add_argument("--expected-version")
    args = parser.parse_args()
    if args.package and args.package_url:
        parser.error("Choose --package or --package-url")
    output = (args.output_dir or Path(tempfile.mkdtemp(prefix="ess-package-probe-"))).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("--output-dir must be empty")
    output.chmod(0o700)
    logs = []
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("AWS_", "ESS_"))}
    env.update(PAGER="cat", MIX_ENV="prod", AWS_EC2_METADATA_DISABLED="true",
               AWS_CONFIG_FILE=str(output / "absent-aws-config"),
               AWS_SHARED_CREDENTIALS_FILE=str(output / "absent-aws-credentials"),
               ESS_CONFIG_DIR=str(output / "config"))
    archive = output / "ex_storage_service_cli.tar"
    try:
        source_sha = run(["git", "rev-parse", "HEAD"], ROOT, env, logs).decode().strip()
        source_dirty = bool(run(["git", "status", "--porcelain"], ROOT, env, []).strip())
        if args.package_url:
            if not re.fullmatch(r"https://repo\.hex\.pm/tarballs/ex_storage_service_cli-[A-Za-z0-9.+-]+\.tar",
                                args.package_url):
                parser.error("--package-url must name the CLI tarball on repo.hex.pm")
            with urllib.request.urlopen(args.package_url, timeout=60) as response:
                data = response.read(PACKAGE_LIMIT + 1)
            if len(data) > PACKAGE_LIMIT:
                raise RuntimeError("Downloaded package exceeds its budget")
            archive.write_bytes(data)
        elif args.package:
            shutil.copyfile(args.package, archive)
        else:
            run(["mix", "hex.build", "--output", str(archive)], CLI, env, logs)

        package = output / "extracted/package"
        package.mkdir(parents=True)
        checksum = extract_package(archive, package)
        version = re.search(r'@version\s+"([^"]+)"', (package / "mix.exs").read_text()).group(1)
        if args.expected_version and version != args.expected_version:
            raise RuntimeError("Package version differs from the requested release")
        env.update(MIX_BUILD_PATH=str(output / "consumer-build"),
                   MIX_DEPS_PATH=str(output / "consumer-deps"))
        run(["mix", "deps.get", "--only", "prod"], package, env, logs)
        run(["mix", "compile", "--warnings-as-errors"], package, env, logs)
        run(["mix", "escript.build"], package, env, logs)
        binary = output / "ess"
        shutil.copy2(package / "ess", binary)
        entries = inspect_escript(binary)

        install_env = dict(env, MIX_HOME=str(output / "install-home"))
        run(["mix", "escript.install", str(binary), "--force"], package, install_env, logs)
        installed = output / "install-home/escripts/ess"
        package.rename(output / "source-unavailable")
        (output / "consumer-build").rename(output / "build-unavailable")
        (output / "consumer-deps").rename(output / "deps-unavailable")
        unrelated = output / "unrelated"
        unrelated.mkdir()
        reported = run([str(installed), "version"], unrelated, env, logs).decode().strip()
        if reported != f"ess version {version}":
            raise RuntimeError("Installed executable reports the wrong version")
        run([str(installed), "s3api", "get-object", "--help"], unrelated, env, logs)
        skeleton = run([str(installed), "s3api", "get-object", "--generate-cli-skeleton", "input"],
                       unrelated, env, logs)
        if not {"Bucket", "Key"} <= json.loads(skeleton).keys():
            raise RuntimeError("Installed model skeleton is incomplete")

        credential_process = check_installed_credential_process(installed, unrelated, env, logs)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), WireFixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = run([str(installed), "--no-sign-request", "--region", "us-east-1",
                          "--endpoint-url", f"http://127.0.0.1:{server.server_port}",
                          "--no-cli-pager", "s3api", "list-buckets", "--output", "json"],
                         unrelated, env, logs)
            if json.loads(result)["Buckets"][0]["Name"] != "fixture-雪" or len(WireFixture.requests) != 1:
                raise RuntimeError("Installed artifact failed the real HTTP/model/native probe")
            raw = run([str(installed), "--no-sign-request", "--region", "us-east-1",
                       "--endpoint", f"http://127.0.0.1:{server.server_port}",
                       "cat", "s3://bucket/raw"], unrelated, env, logs)
            if raw != WireFixture.raw:
                raise RuntimeError("Installed artifact changed binary stdout bytes")
            if platform.system() == "Linux":
                check_installed_pager(installed, unrelated, env, logs, output,
                                      f"http://127.0.0.1:{server.server_port}")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        manifest = {
            "version": version,
            "source_sha": source_sha if not args.package and not args.package_url else None,
            "source_dirty": source_dirty,
            "probe_source_sha": source_sha,
            "package_sha256": digest(archive), "hex_inner_checksum": checksum,
            "escript_sha256": digest(binary), "escript_entries": entries,
            "os": platform.system(), "architecture": platform.machine(),
            "runtime": run(["elixir", "--version"], unrelated, env, logs).decode().strip(),
            "package_source": args.package_url or (str(args.package) if args.package else "local-source-tar"),
            "credential_process": credential_process,
            "checks": ["source-assets", "published-dependencies", "strict-compile", "escript-assets",
                       "local-install", "source-independent-runtime", "model-skeleton", "actual-http-unicode",
                       "actual-http-binary-stdout", "hex-payload-integrity",
                       "installed-credential-process"],
        }
        if platform.system() == "Linux":
            manifest["checks"].append("installed-pager-pty")
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"status": "passed", "output": str(output), **manifest}))
    finally:
        (output / "commands.json").write_text(json.dumps(logs, indent=2) + "\n")


if __name__ == "__main__":
    main()
