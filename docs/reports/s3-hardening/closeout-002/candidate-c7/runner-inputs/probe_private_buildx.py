"""Prepared lightweight API probe. No build, login, builder creation or cleanup.

Only a new task-private BUILDX_CONFIG becomes writable. Do not copy Docker
configuration or credentials; preserve the caller's context/socket/environment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute-after-parent-authorization", action="store_true", required=True)
    parser.add_argument("--namespace-json", type=Path, required=True)
    parser.add_argument("--physical-tmp", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        raise RuntimeError("safe unique probe label required")
    if not re.fullmatch(r"/fast/build/ess-closeout-c[0-9]+-acceptance/tmp", str(args.physical_tmp)):
        raise RuntimeError("known private closeout tmp root required")
    if args.physical_tmp.is_symlink() or not args.physical_tmp.is_dir() or args.output.exists():
        raise RuntimeError("existing real private tmp and new report path required")
    namespace = json.loads(args.namespace_json.read_text())
    if not any(namespace[index:index + 3] == ["--bind", str(args.physical_tmp), "/tmp"]
               for index in range(len(namespace))):
        raise RuntimeError("namespace does not map the supplied private tmp")
    if os.environ.get("BUILDX_CONFIG") or os.environ.get("BUILDX_BUILDER") not in (None, "", "default"):
        raise RuntimeError("unexpected preselected Buildx configuration")
    context = subprocess.check_output(["docker", "context", "show"], text=True, timeout=10).strip()
    if context != "default":
        raise RuntimeError("previously validated default Docker context required")
    private = args.physical_tmp / ("ess-buildx-probe-" + args.label)
    private.mkdir(mode=0o700, exist_ok=False)
    environment = dict(os.environ, PAGER="cat", BUILDX_CONFIG="/tmp/" + private.name)
    operations = [
        ["docker", "context", "show"],
        ["docker", "buildx", "version"],
        ["docker", "buildx", "inspect", "default"],
        ["/tmp/ess-s3-hardening-python/bin/python", "-c",
         "import os;from pathlib import Path;p=Path(os.environ['BUILDX_CONFIG'])/'activity';"
         "p.mkdir(exist_ok=True);source=p/'.tmp-owned-write-probe';"
         "source.write_text('private fixture probe');os.replace(source,p/'owned-write-probe');"
         "print('private activity atomic write passed')"],
    ]
    records = []
    for operation in operations:
        result = subprocess.run(namespace + ["--"] + operation, env=environment,
            capture_output=True, text=True, timeout=30)
        # Never emit arbitrary error text/connection details. Intended stdout is
        # only context name, plugin version, and builder name/driver.
        safe_output = None
        if result.returncode == 0:
            if operation[:3] == ["docker", "buildx", "inspect"]:
                name = re.search(r"^Name:\s+(default)\s*$", result.stdout, re.M)
                driver = re.search(r"^Driver:\s+(docker)\s*$", result.stdout, re.M)
                safe_output = "default docker" if name and driver else "unexpected builder metadata"
            else:
                safe_output = result.stdout.strip()
        records.append(dict(argv=operation, exit_status=result.returncode,
            stdout=safe_output, stderr_bytes=len(result.stderr.encode()),
            stderr_sha256=hashlib.sha256(result.stderr.encode()).hexdigest()))
        if result.returncode:
            break
    passed = (len(records) == 4 and all(item["exit_status"] == 0 for item in records)
        and records[0]["stdout"] == context and "v0.31.1" in records[1]["stdout"]
        and records[2]["stdout"] == "default docker"
        and records[3]["stdout"] == "private activity atomic write passed")
    report = dict(status="passed" if passed else "failed", exit_status=0 if passed else 1,
        acceptance=False, purpose="Buildx client-state namespace prerequisite only",
        private_buildx=str(private), operations=records, docker_context=context,
        namespace_input_sha256=hashlib.sha256(args.namespace_json.read_bytes()).hexdigest(),
        runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        docker_configuration="original read-only config/context/socket/environment retained",
        wrapper_opened_or_copied_credential_contents=False,
        credential_contents_printed_or_copied=False,
        docker_cli_reads_original_configuration_normally=True,
        builder_created_or_bootstrapped=False,
        daemon_build_or_publication_attempted=False, private_files_retained=True,
        limits="Inspect validates the selected existing Docker driver and writable private local store, not a build or acceptance")
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(json.dumps(dict(receipt=str(args.output), status=report["status"], acceptance=False)))
    raise SystemExit(report["exit_status"])


if __name__ == "__main__":
    main()
