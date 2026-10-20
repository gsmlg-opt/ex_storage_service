#!/usr/bin/env python3
"""Prepare restricted bubblewrap namespaces on ephemeral Ubuntu CI runners.

Only the distro-packaged AppArmor profile is loaded with privilege. Workloads
and the real private-tmpfs probe run as the normal runner user. Global namespace
restrictions are recorded, never changed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile

PROFILE = Path("/usr/share/apparmor/extra-profiles/bwrap-userns-restrict")
# Reviewed Noble ABI4 profile from apparmor-profiles 4.0.1really4.0.1-0ubuntu0.24.04.9.
PROFILE_SHA256 = "11d39094f044f0cda0febb3ad517b830301da6b2ce929664af09ee9e4dd264f9"
PROBE_SHA256 = hashlib.sha256(b"x" * 1_048_576).hexdigest()
CHILD_PROBE = """
import hashlib,json,os,sys
from pathlib import Path
root=Path(sys.argv[1])
path=root/'tmp/private.bin'
path.write_bytes(b'x'*1_048_576)
fs=os.statvfs(path)
print(json.dumps(dict(uid=os.geteuid(), namespace=os.readlink('/proc/self/ns/mnt'),
    device=path.stat().st_dev, capacity=fs.f_frsize*fs.f_blocks,
    bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())))
"""


def prepare_runner(run):
    if os.geteuid() == 0:
        raise RuntimeError("isolation setup and workloads must run as the unprivileged runner user")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("privileged policy setup is limited to ephemeral GitHub-hosted CI")
    release = platform.freedesktop_os_release()
    if release.get("ID") != "ubuntu" or release.get("VERSION_ID") != "24.04":
        raise RuntimeError("the packaged isolation policy is verified only for Ubuntu 24.04")
    run(["sudo", "-n", "apt-get", "update"])
    run(["sudo", "-n", "apt-get", "install", "--yes", "apparmor",
         "apparmor-profiles", "bubblewrap", "zsh"])
    if not PROFILE.is_file() or Path(shutil.which("bwrap") or "").resolve() != Path("/usr/bin/bwrap"):
        raise RuntimeError("the distro-packaged bwrap and restricted AppArmor profile are required")
    if hashlib.sha256(PROFILE.read_bytes()).hexdigest() != PROFILE_SHA256:
        raise RuntimeError("the packaged AppArmor profile changed; inspect it before admitting this runner")
    if Path("/sys/module/apparmor/parameters/enabled").read_text().strip() != "Y":
        raise RuntimeError("AppArmor must be enabled for this runner's isolation policy")
    run(["uname", "-a"])
    run(["dpkg-query", "-W", "apparmor", "apparmor-profiles", "bubblewrap", "zsh"])
    run(["dpkg-query", "-S", str(PROFILE), "/usr/bin/bwrap"])
    run(["sha256sum", str(PROFILE), "/usr/bin/bwrap"])
    run(["sudo", "-n", "apparmor_parser", "--version"])
    run(["sudo", "-n", "apparmor_parser", "-r", str(PROFILE)])
    run(["sudo", "-n", "aa-status"])


def probe_private_tmpfs(root):
    if os.geteuid() == 0:
        raise RuntimeError("private-tmpfs acceptance must run unprivileged")
    tmp = root / "tmp"
    tmp.mkdir(exist_ok=True)
    host = dict(uid=os.geteuid(), namespace=os.readlink("/proc/self/ns/mnt"), device=tmp.stat().st_dev)
    argv = ["bwrap", "--die-with-parent", "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc",
            "--bind", str(root), str(root), "--size", "8388608", "--tmpfs", str(tmp),
            sys.executable, "-c", CHILD_PROBE, str(root)]
    completed = subprocess.run(argv, capture_output=True, text=True, check=True, timeout=15)
    value = json.loads(completed.stdout)
    if (value.get("uid") != host["uid"] or value.get("namespace") == host["namespace"]
            or value.get("device") == host["device"] or value.get("capacity") != 8_388_608
            or value.get("bytes") != 1_048_576 or value.get("sha256") != PROBE_SHA256
            or list(tmp.iterdir())):
        raise RuntimeError(f"private-tmpfs isolation/content verification failed: {value}, host={host}")
    return dict(value, host=host, command=argv[:argv.index("-c")])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = dict(status="in_progress", uid=os.geteuid(), commands=[], namespace_policy={})
    for name in ("apparmor_restrict_unprivileged_userns", "unprivileged_userns_clone"):
        path = Path("/proc/sys/kernel") / name
        try:
            receipt["namespace_policy"][name] = path.read_text().strip()
        except OSError as error:
            receipt["namespace_policy"][name] = f"unavailable: {error}"
    context = Path("/proc/self/attr/current")
    try:
        receipt["apparmor_context"] = context.read_text().strip()
    except OSError as error:
        receipt["apparmor_context"] = f"unavailable: {error}"

    def run(argv):
        print(shlex.join(argv), flush=True)
        completed = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, timeout=600)
        with (args.output / "setup.log").open("a") as log:
            log.write(shlex.join(argv) + "\n" + completed.stdout + "\n")
        receipt["commands"].append(dict(command=argv, exit_status=completed.returncode))
        if completed.returncode:
            raise subprocess.CalledProcessError(completed.returncode, argv, completed.stdout)

    try:
        prepare_runner(run)
        with tempfile.TemporaryDirectory(prefix="ess-ci-isolation-") as directory:
            receipt["private_tmpfs"] = probe_private_tmpfs(Path(directory))
        receipt["status"] = "passed"
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        receipt.update(status="failed", error=str(error))
        if isinstance(error, subprocess.CalledProcessError):
            receipt.update(command_output=error.stdout, command_stderr=error.stderr)
        if receipt["commands"]:
            try:
                run(["sudo", "-n", "journalctl", "--dmesg", "--no-pager", "--lines=100",
                     "--since=-5min", "--grep=bwrap|userns"])
            except (OSError, subprocess.SubprocessError) as diagnostic:
                receipt["kernel_diagnostic_error"] = str(diagnostic)
        print(f"isolation prerequisite failed: {error}", file=sys.stderr)
        return 1
    finally:
        (args.output / "isolation.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    sys.exit(main())
