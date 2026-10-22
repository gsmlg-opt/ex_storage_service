"""Preserve Python/Port dispatch, tracing only both fresh data-node observers."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

from common import alive, identity, write_json

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
DATA_NODES = {"data-a", "data-b"}
SYSCALLS = ("read,write,readv,writev,pread64,pwrite64,openat,openat2,close,newfstatat,"
            "statx,fstat,getdents64,lseek,fsync,fdatasync,statfs,fstatfs,futex,"
            "pselect6,select,poll,ppoll,clock_nanosleep,nanosleep")


def observer_target(args):
    normalized = args[1:] if args[:1] == ["-B"] else args
    if not normalized or Path(normalized[0]).resolve() != REPO / "e2e/scripts/resource_observer.py":
        return None
    if len(normalized) < 2:
        raise ValueError("missing observer action")
    action = normalized[1]
    if len(normalized[2:]) % 2:
        raise ValueError("malformed observer flags")
    flags = dict(zip(normalized[2::2], normalized[3::2]))
    sample_root = Path(flags.get("--root", "/invalid"))
    if sample_root.name not in DATA_NODES:
        return None
    cluster = sample_root.parent.parent
    if (sample_root.parent.name != "resource-samples" or
            re.fullmatch(r"/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}", str(cluster)) is None
            or not cluster.is_dir() or cluster.is_symlink()):
        raise ValueError("fresh original cluster root required")
    if action == "sample":
        if (len(normalized[2:]) != 10 or
                set(flags) != {"--root", "--tmp", "--data", "--beam-pid", "--generation"}):
            raise ValueError("unexpected sample observer argv")
        if (flags["--data"] != str(cluster / sample_root.name) or
                flags["--tmp"] != str(cluster / sample_root.name / "tmp")):
            raise ValueError("original data-node storage paths required")
    elif action != "verify-stopped" or len(normalized[2:]) != 2 or set(flags) != {"--root"}:
        raise ValueError("unexpected observer action")
    return normalized, flags, sample_root.name, cluster


def trace_command(strace, fifo, python, observer, args):
    # -D preserves BEAM's original Port PID and pipes. No pathname, buffer,
    # protocol, environment or credential values are decoded by raw=all.
    return [strace, "-D", "-f", "-I", "1", "-qq", "-ttt", "-T",
            "-e", "trace=" + SYSCALLS, "-e", "raw=all", "-e", "signal=none", "-o", str(fifo),
            python, "-B", str(HERE / "instrument_observer.py"), str(observer), *args]


def main():
    python = os.environ["ESS_DIAGNOSTIC_PINNED_PYTHON"]
    evidence = Path(os.environ["ESS_DIAGNOSTIC_EVIDENCE"])
    args = sys.argv[1:]
    normalized = args[1:] if args[:1] == ["-B"] else args
    if normalized and Path(normalized[0]).resolve() == REPO / "e2e/s3_hardening_cluster_resources.py":
        os.execv(python, [python, "-B", str(HERE / "subset.py"), *normalized])
    target = observer_target(args)
    if target is None:
        os.execv(python, [python, *args])
    normalized, flags, node, cluster = target
    diagnostic = cluster / ("observer-diagnostic-" + node)
    if normalized[1] == "verify-stopped":
        completed = subprocess.run([python, *args])
        if completed.returncode:
            raise SystemExit(completed.returncode)
        deadline = time.monotonic() + 12
        terminal = diagnostic / "monitor-terminal.json"
        while not terminal.is_file() and time.monotonic() < deadline:
            time.sleep(.025)
        receipt = json.loads(terminal.read_text())
        monitor = json.loads((diagnostic / "monitor-ready.json").read_text())["monitor"]
        deadline = time.monotonic() + 2
        while alive(monitor) and time.monotonic() < deadline:
            time.sleep(.025)
        shutil.copytree(diagnostic, evidence / diagnostic.name,
                        ignore=shutil.ignore_patterns("*.fifo", "*.sock"))
        if alive(monitor) or receipt["status"] != "complete":
            raise RuntimeError("diagnostic trace/scheduler coverage or monitor cleanup incomplete")
        return
    observer = REPO / "e2e/scripts/resource_observer.py"
    diagnostic.mkdir(mode=0o700)
    fifo = diagnostic / "syscalls.fifo"
    os.mkfifo(fifo, 0o600)
    launch = dict(observer=identity(os.getpid()), beam=identity(int(flags["--beam-pid"])),
                  generation=flags["--generation"], original_argv=[python, *args],
                  original_source_sha256=hashlib.sha256(observer.read_bytes()).hexdigest())
    write_json(diagnostic / "launch.json", launch)
    write_json(evidence / (node + "-launch.json"),
               dict(root=str(cluster), diagnostic=str(diagnostic), **launch))
    with (diagnostic / "monitor.log").open("xb") as log:
        subprocess.Popen([python, "-B", str(HERE / "monitor.py"), str(diagnostic)],
                         stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, close_fds=True)
    deadline = time.monotonic() + 5
    while not (diagnostic / "monitor-ready.json").is_file():
        if time.monotonic() >= deadline:
            raise RuntimeError("diagnostic monitor readiness timeout")
        time.sleep(.005)
    strace = shutil.which("strace")
    if strace is None:
        raise RuntimeError("strace unavailable; do not change ptrace policy")
    os.environ["ESS_OBSERVER_DIAGNOSTIC_ROOT"] = str(diagnostic)
    argv = trace_command(strace, fifo, python, observer, normalized[1:])
    (diagnostic / "strace-command.json").write_text(json.dumps(argv) + "\n")
    os.execv(strace, argv)


if __name__ == "__main__":
    main()
