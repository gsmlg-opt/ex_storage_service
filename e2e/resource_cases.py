"""Explicit resource case coverage and read-only runner provenance."""
from collections import Counter
import os
from pathlib import Path
import platform
import re
import subprocess
import time

MIB = 1024 * 1024
SIZES = (64 * MIB, 1024 * MIB)
CONCURRENCIES = (1, 4)
LOCAL = ("local-put", "local-get", "local-copy", "local-multipart", "local-upload-part-copy",
         "local-slow-put", "local-slow-get", "gateway-put", "gateway-cold-get",
         "gateway-copy-to-local", "gateway-hit-slow-get", "gateway-slow-put",
         "gateway-paced-upstream-put", "gateway-paced-upstream-cold-get")
CLUSTER = ("strict-quorum-replica-put", "authenticated-peer-get", "fenced-repair-pull",
           "fenced-repair-push", "cross-cluster-worker-paced-sink")


def manifest(mode):
    names = {"all": LOCAL, "upstream": LOCAL[-2:], "cluster": CLUSTER}[mode]
    return [dict(name=name, size=size, concurrency=concurrency) for size in SIZES
            for concurrency in CONCURRENCIES for name in names]


def verify_coverage(groups, expected):
    def identity(row):
        return row["name"], row["size"], row["concurrency"]
    if Counter(map(identity, groups)) != Counter(map(identity, expected)):
        raise AssertionError("resource case coverage missing, duplicated or unexpected")
    if any(row.get("failures") for row in groups):
        raise AssertionError("failed resource case cannot certify complete coverage")


def runner_details(root):
    """Record limits before measured work; do not change host controls."""
    root = Path(root)
    result = dict(uname=list(platform.uname()), cpu_count=os.cpu_count(),
                  affinity=sorted(os.sched_getaffinity(0)), load=os.getloadavg(),
                  monotonic_clock=vars(time.get_clock_info("monotonic")),
                  disk=dict(device=os.stat(root).st_dev, path=str(root)))
    filesystem = os.statvfs(root)
    result["disk"].update(block_bytes=filesystem.f_frsize,
                          available_bytes=filesystem.f_bavail * filesystem.f_frsize)
    for name in ("meminfo", "pressure/cpu", "pressure/io", "pressure/memory", "self/cgroup"):
        path = Path("/proc") / name
        result[name] = path.read_text() if path.exists() else "unavailable"
    result["cgroups"] = {}
    for line in result["self/cgroup"].splitlines():
        if line.startswith("0::"):
            group = Path("/sys/fs/cgroup") / line[3:].lstrip("/")
            while group.is_relative_to("/sys/fs/cgroup"):
                result["cgroups"][str(group)] = {name: (group / name).read_text().strip()
                    for name in ("cpu.max", "memory.max", "cpuset.cpus.effective", "io.max")
                    if (group / name).exists()}
                group = group.parent
    lock = Path("mix.lock").read_text()
    result["dependencies"] = dict(re.findall(
        r'"(concord|ex_turso|viewstamped_replication|http_fetch)": \{:hex, :\w+, "([^"]+)"', lock))
    result["python"] = platform.python_version()
    result["client_checksum_environment"] = {name: os.environ.get(name)
        for name in ("AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION")}
    result["toolchain"] = subprocess.check_output(["elixir", "--version"], text=True,
        env=dict(os.environ, ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2"), timeout=15)
    return result
