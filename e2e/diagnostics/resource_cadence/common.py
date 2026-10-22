"""Diagnostic-only Linux snapshots. No production module imports."""
import os
import json
import resource
from pathlib import Path
import time

MODES = ("probe-only", "syscall-trace")


def diagnostic_mode(value=None):
    mode = os.environ.get("ESS_DIAGNOSTIC_MODE", "probe-only") if value is None else value
    if mode not in MODES:
        raise ValueError("ESS_DIAGNOSTIC_MODE must be probe-only or syscall-trace")
    return mode


def datagram_limit(mode):
    return 2048 if diagnostic_mode(mode) == "probe-only" else 32768


def compact_thread_snapshot():
    """One schedstat read per side; no full procfs snapshot in the observer."""
    start = clocks()
    schedstat = read("/proc/thread-self/schedstat", limit=128)
    usage = resource.getrusage(resource.RUSAGE_THREAD)
    return dict(window=dict(start=start, end=clocks()), schedstat=schedstat,
                user_ns=int(usage.ru_utime * 1_000_000_000),
                system_ns=int(usage.ru_stime * 1_000_000_000),
                voluntary_ctxt_switches=usage.ru_nvcsw,
                nonvoluntary_ctxt_switches=usage.ru_nivcsw)


def compact_thread_delta(before, after):
    result = thread_delta(before, after)
    result["thread_cpu_ns"] = (after["window"]["end"]["thread_cpu_ns"]
                               - before["window"]["end"]["thread_cpu_ns"])
    for name in ("user_ns", "system_ns", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"):
        result[name] = after[name] - before[name]
    return result


def delivery_errors(mode, trace_seen, tracers, discarded, instrument, socket_rows):
    errors = set()
    if diagnostic_mode(mode) == "syscall-trace":
        if not trace_seen:
            errors.add("no_syscall_trace")
        if not tracers:
            errors.add("no_fenced_tracer_identity")
    elif tracers:
        errors.add("unexpected_tracer_in_probe_only")
    if any(discarded.values()):
        errors.add("retention_cap_exhausted")
    if (instrument is None or instrument.get("mode") != mode
            or instrument["counts"]["dropped"] or instrument["counts"]["sent"] != socket_rows):
        errors.add("incomplete_probe_delivery")
    return errors


def write_json(path, value):
    """Publish readiness/identity receipts only after their complete write."""
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    os.replace(temporary, path)


class Retention:
    """Keep draining after a cap, recording explicit incomplete coverage."""
    def __init__(self, streams, caps):
        self.streams = streams
        self.caps = caps
        self.written = {key: 0 for key in caps}
        self.discarded = {key: 0 for key in caps}

    def retain(self, kind, data):
        if self.discarded[kind] or self.written[kind] + len(data) > self.caps[kind]:
            self.discarded[kind] += len(data)
        else:
            self.streams[kind].write(data)
            self.written[kind] += len(data)


def clocks():
    return dict(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                thread_cpu_ns=time.thread_time_ns())


def identity(pid):
    try:
        f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return dict(pid=int(pid), start_ticks=int(f[19]), state=f[0])
    except (OSError, ValueError):
        return None


def alive(expected):
    actual = identity(expected["pid"])
    return bool(actual and actual["start_ticks"] == expected["start_ticks"]
                and actual["state"] != "Z")


def read(path, limit=32768):
    try:
        with open(path, "rb", buffering=0) as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            return {"unavailable": "oversized"}
        return raw.decode(errors="replace")
    except OSError as error:
        return {"unavailable": type(error).__name__}


def thread_snapshot(pid, tid):
    before = clocks()
    root = Path(f"/proc/{pid}/task/{tid}")
    result = dict(tid=tid, identity=identity(tid))
    for name in ("schedstat", "sched", "status", "wchan", "stat"):
        value = read(root / name)
        if name == "status" and isinstance(value, str):
            value = "\n".join(line for line in value.splitlines() if line.split(":", 1)[0]
                in {"State", "Tgid", "Pid", "PPid", "TracerPid", "Threads",
                    "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"})
        result[name] = value
    result["window"] = dict(start=before, end=clocks())
    return result


def cgroups(pid):
    value = read(f"/proc/{pid}/cgroup")
    rows = {}
    if isinstance(value, str):
        for line in value.splitlines():
            if line.startswith("0::"):
                relative = line[3:].lstrip("/")
                if ".." in Path(relative).parts:
                    continue
                root = Path("/sys/fs/cgroup") / relative
                while root.is_relative_to("/sys/fs/cgroup"):
                    rows[str(root)] = {name: read(root / name) for name in
                        ("cpu.stat", "cpu.max", "cpu.pressure", "io.pressure",
                         "cpuset.cpus.effective")}
                    if root == Path("/sys/fs/cgroup"):
                        break
                    root = root.parent
    return dict(membership=value, ancestors=rows)


def counter_delta(before, after):
    """Deltas only for common numeric cpu.stat counters; unavailable stays explicit."""
    def parse(value):
        if not isinstance(value, str):
            return {}
        result = {}
        for line in value.splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[1].isdigit():
                result[fields[0]] = int(fields[1])
        return result
    result = {}
    for path, old in before.get("ancestors", {}).items():
        new = after.get("ancestors", {}).get(path, {})
        a, b = parse(old.get("cpu.stat")), parse(new.get("cpu.stat"))
        result[path] = {key: b[key] - value for key, value in a.items()
                        if key in b and b[key] >= value}
    return result


def thread_delta(before, after):
    result = {}
    a, b = before.get("schedstat"), after.get("schedstat")
    if isinstance(a, str) and isinstance(b, str):
        old, new = a.split(), b.split()
        if len(old) == len(new) == 3 and all(v.isdigit() for v in old + new):
            result["schedstat"] = dict(zip(
                ("executing_ns", "runqueue_wait_ns", "timeslices"),
                (int(y) - int(x) for x, y in zip(old, new))))
    for name in ("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"):
        def value(snapshot):
            status = snapshot.get("status")
            if isinstance(status, str):
                for line in status.splitlines():
                    if line.startswith(name + ":"):
                        return int(line.split()[1])
        old, new = value(before), value(after)
        if old is not None and new is not None:
            result[name] = new - old
    return result
