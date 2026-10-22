"""Owned observer monitor + bounded, continuously drained full-phase syscall stream."""
import json
import os
from pathlib import Path
import selectors
import socket
import sys
import time
from common import (Retention, alive, cgroups, clocks, counter_delta, identity,
                    read, thread_delta, thread_snapshot, write_json)

ROOT = Path(sys.argv[1]).resolve()
launch = json.loads((ROOT / "launch.json").read_text())
expected = launch["observer"]
pid = expected["pid"]
caps = {"syscalls": 1024 * 1024 * 1024, "probes": 512 * 1024 * 1024,
        "scheduler": 256 * 1024 * 1024}
files = {key: (ROOT / (key + (".log" if key == "syscalls" else ".jsonl"))).open("xb")
         for key in caps}
retention = Retention(files, caps)
written, discarded = retention.written, retention.discarded
tracers = {}
errors = set()
socket_rows = 0
trace_seen = False
previous_cgroup = {}
previous_threads = {}
expiry = time.monotonic() + 960


def retain(kind, data):
    retention.retain(kind, data)


def row(kind, value):
    retain(kind, json.dumps(value, separators=(",", ":")).encode() + b"\n")


def snapshot():
    global previous_cgroup, previous_threads
    start = clocks()
    current = identity(pid)
    threads = []
    try:
        for directory in sorted(Path(f"/proc/{pid}/task").iterdir()):
            if directory.name.isdecimal():
                thread = thread_snapshot(pid, int(directory.name))
                old = previous_threads.get(thread["tid"])
                if old is not None and (old.get("identity") or {}).get("start_ticks") == (thread.get("identity") or {}).get("start_ticks"):
                    thread["scheduling_delta"] = thread_delta(old, thread)
                threads.append(thread)
                status = thread.get("status")
                if isinstance(status, str):
                    for line in status.splitlines():
                        if line.startswith("TracerPid:"):
                            number = int(line.split()[1])
                            if number:
                                found = identity(number)
                                if found:
                                    tracers[(found["pid"], found["start_ticks"])] = found
    except OSError:
        pass
    group = cgroups(pid)
    stat = read("/proc/stat")
    stat = stat.splitlines()[0] if isinstance(stat, str) else stat
    value = dict(kind="scheduler", observer=current, threads=threads, cgroups=group,
                 cpu_stat_deltas=counter_delta(previous_cgroup, group),
                 cpu_aggregate=stat, load=read("/proc/loadavg"),
                 pressure={key: read("/proc/pressure/" + key) for key in ("cpu", "io")},
                 window=dict(start=start, end=clocks()))
    previous_cgroup = group
    previous_threads = {thread["tid"]: thread for thread in threads if thread.get("identity")}
    row("scheduler", value)


selector = selectors.DefaultSelector()
fifo = os.open(ROOT / "syscalls.fifo", os.O_RDWR | os.O_NONBLOCK)
channel = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
channel.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
channel.bind(str(ROOT / "events.sock"))
channel.setblocking(False)
selector.register(fifo, selectors.EVENT_READ, "trace")
selector.register(channel, selectors.EVENT_READ, "probe")
write_json(ROOT / "monitor-ready.json", dict(
    monitor=identity(os.getpid()), observer=expected, caps_bytes=caps, clocks=clocks(),
    sched_schedstats=read("/proc/sys/kernel/sched_schedstats"),
    ptrace_scope=read("/proc/sys/kernel/yama/ptrace_scope")))
deadline = time.monotonic()
quiet_since = None
try:
    while True:
        now = time.monotonic()
        if now >= expiry:
            errors.add("monitor_lifetime_exceeded")
            break
        if now >= deadline:
            snapshot()
            deadline += max(1, int((now - deadline) / .025) + 1) * .025
        events = selector.select(max(0, min(.025, deadline - time.monotonic())))
        for key, _ in events:
            if key.data == "trace":
                chunk = os.read(fifo, 65536)
                if chunk:
                    trace_seen = True
                    quiet_since = None
                    retain("syscalls", chunk)
            else:
                chunk, _address = channel.recvfrom(32769)
                if len(chunk) > 32768:
                    errors.add("oversized_probe_event")
                else:
                    retain("probes", chunk + b"\n")
                    socket_rows += 1
                quiet_since = None
        # Never detach or stop a live tracer; natural observer phase exit is
        # required. The original fixture alone owns observer shutdown.
        if not alive(expected) and not any(alive(value) for value in tracers.values()):
            if quiet_since is None:
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= .25:
                break
        else:
            quiet_since = None
except BaseException as error:
    errors.add(type(error).__name__)
finally:
    for stream in files.values():
        stream.close()
    selector.close()
    channel.close()
    os.close(fifo)
    terminal_path = ROOT / "observer-instrumentation-terminal.json"
    instrument = json.loads(terminal_path.read_text()) if terminal_path.is_file() else None
    if not trace_seen:
        errors.add("no_syscall_trace")
    if not tracers:
        errors.add("no_fenced_tracer_identity")
    if any(discarded.values()):
        errors.add("retention_cap_exhausted")
    if instrument is None or instrument["counts"]["dropped"] or instrument["counts"]["sent"] != socket_rows:
        errors.add("incomplete_probe_delivery")
    if alive(expected) or any(alive(value) for value in tracers.values()):
        errors.add("owned_identity_still_running")
    terminal = dict(status="complete" if not errors else "incomplete", errors=sorted(errors),
        observer=expected, observer_stopped=not alive(expected),
        tracers=[dict(expected=value, observed=identity(value["pid"]), stopped=not alive(value))
                 for value in tracers.values()], monitor=identity(os.getpid()),
        written_bytes=written, discarded_bytes=discarded, socket_rows=socket_rows,
        instrumentation=instrument, clocks=clocks(), acceptance_promotion=False)
    write_json(ROOT / "monitor-terminal.json", terminal)
