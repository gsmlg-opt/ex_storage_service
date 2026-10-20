#!/usr/bin/env python3
"""Stdlib-only Port observer. Stdout is a bounded protocol, never logs.

Collection/schedule clocks are Python clocks. BEAM metrics retain their native
windows; monotonic timestamps from different runtimes are never compared.
"""
import argparse
import json
import os
from pathlib import Path
import re
import resource
import select
import stat
import sys
import time

INTERVAL_NS = 25_000_000
FRAME_BYTES = 4096
ROW_BYTES = 16_384
MAX_DURATION_NS = 10_800_000_000_000
MAX_OUTPUT_BYTES = 1_073_741_824
MAX_ROWS = 432_000
BEAM_METRICS = ("binary", "total", "queue_max", "queue_total")
WINDOW_KEYS = ("start_ns", "end_ns", "start_monotonic_ns", "end_monotonic_ns")


class OwnerClosed(Exception):
    pass


def process_identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"pid": int(pid), "start_ticks": int(fields[19]), "state": fields[0]}
    except (FileNotFoundError, NotADirectoryError):
        return None


def rss(pid):
    # System.pid() supplies the actual observed BEAM, never getppid().
    value = Path(f"/proc/{pid}/status").read_bytes()
    match = re.search(rb"^VmRSS:\s+(\d+) kB$", value, re.M)
    if match is None:
        raise RuntimeError("observed process has no resident memory")
    return int(match[1]) * 1024


def namespaces(pid):
    return {name: os.readlink(f"/proc/{pid}/ns/{name}") for name in ("pid", "mnt")}


def entries(directory):
    try:
        return os.scandir(directory)
    except (FileNotFoundError, NotADirectoryError):
        return None


def temporary_tree(directory, tmp_only=False, ancestors=frozenset()):
    try:
        info = os.stat(directory)
    except (FileNotFoundError, NotADirectoryError):
        return 0
    if not stat.S_ISDIR(info.st_mode):
        return 0
    identity = (info.st_dev, info.st_ino)
    if identity in ancestors:
        raise RuntimeError("cyclic resource observation directory")
    children = entries(directory)
    if children is None:
        return 0
    total = 0
    with children:
        for entry in children:
            if entry.name.startswith("."):
                continue
            try:
                info = entry.stat(follow_symlinks=True)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(info.st_mode):
                total += temporary_tree(entry.path, tmp_only, ancestors | {identity})
            elif stat.S_ISREG(info.st_mode) and (not tmp_only or entry.name.endswith(".tmp")):
                total += info.st_size
    return total


def temporary_bytes(tmp, data):
    total = temporary_tree(tmp)
    children = entries(data)
    if children is not None:
        with children:
            for entry in children:
                if not entry.name.startswith("."):
                    total += temporary_tree(os.path.join(entry.path, "cache"), True)
    return total


def probe(function):
    window = {"start_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns()}
    value = function()
    window.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns())
    return value, window


def encode(value, limit):
    encoded = json.dumps(value, separators=(",", ":")).encode() + b"\n"
    if len(encoded) > limit:
        raise ValueError("oversized resource frame")
    return encoded


def write_all(fd, encoded):
    remaining = memoryview(encoded)
    while remaining:
        count = os.write(fd, remaining)
        if count <= 0:
            raise OSError("resource output made no progress")
        remaining = remaining[count:]


def read_frame(fd, timeout=5):
    # limit+1 allocation also bounds partial/no-newline malicious replies.
    deadline = time.monotonic() + timeout
    frame = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([fd], [], [], max(0, remaining))[0]:
            raise TimeoutError("resource response deadline exceeded")
        chunk = os.read(fd, FRAME_BYTES + 1 - len(frame))
        if not chunk:
            raise OwnerClosed()
        frame.extend(chunk)
        if len(frame) > FRAME_BYTES:
            raise ValueError("oversized resource response")
        if b"\n" in frame:
            if not frame.endswith(b"\n") or frame.count(b"\n") != 1:
                raise ValueError("multiple resource responses")
            return json.loads(frame)


def validate_reply(reply, generation, sequence):
    expected = {"generation", "sequence", "probes", *BEAM_METRICS}
    if not isinstance(reply, dict) or set(reply) != expected:
        raise ValueError("malformed resource response")
    if reply["generation"] != generation or type(reply["sequence"]) is not int or reply["sequence"] != sequence:
        raise ValueError("stale resource response")
    if not isinstance(reply["probes"], dict) or set(reply["probes"]) != set(BEAM_METRICS):
        raise ValueError("malformed resource windows")
    for metric in BEAM_METRICS:
        window = reply["probes"][metric]
        if type(reply[metric]) is not int or reply[metric] < 0:
            raise ValueError("invalid resource value")
        if not isinstance(window, dict) or set(window) != set(WINDOW_KEYS) or any(
                type(window[key]) is not int for key in WINDOW_KEYS):
            raise ValueError("invalid resource window")
        if window["end_ns"] < window["start_ns"] or window["end_monotonic_ns"] < window["start_monotonic_ns"]:
            raise ValueError("reversed resource window")
    return reply


def wait_slot(fd, deadline):
    remaining = (deadline - time.monotonic_ns()) / 1_000_000_000
    if remaining > 0 and select.select([fd], [], [], remaining)[0]:
        if not os.read(fd, 1):
            raise OwnerClosed()
        raise ValueError("unsolicited resource response")


def sample(args):
    beam = process_identity(args.beam_pid)
    observer = process_identity(os.getpid())
    if beam is None or namespaces(args.beam_pid) != namespaces(os.getpid()):
        raise RuntimeError("resource observer namespace/BEAM identity mismatch")
    identity = {"generation": args.generation, "beam": beam, "observer": observer,
                "namespaces": namespaces(os.getpid()), "python": sys.executable,
                "tmp_device": os.stat(args.tmp).st_dev if os.path.exists(args.tmp) else None}
    # Immutable identity survives SIGKILL. Harness verifies actual /proc identity
    # after shutdown, rather than treating Port closure as termination proof.
    with (Path(args.root) / f"observer-{args.generation}.json").open("xb") as output:
        output.write(encode(identity, ROW_BYTES))
    output = os.open(Path(args.root) / "samples.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    sequence, deadline = 0, time.monotonic_ns()
    expires = deadline + MAX_DURATION_NS
    output_bytes = os.fstat(output).st_size
    try:
        while True:
            if time.monotonic_ns() >= expires or sequence >= MAX_ROWS:
                raise RuntimeError("resource observer lifetime/row limit exceeded")
            wait_slot(0, deadline)
            collection = {"start_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns()}
            write_all(1, encode({"generation": args.generation, "sequence": sequence}, FRAME_BYTES))
            reply = validate_reply(read_frame(0), args.generation, sequence)
            resident, rss_window = probe(lambda: rss(args.beam_pid))
            temporary, tmp_window = probe(lambda: temporary_bytes(args.tmp, args.data))
            usage = resource.getrusage(resource.RUSAGE_SELF)
            observer_usage = {"rss": rss(os.getpid()), "user_seconds": usage.ru_utime,
                              "system_seconds": usage.ru_stime, "max_rss": usage.ru_maxrss * 1024}
            collection.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns())
            row = {"sampler_id": args.generation, "sequence": sequence,
                   "scheduled_monotonic_ns": deadline, "time": time.time_ns() // 1_000_000,
                   "collection": collection, "rss": resident, "temporary": temporary,
                   "observer": observer_usage, **{key: reply[key] for key in BEAM_METRICS},
                   "probes": {**reply["probes"], "rss": rss_window, "temporary": tmp_window}}
            encoded = encode(row, ROW_BYTES)
            if output_bytes + len(encoded) > MAX_OUTPUT_BYTES:
                raise RuntimeError("resource observer output limit exceeded")
            write_all(output, encoded)
            output_bytes += len(encoded)
            sequence += 1
            now = time.monotonic_ns()
            deadline += max(1, (now - deadline) // INTERVAL_NS + 1) * INTERVAL_NS
    finally:
        os.close(output)


def verify_stopped(root, timeout=5):
    paths = sorted(path for path in Path(root).glob("observer-*.json")
                   if path.name != "observer-stop-verification.json")
    deadline = time.monotonic() + timeout
    while True:
        results = []
        for path in paths:
            expected = json.loads(path.read_text())["observer"]
            actual = process_identity(expected["pid"])
            stopped = actual is None or actual["start_ticks"] != expected["start_ticks"] or actual["state"] == "Z"
            results.append({"identity_file": str(path), "expected": expected,
                            "observed": actual, "stopped": stopped})
        if all(item["stopped"] for item in results):
            return results
        if time.monotonic() >= deadline:
            raise RuntimeError("owned resource observer OS identity is still running")
        time.sleep(.025)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    observe = subparsers.add_parser("sample")
    for name in ("root", "tmp", "data", "generation"):
        observe.add_argument("--" + name, required=True)
    observe.add_argument("--beam-pid", type=int, required=True)
    stopped = subparsers.add_parser("verify-stopped")
    stopped.add_argument("--root", required=True)
    args = parser.parse_args()
    if args.action == "verify-stopped":
        receipt = verify_stopped(args.root)
        (Path(args.root) / "observer-stop-verification.json").write_bytes(encode(receipt, 65_536))
    else:
        try:
            sample(args)
        except (OwnerClosed, BrokenPipeError):
            pass


if __name__ == "__main__":
    main()
