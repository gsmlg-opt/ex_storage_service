#!/usr/bin/env python3
"""Stdlib-only Port observer. Stdout is a bounded protocol, never logs.

Collection/schedule clocks are Python clocks. BEAM metrics retain their native
windows; monotonic timestamps from different runtimes are never compared.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import resource
import select
import stat
import sys
import threading
import time

INTERVAL_NS = 25_000_000
FRAME_BYTES = 4096
ROW_BYTES = 16_384
MAX_DURATION_NS = 10_800_000_000_000
MAX_OUTPUT_BYTES = 1_073_741_824
MAX_ROWS = 432_000
# Fixed before C3: 128 rows cover 3.2s of 25ms sampling, including the
# independently observed C2 1.827s output stall. Never scale with object size.
EXPORT_QUEUE_ROWS = 128
EXPORT_DRAIN_SECONDS = 5
BEAM_METRICS = ("binary", "total", "queue_max", "queue_total")
WINDOW_KEYS = ("start_ns", "end_ns", "start_monotonic_ns", "end_monotonic_ns")


class OwnerClosed(Exception):
    pass


def process_identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"pid": int(pid), "start_ticks": int(fields[19]), "state": fields[0]}
    except (FileNotFoundError, NotADirectoryError, ProcessLookupError):
        # /proc reads can report ESRCH when the process exits after open.
        return None


def rss(pid):
    # System.pid() supplies the actual observed BEAM, never getppid().
    value = Path(f"/proc/{pid}/status").read_bytes()
    match = re.search(rb"^VmRSS:\s+(\d+) kB$", value, re.M)
    if match is None:
        raise RuntimeError("observed process has no resident memory")
    return int(match[1]) * 1024


def owned_rss(expected):
    try:
        return rss(expected["pid"])
    except (FileNotFoundError, ProcessLookupError, RuntimeError):
        actual = process_identity(expected["pid"])
        if (actual is None or actual["start_ticks"] != expected["start_ticks"]
                or actual["state"] == "Z"):
            # The owner can exit after replying but before this OS probe.
            # Drain admitted rows; never publish the incomplete observation.
            raise OwnerClosed() from None
        raise


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
                if entry.is_dir(follow_symlinks=True):
                    total += temporary_tree(entry.path, tmp_only, ancestors | {identity})
                elif not tmp_only or entry.name.endswith(".tmp"):
                    info = entry.stat(follow_symlinks=True)
                    if stat.S_ISREG(info.st_mode):
                        total += info.st_size
            except FileNotFoundError:
                continue
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


class ExportQueue(queue.Queue):
    high_water = 0

    def _put(self, value):
        super()._put(value)
        # Queue calls this while holding its own mutex: consumption cannot
        # race the recorded depth. No I/O occurs while holding that mutex.
        self.high_water = max(self.high_water, len(self.queue))


class SampleWriter:
    """One bounded serial exporter; only this thread owns the sample FD."""

    def __init__(self, output, generation, expires):
        self.output, self.generation, self.expires = output, generation, expires
        self.start = os.fstat(output).st_size
        self.rows = ExportQueue(maxsize=EXPORT_QUEUE_ROWS)
        self.stopping = threading.Event()
        self.error = None
        self.admitted = self.persisted = self.output_bytes = 0
        self.digest = hashlib.sha256()
        self.previous_export = None
        # A blocked raw OS write cannot be cancelled safely by closing/reusing
        # its FD in another thread. A failed bounded join exits the OS helper;
        # the public verifier requires its actual death and terminal receipt.
        self.thread = threading.Thread(target=self.run, name="resource-export", daemon=True)
        self.thread.start()

    def check_capacity(self):
        if self.error is not None:
            raise RuntimeError("resource export failed") from self.error
        if self.rows.full():
            raise RuntimeError("resource export queue saturated")

    def submit(self, row):
        encoded = encode(row, ROW_BYTES)
        # There is one producer, and capacity is checked before requesting a
        # fresh sample. The writer can only free slots during collection.
        self.rows.put_nowait(encoded)
        self.admitted += 1

    def run(self):
        try:
            while not self.stopping.is_set() or not self.rows.empty():
                try:
                    raw = self.rows.get(timeout=.025)
                except queue.Empty:
                    continue
                row = json.loads(raw)
                del raw
                if row["sampler_id"] != self.generation or row["sequence"] != self.persisted:
                    raise ValueError("noncontiguous resource export")
                row["previous_export"] = self.previous_export
                cpu_started = time.thread_time_ns()
                encoded, encoding_window = probe(lambda: encode(row, ROW_BYTES))
                encoding_window["cpu_ns"] = time.thread_time_ns() - cpu_started
                if self.start + self.output_bytes + len(encoded) > MAX_OUTPUT_BYTES:
                    raise RuntimeError("resource observer output limit exceeded")
                if time.monotonic_ns() >= self.expires:
                    raise RuntimeError("resource observer export lifetime exceeded")
                cpu_started = time.thread_time_ns()
                _, write_window = probe(lambda: write_all(self.output, encoded))
                write_window["cpu_ns"] = time.thread_time_ns() - cpu_started
                self.digest.update(encoded)
                self.output_bytes += len(encoded)
                self.persisted += 1
                # Completed output diagnostics belong to the next persisted
                # row, whose fresh collection may precede this write's end.
                self.previous_export = {"sampler_id": self.generation, "sequence": row["sequence"],
                                        "encoded_bytes": len(encoded), "encoding": encoding_window,
                                        "write": write_window}
        except BaseException as error:
            self.error = error
        finally:
            try:
                os.close(self.output)
            except OSError as error:
                self.error = self.error or error

    def finish(self):
        self.stopping.set()
        self.thread.join(timeout=EXPORT_DRAIN_SECONDS)
        if self.thread.is_alive():
            raise RuntimeError("resource export drain deadline exceeded")
        if self.error is not None:
            raise RuntimeError("resource export failed") from self.error
        if self.admitted != self.persisted:
            raise RuntimeError("resource export incomplete")

    def manifest(self, identity):
        return {"schema": 1, "generation": self.generation, "observer": identity,
                "status": "passed", "admitted_rows": self.admitted, "persisted_rows": self.persisted,
                "first_sequence": 0 if self.persisted else None,
                "last_sequence": self.persisted - 1 if self.persisted else None,
                "output_start": self.start, "output_bytes": self.output_bytes,
                "sha256": self.digest.hexdigest(), "queue_capacity": EXPORT_QUEUE_ROWS,
                "queued_byte_limit": EXPORT_QUEUE_ROWS * ROW_BYTES, "row_byte_limit": ROW_BYTES,
                "maximum_queued_rows": self.rows.high_water}


def sample(args):
    if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.generation) is None:
        raise ValueError("invalid resource generation")
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
    writer = SampleWriter(output, args.generation, expires)
    try:
        while True:
            if time.monotonic_ns() >= expires or sequence >= MAX_ROWS:
                raise RuntimeError("resource observer lifetime/row limit exceeded")
            wait_slot(0, deadline)
            if time.monotonic_ns() >= expires:
                raise RuntimeError("resource observer lifetime exceeded")
            writer.check_capacity()
            collection = {"start_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns()}
            exchange = dict(collection)
            write_all(1, encode({"generation": args.generation, "sequence": sequence}, FRAME_BYTES))
            reply = validate_reply(read_frame(0), args.generation, sequence)
            exchange.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns())
            resident, rss_window = probe(lambda: owned_rss(beam))
            temporary, tmp_window = probe(lambda: temporary_bytes(args.tmp, args.data))
            usage = resource.getrusage(resource.RUSAGE_SELF)
            observer_usage = {"rss": rss(os.getpid()), "user_seconds": usage.ru_utime,
                              "system_seconds": usage.ru_stime, "max_rss": usage.ru_maxrss * 1024}
            collection.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns())
            row = {"sampler_id": args.generation, "sequence": sequence,
                   "scheduled_monotonic_ns": deadline, "time": time.time_ns() // 1_000_000,
                   "collection": collection, "rss": resident, "temporary": temporary,
                   "beam_exchange": exchange,
                   "observer": observer_usage, **{key: reply[key] for key in BEAM_METRICS},
                   "probes": {**reply["probes"], "rss": rss_window, "temporary": tmp_window}}
            writer.submit(row)
            sequence += 1
            now = time.monotonic_ns()
            deadline += max(1, (now - deadline) // INTERVAL_NS + 1) * INTERVAL_NS
    except (OwnerClosed, BrokenPipeError):
        pass  # A real owner shutdown still requires the complete bounded drain.
    finally:
        writer.finish()
    # Written only after successful collection shutdown, drain and FD close.
    # Missing, partial or failed publication (including ENOSPC) cannot certify
    # an export. The immutable OS identity remains independently inspectable.
    with (Path(args.root) / f"resource-export-{args.generation}.json").open("xb") as output:
        output.write(encode(writer.manifest(observer), ROW_BYTES))


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


def verify_exports(root, stopped):
    """Validate terminal writer attestations, separately from OS death.

    The writer validates contiguous rows and hashes bytes while exporting.
    Shutdown checks bounded manifests and exact file extents, without rescanning
    up to 1GiB inside the harness's ten-second process-cleanup deadline. Recorded
    per-generation hashes remain available for independent artifact auditing.
    """
    if not stopped:
        raise RuntimeError("no resource observer generation")
    manifests, extents = set(), []
    for item in stopped:
        identity = json.loads(Path(item["identity_file"]).read_bytes())
        generation = identity["generation"]
        if not isinstance(generation, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", generation) is None:
            raise ValueError("invalid resource generation")
        path = Path(root) / f"resource-export-{generation}.json"
        manifests.add(path)
        with path.open("rb") as source:
            encoded = source.read(ROW_BYTES + 1)
        if len(encoded) > ROW_BYTES:
            raise ValueError("oversized resource export manifest")
        receipt = json.loads(encoded)
        expected = {"schema", "generation", "observer", "status", "admitted_rows", "persisted_rows",
                    "first_sequence", "last_sequence", "output_start", "output_bytes", "sha256",
                    "queue_capacity", "queued_byte_limit", "row_byte_limit", "maximum_queued_rows"}
        if (not isinstance(receipt, dict) or set(receipt) != expected or type(receipt["schema"]) is not int or
                receipt["schema"] != 1 or receipt["status"] != "passed" or receipt["generation"] != generation):
            raise ValueError("unsuccessful resource export manifest")
        if any(receipt["observer"][key] != item["expected"][key] for key in ("pid", "start_ticks")):
            raise ValueError("resource export observer identity mismatch")
        integers = ("admitted_rows", "persisted_rows", "output_start", "output_bytes", "queue_capacity",
                    "queued_byte_limit", "row_byte_limit", "maximum_queued_rows")
        if any(type(receipt[key]) is not int or receipt[key] < 0 for key in integers):
            raise ValueError("invalid resource export bounds")
        count = receipt["persisted_rows"]
        if receipt["admitted_rows"] != count or count > MAX_ROWS:
            raise ValueError("incomplete resource export")
        if ((count and any(type(receipt[key]) is not int for key in ("first_sequence", "last_sequence"))) or
                receipt["first_sequence"] != (0 if count else None) or receipt["last_sequence"] != (count - 1 if count else None)):
            raise ValueError("noncontiguous resource export manifest")
        if (receipt["queue_capacity"] != EXPORT_QUEUE_ROWS or receipt["row_byte_limit"] != ROW_BYTES or
                receipt["queued_byte_limit"] != EXPORT_QUEUE_ROWS * ROW_BYTES or
                receipt["maximum_queued_rows"] > EXPORT_QUEUE_ROWS or
                receipt["output_bytes"] > count * ROW_BYTES or
                (count > 0 and receipt["output_bytes"] == 0) or
                (count == 0 and receipt["sha256"] != hashlib.sha256().hexdigest()) or
                not isinstance(receipt["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", receipt["sha256"]) is None):
            raise ValueError("invalid resource export limits/digest")
        extents.append((receipt["output_start"], receipt["output_bytes"]))
        item["export_manifest"] = str(path)
        item["export"] = receipt
    if manifests != set(Path(root).glob("resource-export-*.json")):
        raise ValueError("unbound resource export manifest")
    position = 0
    for start, length in sorted(extents):
        if start != position:
            raise ValueError("noncontiguous resource export extents")
        position += length
    if position > MAX_OUTPUT_BYTES or position != (Path(root) / "samples.jsonl").stat().st_size:
        raise ValueError("resource export file size mismatch")
    return stopped


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
        path = Path(args.root) / "observer-stop-verification.json"
        path.unlink(missing_ok=True)
        try:
            receipt = verify_exports(args.root, verify_stopped(args.root))
        except Exception as error:
            path.write_bytes(encode({"status": "failed", "error_type": type(error).__name__}, ROW_BYTES))
            raise
        path.write_bytes(encode(receipt, 65_536))
    else:
        sample(args)


if __name__ == "__main__":
    main()
