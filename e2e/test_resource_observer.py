"""Focused Port isolation checks; the original 100ms validator is unchanged."""
import copy
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from resource_cadence import measure_samples
from scripts import resource_observer as observer

REPO = Path(__file__).resolve().parents[1]
REPORT = Path(os.environ["SAMPLER_EVIDENCE_ROOT"]) if "SAMPLER_EVIDENCE_ROOT" in os.environ else None


def retain(name, value, raw=None):
    if REPORT is not None:
        REPORT.mkdir(parents=True, exist_ok=True)
        (REPORT / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
        if raw is not None:
            (REPORT / (name + "-samples.jsonl")).write_bytes(raw)


def environment(root):
    return dict(os.environ, PAGER="cat", E2E_RESOURCE_ROOT=str(root),
                E2E_RESOURCE_PYTHON=sys.executable, ESS_TMP_ROOT=str(root / "tmp"),
                ESS_DATA_ROOT=str(root / "data"), ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")


def roots(directory):
    root = Path(directory)
    (root / "tmp").mkdir()
    (root / "data").mkdir()
    return root


def reply(request):
    _, window = observer.probe(lambda: None)
    return {**request, "binary": 1, "total": 2, "queue_max": 0, "queue_total": 0,
            "probes": {metric: dict(window) for metric in observer.BEAM_METRICS}}


def public_verify(root):
    return subprocess.run([sys.executable, "-B", str(REPO / "e2e/scripts/resource_observer.py"),
                           "verify-stopped", "--root", str(root)],
                          capture_output=True, text=True, timeout=10)


def finish_empty_observer(root, generation):
    result = subprocess.run([sys.executable, "-B", str(REPO / "e2e/scripts/resource_observer.py"),
                             "sample", "--root", str(root), "--tmp", str(root / "tmp"),
                             "--data", str(root / "data"), "--beam-pid", str(os.getpid()),
                             "--generation", generation], input=b"", capture_output=True, timeout=3)
    if result.returncode:
        raise AssertionError(result.stderr.decode())
    return json.loads((root / f"observer-{generation}.json").read_text())


def start_observer(root, generation, wrapper="", pass_fds=()):
    code = ("import os,sys,time;sys.path.insert(0," + repr(str(REPO / "e2e/scripts")) + ")\n"
            "import resource_observer as o\n" + wrapper + "\no.main()\n")
    return subprocess.Popen([sys.executable, "-B", "-c", code, "sample", "--root", str(root),
                             "--tmp", str(root / "tmp"), "--data", str(root / "data"),
                             "--beam-pid", str(os.getpid()), "--generation", generation],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            pass_fds=pass_fds, env=dict(os.environ, PAGER="cat"))


def respond(process, generation, sequence):
    if not select.select([process.stdout], [], [], 3)[0]:
        raise AssertionError("fresh resource request missing")
    request = json.loads(process.stdout.readline(4097))
    if request != {"generation": generation, "sequence": sequence}:
        raise AssertionError(request)
    process.stdin.write(observer.encode(reply(request), observer.FRAME_BYTES))
    process.stdin.flush()


class ObserverProtocolTest(unittest.TestCase):
    def test_malformed_stale_and_invalid_windows_are_rejected(self):
        request = {"generation": "generation", "sequence": 0}
        valid = reply(request)
        self.assertIs(valid, observer.validate_reply(valid, "generation", 0))
        cases = [None, {}, dict(valid, sequence=1), dict(valid, generation="old"),
                 dict(valid, sequence=True), dict(valid, binary=-1), dict(valid, extra=1)]
        bad_window = copy.deepcopy(valid)
        bad_window["probes"]["binary"]["end_ns"] = 0
        cases.append(bad_window)
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                observer.validate_reply(value, "generation", 0)

    def test_partial_oversized_multiple_and_malformed_frames_are_bounded(self):
        for value in (b"x" * 4097, b"{}\n{}\n", b"malformed\n"):
            with self.subTest(value=value[:20]):
                read, write = os.pipe()
                try:
                    os.write(write, value)
                    with self.assertRaises((ValueError, json.JSONDecodeError)):
                        observer.read_frame(read, timeout=.1)
                finally:
                    os.close(read)
                    os.close(write)
        read, write = os.pipe()
        try:
            os.write(write, b"{")
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                observer.read_frame(read, timeout=.05)
            self.assertLess(time.monotonic() - start, .2)
            os.close(write)
            write = None
            with self.assertRaises(observer.OwnerClosed):
                observer.read_frame(read, timeout=.1)
        finally:
            os.close(read)
            if write is not None:
                os.close(write)

    def test_cleanup_verification_rejects_a_live_os_identity(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-live-") as directory:
            root = Path(directory)
            (root / "observer-stop-verification.json").write_text("[]")
            (root / "observer-live.json").write_text(json.dumps({"observer": observer.process_identity(os.getpid())}))
            with self.assertRaisesRegex(RuntimeError, "still running"):
                observer.verify_stopped(root, timeout=.025)

    def test_public_verification_rejects_a_dead_observer_without_export_completion(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-no-completion-") as directory:
            root = Path(directory)
            process = subprocess.Popen([sys.executable, "-B", "-c", "pass"])
            identity = observer.process_identity(process.pid)
            process.wait(timeout=3)
            (root / "observer-missing.json").write_text(json.dumps(
                {"generation": "missing", "observer": identity}))
            self.assertTrue(all(item["stopped"] for item in observer.verify_stopped(root)))
            result = public_verify(root)
            retain("missing-completion", {"returncode": result.returncode, "stderr": result.stderr})
            self.assertNotEqual(0, result.returncode, "OS death alone certified an unexported generation")

    def test_public_cleanup_verification_can_repeat_with_a_new_observer(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-repeat-") as directory:
            root = roots(directory)
            identities = []
            for index in range(2):
                identities.append(finish_empty_observer(root, f"repeat-{index}")["observer"])
                result = public_verify(root)
                self.assertEqual(0, result.returncode, result.stderr)
                receipt = json.loads((root / "observer-stop-verification.json").read_text())
                self.assertEqual(len(identities), len(receipt))
                self.assertEqual({item["pid"] for item in identities}, {item["expected"]["pid"] for item in receipt})
                self.assertTrue(all(item["stopped"] for item in receipt))

    def test_public_verification_rejects_invalid_generation_completion(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-bad-completion-") as directory:
            root = roots(directory)
            finish_empty_observer(root, "terminal")
            path = root / "resource-export-terminal.json"
            valid = json.loads(path.read_text())
            cases = [[], {}, dict(valid, schema=True), dict(valid, status="failed"),
                     dict(valid, generation="other"), dict(valid, admitted_rows=1),
                     dict(valid, persisted_rows=True), dict(valid, first_sequence=0),
                     dict(valid, output_start=1), dict(valid, sha256="invalid"),
                     dict(valid, sha256="0" * 64), dict(valid, maximum_queued_rows=129),
                     dict(valid, unexpected=True)]
            wrong_identity = copy.deepcopy(valid)
            wrong_identity["observer"]["start_ticks"] += 1
            cases.append(wrong_identity)
            for value in cases:
                with self.subTest(manifest=value):
                    path.write_text(json.dumps(value))
                    self.assertNotEqual(0, public_verify(root).returncode)
            path.write_text(json.dumps(valid))
            self.assertEqual(0, public_verify(root).returncode)
            path.unlink()
            self.assertNotEqual(0, public_verify(root).returncode)
            failed = json.loads((root / "observer-stop-verification.json").read_text())
            self.assertEqual("failed", failed["status"], "stale successful cleanup receipt survived")
            path.write_text("malformed")
            self.assertNotEqual(0, public_verify(root).returncode)
            self.assertEqual("failed", json.loads((root / "observer-stop-verification.json").read_text())["status"])
            path.write_text(json.dumps(valid))
            (root / "samples.jsonl").write_bytes(b"partial")
            self.assertNotEqual(0, public_verify(root).returncode)

    def test_cleanup_verification_rejects_other_malformed_identity_files(self):
        for filename in ("observer-123.json", "observer-unknown.json",
                         "observer-stop-verification-extra.json"):
            for value in ([], {}, {"observer": {"pid": os.getpid()}}):
                with self.subTest(filename=filename, value=value), tempfile.TemporaryDirectory(prefix="ess-observer-malformed-") as directory:
                    root = Path(directory)
                    (root / filename).write_text(json.dumps(value))
                    with self.assertRaises((TypeError, KeyError)):
                        observer.verify_stopped(root, timeout=.025)

    def run_protocol(self, mode):
        with tempfile.TemporaryDirectory(prefix="ess-observer-protocol-") as directory:
            root = roots(directory)
            wrapper = ""
            if mode == "slow-export":
                wrapper = '''
write = o.write_all
def slow(fd, value):
    if fd != 1:
        time.sleep(.16)
    write(fd, value)
o.write_all = slow
'''
            if mode == "slow-scan":
                wrapper = '''
scan = o.temporary_bytes
def slow(tmp, data):
    time.sleep(.16)
    return scan(tmp, data)
o.temporary_bytes = slow
'''
            code = ("import sys,time;sys.path.insert(0," + repr(str(REPO / "e2e/scripts")) + ")\n"
                    "import resource_observer as o\n" + wrapper + "o.main()\n")
            command = [sys.executable, "-B", "-c", code, "sample", "--root", str(root),
                       "--tmp", str(root / "tmp"), "--data", str(root / "data"),
                       "--beam-pid", str(os.getpid()), "--generation", "protocol-test"]
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, env=dict(os.environ, PAGER="cat"))
            try:
                count = 1 if mode in ("stale", "malformed", "oversized") else 7
                for sequence in range(count):
                    self.assertTrue(select.select([process.stdout], [], [], 3)[0], "request missing")
                    request = json.loads(process.stdout.readline(4097))
                    self.assertEqual({"generation": "protocol-test", "sequence": sequence}, request)
                    if mode == "delayed" and sequence == 2:
                        time.sleep(.16)
                    value = reply(request)
                    if mode == "stale":
                        value["sequence"] = sequence + 1
                    encoded = observer.encode(value, 4096)
                    if mode == "malformed":
                        encoded = b"not-json\n"
                    if mode == "oversized":
                        encoded = b"x" * 4097
                    process.stdin.write(encoded)
                    process.stdin.flush()
                if mode in ("stale", "malformed", "oversized"):
                    process.wait(timeout=3)
                    self.assertNotEqual(0, process.returncode)
                else:
                    # EOF is a real owner shutdown, including during sleep.
                    process.stdin.close()
                    process.stdin = None
                    process.wait(timeout=3)
                    self.assertEqual(0, process.returncode, process.stderr.read().decode())
                raw = (root / "samples.jsonl").read_bytes()
                rows = [json.loads(line) for line in raw.splitlines()]
                if mode in ("stale", "malformed", "oversized"):
                    self.assertEqual([], rows, "invalid response exported an observation")
                else:
                    self.assertGreaterEqual(len(rows), 6)
                    for row in rows:
                        exchange = row["beam_exchange"]
                        self.assertLessEqual(row["scheduled_monotonic_ns"], row["collection"]["start_monotonic_ns"])
                        self.assertLessEqual(exchange["start_monotonic_ns"], exchange["end_monotonic_ns"])
                        self.assertLessEqual(exchange["end_monotonic_ns"], row["probes"]["rss"]["start_monotonic_ns"])
                    measured = measure_samples(rows, rows[:1], rows[0]["collection"]["start_ns"],
                                               rows[-1]["collection"]["end_ns"], {})
                    if mode == "slow-export":
                        # Transient file export no longer blocks fresh probes;
                        # sustained export saturation is tested separately.
                        self.assertEqual([], measured["failures"], measured)
                    else:
                        self.assertTrue(any("over100ms" in failure or "probe_too_slow" in failure
                                            for failure in measured["failures"]), measured)
                        scheduled = [row["scheduled_monotonic_ns"] for row in rows]
                        self.assertTrue(any(b - a > observer.INTERVAL_NS for a, b in zip(scheduled, scheduled[1:])))
                    retain(mode, measured, raw)
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)
            self.assertTrue(all(item["stopped"] for item in observer.verify_stopped(root)))

    def test_invalid_protocol_never_exports_a_sample(self):
        for mode in ("stale", "malformed", "oversized"):
            with self.subTest(mode=mode):
                self.run_protocol(mode)

    def test_delayed_ipc_fails_the_original_validator(self):
        self.run_protocol("delayed")

    def test_slow_scan_fails_the_original_validator(self):
        self.run_protocol("slow-scan")

    def test_transient_slow_export_preserves_the_original_coverage(self):
        self.run_protocol("slow-export")

    def test_blocked_export_preserves_fresh_samples_and_original_coverage(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-export-barrier-") as directory:
            root = roots(directory)
            entered_read, entered_write = os.pipe()
            release_read, release_write = os.pipe()
            wrapper = '''
write = o.write_all
def blocked(fd, value):
    if fd != 1 and o.json.loads(value)["sequence"] == 1:
        os.write(ENTERED, str(time.monotonic_ns()).encode() + b"\\n")
        if os.read(RELEASE, 1) != b"r":
            raise RuntimeError("export barrier was not released")
    write(fd, value)
o.write_all = blocked
'''.replace("ENTERED", str(entered_write)).replace("RELEASE", str(release_read))
            code = ("import os,sys,time;sys.path.insert(0," + repr(str(REPO / "e2e/scripts")) + ")\n"
                    "import resource_observer as o\n" + wrapper + "o.main()\n")
            command = [sys.executable, "-B", "-c", code, "sample", "--root", str(root),
                       "--tmp", str(root / "tmp"), "--data", str(root / "data"),
                       "--beam-pid", str(os.getpid()), "--generation", "export-barrier"]
            process = None
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, pass_fds=(entered_write, release_read),
                                           env=dict(os.environ, PAGER="cat"))
                os.close(entered_write)
                entered_write = None
                os.close(release_read)
                release_read = None
                sequence = 0
                fresh_while_blocked = []
                for sequence in range(2):
                    self.assertTrue(select.select([process.stdout], [], [], 3)[0], "request missing")
                    request = json.loads(process.stdout.readline(4097))
                    self.assertEqual({"generation": "export-barrier", "sequence": sequence}, request)
                    value = reply(request)
                    value["binary"] = sequence + 1
                    process.stdin.write(observer.encode(value, observer.FRAME_BYTES))
                    process.stdin.flush()
                    if sequence == 1:
                        self.assertTrue(select.select([entered_read], [], [], 3)[0], "export barrier missing")
                        entered_ns = int(os.read(entered_read, 128))
                        # Hold an acknowledged output-write barrier, rather than
                        # guessing when the observer reached it from a sleep.
                        deadline = entered_ns + 150_000_000
                        sequence += 1
                        while time.monotonic_ns() < deadline:
                            remaining = max(0, deadline - time.monotonic_ns()) / 1_000_000_000
                            if not select.select([process.stdout], [], [], remaining)[0]:
                                break
                            request = json.loads(process.stdout.readline(4097))
                            self.assertEqual({"generation": "export-barrier", "sequence": sequence}, request)
                            value = reply(request)
                            value["binary"] = sequence + 1
                            process.stdin.write(observer.encode(value, observer.FRAME_BYTES))
                            process.stdin.flush()
                            fresh_while_blocked.append(sequence)
                            sequence += 1
                        released_ns = time.monotonic_ns()
                        os.write(release_write, b"r")
                while sequence < 12:
                    self.assertTrue(select.select([process.stdout], [], [], 3)[0], "request missing")
                    request = json.loads(process.stdout.readline(4097))
                    self.assertEqual({"generation": "export-barrier", "sequence": sequence}, request)
                    value = reply(request)
                    value["binary"] = sequence + 1
                    process.stdin.write(observer.encode(value, observer.FRAME_BYTES))
                    process.stdin.flush()
                    sequence += 1
                process.stdin.close()
                process.stdin = None
                process.wait(timeout=3)
                self.assertEqual(0, process.returncode, process.stderr.read().decode())
            finally:
                if process is not None:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=3)
                for fd in (entered_read, entered_write, release_read, release_write):
                    if fd is not None:
                        os.close(fd)
            self.assertTrue(all(item["stopped"] for item in observer.verify_stopped(root)))
            raw = (root / "samples.jsonl").read_bytes()
            lines = raw.splitlines(keepends=True)
            rows = [json.loads(line) for line in lines]
            self.assertEqual(list(range(12)), [row["sequence"] for row in rows])
            self.assertEqual(list(range(1, 13)), [row["binary"] for row in rows])
            measured = measure_samples(rows, rows[:1], rows[0]["collection"]["start_ns"],
                                       rows[-1]["collection"]["end_ns"], {})
            retain("blocked-export-barrier", {"entered_monotonic_ns": entered_ns,
                                              "released_monotonic_ns": released_ns,
                                              "fresh_sequences_while_blocked": fresh_while_blocked,
                                              "measurement": measured}, raw)
            self.assertGreaterEqual(released_ns - entered_ns, 150_000_000)
            self.assertGreaterEqual(len(fresh_while_blocked), 4,
                                    "synchronous export prevented fresh BEAM requests")
            self.assertEqual([], measured["failures"], measured)
            self.assertIsNone(rows[0]["previous_export"])
            for index, (previous, current, line) in enumerate(zip(rows, rows[1:], lines)):
                exported = current["previous_export"]
                self.assertEqual({"sampler_id", "sequence", "encoded_bytes", "encoding", "write"},
                                 set(exported))
                self.assertEqual(previous["sampler_id"], exported["sampler_id"])
                self.assertEqual(previous["sequence"], exported["sequence"])
                self.assertEqual(len(line), exported["encoded_bytes"])
                for name in ("encoding", "write"):
                    window = exported[name]
                    self.assertEqual(set(observer.WINDOW_KEYS) | {"cpu_ns"}, set(window))
                    self.assertTrue(all(type(value) is int for value in window.values()))
                    self.assertGreaterEqual(window["cpu_ns"], 0)
                    self.assertLessEqual(window["start_ns"], window["end_ns"])
                    self.assertLessEqual(window["start_monotonic_ns"], window["end_monotonic_ns"])
                self.assertLessEqual(previous["collection"]["end_monotonic_ns"],
                                     exported["encoding"]["start_monotonic_ns"])
                self.assertLessEqual(exported["encoding"]["end_monotonic_ns"],
                                     exported["write"]["start_monotonic_ns"])
                self.assertLessEqual(exported["write"]["end_monotonic_ns"],
                                     rows[index + 2]["previous_export"]["encoding"]["start_monotonic_ns"]
                                     if current is not rows[-1] else time.monotonic_ns())
            blocked = rows[2]["previous_export"]["write"]
            self.assertLessEqual(blocked["start_monotonic_ns"], entered_ns)
            self.assertGreaterEqual(blocked["end_monotonic_ns"], released_ns)
            self.assertGreater(blocked["end_monotonic_ns"] - blocked["start_monotonic_ns"], 100_000_000)
            self.assertLess(rows[2]["previous_export"]["encoding"]["end_monotonic_ns"] -
                            rows[2]["previous_export"]["encoding"]["start_monotonic_ns"], 100_000_000)
            for window in (rows[1]["collection"], rows[1]["beam_exchange"], *rows[1]["probes"].values()):
                self.assertLess(window["end_monotonic_ns"] - window["start_monotonic_ns"], 100_000_000)
            # A completed export appears only in its next row; the final write
            # has no following row and must not invent a terminal observation.
            self.assertEqual(list(range(11)), [row["previous_export"]["sequence"] for row in rows[1:]])
            result = public_verify(root)
            self.assertEqual(0, result.returncode, result.stderr)
            manifest = json.loads((root / "resource-export-export-barrier.json").read_text())
            self.assertEqual(12, manifest["admitted_rows"])
            self.assertEqual(12, manifest["persisted_rows"])
            self.assertEqual(128, manifest["queue_capacity"])
            self.assertEqual(2 * 1024 * 1024, manifest["queued_byte_limit"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), manifest["sha256"])
            self.assertEqual(len(raw), manifest["output_bytes"])
            retain("blocked-export-completion", manifest)


class ExportBoundsTest(unittest.TestCase):
    def writer(self, root, expires=None):
        output = os.open(root / "samples.jsonl", os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        return observer.SampleWriter(output, "writer-test", expires or time.monotonic_ns() + 10_000_000_000)

    def row(self, sequence):
        return {"sampler_id": "writer-test", "sequence": sequence}

    def test_sustained_export_stall_fails_before_admitting_an_unretainable_sample(self):
        with tempfile.TemporaryDirectory(prefix="ess-export-full-") as directory:
            root = Path(directory)
            entered, release = threading.Event(), threading.Event()
            write = observer.write_all
            def blocked(fd, value):
                if json.loads(value)["sequence"] == 0:
                    entered.set()
                    if not release.wait(timeout=3):
                        raise RuntimeError("test did not release export")
                write(fd, value)
            writer = None
            with mock.patch.object(observer, "write_all", blocked):
                try:
                    writer = self.writer(root)
                    writer.check_capacity()
                    writer.submit(self.row(0))
                    self.assertTrue(entered.wait(timeout=1))
                    for sequence in range(1, 129):
                        writer.check_capacity()
                        writer.submit(self.row(sequence))
                    self.assertEqual(128, writer.rows.qsize())
                    self.assertEqual(128, writer.rows.high_water)
                    self.assertLessEqual(sum(map(len, writer.rows.queue)), 2 * 1024 * 1024)
                    with self.assertRaisesRegex(RuntimeError, "queue saturated"):
                        writer.check_capacity()  # Production calls this before the next BEAM request.
                    self.assertEqual(129, writer.admitted)
                finally:
                    release.set()
                    if writer is not None:
                        writer.finish()
            rows = [json.loads(line) for line in (root / "samples.jsonl").read_bytes().splitlines()]
            self.assertEqual(list(range(129)), [row["sequence"] for row in rows])
            self.assertEqual(writer.admitted, writer.persisted)
            self.assertFalse(writer.thread.is_alive())
            retain("queue-saturation", {"capacity": 128, "admitted": writer.admitted,
                   "persisted": writer.persisted, "maximum_queued_rows": writer.rows.high_water})

    def test_export_errors_and_limits_fail_and_close_the_owned_writer(self):
        for mode in ("write-error", "partial-error", "row-bytes", "output-bytes", "lifetime"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="ess-export-error-") as directory:
                root = Path(directory)
                write = observer.write_all
                def faulty(fd, value):
                    if mode == "partial-error":
                        os.write(fd, value[:17])
                    if mode in ("write-error", "partial-error"):
                        raise OSError("injected export failure")
                    write(fd, value)
                with mock.patch.object(observer, "write_all", faulty), mock.patch.object(
                        observer, "MAX_OUTPUT_BYTES", 1 if mode == "output-bytes" else observer.MAX_OUTPUT_BYTES):
                    expires = time.monotonic_ns() - 1 if mode == "lifetime" else None
                    writer = self.writer(root, expires)
                    row = self.row(0)
                    if mode == "row-bytes":
                        # The collection fits, but completed-export metadata
                        # cannot be added without violating the final row cap.
                        row["padding"] = "x" * (observer.ROW_BYTES - len(observer.encode(row, observer.ROW_BYTES)) - 16)
                    writer.submit(row)
                    with self.assertRaisesRegex(RuntimeError, "export failed"):
                        writer.finish()
                self.assertFalse(writer.thread.is_alive())
                with self.assertRaises(OSError):
                    os.fstat(writer.output)
                self.assertFalse(list(root.glob("resource-export-*.json")))

    def test_raw_partial_writes_complete_without_losing_bytes(self):
        with tempfile.TemporaryDirectory(prefix="ess-export-partial-") as directory:
            root = Path(directory)
            write = os.write
            def partial(fd, value):
                return write(fd, value[:7])
            with mock.patch.object(observer.os, "write", partial):
                writer = self.writer(root)
                writer.submit(self.row(0))
                writer.finish()
            raw = (root / "samples.jsonl").read_bytes()
            self.assertEqual(0, json.loads(raw)["sequence"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), writer.digest.hexdigest())
            self.assertEqual(len(raw), writer.output_bytes)

    def export_barrier_case(self, mode):
        with tempfile.TemporaryDirectory(prefix="ess-export-drain-") as directory:
            root = roots(directory)
            entered_read, entered_write = os.pipe()
            release_read, release_write = os.pipe()
            wrapper = '''
o.EXPORT_DRAIN_SECONDS = DRAIN_VALUE
write = o.write_all
def blocked(fd, value):
    if fd != 1 and o.json.loads(value)["sequence"] == 0:
        os.write(ENTERED, b"e")
        if os.read(RELEASE, 1) != b"r":
            raise RuntimeError("export barrier was not released")
    write(fd, value)
o.write_all = blocked
'''.replace("DRAIN_VALUE", ".1" if mode == "drain-timeout" else "5").replace(
                "ENTERED", str(entered_write)).replace("RELEASE", str(release_read))
            process = None
            try:
                process = start_observer(root, mode, wrapper, (entered_write, release_read))
                os.close(entered_write)
                entered_write = None
                os.close(release_read)
                release_read = None
                respond(process, mode, 0)
                self.assertTrue(select.select([entered_read], [], [], 3)[0])
                self.assertEqual(b"e", os.read(entered_read, 1))
                count = 129 if mode == "saturation" else 3
                for sequence in range(1, count):
                    respond(process, mode, sequence)
                if mode == "saturation":
                    self.assertFalse(select.select([process.stdout], [], [], .1)[0],
                                     "a fresh sample was requested without queue capacity")
                started = time.monotonic()
                process.stdin.close()
                process.stdin = None
                if mode != "drain-timeout":
                    os.write(release_write, b"r")
                process.wait(timeout=3)
                stderr = process.stderr.read().decode()
                stopped = observer.verify_stopped(root)
                self.assertTrue(all(item["stopped"] for item in stopped))
                verified = public_verify(root)
                raw = (root / "samples.jsonl").read_bytes()
                rows = [json.loads(line) for line in raw.splitlines()]
                if mode == "drain":
                    self.assertEqual(0, process.returncode, stderr)
                    self.assertEqual(0, verified.returncode, verified.stderr)
                else:
                    self.assertNotEqual(0, process.returncode)
                    self.assertNotEqual(0, verified.returncode)
                    self.assertIn("drain deadline" if mode == "drain-timeout" else "queue saturated", stderr)
                if mode == "drain-timeout":
                    self.assertLess(time.monotonic() - started, 1)
                    self.assertEqual([], rows)
                else:
                    self.assertEqual(list(range(count)), [row["sequence"] for row in rows],
                                     "an admitted sample was silently discarded")
                retain(mode + "-shutdown", {"returncode": process.returncode, "stderr": stderr,
                       "public_verify_returncode": verified.returncode, "shutdown": stopped}, raw)
            finally:
                if process is not None:
                    if process.poll() is None:
                        os.write(release_write, b"r")
                        process.kill()
                    process.communicate(timeout=3)
                for fd in (entered_read, entered_write, release_read, release_write):
                    if fd is not None:
                        os.close(fd)

    def test_eof_drains_every_admitted_row(self):
        self.export_barrier_case("drain")

    def test_drain_timeout_is_a_process_failure_without_detached_success(self):
        self.export_barrier_case("drain-timeout")

    def test_main_saturation_stops_before_requesting_an_unretainable_fresh_sample(self):
        self.export_barrier_case("saturation")

    def test_main_limits_and_manifest_publication_fail_closed(self):
        for mode, wrapper, count in (
                ("row-limit", "o.MAX_ROWS = 1", 1),
                ("lifetime", '''
slot, clock = o.wait_slot, o.time.monotonic_ns
calls = 0
def cross_expiry(fd, deadline):
    global calls
    slot(fd, deadline)
    calls += 1
    if calls == 2:
        o.time.monotonic_ns = lambda: clock() + o.MAX_DURATION_NS + 1
o.wait_slot = cross_expiry
''', 1),
                ("manifest-error", '''
open_file = o.Path.open
def fail(self, *args, **kwargs):
    if self.name.startswith("resource-export-"):
        raise OSError("injected ENOSPC publishing completion")
    return open_file(self, *args, **kwargs)
o.Path.open = fail
''', 1)):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="ess-export-main-limit-") as directory:
                root = roots(directory)
                process = start_observer(root, mode, wrapper)
                try:
                    for sequence in range(count):
                        respond(process, mode, sequence)
                    if mode == "manifest-error":
                        process.stdin.close()
                        process.stdin = None
                    process.wait(timeout=3)
                    self.assertNotEqual(0, process.returncode)
                    self.assertTrue(all(item["stopped"] for item in observer.verify_stopped(root)))
                    self.assertNotEqual(0, public_verify(root).returncode)
                    self.assertFalse(list(root.glob("resource-export-*.json")))
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=3)


class PortIsolationTest(unittest.TestCase):
    def test_beam_rejects_malformed_stale_and_oversized_helper_requests(self):
        for mode in ("malformed", "stale", "oversized"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="ess-port-frame-") as directory:
                root = roots(directory)
                fixture = root / "e2e/scripts/resource_observer.py"
                fixture.parent.mkdir(parents=True)
                fixture.write_text('''import json,os,sys
from pathlib import Path
Path("helper-pid").write_text(str(os.getpid()))
generation = sys.argv[sys.argv.index("--generation") + 1]
value = {"generation": generation, "sequence": 1}
mode = MODE
encoded = b"bad-json\\n" if mode == "malformed" else (
    b"x" * 4097 if mode == "oversized" else json.dumps(value).encode() + b"\\n")
os.write(1, encoded)
while os.read(0, 4096):
    pass
'''.replace("MODE", repr(mode)))
                script = '''
{owner, _} = Code.eval_file(SOURCE)
ref = Process.monitor(owner)
receive do {:DOWN, ^ref, :process, ^owner, reason} ->
  true = reason != :normal
  IO.puts("OWNER_DOWN")
after 5_000 -> raise "invalid request did not stop owner" end
Process.sleep(200)
'''.replace("SOURCE", json.dumps(str(REPO / "e2e/scripts/resource_sampler.exs")))
                completed = subprocess.run(["elixir", "-e", script], cwd=root, env=environment(root),
                                           capture_output=True, text=True, timeout=10)
                self.assertEqual(0, completed.returncode, completed.stderr)
                self.assertIn("OWNER_DOWN", completed.stdout)
                pid = int((root / "helper-pid").read_text())
                actual = observer.process_identity(pid)
                self.assertTrue(actual is None or actual["state"] == "Z", actual)

    def test_fifo_contention_preserves_port_observer_coverage(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-fifo-") as directory:
            root = roots(directory)
            (root / "tmp/probe.tmp").write_bytes(b"staging")
            cache = root / "data/bucket/cache/nested"
            cache.mkdir(parents=True)
            (cache / "probe.tmp").write_bytes(b"cache-bytes")
            writers = []
            for index in (1, 2):
                path = root / f"fifo-{index}"
                os.mkfifo(path)
                writers.append(os.open(path, os.O_RDWR | os.O_NONBLOCK))
            process = None
            release = None
            try:
                process = subprocess.Popen(["elixir", str(REPO / "e2e/scripts/fixtures/resource_fifo_probe.exs")], cwd=REPO,
                    env=dict(environment(root), DIAGNOSTIC_BLOCKERS="2"),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                self.assertTrue(select.select([process.stdout], [], [], 10)[0])
                ready = json.loads(process.stdout.readline())
                self.assertEqual("hold_ready", ready["event"])
                def unblock():
                    for writer in writers:
                        os.write(writer, b"x")
                release = threading.Timer(.2, unblock)
                release.start()
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(0, process.returncode, stderr)
                complete = json.loads(stdout.strip())
                self.assertEqual("complete", complete["event"])
                self.assertTrue(all(value["duration_ns"] >= 180_000_000 for value in complete["fifo_reads"]))
                raw = (root / "samples.jsonl").read_bytes()
                rows = [json.loads(line) for line in raw.splitlines()]
                baseline = [row for row in rows if row["collection"]["end_ns"] < complete["start_ns"]]
                measured = measure_samples(rows, baseline, complete["start_ns"], complete["end_ns"], {})
                self.assertEqual([], measured["failures"], measured)
                self.assertTrue(all(row["temporary"] == 18 for row in baseline))
                receipt = observer.verify_stopped(root)
                self.assertEqual(1, len(receipt))
                retain("fifo-port-green", {"measurement": measured, "beam": complete,
                       "shutdown": receipt}, raw)
            finally:
                for writer in writers:
                    os.write(writer, b"x")
                if release is not None:
                    release.join(timeout=1)
                if process is not None and process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)
                for writer in writers:
                    os.close(writer)

    def test_private_bwrap_tmpfs_is_observed_inside_beam_namespace(self):
        self.assertIsNotNone(shutil.which("bwrap"), "private namespace acceptance requires bwrap")
        with tempfile.TemporaryDirectory(prefix="ess-observer-bwrap-") as directory:
            root = roots(directory)
            script = '''
tmp = System.fetch_env!("ESS_TMP_ROOT")
File.write!(Path.join(tmp, "private.tmp"), :binary.copy("x", 1_048_576))
Code.eval_file("e2e/scripts/resource_sampler.exs")
Process.sleep(300)
File.rm!(Path.join(tmp, "private.tmp"))
Process.sleep(150)
'''
            command = ["bwrap", "--die-with-parent", "--ro-bind", "/", "/", "--dev", "/dev",
                       "--proc", "/proc", "--bind", str(root), str(root), "--size", "8388608",
                       "--tmpfs", str(root / "tmp"), "elixir", "-e", script]
            completed = subprocess.run(command, cwd=REPO, env=environment(root), capture_output=True,
                                       text=True, timeout=15)
            self.assertEqual(0, completed.returncode, completed.stderr)
            identity = json.loads(next(root.glob("observer-*.json")).read_text())
            self.assertNotEqual(observer.namespaces(os.getpid())["mnt"], identity["namespaces"]["mnt"])
            self.assertNotEqual((root / "tmp").stat().st_dev, identity["tmp_device"])
            raw = (root / "samples.jsonl").read_bytes()
            rows = [json.loads(line) for line in raw.splitlines()]
            self.assertTrue(any(row["temporary"] == 1_048_576 for row in rows))
            self.assertEqual(0, rows[-1]["temporary"])
            self.assertEqual([], list((root / "tmp").iterdir()))
            retain("bwrap-private-tmpfs", {"identity": identity,
                   "shutdown": observer.verify_stopped(root)}, raw)

    def test_owner_and_worker_death_stop_the_other_side(self):
        for mode in ("owner", "worker"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="ess-observer-death-") as directory:
                root = roots(directory)
                script = '''
{owner, _} = Code.eval_file("e2e/scripts/resource_sampler.exs")
ref = Process.monitor(owner)
Process.sleep(300)
IO.puts("ready")
''' + ('''
Process.exit(owner, :kill)
''' if mode == "owner" else "") + '''
receive do {:DOWN, ^ref, :process, ^owner, reason} -> IO.puts(inspect(reason))
after 5_000 -> raise "Port owner did not stop" end
Process.sleep(150)
'''
                process = subprocess.Popen(["elixir", "-e", script], cwd=REPO, env=environment(root),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertTrue(select.select([process.stdout], [], [], 10)[0])
                    self.assertEqual("ready\n", process.stdout.readline())
                    identity = json.loads(next(root.glob("observer-*.json")).read_text())
                    groups = {"beam": os.getpgid(process.pid),
                              "observer": os.getpgid(identity["observer"]["pid"])}
                    if mode == "worker":
                        os.kill(identity["observer"]["pid"], signal.SIGKILL)
                    stdout, stderr = process.communicate(timeout=7)
                    self.assertEqual(0, process.returncode, stderr)
                    self.assertEqual(":killed" if mode == "owner" else ":normal", stdout.strip())
                    receipt = observer.verify_stopped(root)
                    self.assertEqual(1, len(receipt))
                    verified = public_verify(root)
                    if mode == "owner":
                        self.assertEqual(0, verified.returncode, verified.stderr)
                    else:
                        self.assertNotEqual(0, verified.returncode)
                    retain(mode + "-death", {"identity": identity, "shutdown": receipt,
                           "process_groups": groups, "public_verify_returncode": verified.returncode})
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate(timeout=5)

    def test_normal_owned_process_group_shutdown_finishes_export(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-group-stop-") as directory:
            root = roots(directory)
            script = '''
Code.eval_file("e2e/scripts/resource_sampler.exs")
Process.sleep(200)
IO.puts("ready")
receive do :never -> :ok after 10_000 -> :ok end
'''
            process = subprocess.Popen(["elixir", "-e", script], cwd=REPO, env=environment(root),
                                       start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True)
            try:
                self.assertTrue(select.select([process.stdout], [], [], 5)[0])
                self.assertEqual("ready\n", process.stdout.readline())
                identity = json.loads(next(root.glob("observer-*.json")).read_text())
                groups = {"beam": os.getpgid(process.pid),
                          "observer": os.getpgid(identity["observer"]["pid"])}
                os.killpg(process.pid, signal.SIGTERM)
                process.communicate(timeout=5)
                result = public_verify(root)
                retain("normal-group-shutdown", {"process_groups": groups,
                       "beam_returncode": process.returncode, "public_verify_returncode": result.returncode,
                       "stderr": result.stderr})
                self.assertEqual(0, result.returncode, result.stderr)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
