"""Focused Port isolation checks; the original 100ms validator is unchanged."""
import copy
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

    def test_public_cleanup_verification_can_repeat_with_a_new_observer(self):
        with tempfile.TemporaryDirectory(prefix="ess-observer-repeat-") as directory:
            root = Path(directory)
            command = [sys.executable, "-B", str(REPO / "e2e/scripts/resource_observer.py"),
                       "verify-stopped", "--root", str(root)]
            identities = []
            for _ in range(2):
                process = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"])
                try:
                    identity = observer.process_identity(process.pid)
                    identities.append(identity)
                    (root / f"observer-{process.pid}.json").write_text(json.dumps({"observer": identity}))
                finally:
                    process.terminate()
                    process.wait(timeout=3)
                result = subprocess.run(command, capture_output=True, text=True, timeout=3)
                self.assertEqual(0, result.returncode, result.stderr)
                receipt = json.loads((root / "observer-stop-verification.json").read_text())
                self.assertEqual(len(identities), len(receipt))
                self.assertEqual({item["pid"] for item in identities}, {item["expected"]["pid"] for item in receipt})
                self.assertTrue(all(item["stopped"] for item in receipt))

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
                    measured = measure_samples(rows, rows[:1], rows[0]["collection"]["start_ns"],
                                               rows[-1]["collection"]["end_ns"], {})
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

    def test_slow_export_fails_the_original_validator(self):
        self.run_protocol("slow-export")


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

    def test_deterministic_fifo_old_red_and_port_green(self):
        old = json.loads((REPO / "docs/reports/s3-hardening/sampler-dirty-io-diagnostic-r1/"
                          "rerun-1/sdio2-blocked/results.json").read_text())
        self.assertIn("cadence:rss:probe_too_slow", old["measurement"]["failures"])
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
                process = subprocess.Popen(["elixir", str(REPO / "docs/reports/s3-hardening/"
                    "sampler-dirty-io-diagnostic-r1/probe.exs")], cwd=REPO,
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
                       "shutdown": receipt, "old_red": old["measurement"]["failures"]}, raw)
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
                    if mode == "worker":
                        os.kill(identity["observer"]["pid"], signal.SIGKILL)
                    stdout, stderr = process.communicate(timeout=7)
                    self.assertEqual(0, process.returncode, stderr)
                    self.assertEqual(":killed" if mode == "owner" else ":normal", stdout.strip())
                    receipt = observer.verify_stopped(root)
                    self.assertEqual(1, len(receipt))
                    retain(mode + "-death", {"identity": identity, "shutdown": receipt})
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
