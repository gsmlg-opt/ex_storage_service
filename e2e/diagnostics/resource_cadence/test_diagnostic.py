"""Pure portability, lifecycle and fail-closed fixtures; no runtime workload."""
import ast
import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import common
import dispatch
import instrument_observer
import run
from subset import prepare

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


class DiagnosticFixtures(unittest.TestCase):
    def test_all_tools_parse_without_execution(self):
        for path in HERE.glob("*.py"):
            compile(path.read_bytes(), str(path), "exec")

    def test_original_sampling_and_coverage_constants(self):
        def constants(path):
            tree = ast.parse(path.read_text())
            return {node.targets[0].id: ast.literal_eval(node.value) for node in tree.body
                    if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                    and isinstance(node.value, ast.Constant)}
        observer = constants(REPO / "e2e/scripts/resource_observer.py")
        self.assertEqual(observer["INTERVAL_NS"], 25_000_000)
        self.assertEqual(observer["MAX_DURATION_NS"], 10_800_000_000_000)
        self.assertEqual(observer["EXPORT_QUEUE_ROWS"], 128)
        self.assertEqual(constants(REPO / "e2e/resource_cadence.py")["MAX_INTERVAL_NS"], 100_000_000)

    def test_subset_changes_only_append_after_original_assertion(self):
        source = (REPO / "e2e/s3_hardening_cluster_resources.py").read_text()
        original = ast.parse(source)
        prepared = prepare(source, "fixture.py")
        main = next(node for node in prepared.body if isinstance(node, ast.FunctionDef) and node.name == "main")
        measured = next(node for node in main.body if isinstance(node, ast.FunctionDef) and node.name == "measured")
        self.assertIsInstance(measured.body[-2], ast.Assert)
        call = measured.body.pop()
        self.assertEqual(ast.unparse(call), "_diagnostic_stop(name, size, concurrency)")
        self.assertEqual(ast.dump(prepared, include_attributes=False), ast.dump(original, include_attributes=False))
        with self.assertRaises(AssertionError):
            prepare("def main():\n def measured():\n  pass\n", "changed.py")

    def test_dispatch_targets_both_original_data_nodes_only(self):
        root = Path("/tmp/ess-s3-hardening-cluster-" + uuid.uuid4().hex[:8])
        root.mkdir(mode=0o700)
        self.addCleanup(shutil.rmtree, root)
        for node in ("data-a", "data-b", "meta"):
            args = [str(REPO / "e2e/scripts/resource_observer.py"), "sample",
                    "--root", str(root / "resource-samples" / node),
                    "--tmp", str(root / node / "tmp"), "--data", str(root / node),
                    "--beam-pid", "123", "--generation", "fixture"]
            target = dispatch.observer_target(["-B", *args])
            if node == "meta":
                self.assertIsNone(target)
            else:
                self.assertEqual(target[2:], (node, root))
                with self.assertRaises(ValueError):
                    dispatch.observer_target([*args, "--generation", "duplicate"])
                invalid = copy.deepcopy(args)
                invalid[invalid.index("--tmp") + 1] = "/tmp/foreign"
                with self.assertRaises(ValueError):
                    dispatch.observer_target(invalid)
                verify = [args[0], "verify-stopped", "--root", str(root / "resource-samples" / node)]
                self.assertEqual(dispatch.observer_target(verify)[2], node)
        self.assertIsNone(dispatch.observer_target(["-c", "print(1)"]))

    def test_raw_full_lifetime_trace_preserves_port_process(self):
        argv = dispatch.trace_command("/usr/bin/strace", Path("/tmp/fixture.fifo"),
                                      "/venv/bin/python", Path("/repo/observer.py"), ["sample"])
        self.assertEqual(argv[1:4], ["-D", "-f", "-I"])
        self.assertIn("raw=all", argv)
        self.assertIn("signal=none", argv)
        self.assertFalse(any("syscall-limit" in value or value == "-yy" for value in argv))
        self.assertNotIn("execve", dispatch.SYSCALLS.split(","))
        self.assertNotIn("network", dispatch.SYSCALLS)
        self.assertEqual(argv[-1], "sample")

    def test_probe_only_default_and_strict_mode_selection(self):
        with patch.dict(common.os.environ, {}, clear=True):
            self.assertEqual(common.diagnostic_mode(), "probe-only")
        for mode in common.MODES:
            self.assertEqual(common.diagnostic_mode(mode), mode)
        for mode in ("", "trace", "off"):
            with self.assertRaises(ValueError):
                common.diagnostic_mode(mode)
        self.assertEqual(common.datagram_limit("probe-only"), 2048)
        self.assertEqual(common.datagram_limit("syscall-trace"), 32768)

    def test_probe_only_execs_python_without_finding_or_launching_strace(self):
        with patch.object(dispatch.shutil, "which", side_effect=AssertionError("no tracer lookup")):
            argv = dispatch.observer_command("probe-only", "/venv/bin/python", Path("/repo/observer.py"),
                                              ["sample"], Path("/unused.fifo"))
        self.assertEqual(argv, ["/venv/bin/python", "-B", str(HERE / "instrument_observer.py"),
                                "/repo/observer.py", "sample"])
        with patch.object(dispatch.shutil, "which", return_value="/usr/bin/strace"):
            traced = dispatch.observer_command("syscall-trace", "/venv/bin/python", Path("/repo/observer.py"),
                                                ["sample"], Path("/used.fifo"))
        self.assertEqual(traced[:3], ["/usr/bin/strace", "-D", "-f"])

    def test_compact_snapshot_reads_schedstat_exactly_once(self):
        usage = SimpleNamespace(ru_utime=.002, ru_stime=.003, ru_nvcsw=5, ru_nivcsw=6)
        with patch.object(common, "read", return_value="100 200 3") as read, \
                patch.object(common.resource, "getrusage", return_value=usage) as getrusage:
            result = common.compact_thread_snapshot()
        read.assert_called_once_with("/proc/thread-self/schedstat", limit=128)
        getrusage.assert_called_once_with(common.resource.RUSAGE_THREAD)
        self.assertEqual(result["user_ns"], 2_000_000)
        self.assertEqual(result["system_ns"], 3_000_000)
        self.assertNotIn("status", result)
        self.assertNotIn("sched", result)

    def test_probe_only_preserves_original_window_and_sends_after_brackets(self):
        before = dict(window=dict(start=dict(wall_ns=1792672998047514496, monotonic_ns=816671699625,
                                             thread_cpu_ns=1000),
                                  end=dict(wall_ns=1792672998047514500, monotonic_ns=816671699629,
                                           thread_cpu_ns=2000)),
                      schedstat="100 200 3", user_ns=10, system_ns=20,
                      voluntary_ctxt_switches=5, nonvoluntary_ctxt_switches=6)
        after = copy.deepcopy(before)
        after["window"]["end"]["thread_cpu_ns"] = 5000
        after.update(schedstat="130 900 5", user_ns=14, system_ns=28,
                     voluntary_ctxt_switches=7, nonvoluntary_ctxt_switches=9)
        order = []
        snapshots = iter((before, after))
        def snapshot():
            order.append("snapshot")
            return next(snapshots)
        window = dict(start_ns=1792672998047514496, end_ns=1792672998155276480,
                      start_monotonic_ns=816671699625, end_monotonic_ns=816779462080)
        value = object()
        def original(function):
            order.append("original")
            self.assertEqual(function(), "fixture")
            return value, window
        channel = unittest.mock.Mock()
        channel.send.side_effect = lambda _encoded: order.append("send")
        counts = dict(sent=0, dropped=0)
        with patch.object(instrument_observer, "compact_thread_snapshot", side_effect=snapshot), \
                patch.object(instrument_observer, "thread_snapshot", side_effect=AssertionError("no full snapshot")):
            probe = instrument_observer.decorate_probe(original, "probe-only", channel, counts)
            observed_value, observed_window = probe(lambda: "fixture")
        self.assertIs(observed_value, value)
        self.assertIs(observed_window, window)
        self.assertEqual(order, ["snapshot", "original", "snapshot", "send"])
        encoded = channel.send.call_args.args[0]
        self.assertLessEqual(len(encoded), 2048)
        event = json.loads(encoded)
        self.assertEqual(event["mode"], "probe-only")
        self.assertEqual(event["original_window"], window)
        self.assertEqual(event["scheduling_delta"], dict(
            schedstat=dict(executing_ns=30, runqueue_wait_ns=700, timeslices=2),
            thread_cpu_ns=3000, user_ns=4, system_ns=8,
            voluntary_ctxt_switches=2, nonvoluntary_ctxt_switches=3))
        self.assertEqual(counts, dict(sent=1, dropped=0))

    def test_disabled_syscalls_still_require_full_delivery_and_correct_mode(self):
        instrument = dict(mode="probe-only", counts=dict(sent=4, dropped=0))
        self.assertEqual(common.delivery_errors("probe-only", False, {}, {}, instrument, 4), set())
        for bad in (None, dict(mode="probe-only", counts=dict(sent=3, dropped=0)),
                    dict(mode="probe-only", counts=dict(sent=4, dropped=1)),
                    dict(mode="syscall-trace", counts=dict(sent=4, dropped=0))):
            self.assertIn("incomplete_probe_delivery",
                          common.delivery_errors("probe-only", False, {}, {}, bad, 4))
        self.assertIn("retention_cap_exhausted", common.delivery_errors(
            "probe-only", False, {}, {"probes": 1}, instrument, 4))
        self.assertIn("unexpected_tracer_in_probe_only", common.delivery_errors(
            "probe-only", False, {123: {}}, {}, instrument, 4))
        traced = dict(mode="syscall-trace", counts=dict(sent=4, dropped=0))
        self.assertEqual(common.delivery_errors("syscall-trace", False, {}, {}, traced, 4),
                         {"no_syscall_trace", "no_fenced_tracer_identity"})

    def test_compact_datagram_loss_is_counted_without_changing_original_result(self):
        clock = dict(wall_ns=1, monotonic_ns=2, thread_cpu_ns=3)
        snapshot = dict(window=dict(start=clock, end=clock), schedstat="100 200 3",
                        user_ns=10, system_ns=20, voluntary_ctxt_switches=5,
                        nonvoluntary_ctxt_switches=6)
        for oversized in (False, True):
            window = dict(start_ns=1, end_ns=2, start_monotonic_ns=3, end_monotonic_ns=4)
            if oversized:
                window["oversized_fixture"] = "x" * 2048
            channel = unittest.mock.Mock()
            channel.send.side_effect = BlockingIOError()
            counts = dict(sent=0, dropped=0)
            with patch.object(instrument_observer, "compact_thread_snapshot", return_value=snapshot):
                probe = instrument_observer.decorate_probe(lambda _function: (42, window),
                                                            "probe-only", channel, counts)
                value, actual_window = probe(lambda: None)
            self.assertEqual(value, 42)
            self.assertIs(actual_window, window)
            self.assertEqual(counts, dict(sent=0, dropped=1))
            if oversized:
                channel.send.assert_not_called()
            else:
                channel.send.assert_called_once()

    def test_mode_change_after_initialization_cannot_launch_workload(self):
        with tempfile.TemporaryDirectory(prefix="ess-cadence-pure-") as directory:
            output = Path(directory)
            run.write(output / "diagnostic.json", dict(mode="syscall-trace"))
            with patch.dict(run.os.environ, ESS_DIAGNOSTIC_MODE="probe-only"), \
                    patch.object(run.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(ValueError, "mode changed"):
                    run.execute(sys.executable, "fixture", output)
                launch.assert_not_called()

    def test_caps_continue_draining_and_reject_later_smaller_chunks(self):
        stream = io.BytesIO()
        retention = common.Retention({"trace": stream}, {"trace": 4})
        for chunk in (b"abc", b"de", b"f"):
            retention.retain("trace", chunk)
        self.assertEqual(stream.getvalue(), b"abc")
        self.assertEqual(retention.written, {"trace": 3})
        self.assertEqual(retention.discarded, {"trace": 3})

    def test_scheduler_and_throttling_deltas_and_unavailable(self):
        old = dict(schedstat="100 200 3", status="voluntary_ctxt_switches: 3\nnonvoluntary_ctxt_switches: 4")
        new = dict(schedstat="130 900 5", status="voluntary_ctxt_switches: 5\nnonvoluntary_ctxt_switches: 7")
        self.assertEqual(common.thread_delta(old, new), dict(
            schedstat=dict(executing_ns=30, runqueue_wait_ns=700, timeslices=2),
            voluntary_ctxt_switches=2, nonvoluntary_ctxt_switches=3))
        self.assertEqual(common.thread_delta(dict(schedstat={}), {}), {})
        before = dict(ancestors={"a": {"cpu.stat": "usage_usec 10\nnr_throttled 2\nthrottled_usec 3"}})
        after = dict(ancestors={"a": {"cpu.stat": "usage_usec 14\nnr_throttled 5\nthrottled_usec 110"}})
        self.assertEqual(common.counter_delta(before, after), {
            "a": {"usage_usec": 4, "nr_throttled": 3, "throttled_usec": 107}})
        self.assertEqual(common.counter_delta(dict(ancestors={"a": {"cpu.stat": {}}}), {}), {"a": {}})

    def test_cleanup_fences_recycled_pids_and_never_signals_them(self):
        expected = dict(pid=123, start_ticks=4)
        with patch.object(common, "identity", return_value=dict(pid=123, start_ticks=5, state="S")):
            self.assertFalse(common.alive(expected))
        with patch.object(run, "alive", return_value=False), patch.object(run.os, "kill") as kill:
            run.signal_owned(expected, 15)
            kill.assert_not_called()
        with patch.object(run, "alive", return_value=True), patch.object(run.os, "kill") as kill:
            run.signal_owned(expected, 15)
            kill.assert_called_once_with(123, 15)

    def test_report_can_never_promote_diagnostic(self):
        with tempfile.TemporaryDirectory(prefix="ess-cadence-pure-") as directory:
            path = Path(directory) / "receipt.json"
            run.write(path, dict(status="diagnostic_complete", acceptance_status="passed",
                                 acceptance_promotion=True, publication_eligible=True))
            row = json.loads(path.read_text())
            for key, value in run.CLASSIFICATION.items():
                self.assertEqual(row[key], value)
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_zero_child_exit_without_both_traces_still_fails(self):
        with tempfile.TemporaryDirectory(prefix="ess-cadence-pure-") as directory:
            repo = Path(directory)
            output = repo / "reports"
            output.mkdir()
            run.write(output / "diagnostic.json", dict(status="prepared", source_head="fixture", mode="probe-only"))
            child = unittest.mock.Mock(pid=123, returncode=0)
            child.poll.return_value = 0
            with patch.object(run, "REPO", repo), patch.object(run.subprocess, "Popen", return_value=child), \
                    patch.object(run, "identity", return_value=dict(pid=123, start_ticks=4, state="Z")), \
                    patch.object(run, "alive", return_value=False), patch.object(run, "group_members", return_value=[]):
                self.assertEqual(run.execute(sys.executable, "fixture", output), 1)
            row = json.loads((output / "diagnostic.json").read_text())
            self.assertEqual(row["status"], "failed")
            self.assertIn("both_observer_full_lifetime_receipts_required", row["errors"])
            self.assertTrue(all(value["stopped"] for value in row["owned_identities"]))
            self.assertEqual(row["source_head"], "fixture")
            self.assertFalse(row["publication_eligible"])

    def test_controller_deadline_and_monitor_do_not_detach(self):
        self.assertEqual(run.TIMEOUT_SECONDS, 900)
        monitor = (HERE / "monitor.py").read_text()
        instrument = (HERE / "instrument_observer.py").read_text()
        self.assertIn("expiry = time.monotonic() + 960", monitor)
        self.assertNotIn("os.kill", monitor)
        self.assertIn("delivery_errors(mode, trace_seen", monitor)
        self.assertIn("module.probe = decorate_probe(module.probe", instrument)
        self.assertIn("return value, window", instrument)
        self.assertNotIn("module.sample =", instrument)
        self.assertNotIn("module.temporary_bytes =", instrument)


if __name__ == "__main__":
    unittest.main()
