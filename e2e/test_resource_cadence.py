"""Cadence gaps, stalled probes and raised operations cannot become passing rows."""
import ast
from contextlib import redirect_stdout
import copy
import io
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
import uuid

from resource_cadence import execute_group, measure_samples, measure_online_windows, METRICS

MS = 1_000_000
LIMITS = {"binary": 96 * 1048576, "total": 176 * 1048576, "rss": 320 * 1048576,
          "queue_max": 128, "queue_total": 2048, "temporary": 192 * 1048576}


def sample(milliseconds, duration=1, **values):
    probe = {"start_ns": milliseconds * MS, "end_ns": (milliseconds + duration) * MS,
             "start_monotonic_ns": (milliseconds - 1000) * MS,
             "end_monotonic_ns": (milliseconds + duration - 1000) * MS}
    return {"sampler_id": "generation1", "time": milliseconds + duration,
            "probes": {metric: dict(probe) for metric in METRICS},
            **dict.fromkeys(METRICS, 0), **values}


def measurement(rows, start=100, end=300, limits=LIMITS):
    return measure_samples(rows, [sample(50)], start * MS, end * MS, limits)


class CadenceTest(unittest.TestCase):
    def test_actual_probe_windows_and_group_edges_pass_at_25ms(self):
        measured = measurement([sample(i) for i in range(100, 326, 25)])
        self.assertEqual([], measured["failures"])
        self.assertEqual(26 * MS, measured["cadence"]["temporary"]["maximum_gap_ns"])
        self.assertEqual(1 * MS, measured["cadence"]["rss"]["start_edge_ns"])
        self.assertEqual(0, measured["post_idle"]["temporary"])

    def test_fast_mean_does_not_hide_one_stall(self):
        rows = [sample(i) for i in [100, 125, 150, 175, 325, 350, 375, 400]]
        measured = measurement(rows, end=400)
        self.assertIn("cadence:binary:interval_over100ms", measured["failures"])
        self.assertEqual(151 * MS, measured["cadence"]["binary"]["maximum_gap_ns"])

    def test_slow_temporary_scan_cannot_reuse_memory_cadence(self):
        rows = [sample(i) for i in range(100, 326, 25)]
        rows[3]["probes"]["temporary"]["end_ns"] += 120 * MS
        rows[3]["probes"]["temporary"]["end_monotonic_ns"] += 120 * MS
        measured = measurement(rows)
        self.assertIn("cadence:temporary:probe_too_slow", measured["failures"])
        self.assertIn("cadence:temporary:interval_over100ms", measured["failures"])
        self.assertFalse(any(failure.startswith("cadence:binary:") for failure in measured["failures"]))

    def test_stale_temporary_values_cannot_borrow_fresh_row_timestamp(self):
        rows = [sample(i) for i in range(100, 326, 25)]
        for row in rows:
            row["probes"]["temporary"] = sample(0)["probes"]["temporary"]
        self.assertIn("cadence:temporary:missing_active_samples", measurement(rows)["failures"])

    def test_missing_and_malformed_probe_timestamps_fail_explicitly(self):
        for invalid in [None, {}, {"start_ns": "nominal25ms"}]:
            rows = [sample(i) for i in range(100, 326, 25)]
            rows[3]["probes"]["rss"] = invalid
            self.assertIn("cadence:rss:missing_probe_window", measurement(rows)["failures"])

    def test_start_and_end_edges_are_required(self):
        measured = measurement([sample(225)], end=400)
        self.assertIn("cadence:binary:start_edge_over100ms", measured["failures"])
        self.assertIn("cadence:binary:end_edge_over100ms", measured["failures"])

    def test_empty_samples_preserve_explicit_failure_receipt(self):
        measured = measure_samples([], [], 100 * MS, 300 * MS, LIMITS)
        self.assertIn("missing_baseline_samples", measured["failures"])
        self.assertIn("cadence:binary:missing_active_samples", measured["failures"])
        self.assertIn("post_idle_temporary_not_zero", measured["failures"])

    def test_metric_peak_uses_its_probe_window_even_when_export_is_later(self):
        row = sample(275, binary=100)
        row["time"] = 410
        measured = measurement([sample(100), sample(175), row, sample(325)])
        self.assertEqual(100, measured["peak"]["binary"])

    def test_original_numeric_and_post_idle_bounds_remain_enforced(self):
        rows = [sample(i) for i in range(100, 326, 25)]
        rows[3]["binary"] = LIMITS["binary"] + 1
        rows[-1]["temporary"] = 1
        measured = measurement(rows)
        self.assertIn("binary", measured["failures"])
        self.assertIn("post_idle_temporary_not_zero", measured["failures"])

    def test_node_measurements_do_not_share_coverage(self):
        good = [sample(i) for i in range(100, 326, 25)]
        bad = [sample(100), sample(300), sample(325)]
        self.assertEqual([], measurement(good)["failures"])
        self.assertIn("cadence:rss:interval_over100ms", measurement(bad)["failures"])

    def test_runtime_generation_change_and_nonmonotonic_probe_fail(self):
        rows = [sample(i) for i in range(100, 326, 25)]
        rows[3]["sampler_id"] = "generation2"
        rows[4]["probes"]["binary"]["end_monotonic_ns"] -= 200 * MS
        measured = measurement(rows)
        self.assertIn("cadence:rss:sampler_generation_changed", measured["failures"])
        self.assertIn("cadence:binary:nonmonotonic_probe", measured["failures"])

    def test_one_long_overlapping_probe_cannot_cover_entire_group(self):
        measured = measurement([sample(90, duration=250), sample(350)])
        self.assertIn("cadence:rss:probe_too_slow", measured["failures"])


class OperationEvidenceTest(unittest.TestCase):
    def test_raised_operation_retains_times_and_waits_for_other_owned_tasks(self):
        barrier = threading.Barrier(2)
        completed = []
        secret = "private-exception-value"

        def operation(index):
            barrier.wait(timeout=2)
            if index == 0:
                raise RuntimeError(secret)
            time.sleep(.01)
            completed.append(index)
            return {"verified": True}

        window, failure = execute_group(operation, 2)
        self.assertIsInstance(failure, RuntimeError)
        self.assertEqual([1], completed)
        self.assertGreater(window["end_monotonic_ns"], window["start_monotonic_ns"])
        self.assertEqual([None, {"verified": True}], window["details"])
        self.assertEqual([{"index": 0, "error_type": "RuntimeError"}], window["operation_errors"])
        self.assertNotIn(secret, json.dumps(window))

    def test_success_preserves_all_details_and_original_repeat_minimum(self):
        window, failure = execute_group(lambda index: index, 4, repeat_ms=10)
        self.assertIsNone(failure)
        self.assertEqual([0, 1, 2, 3], window["details"])
        self.assertGreaterEqual(window["end_monotonic_ns"] - window["start_monotonic_ns"], 10 * MS)

    def test_both_actual_runners_save_failed_measurement_before_reraising(self):
        for filename in ["s3_hardening_resources.py", "s3_hardening_cluster_resources.py"]:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                source = ast.parse((Path(__file__).parent / filename).read_text())
                main = next(node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == "main")
                function = next(node for node in main.body if isinstance(node, ast.FunctionDef) and node.name == "measured")
                result = {"groups": []}
                error = RuntimeError("operation failed")
                window = {"start_ns": 100 * MS, "end_ns": 300 * MS, "start_ms": 100, "end_ms": 300,
                          "details": [None], "repetitions": 1, "operation_error_type": "RuntimeError"}
                saved = []
                namespace = {"time": SimpleNamespace(sleep=lambda _: None), "uuid": uuid, "json": json,
                             "result": result, "state": {}, "write_state": lambda: None,
                             "samples": lambda *_: [sample(i) for i in range(50, 326, 25)],
                             "evidence": Path(directory), "MIB": 1048576,
                             "execute_group": lambda *_args, **_kwargs: (copy.deepcopy(window), error),
                             "measure_samples": measure_samples,
                             "page_cache": lambda: {}, "retained_disk": lambda: {},
                             "staging_snapshot": lambda: {node: {"files": 0, "bytes": 0}
                                                          for node in ["meta", "data-a", "data-b"]},
                             "save": lambda: saved.append(copy.deepcopy(result))}
                exec(compile(ast.Module(body=[function], type_ignores=[]), filename, "exec"), namespace)
                with redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError) as caught:
                    namespace["measured"]("failed", 67108864, 1, lambda _: None)
                self.assertIs(error, caught.exception)
                self.assertEqual(1, len(result["groups"]))
                self.assertIn("operation_failed", result["groups"][0]["failures"])
                self.assertEqual(100 * MS, result["groups"][0]["start_ns"])
                self.assertEqual(300 * MS, result["groups"][0]["end_ns"])
                if filename == "s3_hardening_resources.py":
                    self.assertEqual(result, json.loads((Path(directory) / "results.json").read_text()))
                else:
                    self.assertEqual(result, saved[-1])


class OnlineWindowTest(unittest.TestCase):
    def rows(self):
        rows = [sample(i) for i in range(25, 326, 25)]
        for i in range(925, 1226, 25):
            row = sample(i)
            row["sampler_id"] = "generation2"
            rows.append(row)
        for row in rows:
            row["collection"] = dict(row["probes"]["binary"])
        return rows

    def windows(self):
        return [{"generation": 1, "boot_started_ns": 0, "start_ns": 100 * MS,
                 "end_ns": 300 * MS, "stop_completed_ns": 350 * MS},
                {"generation": 2, "boot_started_ns": 900 * MS, "start_ns": 1000 * MS,
                 "end_ns": 1200 * MS, "stop_completed_ns": 1250 * MS}]

    def test_planned_restart_absence_is_excluded_only_between_exact_windows(self):
        measured = measure_online_windows(self.rows(), self.windows(), {"queue_max": 128, "queue_total": 2048})
        self.assertEqual([], measured["failures"])
        self.assertEqual(["generation1"], measured["windows"][0]["sampler_ids"])
        self.assertEqual(["generation2"], measured["windows"][1]["sampler_ids"])
        self.assertEqual(100 * MS, measured["windows"][0]["start_ns"])
        self.assertEqual(300 * MS, measured["windows"][0]["end_ns"])

    def test_real_online_gap_still_fails_despite_planned_restart(self):
        rows = [row for row in self.rows() if not 125 <= row["time"] <= 250]
        measured = measure_online_windows(rows, self.windows(), {"queue_max": 128, "queue_total": 2048})
        self.assertIn("generation1:cadence:queue_max:interval_over100ms", measured["failures"])

    def test_no_telemetry_or_reused_boot_identity_cannot_pass(self):
        self.assertIn("missing_online_windows", measure_online_windows([], [], LIMITS)["failures"])
        rows = self.rows()
        for row in rows:
            row["sampler_id"] = "same-identity"
        measured = measure_online_windows(rows, self.windows(), LIMITS)
        self.assertIn("generation2:boot_sampler_identity_reused", measured["failures"])

    def test_online_queue_bound_is_independent_of_memory_cadence(self):
        rows = self.rows()
        rows[6]["queue_max"] = 129
        measured = measure_online_windows(rows, self.windows(), {"queue_max": 128, "queue_total": 2048})
        self.assertIn("generation1:queue_max", measured["failures"])
        self.assertEqual({}, {key: value for key, value in measured["windows"][0]["limits"].items()
                              if key not in ["queue_max", "queue_total"]})


class FaultLifecycleTest(unittest.TestCase):
    def classes(self):
        class FakeRuntime:
            def __init__(self, root, port):
                self.root, self.port, self.generation = root, port, 0
                self.env, self.requires, self.handles = {}, [], []
                self.name, self.cookie, self.process = "test-node", "private-cookie", None

            def start(self):
                self.generation += 1
                self.process = SimpleNamespace(poll=lambda: None)

            def stop(self, crash=False):
                self.process = None

        module = ast.parse((Path(__file__).parent / "s3_hardening_faults.py").read_text())
        classes = [node for node in module.body if isinstance(node, ast.ClassDef)]
        launches = []
        namespace = {"Runtime": FakeRuntime, "time": time, "MIB": 1048576,
                     "subprocess": SimpleNamespace(STDOUT=None, Popen=lambda argv, **kwargs:
                        launches.append((argv, kwargs)) or SimpleNamespace(poll=lambda: None)),
                     "wait_http": lambda *_: None}
        exec(compile(ast.Module(body=classes, type_ignores=[]), "s3_hardening_faults.py", "exec"), namespace)
        return namespace, launches

    def test_runtime_windows_record_exact_health_and_stop_boundaries_per_boot(self):
        namespace, _ = self.classes()
        with tempfile.TemporaryDirectory() as directory:
            runtime = namespace["FaultRuntime"](Path(directory), 1234)
            runtime.start()
            runtime.stop(crash=True)
            runtime.start()
            runtime.stop()
            runtime.stop()
            self.assertEqual(2, len(runtime.online_windows))
            self.assertEqual(["SIGKILL", "SIGTERM"], [row["stop_kind"] for row in runtime.online_windows])
            for row in runtime.online_windows:
                self.assertLessEqual(row["boot_started_ns"], row["start_ns"])
                self.assertLessEqual(row["start_ns"], row["end_ns"])
                self.assertLessEqual(row["end_ns"], row["stop_completed_ns"])
                self.assertFalse(row["unexpectedly_exited"])

    def test_full_disk_runtime_keeps_private8mib_guard_and_loads_own_sampler(self):
        namespace, launches = self.classes()
        with tempfile.TemporaryDirectory() as directory:
            runtime = namespace["FullDiskRuntime"](Path(directory), 1234)
            try:
                runtime.start()
                argv, options = launches[0]
                self.assertEqual(str(8 * 1048576), argv[argv.index("--size") + 1])
                self.assertEqual("e2e/scripts/resource_sampler.exs", argv[argv.index("-r") + 1])
                self.assertEqual(directory, options["env"]["E2E_RESOURCE_ROOT"])
                self.assertEqual(str(Path(directory) / "data/blob/tmp"), options["env"]["ESS_TMP_ROOT"])
            finally:
                for handle in runtime.handles:
                    handle.close()


class DiagnosticRetentionTest(unittest.TestCase):
    def test_actual_cluster_finalizer_retains_only_explicit_trace_file_and_fails_caps(self):
        source = ast.parse((Path(__file__).parent / "s3_hardening_cluster_resources.py").read_text())
        main = next(node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == "main")
        finalizer = next(node.finalbody for node in main.body if isinstance(node, ast.Try) and node.finalbody)
        for marker in ["started", "event_limit_reached", "time_limit_reached"]:
            with self.subTest(marker=marker), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "root"
                evidence = Path(directory) / "evidence"
                (root / "data-a").mkdir(parents=True)
                evidence.mkdir()
                trace = json.dumps({"coverage": marker}) + "\n"
                (root / "data-a/sanitized-upload-errors.jsonl").write_text(trace)
                (root / "data-a/private-state.json").write_text("private-state-must-not-be-copied")
                result = {"status": "passed"}
                namespace = {"root": root, "evidence": evidence, "json": json, "result": result,
                             "proxy": SimpleNamespace(close=lambda: None),
                             "sink": SimpleNamespace(close=lambda: None),
                             "save": lambda: (evidence / "results.json").write_text(json.dumps(result))}
                code = compile(ast.Module(body=finalizer, type_ignores=[]), "cluster-finalizer", "exec")
                if marker == "started":
                    exec(code, namespace)
                    self.assertEqual("passed", result["status"])
                else:
                    with self.assertRaisesRegex(AssertionError, "diagnostic coverage exhausted"):
                        exec(code, namespace)
                    self.assertEqual(["data-a:" + marker], result["diagnostic_coverage_failures"])
                    self.assertEqual("failed", result["status"])
                self.assertEqual(trace, (evidence / "sanitized-upload-errors-data-a.jsonl").read_text())
                self.assertFalse((evidence / "private-state.json").exists())
                self.assertEqual(result, json.loads((evidence / "results.json").read_text()))


if __name__ == "__main__":
    unittest.main()
