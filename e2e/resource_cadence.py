"""Actual probe coverage for resource acceptance; numeric bounds live in runners."""
from concurrent.futures import ThreadPoolExecutor
import statistics
import time

MAX_INTERVAL_NS = 100_000_000
METRICS = ("binary", "total", "rss", "queue_max", "queue_total", "temporary")
BASELINE_METRICS = ("binary", "total", "rss", "temporary")


def execute_group(operation, concurrency, repeat_ms=0):
    """Retain the complete executor window even when one operation raises."""
    window = {"start_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns(),
              "repetitions": 0, "details": []}
    failure = None
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            while window["repetitions"] == 0 or time.monotonic_ns() - window["start_monotonic_ns"] < repeat_ms * 1_000_000:
                futures = [pool.submit(operation, index) for index in range(concurrency)]
                details, errors = [], []
                for index, future in enumerate(futures):
                    try:
                        details.append(future.result())
                    except BaseException as error:
                        details.append(None)
                        errors.append({"index": index, "error_type": type(error).__name__})
                        if failure is None:
                            failure = error
                window["details"] = details
                window["repetitions"] += 1
                if errors:
                    window["operation_errors"] = errors
                    break
    except BaseException as error:
        failure = error
    finally:
        window.update(end_ns=time.time_ns(), end_monotonic_ns=time.monotonic_ns())
        window.update(start_ms=window["start_ns"] // 1_000_000,
                      end_ms=window["end_ns"] // 1_000_000)
        if failure is not None:
            # Exception values may contain private request state. Retain type
            # only; the runner still raises the original after saving evidence.
            window["operation_error_type"] = type(failure).__name__
    return window, failure


def measure_samples(rows, baseline, start_ns, end_ns, limits):
    """Use each metric's own probe window, never a nominal configured interval.

    A probe spans its real scan duration. End(current)-start(previous) is the
    conservative possible observation gap; both group edges use the same bound.
    A stale temporary scan therefore cannot borrow a fresh memory timestamp.
    """
    failures = []
    base = {key: int(statistics.median(row[key] for row in baseline))
            for key in BASELINE_METRICS} if baseline else {}
    if not baseline:
        failures.append("missing_baseline_samples")
    result = {"samples": sum(start_ns // 1_000_000 <= row["time"] <= end_ns // 1_000_000
                             for row in rows),
              "baseline": base, "peak": {}, "delta": {}, "limits": limits,
              "post_idle": rows[-1] if rows else {}, "cadence": {}, "failures": failures}
    for metric in METRICS:
        observations = []
        malformed = False
        for row in rows:
            probe = row.get("probes", {}).get(metric)
            if not isinstance(probe, dict) or any(
                    not isinstance(probe.get(key), int) for key in
                    ("start_ns", "end_ns", "start_monotonic_ns", "end_monotonic_ns")):
                if start_ns // 1_000_000 <= row["time"] <= end_ns // 1_000_000:
                    malformed = True
                continue
            if probe["end_ns"] >= start_ns and probe["start_ns"] <= end_ns:
                observations.append((row, probe))
        coverage = {"samples": len(observations), "max_interval_ns": MAX_INTERVAL_NS}
        result["cadence"][metric] = coverage
        if malformed:
            failures.append("cadence:" + metric + ":missing_probe_window")
        if not observations:
            failures.append("cadence:" + metric + ":missing_active_samples")
            continue
        peak = max(row[metric] for row, _ in observations)
        result["peak"][metric] = peak
        result["delta"][metric] = peak - base.get(metric, 0)
        if metric in limits and result["delta"][metric] > limits[metric]:
            failures.append(metric)
        windows = [probe for _, probe in observations]
        generations = {row.get("sampler_id") for row, _ in observations}
        durations = [probe["end_monotonic_ns"] - probe["start_monotonic_ns"] for probe in windows]
        gaps = [current["end_monotonic_ns"] - previous["start_monotonic_ns"]
                for previous, current in zip(windows, windows[1:])]
        start_edge = max(0, windows[0]["end_ns"] - start_ns)
        end_edge = max(0, end_ns - windows[-1]["start_ns"])
        coverage.update(start_edge_ns=start_edge, end_edge_ns=end_edge,
                        maximum_probe_ns=max(durations), maximum_gap_ns=max(gaps, default=0),
                        intervals_over100ms=sum(gap > MAX_INTERVAL_NS for gap in gaps),
                        sampler_ids=sorted(str(value) for value in generations))
        if None in generations or len(generations) != 1:
            failures.append("cadence:" + metric + ":sampler_generation_changed")
        if any(duration < 0 for duration in durations) or any(gap <= 0 for gap in gaps):
            failures.append("cadence:" + metric + ":nonmonotonic_probe")
        if any(probe["end_ns"] < probe["start_ns"] for probe in windows):
            failures.append("cadence:" + metric + ":invalid_wall_window")
        if max(durations) > MAX_INTERVAL_NS:
            failures.append("cadence:" + metric + ":probe_too_slow")
        if max(gaps, default=0) > MAX_INTERVAL_NS:
            failures.append("cadence:" + metric + ":interval_over100ms")
        if start_edge > MAX_INTERVAL_NS:
            failures.append("cadence:" + metric + ":start_edge_over100ms")
        if end_edge > MAX_INTERVAL_NS:
            failures.append("cadence:" + metric + ":end_edge_over100ms")
    if result["post_idle"].get("temporary") != 0:
        failures.append("post_idle_temporary_not_zero")
    return result


def measure_online_windows(rows, windows, limits):
    """Audit exact health-ready to stop-request windows, separately for each boot.

    Boot/stop completion timestamps identify the sampler generation and retain
    startup/shutdown absence explicitly. They never pad an online window.
    """
    measurements, failures, seen_ids = [], [], set()
    for window in windows:
        boot_rows = [row for row in rows if window["boot_started_ns"] <=
                     row.get("collection", {}).get("start_ns", -1) <= window["stop_completed_ns"]]
        sampler_ids = sorted({row.get("sampler_id") for row in boot_rows if row.get("sampler_id")})
        baseline = [row for row in boot_rows if row["collection"]["end_ns"] <= window["start_ns"]][-10:]
        measured = measure_samples(boot_rows, baseline, window["start_ns"], window["end_ns"], limits)
        if len(sampler_ids) != 1:
            measured["failures"].append("boot_sampler_identity_missing_or_ambiguous")
        if seen_ids.intersection(sampler_ids):
            measured["failures"].append("boot_sampler_identity_reused")
        seen_ids.update(sampler_ids)
        measured.update(**window, sampler_ids=sampler_ids)
        measurements.append(measured)
        failures.extend(f"generation{window['generation']}:{failure}" for failure in measured["failures"])
    if not windows:
        failures.append("missing_online_windows")
    return {"windows": measurements, "failures": failures,
            "excluded_startup_shutdown": "Only boot-start→health-ready and stop-request→stop-complete are excluded; exact timestamps retained. No arbitrary padding."}
