#!/usr/bin/env python3
"""Private terminal C7 resource evidence audit; never runs ESS or live probes.

Only --self-test is allowed while the coordinating owner runs heavy workloads.
The real audit streams exact selected evidence; it does not interpolate samples,
change envelopes, archive data, inspect host processes or certify all A30 clauses.
"""
import argparse
from collections import Counter
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import tempfile
from types import SimpleNamespace
from unittest import mock

CHUNK = 1024 * 1024
JSON_LIMIT = 64 * CHUNK
REPORTS = Path("docs/reports/s3-hardening")
CANDIDATE = REPORTS / "closeout-002/candidate-c7"
OUTPUT = CANDIDATE / "resource-observation-analysis.json"
PUBLIC_TMP = Path("/tmp")
PHYSICAL_TMP = Path("/fast/build/ess-closeout-c7-acceptance/tmp")
METRICS = ("binary", "total", "rss", "queue_max", "queue_total", "temporary")
BASELINES = ("binary", "total", "rss", "temporary")
WINDOW = ("start_ns", "end_ns", "start_monotonic_ns", "end_monotonic_ns")
READBACKS = {"gateway-put": "gateway-put-upstream-readback",
             "gateway-slow-put": "gateway-slow-put-upstream-readback",
             "gateway-paced-upstream-put": "gateway-paced-upstream-put-readback"}


class AuditError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise AuditError(message)


def integer(value):
    return type(value) is int


def regular(path):
    require(path.is_file() and not path.is_symlink() and path.resolve() == path,
            "Missing/nonregular/symlink input")


def sha(path):
    regular(path)
    h = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(CHUNK):
            h.update(block)
    return h.hexdigest()


def read_json(path, limit=JSON_LIMIT):
    regular(path)
    with path.open("rb") as source:
        data = source.read(limit + 1)
    require(len(data) <= limit, "JSON input exceeds declared bound")
    return json.loads(data)


def load_api(repo, name, relative):
    path = repo / relative
    regular(path)
    spec = importlib.util.spec_from_file_location("c7_audit_" + name, path)
    module = importlib.util.module_from_spec(spec)
    # Importing these stdlib-only modules does not invoke their service/host APIs.
    sys.dont_write_bytecode = True
    spec.loader.exec_module(module)
    return module


def scoped(repo, path):
    require(path.is_absolute() and path.resolve() == path, "Exact absolute evidence path required")
    relative = path.relative_to(repo / REPORTS)
    permitted = path.is_relative_to(repo / CANDIDATE) or relative.parts[0].startswith("closeout002-c7")
    require(permitted, "Input outside fixed C7 evidence scope")
    return path


def mapped_fixture(path, physical):
    """Map only public /tmp fixture paths to the explicitly selected physical tmp."""
    path = Path(path)
    require(path.is_absolute() and ".." not in path.parts, "Unsafe fixture path rejected")
    if path.is_relative_to(physical):
        return path
    require(path.is_relative_to(PUBLIC_TMP), "Non-public fixture path rejected")
    return physical / path.relative_to(PUBLIC_TMP)


def identity(row):
    return row["name"], row["size"], row["concurrency"]


def counter_rows(counter):
    return [dict(name=name, size=size, concurrency=concurrency, count=count)
            for (name, size, concurrency), count in sorted(counter.items())]


def limits_for(size, concurrency):
    return {"binary": (64 + 32 * concurrency) * CHUNK,
            "total": (128 + 48 * concurrency) * CHUNK,
            "rss": (256 + 64 * concurrency) * CHUNK,
            "queue_max": 128, "queue_total": 2048,
            "temporary": 2 * size * concurrency + 64 * CHUNK}


def observations(group):
    return group["nodes"] if "nodes" in group else {"local": group}


def hash_value(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def verify_staging(value, mode, phase):
    """Require exact empty file/byte inventories, including zero-byte stages."""
    prefix = mode + "_" + phase + "_staging"
    if mode == "local":
        snapshots = {"local": value}
    else:
        if not isinstance(value, dict) or set(value) != {"meta", "data-a", "data-b"}:
            return [prefix + ":node_coverage"]
        snapshots = value
    failures = []
    for node, snapshot in snapshots.items():
        if (not isinstance(snapshot, dict) or set(snapshot) != {"files", "bytes"} or
                any(not integer(count) or count < 0 for count in snapshot.values())):
            failures.append(prefix + ":" + node + ":malformed_inventory")
        elif any(snapshot.values()):
            failures.append(prefix + ":" + node + ":not_zero")
    return failures


def verify_uploads(data, expected):
    expected_ids = Counter((READBACKS[row["name"]], row["size"], row["concurrency"])
                           for row in expected if row["name"] in READBACKS)
    rows = data.get("upload_verifications", [])
    actual = Counter(map(identity, rows))
    result = {"required": sum(expected_ids.values()), "recorded": len(rows),
              "missing": counter_rows(expected_ids - actual),
              "unexpected": counter_rows(actual - expected_ids), "cases": [], "failures": []}
    for row in rows:
        errors = []
        count, size, wanted = row["concurrency"], row["size"], row.get("expected_sha256")
        objects = row.get("objects", [])
        if not (row.get("status") == "passed" and hash_value(wanted) and
                row.get("checked_objects") == count and row.get("check_count") == 3 * count and
                len(objects) == count and len({item.get("path") for item in objects}) == count):
            errors.append("incomplete_independent_readback")
        if any(item.get("bytes") != size or item.get("sha256") != wanted or
               item.get("metadata_sha256") != wanted for item in objects):
            errors.append("independent_readback_length_hash_or_metadata_mismatch")
        result["cases"].append({key: row.get(key) for key in
                                ("name", "size", "concurrency", "status", "checked_objects", "check_count")}
                               | {"audit_failures": errors})
        result["failures"].extend(errors)
    if expected_ids != actual:
        result["failures"].append("independent_readback_case_coverage")
    result["status"] = "passed" if not result["failures"] else "incomplete_or_failed"
    return result


def verify_cluster_details(group):
    failures = []
    size, concurrency = group["size"], group["concurrency"]
    details = group.get("details", [])
    if len(details) != concurrency:
        return ["cluster_detail_count"]

    def cas(rows, expected_hash=None):
        return (isinstance(rows, list) and len(rows) == 2 and
                {item.get("node") for item in rows} == {"data-a", "data-b"} and
                len({item.get("hash") for item in rows}) == 1 and
                all(item.get("verified") is True and item.get("ready_replicas") == 2 and
                    item.get("size") == size and hash_value(item.get("hash")) and
                    (expected_hash is None or item.get("hash") == expected_hash) for item in rows))

    for detail in details:
        valid = False
        if group["name"] == "strict-quorum-replica-put":
            valid = cas(detail)
        elif group["name"] == "authenticated-peer-get" and isinstance(detail, dict):
            valid = (detail.get("bytes") == size and hash_value(detail.get("hash")) and
                     detail.get("authenticated_peer") == "data-b")
        elif isinstance(detail, dict):
            job = detail.get("job", {})
            valid_job = (job.get("completed") is True and job.get("fenced") is True and
                         job.get("size") == size and hash_value(job.get("hash")))
            if group["name"] in ("fenced-repair-pull", "fenced-repair-push"):
                valid = valid_job and job.get("kind") == "repair_blob" and cas(detail.get("cas"), job.get("hash"))
            elif group["name"] == "cross-cluster-worker-paced-sink":
                sink = detail.get("independent_sink", {})
                valid = (valid_job and job.get("kind") == "cross_cluster_put" and
                         sink.get("authenticated") is True and sink.get("size") == size and
                         sink.get("hash") == job.get("hash"))
        if not valid:
            failures.append("cluster_length_hash_authentication_or_fence_assertion")
    return failures


class Measurement:
    """Bounded streaming equivalent of measure_samples using retained baselines.

    Baseline medians are retained harness inputs: their precise pre-executor
    snapshot boundary is not reconstructed from later sample export timing.
    """
    def __init__(self, group, recorded, bound):
        self.group, self.recorded, self.bound = group, recorded, bound
        self.samples = 0
        self.states = {metric: {"count": 0, "generations": set(), "maximum_gap_ns": 0,
                                "maximum_probe_ns": 0, "intervals_over100ms": 0,
                                "malformed": False, "negative_duration": False,
                                "nonpositive_gap": False, "invalid_wall": False}
                       for metric in METRICS}
        self.post_seen = False
        self.post_matches = False

    def add(self, row):
        if self.post_seen:
            return
        start, end = self.group["start_ns"], self.group["end_ns"]
        active = start // 1_000_000 <= row["time"] <= end // 1_000_000
        self.samples += active
        for metric, state in self.states.items():
            probe = row.get("probes", {}).get(metric)
            if not isinstance(probe, dict) or any(not integer(probe.get(key)) for key in WINDOW):
                state["malformed"] |= active
                continue
            if probe["end_ns"] < start or probe["start_ns"] > end:
                continue
            value = row.get(metric)
            require(integer(value) and value >= 0, "Invalid resource sample metric")
            duration = probe["end_monotonic_ns"] - probe["start_monotonic_ns"]
            state["maximum_probe_ns"] = max(state["maximum_probe_ns"], duration)
            state["negative_duration"] |= duration < 0
            state["invalid_wall"] |= probe["end_ns"] < probe["start_ns"]
            state["peak"] = max(state.get("peak", value), value)
            state["generations"].add(row.get("sampler_id"))
            if state["count"]:
                gap = probe["end_monotonic_ns"] - state["previous"]["start_monotonic_ns"]
                state["maximum_gap_ns"] = max(state["maximum_gap_ns"], gap)
                state["intervals_over100ms"] += gap > self.bound
                state["nonpositive_gap"] |= gap <= 0
            else:
                state["first"] = probe
            state["previous"] = probe
            state["count"] += 1
        post = self.recorded.get("post_idle", {})
        if (row.get("sampler_id"), row.get("sequence")) == (post.get("sampler_id"), post.get("sequence")):
            self.post_seen, self.post_matches = True, row == post

    def finish(self):
        group, recorded = self.group, self.recorded
        base = recorded.get("baseline", {})
        failures, peak, delta, cadence = [], {}, {}, {}
        if any(not integer(base.get(metric)) for metric in BASELINES):
            failures.append("missing_baseline_samples")
        for metric, state in self.states.items():
            current = {"samples": state["count"], "max_interval_ns": self.bound}
            cadence[metric] = current
            if state["malformed"]:
                failures.append("cadence:" + metric + ":missing_probe_window")
            if not state["count"]:
                failures.append("cadence:" + metric + ":missing_active_samples")
                continue
            peak[metric] = state["peak"]
            delta[metric] = peak[metric] - base.get(metric, 0)
            if metric in recorded.get("limits", {}) and delta[metric] > recorded["limits"][metric]:
                failures.append(metric)
            first, last = state["first"], state["previous"]
            current.update(start_edge_ns=max(0, first["end_ns"] - group["start_ns"]),
                           end_edge_ns=max(0, group["end_ns"] - last["start_ns"]),
                           maximum_probe_ns=state["maximum_probe_ns"], maximum_gap_ns=state["maximum_gap_ns"],
                           intervals_over100ms=state["intervals_over100ms"],
                           sampler_ids=sorted(str(value) for value in state["generations"]))
            flags = {"sampler_generation_changed": None in state["generations"] or len(state["generations"]) != 1,
                     "nonmonotonic_probe": state["negative_duration"] or state["nonpositive_gap"],
                     "invalid_wall_window": state["invalid_wall"],
                     "probe_too_slow": state["maximum_probe_ns"] > self.bound,
                     "interval_over100ms": state["maximum_gap_ns"] > self.bound,
                     "start_edge_over100ms": current["start_edge_ns"] > self.bound,
                     "end_edge_over100ms": current["end_edge_ns"] > self.bound}
            failures.extend("cadence:" + metric + ":" + name for name, bad in flags.items() if bad)
        if recorded.get("post_idle", {}).get("temporary") != 0:
            failures.append("post_idle_temporary_not_zero")
        if not self.post_seen or not self.post_matches:
            failures.append("post_idle_not_exact_exported_sample")
        measured = {"samples": self.samples, "peak": peak, "delta": delta, "cadence": cadence}
        mismatches = [key for key, value in measured.items() if recorded.get(key) != value]
        if mismatches:
            failures.append("recorded_measurement_mismatch:" + ",".join(mismatches))
        return {**measured, "baseline": base, "limits": recorded.get("limits"),
                "post_idle": {key: recorded.get("post_idle", {}).get(key) for key in
                              ("time", "sampler_id", "sequence", *METRICS)},
                "post_idle_verified": self.post_seen and self.post_matches, "audit_failures": failures,
                "recorded_failures": recorded.get("failures", [])}


def audit_samples(root, measurements, observer):
    identities = sorted(path for path in root.glob("observer-*.json")
                        if path.name != "observer-stop-verification.json")
    result = {"root": str(root), "status": "failed", "exports": [], "failures": [],
              "cleanup": {"status": "not_verified", "basis": "retained same-namespace stop receipt only; no live host process scan"}}
    require(identities, "Missing observer generation identities")
    stops = read_json(root / "observer-stop-verification.json", 65536)
    require(isinstance(stops, list), "Observer stop verification did not pass")
    by_name = {Path(item["identity_file"]).name: item for item in stops}
    require(len(by_name) == len(stops) == len(identities), "Observer stop receipt generation coverage mismatch")
    mapped_stops, identity_receipts = [], []
    for path in identities:
        identity_data = read_json(path, observer.ROW_BYTES)
        item = by_name.get(path.name)
        require(item is not None and item.get("expected") == identity_data["observer"] and item.get("stopped") is True,
                "Observer stop receipt identity mismatch")
        actual, expected = item.get("observed"), identity_data["observer"]
        require(actual is None or actual.get("start_ticks") != expected["start_ticks"] or actual.get("state") == "Z",
                "Observer stop receipt retains live identity")
        physical_reference = mapped_fixture(item["identity_file"], PHYSICAL_TMP)
        identity_receipts.append({"generation": identity_data["generation"], "expected": expected,
                                  "observed_at_harness_shutdown": actual, "stopped": True,
                                  "public_or_physical_identity_reference": item["identity_file"],
                                  "mapped_physical_reference": str(physical_reference),
                                  "retained_identity_sha256": sha(path),
                                  "recorded_namespaces": identity_data.get("namespaces")})
        mapped_stops.append({"identity_file": str(path), "expected": expected, "stopped": True})
    for item in mapped_stops:
        generation = read_json(Path(item["identity_file"]), observer.ROW_BYTES)["generation"]
        regular(root / ("resource-export-" + generation + ".json"))
    sample = root / "samples.jsonl"
    regular(sample)
    observer.verify_exports(root, mapped_stops)
    total_rows, global_peak, observer_peak, schedule_lag, collection_ns, exchange_ns = 0, {}, 0, 0, 0, 0
    export_encoding_ns = export_write_ns = 0
    with sample.open("rb") as source:
        for item in sorted(mapped_stops, key=lambda row: row["export"]["output_start"]):
            manifest = item["export"]
            require(source.tell() == manifest["output_start"], "Noncontiguous physical export extent")
            h, count, size = hashlib.sha256(), 0, 0
            while size < manifest["output_bytes"]:
                line = source.readline(manifest["row_byte_limit"] + 1)
                require(line and line.endswith(b"\n") and len(line) <= manifest["row_byte_limit"] and
                        size + len(line) <= manifest["output_bytes"], "Invalid resource export row extent")
                h.update(line)
                row = json.loads(line)
                require(row.get("sampler_id") == manifest["generation"] and integer(row.get("sequence")) and
                        row["sequence"] == count, "Resource generation/sequence mismatch")
                for measurement in measurements:
                    measurement.add(row)
                for metric in METRICS:
                    require(integer(row.get(metric)) and row[metric] >= 0, "Invalid resource sample metric")
                    global_peak[metric] = max(global_peak.get(metric, 0), row[metric])
                observer_peak = max(observer_peak, row.get("observer", {}).get("rss", 0))
                collection = row.get("collection", {})
                exchange = row.get("beam_exchange", {})
                require(all(integer(collection.get(key)) and integer(exchange.get(key)) for key in WINDOW) and
                        integer(row.get("scheduled_monotonic_ns")), "Missing native timing window")
                schedule_lag = max(schedule_lag, collection["start_monotonic_ns"] - row["scheduled_monotonic_ns"])
                collection_ns = max(collection_ns, collection["end_monotonic_ns"] - collection["start_monotonic_ns"])
                exchange_ns = max(exchange_ns, exchange["end_monotonic_ns"] - exchange["start_monotonic_ns"])
                previous = row.get("previous_export")
                if count:
                    require(isinstance(previous, dict) and previous.get("sampler_id") == manifest["generation"] and
                            previous.get("sequence") == count - 1, "Previous export attribution mismatch")
                    for key in ("encoding", "write"):
                        window = previous.get(key, {})
                        require(all(integer(window.get(part)) for part in WINDOW), "Missing export timing window")
                    export_encoding_ns = max(export_encoding_ns, previous["encoding"]["end_monotonic_ns"] - previous["encoding"]["start_monotonic_ns"])
                    export_write_ns = max(export_write_ns, previous["write"]["end_monotonic_ns"] - previous["write"]["start_monotonic_ns"])
                else:
                    require(previous is None, "First generation row has previous export")
                size += len(line)
                count += 1
            require(count == manifest["admitted_rows"] == manifest["persisted_rows"] and
                    size == manifest["output_bytes"] and h.hexdigest() == manifest["sha256"] and
                    manifest["last_sequence"] == (count - 1 if count else None),
                    "Resource export count/extent/hash mismatch")
            total_rows += count
            result["exports"].append({"manifest": str(root / ("resource-export-" + manifest["generation"] + ".json")),
                                      "generation": manifest["generation"], "verified_rows": count, "bytes": size,
                                      "sha256": h.hexdigest(), "status": "passed"})
        require(source.read(1) == b"", "Unbound trailing resource sample bytes")
    result.update(status="passed", verified_rows=total_rows, global_peak=global_peak,
                  observer_peak_rss_bytes=observer_peak, maximum_schedule_lag_ns=schedule_lag,
                  maximum_collection_ns=collection_ns, maximum_beam_exchange_ns=exchange_ns,
                  maximum_previous_export_encoding_ns=export_encoding_ns,
                  maximum_previous_export_write_ns=export_write_ns)
    result["cleanup"].update(status="passed", retained_stop_receipt_sha256=sha(root / "observer-stop-verification.json"),
                             verified_observer_generations=len(identities), identities=identity_receipts,
                             beam_process_death="not independently certified")
    return result


def matrix(repo, path, mode, args, cases, cadence, observer):
    data = read_json(path)
    expected = cases.manifest("all" if mode == "local" else "cluster")
    declared, actual, wanted = Counter(map(identity, data.get("required_cases", []))), Counter(map(identity, data.get("groups", []))), Counter(map(identity, expected))
    failures = []
    if data.get("status") not in ("passed", "failed"):
        failures.append("nonterminal_results")
    if data.get("status") != "passed":
        failures.append("harness_status_not_passed")
    if declared != wanted:
        failures.append("declared_manifest_differs_from_current_contract")
    if actual != wanted:
        failures.append("missing_duplicated_or_unexpected_cases")
    if data.get("starting_sha") != args.candidate_sha or data.get("source_sha256") != args.source_fingerprint or data.get("ending_sha", args.candidate_sha) != args.candidate_sha:
        failures.append("candidate_source_identity_mismatch")
    if data.get("cleanup_errors") or data.get("diagnostic_coverage_failures"):
        failures.append("harness_cleanup_or_diagnostic_failure")
    groups, by_node = [], {"local": []} if mode == "local" else {node: [] for node in ("meta", "data-a", "data-b")}
    for group in data.get("groups", []):
        errors = list(group.get("failures", []))
        if group.get("operation_error_type") or group.get("operation_errors"):
            errors.append("operation_failed")
        duration = group.get("end_monotonic_ns", 0) - group.get("start_monotonic_ns", 0)
        repetitions = group.get("repetitions", 0)
        if not integer(duration) or duration <= 0 or not integer(repetitions) or repetitions <= 0:
            errors.append("invalid_group_window_or_repetitions")
        group_report = {key: group.get(key) for key in ("name", "size", "concurrency", "repetitions", "operation_error_type", "operation_errors")}
        payload = group["size"] * group["concurrency"] * repetitions
        group_report.update(duration_seconds=duration / 1e9, logical_payload_bytes=payload,
                            logical_payload_mib_per_second=(payload / CHUNK / (duration / 1e9) if duration > 0 else None),
                            recorded_failures=list(group.get("failures", [])), audit_failures=errors, nodes={},
                            staging_before=group.get("staging_before"), staging_after=group.get("staging_after"))
        errors.extend(verify_staging(group.get("staging_before"), mode, "before"))
        errors.extend(verify_staging(group.get("staging_after"), mode, "after"))
        if mode == "cluster":
            errors.extend(verify_cluster_details(group))
            if set(group.get("nodes", {})) != set(by_node):
                errors.append("cluster_node_measurement_coverage")
            group_report.update(page_cache_host_before=group.get("page_cache_host_before"),
                                page_cache_host_after=group.get("page_cache_host_after"))
        for node, recorded in observations(group).items():
            if node not in by_node:
                continue
            if recorded.get("limits") != limits_for(group["size"], group["concurrency"]):
                errors.append(node + ":predeclared_envelope_mismatch")
            measurement = Measurement(group, recorded, cadence.MAX_INTERVAL_NS)
            by_node[node].append((measurement, group_report))
        groups.append(group_report)
    sample_roots = {"local": path.parent} if mode == "local" else {node: path.parent / "samples" / node for node in by_node}
    exports = []
    for node, sample_root in sample_roots.items():
        try:
            result = audit_samples(sample_root, [measurement for measurement, _ in by_node[node]], observer)
            exports.append({"node": node, **result})
        except Exception as error:
            exports.append({"node": node, "root": str(sample_root), "status": "failed", "error_type": type(error).__name__,
                            "failure": str(error) if isinstance(error, AuditError) else "Native verifier or sample parser rejected evidence"})
            failures.append(node + ":observer_export_or_stop_verification")
        for measurement, report in by_node[node]:
            verified = measurement.finish()
            report["nodes"][node] = verified
            report["audit_failures"].extend(node + ":" + item for item in verified["audit_failures"])
    if any(group["audit_failures"] for group in groups):
        failures.append("one_or_more_group_assertions_failed")
    upload = verify_uploads(data, expected) if mode == "local" else None
    if upload is not None and upload["status"] != "passed":
        failures.append("independent_upstream_upload_verification")
    failures.extend(verify_staging(data.get("final_staging"), mode, "final"))
    if mode == "cluster":
        fixture = data.get("fixture", [])
        if {row.get("node") for row in fixture} != set(by_node) or any(row.get("rf") != 2 or row.get("w") != 2 for row in fixture):
            failures.append("strict_rf_w_fixture_not_confirmed")
    peaks = {metric: max((node["peak"].get(metric, 0) for group in groups for node in group["nodes"].values()), default=0) for metric in METRICS}
    deltas = {metric: max((node["delta"].get(metric, 0) for group in groups for node in group["nodes"].values()), default=0) for metric in METRICS}
    cadences = [value for group in groups for node in group["nodes"].values() for value in node["cadence"].values()]
    per_size = []
    for size, concurrency in sorted({(row["size"], row["concurrency"]) for row in expected}):
        selected = [group for group in groups if (group["size"], group["concurrency"]) == (size, concurrency)]
        node_rows = [node for group in selected for node in group["nodes"].values()]
        per_size.append({"size": size, "concurrency": concurrency,
                         "required": sum((row["size"], row["concurrency"]) == (size, concurrency) for row in expected),
                         "executed": len(selected), "failure_free": sum(not group["audit_failures"] for group in selected),
                         "maximum_delta": {metric: max((node["delta"].get(metric, 0) for node in node_rows), default=0) for metric in METRICS},
                         "maximum_peak": {metric: max((node["peak"].get(metric, 0) for node in node_rows), default=0) for metric in METRICS},
                         "logical_payload_mib_per_second": [group["logical_payload_mib_per_second"] for group in selected]})
    return {"mode": mode, "path": str(path.relative_to(repo)), "sha256": sha(path),
            "status": "passed" if not failures else "incomplete_or_failed", "harness_status": data.get("status"),
            "required": sum(wanted.values()), "executed": len(groups),
            "failure_free": sum(not group["audit_failures"] for group in groups),
            "missing": counter_rows(wanted - actual), "unexpected_or_duplicates": counter_rows(actual - wanted),
            "declared_manifest_missing": counter_rows(wanted - declared), "declared_manifest_unexpected": counter_rows(declared - wanted),
            "failures": failures, "maximum_peak": peaks, "maximum_delta": deltas,
            "maximum_probe_ns": max((value.get("maximum_probe_ns", 0) for value in cadences), default=0),
            "maximum_gap_ns": max((value.get("maximum_gap_ns", 0) for value in cadences), default=0),
            "intervals_over100ms": sum(value.get("intervals_over100ms", 0) for value in cadences),
            "runner": data.get("runner"), "versions": data.get("versions"),
            "cleanup_errors": data.get("cleanup_errors", []), "final_staging": data.get("final_staging"),
            "upload_verifications": upload, "exports": exports, "per_size_concurrency": per_size, "groups": groups}


def analyze(args):
    require(args.execute_analysis and args.terminal_lanes_confirmed,
            "Explicit terminal-lanes confirmation and execution required")
    repo, physical = Path(args.repo).resolve(), Path(args.physical_tmp)
    require(physical == PHYSICAL_TMP and physical.resolve() == physical, "Only explicit C7 physical tmp mapping allowed")
    require(re.fullmatch(r"[0-9a-f]{40}", args.candidate_sha or "") and
            re.fullmatch(r"[0-9a-f]{64}", args.source_fingerprint or ""), "Candidate SHA and source fingerprint required")
    output = repo / OUTPUT
    require(not output.exists(), "Never overwrite a prior resource analysis")
    cases = load_api(repo, "cases", "e2e/resource_cases.py")
    cadence = load_api(repo, "cadence", "e2e/resource_cadence.py")
    observer = load_api(repo, "observer", "e2e/scripts/resource_observer.py")
    require(cadence.MAX_INTERVAL_NS == 100_000_000, "Retained 100 ms observation contract changed")
    result = {"schema": 1, "candidate_sha": args.candidate_sha, "source_sha256": args.source_fingerprint,
              "verified_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "status": "incomplete_or_failed",
              "method": "Independent bounded streaming generation/sequence/count/extent/hash and active-window verification; no interpolation.",
              "path_mapping": {"public": str(PUBLIC_TMP), "physical": str(physical), "applies_only_to": "explicit fixture references; no tmp traversal"},
              "terminal_lanes_confirmed_by_invoker": True, "terminal_receipts": [], "matrices": [], "failures": [],
              "limitations": ["Retained harness baseline medians are inputs; exact pre-executor snapshot boundaries are not reconstructed.",
                              "Throughput is logical payload bytes over the complete executor interval, including retained repetitions, not network I/O accounting or latency SLA.",
                              "Observer death is verified from retained same-namespace OS-identity stop receipts; no current host process/container scan or BEAM death claim.",
                              "OS page-cache and observer RSS are reported separately from service RSS; process/connection cleanup beyond retained receipts is not inferred.",
                              "This audits local/gateway and cluster resource matrices; other mandatory A30 fault/resource cases remain separate acceptance lanes."],
              "provenance": {str(path): sha(repo / path) for path in
                             (Path("e2e/resource_cases.py"), Path("e2e/resource_cadence.py"), Path("e2e/scripts/resource_observer.py"))}}
    result["provenance"]["private_analysis_utility_sha256"] = sha(Path(__file__))
    require(args.terminal_receipt, "At least one exact terminal driver receipt required")
    for supplied in args.terminal_receipt:
        path = scoped(repo, Path(supplied))
        terminal = read_json(path)
        exits = {key: terminal[key] for key in ("exit_status", "exit_code") if key in terminal}
        require(exits and all(integer(value) for value in exits.values()), "Terminal receipt lacks actual exit status")
        result["terminal_receipts"].append({"path": str(path.relative_to(repo)), "sha256": sha(path), "actual_exit": exits})
    for mode, supplied in (("local", args.local_results), ("cluster", args.cluster_results)):
        require(supplied, "Exact local and cluster result paths required")
        path = scoped(repo, Path(supplied))
        try:
            result["matrices"].append(matrix(repo, path, mode, args, cases, cadence, observer))
        except Exception as error:
            result["matrices"].append({"mode": mode, "path": str(path.relative_to(repo)), "status": "incomplete_or_failed",
                                       "error_type": type(error).__name__,
                                       "failure": str(error) if isinstance(error, AuditError) else "Invalid/missing matrix evidence"})
    if all(row["status"] == "passed" for row in result["matrices"]):
        result["status"] = "passed"
    else:
        result["failures"].append("resource_matrices_incomplete_or_failed")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as target:
        json.dump(result, target, indent=2, sort_keys=True)
        target.write("\n")
    print(json.dumps({"status": result["status"], "output": str(OUTPUT),
                      "matrices": [{key: row.get(key) for key in
                                    ("mode", "status", "required", "executed", "failure_free", "maximum_probe_ns", "maximum_gap_ns")}
                                   for row in result["matrices"]]}))
    return 0 if result["status"] == "passed" else 1


def self_test_staging():
    """Exercise matrix-level acceptance using only in-memory synthetic records."""
    zero = {"files": 0, "bytes": 0}
    nodes = ("meta", "data-a", "data-b")
    checked = 0
    invalid = [None, [], {}, {"files": 0}, {"bytes": 0},
               {"files": 1, "bytes": 0}, {"files": 0, "bytes": 1},
               {"files": -1, "bytes": 0}, {"files": 0, "bytes": -1},
               {"files": False, "bytes": 0}, {"files": 0, "bytes": False},
               {"files": 0.0, "bytes": 0}, {"files": 0, "bytes": "0"},
               {"files": 0, "bytes": 0, "extra": 0}]
    args = SimpleNamespace(candidate_sha="a" * 40, source_fingerprint="b" * 64)
    cadence = SimpleNamespace(MAX_INTERVAL_NS=100_000_000)
    repo, path = Path("/synthetic"), Path("/synthetic/results.json")
    module = sys.modules[__name__]
    for mode in ("local", "cluster"):
        stages = dict(zero) if mode == "local" else {node: dict(zero) for node in nodes}
        name = "local-get" if mode == "local" else "authenticated-peer-get"
        group = {"name": name, "size": CHUNK, "concurrency": 1, "repetitions": 1,
                 "start_monotonic_ns": 1, "end_monotonic_ns": 2,
                 "limits": limits_for(CHUNK, 1),
                 "staging_before": stages, "staging_after": stages}
        if mode == "cluster":
            group.update(nodes={node: {"limits": limits_for(CHUNK, 1)} for node in nodes},
                         details=[{"bytes": CHUNK, "hash": "c" * 64, "authenticated_peer": "data-b"}])
        expected = [{key: group[key] for key in ("name", "size", "concurrency")}]
        valid = {"status": "passed", "starting_sha": args.candidate_sha,
                 "ending_sha": args.candidate_sha, "source_sha256": args.source_fingerprint,
                 "required_cases": expected, "groups": [group], "final_staging": stages,
                 "fixture": [{"node": node, "rf": 2, "w": 2} for node in nodes]}
        cases = SimpleNamespace(manifest=lambda _: expected)

        def audit(data):
            with mock.patch.object(module, "read_json", return_value=data), \
                 mock.patch.object(module, "sha", return_value="d" * 64), \
                 mock.patch.object(module, "audit_samples", return_value={"status": "passed"}), \
                 mock.patch.object(Measurement, "finish", return_value={
                     "audit_failures": [], "peak": {}, "delta": {}, "cadence": {}}):
                return matrix(repo, path, mode, args, cases, cadence, None)

        require(audit(valid)["status"] == "passed", "Valid zero staging inventory rejected")
        checked += 1
        for phase in ("staging_before", "staging_after", "final_staging"):
            for bad in ["missing", *invalid]:
                data = json.loads(json.dumps(valid))
                target = data if phase == "final_staging" else data["groups"][0]
                if bad == "missing":
                    target.pop(phase)
                elif mode == "local":
                    target[phase] = bad
                else:
                    target[phase]["data-a"] = bad
                result = audit(data)
                require(result["status"] != "passed", mode + " invalid " + phase + " inventory accepted")
                errors = result["failures"] + [error for row in result["groups"] for error in row["audit_failures"]]
                require(any("staging" in error for error in errors), "Inventory rejection lacks staging diagnostic")
                checked += 1
            if mode == "cluster":
                for keys in (nodes[:-1], (*nodes, "unexpected")):
                    data = json.loads(json.dumps(valid))
                    target = data if phase == "final_staging" else data["groups"][0]
                    target[phase] = {node: dict(zero) for node in keys}
                    require(audit(data)["status"] != "passed", "Incorrect cluster staging node coverage accepted")
                    checked += 1
    return checked


def self_test(repo):
    cadence = load_api(repo, "cadence", "e2e/resource_cadence.py")
    cases = load_api(repo, "cases", "e2e/resource_cases.py")
    start, end = 1_000_000_000, 1_080_000_000
    rows = []
    for index in range(8):
        at = 980_000_000 + index * 20_000_000
        window = {"start_ns": at, "end_ns": at + 1_000_000,
                  "start_monotonic_ns": at - 4_000_000_000,
                  "end_monotonic_ns": at - 3_999_000_000}
        rows.append({"sampler_id": "synthetic", "sequence": index, "time": (at + 1_000_000) // 1_000_000,
                     **{metric: (0 if metric == "temporary" else index + 1) for metric in METRICS},
                     "probes": {metric: dict(window) for metric in METRICS}})
    baseline = rows[:1]
    recorded = cadence.measure_samples(rows, baseline, start, end, limits_for(CHUNK, 1))
    measured = Measurement({"start_ns": start, "end_ns": end}, recorded, cadence.MAX_INTERVAL_NS)
    for row in rows:
        measured.add(row)
    require(not measured.finish()["audit_failures"], "Streaming cadence differs from authoritative API")
    damaged = dict(recorded, peak={"rss": -1})
    measured = Measurement({"start_ns": start, "end_ns": end}, damaged, cadence.MAX_INTERVAL_NS)
    for row in rows:
        measured.add(row)
    require(any(item.startswith("recorded_measurement_mismatch") for item in measured.finish()["audit_failures"]),
            "Recorded peak tampering not detected")
    try:
        cases.verify_coverage(cases.manifest("all")[:-1], cases.manifest("all"))
    except AssertionError:
        pass
    else:
        raise AuditError("Missing manifest case not detected")
    delayed = json.loads(json.dumps(rows))
    for row in delayed[3:]:
        row["time"] += 200
        for probe in row["probes"].values():
            for key in WINDOW:
                probe[key] += 200_000_000
    bad_recorded = cadence.measure_samples(delayed, delayed[:1], start, end + 200_000_000, limits_for(CHUNK, 1))
    bad_measurement = Measurement({"start_ns": start, "end_ns": end + 200_000_000}, bad_recorded, cadence.MAX_INTERVAL_NS)
    for row in delayed:
        bad_measurement.add(row)
    bad_result = bad_measurement.finish()
    require("cadence:rss:interval_over100ms" in bad_result["audit_failures"] and
            not any(failure.startswith("recorded_measurement_mismatch") for failure in bad_result["audit_failures"]),
            "Streaming audit changed or concealed original 100 ms failures")
    fixture = {"name": "gateway-put-upstream-readback", "size": CHUNK, "concurrency": 1,
               "status": "passed", "expected_sha256": "a" * 64, "checked_objects": 1, "check_count": 3,
               "objects": [{"path": "/bucket/key", "bytes": CHUNK, "sha256": "a" * 64, "metadata_sha256": "a" * 64}]}
    expected = [{"name": "gateway-put", "size": CHUNK, "concurrency": 1}]
    require(verify_uploads({"upload_verifications": [fixture]}, expected)["status"] == "passed", "Valid readback rejected")
    fixture["objects"][0]["sha256"] = "b" * 64
    require(verify_uploads({"upload_verifications": [fixture]}, expected)["status"] != "passed", "Readback corruption accepted")
    require(mapped_fixture("/tmp/exact-fixture/file", PHYSICAL_TMP) == PHYSICAL_TMP / "exact-fixture/file", "Path mapping failed")
    with tempfile.TemporaryDirectory(prefix="ess-resource-analysis-synthetic-") as directory:
        root = Path(directory)
        source = root / "sample"
        source.write_bytes(b"synthetic\n")
        require(sha(source) == hashlib.sha256(b"synthetic\n").hexdigest(), "Streaming hash failed")
        observer = load_api(repo, "observer", "e2e/scripts/resource_observer.py")
        native_rows, encoded = [], []
        for row in rows:
            at = row["probes"]["rss"]["start_ns"]
            sample = dict(row, collection={"start_ns": at, "end_ns": at + 1_000_000,
                          "start_monotonic_ns": at + 9_000_000_000,
                          "end_monotonic_ns": at + 9_001_000_000},
                          beam_exchange={"start_ns": at, "end_ns": at + 500_000,
                          "start_monotonic_ns": at + 9_000_000_000,
                          "end_monotonic_ns": at + 9_000_500_000},
                          scheduled_monotonic_ns=at + 9_000_000_000, observer={"rss": 20},
                          previous_export=None if not native_rows else {
                              "sampler_id": "synthetic", "sequence": len(native_rows) - 1,
                              "encoding": {"start_ns": at, "end_ns": at + 1,
                                  "start_monotonic_ns": at, "end_monotonic_ns": at + 1},
                              "write": {"start_ns": at, "end_ns": at + 1,
                                  "start_monotonic_ns": at, "end_monotonic_ns": at + 1}})
            native_rows.append(sample)
            encoded.append(json.dumps(sample, separators=(",", ":")).encode() + b"\n")
        raw = b"".join(encoded)
        (root / "samples.jsonl").write_bytes(raw)
        observer_identity = {"pid": 123, "start_ticks": 456, "state": "S"}
        identity_file = root / "observer-synthetic.json"
        identity_file.write_text(json.dumps({"generation": "synthetic", "observer": observer_identity,
                                           "beam": {"pid": 100, "start_ticks": 101, "state": "S"}}))
        (root / "observer-stop-verification.json").write_text(json.dumps([
            {"identity_file": "/tmp/synthetic/observer-synthetic.json", "expected": observer_identity,
             "observed": None, "stopped": True}]))
        export = {"schema": 1, "generation": "synthetic", "observer": observer_identity, "status": "passed",
                  "admitted_rows": len(native_rows), "persisted_rows": len(native_rows),
                  "first_sequence": 0, "last_sequence": len(native_rows) - 1, "output_start": 0,
                  "output_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                  "queue_capacity": observer.EXPORT_QUEUE_ROWS,
                  "queued_byte_limit": observer.EXPORT_QUEUE_ROWS * observer.ROW_BYTES,
                  "row_byte_limit": observer.ROW_BYTES, "maximum_queued_rows": 1}
        export_file = root / "resource-export-synthetic.json"
        export_file.write_text(json.dumps(export))
        native_recorded = cadence.measure_samples(native_rows, native_rows[:1], start, end, limits_for(CHUNK, 1))
        native_measurement = Measurement({"start_ns": start, "end_ns": end}, native_recorded, cadence.MAX_INTERVAL_NS)
        audited = audit_samples(root, [native_measurement], observer)
        require(audited["status"] == "passed" and audited["verified_rows"] == len(native_rows),
                "Valid native export rejected")
        require(not native_measurement.finish()["audit_failures"], "Native streamed cadence mismatch")
        corruptions = [dict(export, sha256="0" * 64),
                       dict(export, admitted_rows=len(native_rows) - 1),
                       dict(export, output_start=1)]
        for corrupted in corruptions:
            export_file.write_text(json.dumps(corrupted))
            try:
                audit_samples(root, [], observer)
            except (AuditError, ValueError):
                pass
            else:
                raise AuditError("Corrupt native manifest accepted")
        export_file.write_text(json.dumps(export))
        corrupt_row = dict(native_rows[2], sequence=99)
        (root / "samples.jsonl").write_bytes(b"".join(encoded[:2] +
            [json.dumps(corrupt_row, separators=(",", ":")).encode() + b"\n"] + encoded[3:]))
        try:
            audit_samples(root, [], observer)
        except (AuditError, ValueError):
            pass
        else:
            raise AuditError("Noncontiguous native sequence accepted")
    staging_checks = self_test_staging()
    print(json.dumps({"status": "passed", "synthetic_checks": 14 + staging_checks,
                      "staging_inventory_synthetic_checks": staging_checks, "real_resource_data_scanned": False,
                      "live_host_process_reads": False}))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--execute-analysis", action="store_true")
    parser.add_argument("--terminal-lanes-confirmed", action="store_true")
    parser.add_argument("--repo", default="/home/gao/Workspace/gsmlg-opt/ex_storage_service")
    parser.add_argument("--candidate-sha")
    parser.add_argument("--source-fingerprint")
    parser.add_argument("--local-results")
    parser.add_argument("--cluster-results")
    parser.add_argument("--terminal-receipt", action="append", default=[])
    parser.add_argument("--physical-tmp", default=str(PHYSICAL_TMP))
    args = parser.parse_args()
    require(not (args.self_test and args.execute_analysis), "Self-test and real analysis are mutually exclusive")
    return self_test(Path(args.repo).resolve()) if args.self_test else analyze(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AuditError as error:
        print("Resource analysis blocked: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        print("Resource analysis blocked by " + type(error).__name__ + "; no private exception data printed", file=sys.stderr)
        sys.exit(1)
