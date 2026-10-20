#!/usr/bin/env python3
"""Run complete fast server suites plus the retained daily CLI contracts.

Only the existing :cluster and :large_stream tags stay in separate heavy lanes.
Keep the discovered file inventory and actual ExUnit counts in the receipt.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import signal

REPO = Path(__file__).resolve().parents[1]
SERVER_APPS = ("ex_storage_service", "ex_storage_service_cluster",
               "ex_storage_service_s3", "ex_storage_service_web")
# Floors detect accidental health-only/empty selections; they do not select tests.
MINIMUM_TESTS = dict(zip(SERVER_APPS, (587, 40, 357, 21)))
SUMMARY = re.compile(r"(\d+) tests?, (\d+) failures?(?:, (\d+) excluded)?(?:, (\d+) skipped)?")


def counts(log):
    app, result = None, {}
    for line in log.splitlines():
        if line.startswith("==> "):
            app = line[4:].strip()
        match = SUMMARY.search(line)
        if match and app:
            total, failures, excluded, skipped = (int(value or 0) for value in match.groups())
            result[app] = dict(tests=total, failures=failures, excluded=excluded, skipped=skipped,
                               executed=total - excluded - skipped)
    return result


def validate(result, minimum_tests=MINIMUM_TESTS):
    errors = []
    for app, minimum in minimum_tests.items():
        row = result.get(app, {})
        if row.get("tests", 0) < minimum or row.get("executed", 0) <= 0:
            errors.append(f"{app}: missing/shrunken suite (minimum {minimum}, got {row})")
        if row.get("failures", 0) or row.get("skipped", 0):
            errors.append(f"{app}: failing or skipped correctness tests")
        if row.get("excluded", 0) > (1 if app in SERVER_APPS[:2] else 0):
            errors.append(f"{app}: unexpected excluded tests")
    return errors


def run(argv, environment, log_path):
    with log_path.open("w") as log:
        process = subprocess.Popen(argv, cwd=REPO, env=environment, text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            for line in process.stdout:
                log.write(line)
                print(line, end="", flush=True)
            return process.wait()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--extended", action="store_true", help="also run the complete CLI suite")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    selected = SERVER_APPS + (("ex_storage_service_cli",) if args.extended else ())
    minimum_tests = dict(MINIMUM_TESTS, **({"ex_storage_service_cli": 3491} if args.extended else {}))
    inventory = {app: sorted(str(path.relative_to(REPO)) for path in
                            (REPO / "apps" / app / "test").rglob("*_test.exs"))
                 for app in selected}
    argv = ["mix", "test", "--max-cases", "1", "--no-color",
            *(f"apps/{app}/test" for app in selected)]
    receipt = {"command": argv, "inventory": inventory,
               "minimum_tests": minimum_tests, "status": "in_progress"}
    receipt_path = args.output / "correctness.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    environment = dict(os.environ, PAGER="cat", MIX_ENV="test")
    environment.setdefault("ERL_FLAGS", "+S 4:4 +SDcpu 2 +SDio 2")
    code = run(argv, environment, args.output / "server.log")
    result = counts((args.output / "server.log").read_text())
    errors = validate(result, minimum_tests)
    receipt.update(app_counts=result, exit_status=code, errors=errors)
    if code == 0 and not errors and not args.extended:
        cli = ["bash", "e2e/run_daily_unit_tests.sh", "cli"]
        cli_code = run(cli, environment, args.output / "cli.log")
        receipt.update(cli_command=cli, cli_exit_status=cli_code,
                       cli_counts=counts((args.output / "cli.log").read_text()))
        if receipt["cli_counts"].get("ex_storage_service_cli", {}).get("executed", 0) <= 0:
            errors.append("missing/empty CLI contract suite")
        code = cli_code
    receipt["status"] = "passed" if code == 0 and not errors else "blocked"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda number, _frame: sys.exit(128 + number))
    sys.exit(main())
