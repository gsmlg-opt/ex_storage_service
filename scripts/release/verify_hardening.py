#!/usr/bin/env python3
"""Require full acceptance of the prepared release's exact tested source."""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
RELEASE_SPEC = importlib.util.spec_from_file_location("hardening_release_tools", Path(__file__).with_name("release.py"))
release_tools = importlib.util.module_from_spec(RELEASE_SPEC)
RELEASE_SPEC.loader.exec_module(release_tools)


def validate_run(run, jobs, expected_sha, expected_repo):
    if (run.get("head_sha") != expected_sha or run.get("event") != "workflow_dispatch"
            or run.get("status") != "completed" or run.get("conclusion") != "success"
            or run.get("path") != ".github/workflows/s3-hardening.yml"
            or run.get("head_repository", {}).get("full_name") != expected_repo):
        raise ValueError("Full hardening run must be successful and belong to this exact source/repository")
    aggregate = [job for job in jobs if job.get("name") == "Extended audit of all 38 scenarios and every lane"]
    if len(aggregate) != 1 or aggregate[0].get("conclusion") != "success":
        raise ValueError("Missing, skipped, cancelled or failed full acceptance job")


def validate_prepared(prepared, release):
    source = prepared.get("source_sha")
    if prepared.get("release_sha") != release:
        raise ValueError("Prepared release checkout changed before acceptance validation")
    if not isinstance(source, str) or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("Prepared source must be a full commit SHA")
    versions = release_tools.versions(ROOT)
    if versions != prepared.get("component_versions") or set(versions.values()) != {prepared.get("version")}:
        raise ValueError("Prepared release component versions changed")
    if source == release:
        return source
    parents = subprocess.check_output(["git", "show", "-s", "--format=%P", release], cwd=ROOT, text=True).split()
    message = subprocess.check_output(["git", "show", "-s", "--format=%B", release], cwd=ROOT, text=True)
    if parents != [source] or re.findall(r"^Release-Source: ([0-9a-f]{40})$", message, re.M) != [source]:
        raise ValueError("Prepared source must be the version-only release commit's declared parent")
    changed = set(subprocess.check_output(["git", "diff", "--name-only", source, release],
                                          cwd=ROOT, text=True).splitlines())
    if not changed or not changed.issubset(release_tools.VERSION_FILES):
        raise ValueError("Prepared release contains changes outside component versions")
    for name in release_tools.VERSION_FILES:
        before = subprocess.check_output(["git", "show", f"{source}:{name}"], cwd=ROOT, text=True)
        after = (ROOT / name).read_text()
        expected = re.sub(release_tools.version_pattern(name),
                          lambda match: match[1] + prepared["version"] + match[3], before)
        if expected != after:
            raise ValueError("Prepared release must contain only exact version replacements")
    return source


def audit_source(source, release, command):
    # release.py prepare has already checked the canonical tag, parent and exact
    # version replacements. Audit at its original source, including on a retry;
    # never exclude version bytes from the ordinary source fingerprint.
    try:
        if source != release:
            subprocess.run(["git", "checkout", "--detach", source], cwd=ROOT, check=True)
        subprocess.run(command, cwd=ROOT, check=True)
    finally:
        if source != release:
            subprocess.run(["git", "checkout", "--detach", release], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    args = parser.parse_args()
    if args.run_id <= 0:
        parser.error("positive acceptance run ID required")
    release = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    prepared = json.loads(args.prepared.read_text())
    source = validate_prepared(prepared, release)
    endpoint = f"repos/{args.repo}/actions/runs/{args.run_id}"
    run = json.loads(subprocess.check_output(["gh", "api", endpoint], cwd=ROOT, text=True))
    pages = json.loads(subprocess.check_output(
        ["gh", "api", "--paginate", "--slurp", endpoint + "/jobs?per_page=100"], cwd=ROOT, text=True))
    validate_run(run, [job for page in pages for job in page["jobs"]], source, args.repo)
    # This artifact is uploaded only after the actual aggregate succeeds. Audit
    # its records/logs again against the checked-out candidate, not a green badge.
    subprocess.run(["gh", "run", "download", str(args.run_id), "--repo", args.repo,
                    "--name", "s3-hardening-verification", "--dir", "docs/reports/s3-hardening"],
                   cwd=ROOT, check=True)
    output = f"docs/reports/s3-hardening/ci-{args.run_id}-{run['run_attempt']}"
    audit_source(source, release, [sys.executable, "e2e/hardening_acceptance.py", "verify", "--output", output])


if __name__ == "__main__":
    main()
