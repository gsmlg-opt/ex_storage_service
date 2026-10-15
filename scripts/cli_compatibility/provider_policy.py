#!/usr/bin/env python3
"""Reject client regressions while retaining exact, independently proven provider failures."""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate(report, expected, reference, artifact_sha256):
    require(report.get("schema_version") == 1, "Unknown provider report schema")
    require(report.get("independent_aws") is True, "Independent AWS controls are required")
    aws = report.get("aws_reference", {})
    require(aws.get("artifact_sha256") == reference["binary_sha256"], "Unpinned AWS executable")
    require(aws.get("version", "").startswith("aws-cli/" + reference["version"] + " "),
            "Unexpected AWS executable version")
    require(report.get("artifact_sha256") == artifact_sha256, "Report describes another CLI artifact")
    providers = report.get("providers", [])
    require(len(providers) == len(expected["providers"]) and
            {row["provider"] for row in providers} == set(expected["providers"]),
            "Missing or duplicate provider results")
    for row in providers:
        name = row["provider"]
        policy = expected["providers"][name]
        require(row["image_id"].removeprefix("sha256:") == policy["image_id"],
                name + ": unexpected image config")
        require(any(value.split("@")[-1] == policy["image_digest"]
                    for value in row["image_digests"]), name + ": unexpected image digest")
        checks = row["checks"]
        require(len(checks) == len(set(checks)) and set(checks) == set(policy["required_checks"]),
                name + ": a required provider contract is missing or changed")
        failures = lambda rows: sorted((entry["check"], entry["error"]) for entry in rows)
        require(failures(row["failures"]) == failures(policy["known_failures"]),
                name + ": unexpected provider failure or changed known limitation")

        cleanup = row["cleanup"]
        removed = {entry["bucket"] for entry in cleanup if entry["status"] == "removed"}
        require(len(removed) == 2, name + ": both owned fixture buckets must be removed")
        for entry in cleanup:
            valid = entry["status"] == "removed" or (
                name == "ess" and (entry["status"], entry.get("upstream_issue")) in {
                    ("fixture-only known-key cleanup", 22), ("fixture-only individual cleanup", 20)
                }
            )
            require(valid and entry["bucket"] in removed and
                    entry["bucket"].startswith("ess-cli-probe-" + name + "-"),
                    name + ": failed or unexpected fixture cleanup")

        metadata = row["content_encoding"]
        for encoding in ("binary", "gzip", "deflate"):
            observed = metadata[encoding]
            requested = None if encoding == "binary" else encoding
            retained = requested if name == "rustfs" else None
            require(observed["requested"] == requested and observed["raw_bytes_verified"] is True and
                    observed["aws_head_content_encoding"] == retained,
                    name + ": independent Content-Encoding or raw-byte control changed")

        keys = row["listed_raw_keys"]
        uploaded = keys["uploaded"]
        expected_keys = uploaded if name == "rustfs" else [key.replace("+", " ") for key in uploaded]
        require(len(uploaded) == 3 and all("% +?#/雪/" in key for key in uploaded) and
                sorted(keys["listed_by_aws"]) == sorted(expected_keys),
                name + ": independent literal-key control changed")

        if name == "ess":
            for key, status, code in [("delete-ascii", 0, ""), ("delete-雪", 254, "MalformedXML")]:
                boundary = row["delete_boundary"][key]
                require(boundary["ess_status"] == boundary["aws_status"] == status and
                        code in boundary["ess_error"] and code in boundary["aws_error"],
                        "ESS independent DeleteObjects control changed")
        else:
            boundary = row["key_boundary"]
            require(boundary["key"] == "dot//./../key" and
                    boundary["ess_status"] == boundary["aws_status"] == 254 and
                    "InvalidArgument" in boundary["ess_error"] and
                    "InvalidArgument" in boundary["aws_error"],
                    "RustFS independent dot-segment control changed")
    return {"status": "known-provider-limitations", "client_regressions": 0,
            "provider_failures_retained": sum(len(row["failures"]) for row in providers)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--escript", type=Path, required=True)
    args = parser.parse_args()
    with args.escript.open("rb") as stream:
        artifact = hashlib.file_digest(stream, "sha256").hexdigest()
    report = json.loads(args.report.read_text())
    expected = json.loads((ROOT / "docs/cli/compatibility/provider-expectations.json").read_text())
    reference = json.loads((ROOT / "docs/cli/compatibility/reference-lock.json").read_text())["aws_cli"]
    print(json.dumps(validate(report, expected, reference, artifact)))


if __name__ == "__main__":
    main()
