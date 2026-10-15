#!/usr/bin/env python3
"""Seven pinned AWS paginator oracles, including opaque CLI token resume."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import parse_qs, quote, urlsplit

from capture_operations import AWS_SHA256, ROOT, run, xml


def pages(model, name):
    operation = model["operations"][name]
    reference = operation["output"]
    result = []
    for index in (0, 1):
        keys = ["key%2F+ 雪", "second/雪"] if index == 0 else ["third/雪"]
        values = {"IsTruncated": index == 0, "Name": "bucket", "Prefix": "prefix%2F+/雪", "Delimiter": "/"}
        if name in ("ListObjects", "ListObjectsV2"):
            values["Contents"] = [{"Key": key, "Size": number + 1} for number, key in enumerate(keys)]
            if name == "ListObjectsV2":
                values.update(KeyCount=len(keys), NextContinuationToken="opaque%2F+==/雪" if index == 0 else None)
            # ListObjects intentionally omits NextMarker: AWS must use the last key.
        elif name == "ListObjectVersions":
            values["Versions"] = [{"Key": key, "VersionId": "version%2F+==", "IsLatest": True} for key in keys]
            values["DeleteMarkers"] = [{"Key": "deleted/" + str(index), "VersionId": "delete%2F+=="}]
            if index == 0:
                values.update(NextKeyMarker="next%2F+ 雪", NextVersionIdMarker="version-marker%2F+==")
        elif name == "ListMultipartUploads":
            values["Uploads"] = [{"Key": key, "UploadId": "upload%2F+=="} for key in keys]
            if index == 0:
                values.update(NextKeyMarker="next%2F+ 雪", NextUploadIdMarker="upload-marker%2F+==")
        elif name == "ListParts":
            values["Parts"] = [{"PartNumber": offset + 1 + index * 2, "ETag": '"part"', "Size": 3} for offset, _key in enumerate(keys)]
            if index == 0:
                values["NextPartNumberMarker"] = 2
        else:
            values = {"Buckets": [{"Name": "bucket-" + str(index) + "-" + str(offset)} for offset, _key in enumerate(keys)]}
            if name == "ListBuckets":
                values["Owner"] = {"ID": "first-owner" if index == 0 else "second-owner"}
            if index == 0:
                values["ContinuationToken"] = "opaque%2F+==/雪"
        if "CommonPrefixes" in model["shapes"][reference["shape"]]["members"]:
            values["CommonPrefixes"] = [{"Prefix": "common/" + str(index) + "/"}]
        if name in ("ListObjects", "ListObjectsV2", "ListObjectVersions"):
            values["EncodingType"] = "url"

            def encode(value):
                if isinstance(value, list):
                    return [encode(item) for item in value]
                if isinstance(value, dict):
                    return {key: quote(item, safe="") if key in {"Key", "Prefix", "Delimiter", "NextKeyMarker"} and isinstance(item, str) else encode(item) for key, item in value.items()}
                return value

            values = encode(values)
        members = model["shapes"][reference["shape"]]["members"]
        values = {key: value for key, value in values.items() if key in members and value is not None}
        body = xml(model["shapes"], reference, values, "ListResult").encode()
        result.append({"status": 200, "headers": {"content-type": "application/xml", "x-amz-request-charged": "requester"}, "body_base64": base64.b64encode(body).decode()})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--ess", help="optional private escript diagnostics")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256
    model_path = ROOT / "priv/compatibility/aws/service-2.json"
    model = json.loads(model_path.read_text())
    paginator_path = ROOT / "priv/compatibility/aws/paginators-1.json"
    paginators = json.loads(paginator_path.read_text())["pagination"]
    report = {"aws_sha256": AWS_SHA256, "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
              "paginators_sha256": hashlib.sha256(paginator_path.read_bytes()).hexdigest(), "cases": []}
    with tempfile.TemporaryDirectory(prefix="ess-pagination-oracle-") as temporary:
        env = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "ESS_", "MC_"))}
        env.update(AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret", AWS_CONFIG_FILE="/dev/null",
                   AWS_SHARED_CREDENTIALS_FILE="/dev/null", AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
                   AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off", NO_PROXY="*", PAGER="cat", ERL_FLAGS="+S 2:2",
                   ESS_CONFIG_DIR=str(Path(temporary) / "config"), ESS_STATE_DIR=str(Path(temporary) / "state"))
        for name, paginator in paginators.items():
            operation = model["operations"][name]
            shape = model["shapes"][operation["input"]["shape"]]
            token_members = paginator["input_token"] if isinstance(paginator["input_token"], list) else [paginator["input_token"]]
            service_tokens = [shape["members"][key]["locationName"] for key in token_members]
            responses = pages(model, name)

            def select(request, _index=0):
                query = parse_qs(urlsplit(request["target"]).query)
                return responses[1 if any(key in query for key in service_tokens) else 0]

            cli = re.sub(r"(?<!^)(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", "-", name).lower()
            command = ["s3api", cli]
            if "Bucket" in shape["members"]:
                command += ["--bucket", "bucket"]
            if name == "ListParts":
                command += ["--key", "key", "--upload-id", "upload-id"]
            tokens = {}
            scenarios = [("aggregate", ["--page-size", "2"]), ("truncate-within", ["--page-size", "2", "--max-items", "1"]),
                         ("resume-within", None), ("truncate-boundary", ["--page-size", "2", "--max-items", "2"]),
                         ("resume-boundary", None), ("no-paginate", ["--no-paginate"])]
            for scenario, options in scenarios:
                if options is None:
                    kind = "truncate-within" if scenario == "resume-within" else "truncate-boundary"
                    options = ["--page-size", "2", "--starting-token", tokens[kind]]
                selected = command + options
                result = run(args.aws, selected, select, env, Path(temporary), paginate=True)
                if result.get("status") != 0 or result["peer_errors"]:
                    raise RuntimeError(name + " " + scenario + ": " + str(result))
                if scenario.startswith("truncate"):
                    tokens[scenario] = json.loads(result["stdout"])["NextToken"]
                case = {"operation": name, "scenario": scenario, "args": selected, "aws": result,
                        "responses": [select(request) for request in result["requests"]]}
                if args.ess:
                    case["initial_ess"] = run(args.ess, selected, select, env, Path(temporary), ess=True, paginate=True)
                report["cases"].append(case)
                print(name, scenario, "aws=" + str(result["status"]), "requests=" + str(len(result["requests"])), "ess=" + str(case.get("initial_ess", {}).get("status", "not-run")), flush=True)
                args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
