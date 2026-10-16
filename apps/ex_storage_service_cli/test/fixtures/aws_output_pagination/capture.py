#!/usr/bin/env python3
"""Capture pinned AWS executable output/query pagination over loopback only."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import parse_qs, urlsplit

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "differential"))
from capture_operations import AWS_SHA256, ROOT, run


def response(index):
    keys = ["first 雪", "second%2F+"] if index == 0 else ["third/é", "fourth"]
    contents = "".join(
        f"<Contents><Key>{key}</Key><Size>{number}</Size></Contents>"
        for number, key in enumerate(keys, 1)
    )
    continuation = "<NextContinuationToken>page-two+/=</NextContinuationToken>" if index == 0 else ""
    body = (
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>bucket</Name><IsTruncated>{str(index == 0).lower()}</IsTruncated>"
        f"<KeyCount>2</KeyCount>{contents}{continuation}</ListBucketResult>"
    ).encode()
    return {"status": 200, "headers": {"content-type": "application/xml"},
            "body_base64": base64.b64encode(body).decode()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.aws).read_bytes()).hexdigest() == AWS_SHA256
    responses = [response(0), response(1)]

    def select(request, _index=0):
        query = parse_qs(urlsplit(request["target"]).query)
        token = query.get("continuation-token")
        assert token in (None, ["page-two+/="]), token
        return responses[1 if token else 0]

    report = {"schema_version": 1, "aws_cli_version": "2.31.18", "aws_sha256": AWS_SHA256,
              "model_sha256": hashlib.sha256((ROOT / "priv/compatibility/aws/service-2.json").read_bytes()).hexdigest(),
              "cases": []}
    command = ["s3api", "list-objects-v2", "--bucket", "bucket", "--page-size", "2"]
    with tempfile.TemporaryDirectory(prefix="ess-output-pagination-") as temporary:
        directory = Path(temporary)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AWS_", "ESS_", "MC_")) and "proxy" not in key.lower()}
        env.update(AWS_ACCESS_KEY_ID="fixture-access", AWS_SECRET_ACCESS_KEY="fixture-secret",
                   AWS_CONFIG_FILE="/dev/null", AWS_SHARED_CREDENTIALS_FILE="/dev/null",
                   AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="1", AWS_PAGER="",
                   AWS_CLI_AUTO_PROMPT="off", NO_PROXY="*", PAGER="cat")

        def capture(selected):
            result = run(args.aws, selected, select, env, directory, paginate=True)
            assert result.get("status") == 0 and not result["peer_errors"], result
            return {"args": selected, "aws": result,
                    "responses": [select(request) for request in result["requests"]]}

        control = capture(command + ["--output", "json", "--max-items", "3"])
        token = json.loads(control["aws"]["stdout"])["NextToken"]
        report["token_control"] = control
        report["resume_token"] = token
        for output in ["text", "yaml-stream"]:
            for name, query, options in [
                ("full-pages", None, []),
                ("aggregate", "Contents[].Key", []),
                ("scalar-query", "length(Contents)", []),
                ("truncate", "Contents[].Key", ["--max-items", "3"]),
                ("full-truncate", None, ["--max-items", "3"]),
                ("token-query", "NextToken", ["--max-items", "3"]),
                ("resume", "Contents[].Key", ["--starting-token", token]),
            ]:
                query_options = ["--query", query] if query else []
                case = capture(command + ["--output", output] + query_options + options)
                case.update(output=output, scenario=name)
                report["cases"].append(case)
                print(output, name, repr(case["aws"]["stdout"]), flush=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
