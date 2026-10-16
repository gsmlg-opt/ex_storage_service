#!/usr/bin/env python3
"""Observe the locked mc executable's local name/size/time comparisons."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

MC_SHA256 = "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mc", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert hashlib.sha256(Path(args.mc).read_bytes()).hexdigest() == MC_SHA256
    cases = []
    for name, source, target, source_time, target_time in [
        ("source-only", "abc", None, 1767225600, 1767225600),
        ("target-only", None, "xyz", 1767225600, 1767225600),
        ("different-size", "long", "old", 1767225600, 1767225600),
        ("newer-source", "abc", "xyz", 1767225660, 1767225600),
        ("older-source", "abc", "xyz", 1767225540, 1767225600),
        ("same-size-time-different-bytes", "abc", "xyz", 1767225600, 1767225600),
    ]:
        with tempfile.TemporaryDirectory(prefix="ess-mc-diff-") as directory:
            root = Path(directory)
            for side, data, stamp in [("source", source, source_time), ("target", target, target_time)]:
                (root / side).mkdir()
                if data is not None:
                    (root / side / "item").write_text(data)
                    os.utime(root / side / "item", (stamp, stamp))
            result = subprocess.run(
                [args.mc, "--config-dir", str(root / "config"), "--json", "diff", str(root / "source"), str(root / "target")],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
                env={**os.environ, "MC_NO_COLOR": "1"},
            )
            assert result.returncode == 0, (name, result.stderr)
            rows = [json.loads(line) for line in result.stdout.decode().splitlines()]
            normalized = json.loads(json.dumps(rows).replace(directory, "@TMP@"))
            cases.append({"name": name, "source": source, "target": target,
                          "source_time": source_time, "target_time": target_time,
                          "exit": result.returncode, "rows": normalized,
                          "stderr": result.stderr.decode().replace(directory, "@TMP@")})
    args.output.write_text(json.dumps({"mc_commit": "77f82e18b5401a65958f1619df6ebb994634bd88",
                                      "mc_sha256": MC_SHA256, "cases": cases}, indent=2) + "\n")


if __name__ == "__main__":
    main()
