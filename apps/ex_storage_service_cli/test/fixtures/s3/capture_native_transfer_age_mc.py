"""Capture absolute transfer-age equality using the locked mc executable."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


EXPECTED_SHA256 = "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"
CUTOFF_SECONDS = 1767225600  # 2026-01-01T00:00:00Z


def capture(binary):
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    if digest != EXPECTED_SHA256:
        raise SystemExit("Expected the locked mc executable")
    cases = []
    with tempfile.TemporaryDirectory(prefix="ess-transfer-age-mc-") as temporary:
        root = Path(temporary)
        for flag in ["older-than", "newer-than"]:
            for delta in [-1, 0, 1]:
                source = root / f"source-{flag}-{delta}"
                destination = root / f"destination-{flag}-{delta}"
                source.write_bytes(b"raw")
                os.utime(source, (CUTOFF_SECONDS + delta, CUTOFF_SECONDS + delta))
                result = subprocess.run(
                    [str(binary), "--json", "--config-dir", str(root / "config"),
                     "cp", "--" + flag, "2026-01-01T00:00:00Z", str(source), str(destination)],
                    capture_output=True, timeout=30,
                )
                copied = destination.exists()
                expected = delta < 0 if flag == "older-than" else delta >= 0
                if result.returncode != 0 or copied != expected:
                    raise SystemExit("Pinned mc absolute-age behavior differs from the retained decision")
                if copied and destination.read_bytes() != b"raw":
                    raise SystemExit("Pinned mc did not preserve the source bytes")
                cases.append({"flag": flag, "mtime_delta_seconds": delta,
                              "exit_code": result.returncode, "copied": copied})
    return {"reference_commit": "77f82e18b5401a65958f1619df6ebb994634bd88",
            "binary_sha256": digest, "cutoff": "2026-01-01T00:00:00Z", "cases": cases}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mc", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(capture(args.mc.resolve()), indent=2) + "\n")
