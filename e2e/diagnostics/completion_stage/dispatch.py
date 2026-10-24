"""Trace only the second original synchronized Phase 1 race on disposable nodes."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
PHASE1 = HERE.parents[2] / "e2e/s3_hardening_phase1.py"


def main():
    pinned = os.environ["ESS_DIAGNOSTIC_PINNED_PYTHON"]
    python = Path(pinned)
    if (not python.is_absolute() or not python.is_file()
            or not os.access(python, os.X_OK)
            or not (python.parent.parent / "pyvenv.cfg").is_file()):
        raise ValueError("absolute executable virtualenv Python required")
    args = sys.argv[1:]
    normalized = args[1:] if args[:1] == ["-B"] else args
    if not normalized or Path(normalized[0]).resolve() != PHASE1:
        os.execv(pinned, [pinned, *args])

    evidence = Path(os.environ["ESS_DIAGNOSTIC_EVIDENCE"])
    if not evidence.is_absolute() or not evidence.is_dir() or evidence.is_symlink():
        raise ValueError("absolute fresh diagnostic evidence directory required")
    if any(evidence.glob("ess_data_*.jsonl*")) or any(evidence.glob("*-receipt.json")):
        raise ValueError("diagnostic evidence directory already contains stage output")
    spec = importlib.util.spec_from_file_location("original_phase1", PHASE1)
    profile = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(profile)
    original_synchronized = profile.synchronized
    race_count = 0

    def control(action):
        subprocess.run([
            "elixir", "--name", f"ess_completion_probe_{action}_{os.getpid()}@127.0.0.1",
            "--cookie", "ess_cluster_e2e_cookie", str(HERE / "controller.exs"),
            action, str(evidence),
        ], check=True, env={**os.environ, "PAGER": "cat"}, timeout=25)

    def synchronized(operations, admission):
        nonlocal race_count
        race_count += 1
        if race_count != 2:
            return original_synchronized(operations, admission)
        if len(operations) != 32:
            raise AssertionError("original completion race must retain all 32 workers")
        try:
            control("start")
            return original_synchronized(operations, admission)
        finally:
            control("stop")

    profile.synchronized = synchronized
    sys.argv = normalized
    profile.main()


if __name__ == "__main__":
    main()
