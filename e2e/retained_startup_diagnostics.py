"""Bounded allowlisted startup diagnostics; raw runtime logs stay private."""
import argparse
import hashlib
import json
from pathlib import Path
import re

LOGS = ("assets.log", "seed.log", "server.log", "restarted-server.log")
PHASES = ("assets", "seed", "server", "restarted-server")
REASONS = ("command_failure", "early_exit", "readiness_timeout")
MARKERS = {
    "address_in_use": r"(?i)eaddrinuse|address already in use|os error 98",
    "application_start_failure": r"(?i)could not start application|failed to start child",
    "missing_shared_library": r"(?i)cannot open shared object file|error while loading shared libraries",
    "undefined_function": r"UndefinedFunctionError|undefined function",
    "argument_error": r"ArgumentError",
    "runtime_error": r"RuntimeError",
    "match_error": r"MatchError",
    "erlang_error": r"ErlangError",
    "oom_marker": r"(?i)out of memory|enomem",
}


def log_summary(root):
    result = []
    for name in LOGS:
        path = root / name
        if not path.is_file():
            continue
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(65536):
                digest.update(chunk)
            length = stream.tell()
            stream.seek(max(0, length - 65536))
            tail = stream.read(65536).decode(errors="replace")
        result.append(dict(name=name, bytes=length, sha256=digest.hexdigest(),
                           scanned_tail_bytes=min(length, 65536),
                           observed_markers=[name for name, pattern in MARKERS.items()
                                             if re.search(pattern, tail)]))
    return result


def record(root, phase, reason, exit_status, ports):
    if (phase not in PHASES or reason not in REASONS or
            (exit_status is not None and not 0 <= exit_status <= 255) or
            len(ports) != 2 or any(not 1 <= port <= 65535 for port in ports)):
        raise ValueError("invalid bounded diagnostic fields")
    return dict(phase=phase, reason=reason, child_exit_status=exit_status,
                s3_port=ports[0], admin_port=ports[1], logs=log_summary(root),
                raw_logs_private=True, markers_are_observations_only=True,
                acceptance_pass=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--reason", choices=REASONS, required=True)
    parser.add_argument("--exit-status", required=True)
    parser.add_argument("--ports", nargs=2, type=int, required=True)
    args = parser.parse_args()
    status = None if args.exit_status == "null" else int(args.exit_status)
    value = record(args.root, args.phase, args.reason, status, args.ports)
    args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
