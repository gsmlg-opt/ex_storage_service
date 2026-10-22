"""Execute the unchanged observer, decorating only probe boundaries."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import threading
from common import (clocks, compact_thread_delta, compact_thread_snapshot,
                    datagram_limit, diagnostic_mode, thread_delta, thread_snapshot, write_json)


def decorate_probe(original_probe, mode, channel, counts):
    mode = diagnostic_mode(mode)
    lock = threading.Lock()
    local = threading.local()

    def probe(function):
        ordinal = getattr(local, "ordinal", 0)
        local.ordinal = ordinal + 1
        tid = threading.get_native_id()
        if mode == "probe-only":
            before = compact_thread_snapshot()
        else:
            before = thread_snapshot(os.getpid(), tid)
        # Neither snapshots nor serialization alter the original probe window.
        value, window = original_probe(function)
        if mode == "probe-only":
            after = compact_thread_snapshot()
            delta = compact_thread_delta(before, after)
        else:
            after = thread_snapshot(os.getpid(), tid)
            delta = thread_delta(before, after)
        # RSS/temporary and encoding/write alternate per original thread.
        event = dict(kind="probe_boundary", mode=mode, tid=tid, ordinal=ordinal,
                     role=("rss" if ordinal % 2 == 0 else "temporary") if tid == os.getpid()
                          else ("encoding" if ordinal % 2 == 0 else "export_write"),
                     original_window=window, before=before, after=after,
                     scheduling_delta=delta)
        encoded = json.dumps(event, separators=(",", ":")).encode()
        try:
            if len(encoded) > datagram_limit(mode):
                raise OSError("oversized diagnostic event")
            channel.send(encoded)
            with lock:
                counts["sent"] += 1
        except (BlockingIOError, OSError):
            with lock:
                counts["dropped"] += 1
        return value, window
    return probe


def main():
    sys.dont_write_bytecode = True
    original = Path(sys.argv[1]).resolve()
    diagnostic = Path(os.environ["ESS_OBSERVER_DIAGNOSTIC_ROOT"])
    mode = diagnostic_mode()
    channel = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    channel.connect(str(diagnostic / "events.sock"))
    channel.setblocking(False)
    counts = dict(sent=0, dropped=0)
    spec = importlib.util.spec_from_file_location("unchanged_resource_observer", original)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.probe = decorate_probe(module.probe, mode, channel, counts)
    sys.argv = [str(original), *sys.argv[2:]]
    try:
        module.main()
    finally:
        write_json(diagnostic / "observer-instrumentation-terminal.json",
                   dict(kind="terminal", mode=mode, pid=os.getpid(), counts=counts, clocks=clocks()))
        channel.close()


if __name__ == "__main__":
    main()
