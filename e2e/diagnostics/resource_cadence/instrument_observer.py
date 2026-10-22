"""Execute the unchanged observer, decorating only probe boundaries."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import threading
from common import clocks, thread_delta, thread_snapshot, write_json

sys.dont_write_bytecode = True
original = Path(sys.argv[1]).resolve()
diagnostic = Path(os.environ["ESS_OBSERVER_DIAGNOSTIC_ROOT"])
channel = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
channel.connect(str(diagnostic / "events.sock"))
channel.setblocking(False)
counts = dict(sent=0, dropped=0)
lock = threading.Lock()
spec = importlib.util.spec_from_file_location("unchanged_resource_observer", original)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
original_probe = module.probe
local = threading.local()


def probe(function):
    ordinal = getattr(local, "ordinal", 0)
    local.ordinal = ordinal + 1
    tid = threading.get_native_id()
    before = thread_snapshot(os.getpid(), tid)
    try:
        value, window = original_probe(function)
    except BaseException:
        raise
    else:
        after = thread_snapshot(os.getpid(), tid)
        # RSS/temporary and encoding/write alternate per original thread.
        event = dict(kind="probe_boundary", tid=tid, ordinal=ordinal,
                     role=("rss" if ordinal % 2 == 0 else "temporary") if tid == os.getpid()
                          else ("encoding" if ordinal % 2 == 0 else "export_write"),
                     original_window=window, before=before, after=after,
                     scheduling_delta=thread_delta(before, after))
        encoded = json.dumps(event, separators=(",", ":")).encode()
        try:
            if len(encoded) > 32768:
                raise ValueError("oversized diagnostic event")
            channel.send(encoded)
            with lock:
                counts["sent"] += 1
        except (BlockingIOError, OSError):
            with lock:
                counts["dropped"] += 1
        return value, window


module.probe = probe
sys.argv = [str(original), *sys.argv[2:]]
try:
    module.main()
finally:
    write_json(diagnostic / "observer-instrumentation-terminal.json",
               dict(kind="terminal", pid=os.getpid(), counts=counts, clocks=clocks()))
    channel.close()
