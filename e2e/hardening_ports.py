"""Explicit listener ports that automatic client/distribution sockets cannot steal.

Ports 49152–65535 belong to the native journal fixture and are excluded too.
Selections are bind-checked and never reissued within this process. The caller
still owns starting its listener; another explicit binder is not excluded by a
closed probe socket. Continuously held bind(0) listeners need no handoff helper.
"""
import errno
from pathlib import Path
from random import SystemRandom
import socket
import subprocess
import sys
import threading


_lock = threading.Lock()
_remaining = None


def _parse_ranges(value, count):
    fields = [int(field) for field in value.split()]
    if len(fields) != count:
        raise ValueError("unexpected ephemeral range fields")
    ranges = tuple(zip(fields[::2], fields[1::2]))
    if any(not 1 <= low <= high <= 65535 for low, high in ranges):
        raise ValueError("invalid ephemeral port range")
    return ranges


def _ephemeral_ranges():
    if sys.platform.startswith("linux"):
        try:
            return _parse_ranges(Path("/proc/sys/net/ipv4/ip_local_port_range").read_text(), 2)
        except (OSError, ValueError) as error:
            raise RuntimeError("cannot determine Linux automatic ephemeral port range") from error
    if sys.platform == "darwin":
        try:
            result = subprocess.run(["sysctl", "-n", "net.inet.ip.portrange.first",
                "net.inet.ip.portrange.last", "net.inet.ip.portrange.hifirst",
                "net.inet.ip.portrange.hilast"], check=True, capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.SubprocessError):
            pass
        else:
            return _parse_ranges(result.stdout, 4)
    # Covers standard Linux/BSD/macOS/Windows automatic ranges when proc/sysctl
    # discovery is unavailable on non-Linux platforms. Linux never guesses.
    return ((32768, 65535),)


def free_port():
    global _remaining
    with _lock:
        if _remaining is None:
            ranges = _ephemeral_ranges()
            _remaining = [port for port in range(1024, 49152)
                          if not any(low <= port <= high for low, high in ranges)]
            SystemRandom().shuffle(_remaining)
        while _remaining:
            port = _remaining.pop()
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", port))
                except OSError as error:
                    if error.errno in (errno.EADDRINUSE, errno.EACCES):
                        continue
                    raise
            return port
    raise RuntimeError("no unused non-ephemeral hardening listener port available")
