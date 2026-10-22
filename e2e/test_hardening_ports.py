"""Listener handoff stays outside automatic ports, including CI reserved ranges."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import textwrap
import unittest
from unittest.mock import patch

import hardening_ports


class HardeningPortsTest(unittest.TestCase):
    def setUp(self):
        self.previous = hardening_ports._remaining
        hardening_ports._remaining = None

    def tearDown(self):
        hardening_ports._remaining = self.previous

    def test_repeated_picks_cannot_be_stolen_by_automatic_listeners(self):
        ranges = hardening_ports._ephemeral_ranges()
        picks = []
        for _ in range(300):
            chosen = hardening_ports.free_port()
            picks.append(chosen)
            self.assertFalse(any(low <= chosen <= high for low, high in ranges))
            # The automatic listener starts during the handoff to the real server.
            with socket.socket() as automatic, socket.socket() as listener:
                automatic.bind(("127.0.0.1", 0))
                automatic.listen()
                self.assertNotEqual(chosen, automatic.getsockname()[1])
                listener.bind(("127.0.0.1", chosen))
                listener.listen()
        self.assertEqual(len(picks), len(set(picks)))

    def test_parallel_outstanding_picks_are_distinct(self):
        with ThreadPoolExecutor(max_workers=8) as workers:
            picks = list(workers.map(lambda _: hardening_ports.free_port(), range(100)))
        self.assertEqual(len(picks), len(set(picks)))

    def test_busy_candidate_is_not_returned(self):
        first, second = hardening_ports.free_port(), hardening_ports.free_port()
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", first))
            occupied.listen()
            hardening_ports._remaining = [second, first]
            self.assertEqual(second, hardening_ports.free_port())

    def test_linux_reads_actual_nondefault_ephemeral_range(self):
        with patch.object(hardening_ports.sys, "platform", "linux"), \
                patch.object(hardening_ports.Path, "read_text", return_value="10000\t65000\n"):
            self.assertEqual(((10000, 65000),), hardening_ports._ephemeral_ranges())
            picks = [hardening_ports.free_port() for _ in range(20)]
        self.assertTrue(all(port < 10000 or port > 65000 for port in picks))

    def test_linux_unknown_range_fails_instead_of_guessing(self):
        for value in ["bad", "60000 30000", "0 65536"]:
            with self.subTest(value=value), patch.object(hardening_ports.sys, "platform", "linux"), \
                    patch.object(hardening_ports.Path, "read_text", return_value=value):
                with self.assertRaisesRegex(RuntimeError, "ephemeral"):
                    hardening_ports.free_port()
        with patch.object(hardening_ports.sys, "platform", "linux"), \
                patch.object(hardening_ports.Path, "read_text", side_effect=OSError("unavailable")):
            with self.assertRaisesRegex(RuntimeError, "ephemeral"):
                hardening_ports.free_port()

    def test_narrow_automatic_range_still_excludes_native_journal_ports(self):
        with patch.object(hardening_ports.sys, "platform", "linux"), \
                patch.object(hardening_ports.Path, "read_text", return_value="10000 12000\n"), \
                patch.object(hardening_ports.SystemRandom, "shuffle", return_value=None):
            picks = [hardening_ports.free_port() for _ in range(50)]
        self.assertTrue(all(1024 <= port < 49152 for port in picks))
        self.assertTrue(all(not 10000 <= port <= 12000 for port in picks))

    def test_darwin_excludes_both_configured_automatic_ranges(self):
        result = subprocess.CompletedProcess([], 0, "20000\n30000\n49152\n65535\n", "")
        with patch.object(hardening_ports.sys, "platform", "darwin"), \
                patch.object(hardening_ports.subprocess, "run", return_value=result):
            self.assertEqual(((20000, 30000), (49152, 65535)), hardening_ports._ephemeral_ranges())

    def test_portable_fallback_excludes_standard_dynamic_ranges(self):
        with patch.object(hardening_ports.sys, "platform", "win32"):
            self.assertEqual(((32768, 65535),), hardening_ports._ephemeral_ranges())
            self.assertLess(hardening_ports.free_port(), 32768)

    def test_darwin_without_sysctl_uses_conservative_fallback(self):
        with patch.object(hardening_ports.sys, "platform", "darwin"), \
                patch.object(hardening_ports.subprocess, "run", side_effect=FileNotFoundError):
            self.assertEqual(((32768, 65535),), hardening_ports._ephemeral_ranges())

    def test_exhausted_non_ephemeral_candidates_fail_explicitly(self):
        with patch.object(hardening_ports, "_ephemeral_ranges", return_value=((1024, 65535),)):
            with self.assertRaisesRegex(RuntimeError, "non-ephemeral"):
                hardening_ports.free_port()

    @unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("unshare"),
                         "isolated Linux network namespace unavailable")
    def test_ci_reserved_range_in_isolated_network_namespace(self):
        code = textwrap.dedent('''
            import json, os, socket, sys
            from pathlib import Path
            assert os.readlink('/proc/self/ns/net') != sys.argv[2], 'refuse host sysctl changes'
            Path('/proc/sys/net/ipv4/ip_local_port_range').write_text('32768 60999')
            Path('/proc/sys/net/ipv4/ip_local_reserved_ports').write_text('49152-65535')
            sys.path.insert(0, sys.argv[1])
            import hardening_ports
            picks = [hardening_ports.free_port() for _ in range(300)]
            assert len(set(picks)) == 300
            assert all(not 32768 <= port <= 60999 for port in picks)
            assert all(port < 49152 for port in picks)
            for chosen in picks[:100]:
                with socket.socket() as automatic, socket.socket() as listener:
                    automatic.bind(('127.0.0.1', 0))
                    automatic.listen()
                    assert automatic.getsockname()[1] != chosen
                    listener.bind(('127.0.0.1', chosen))
                    listener.listen()
            print(json.dumps(dict(unique=len(set(picks)), competing_listeners=100)))
        ''')
        completed = subprocess.run([shutil.which("unshare"), "--user", "--map-root-user", "--net",
                                    sys.executable, "-B", "-c", code, str(Path(__file__).parent.resolve()),
                                    os.readlink("/proc/self/ns/net")],
                                   capture_output=True, text=True, timeout=20)
        if completed.returncode and "unshare failed: Operation not permitted" in completed.stderr:
            self.skipTest("host disallows isolated user/network namespaces")
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual({"unique": 300, "competing_listeners": 100}, json.loads(completed.stdout))


if __name__ == "__main__":
    unittest.main()
