"""Startup evidence identifies listener and outbound port collisions without secrets."""
import ast
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def restore_helpers():
    # These process diagnostics do not need the harness's pinned AWS SDK clients.
    source = ast.parse((Path(__file__).parent / "s3_hardening_restore.py").read_text())
    helpers = {"check", "save", "read", "free_port", "socket_snapshot", "safe_env", "Nodes"}
    body = [node for node in source.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
            and node.name in helpers]
    namespace = dict(globals(), CONTROL="unused-control.exs", COOKIE="private-cookie",
                     MASTER_KEY="private-master-key", INTERNAL_SECRET="private-internal-secret")
    exec(compile(ast.Module(body=body, type_ignores=[]), "s3_hardening_restore.py", "exec"), namespace)
    return SimpleNamespace(**namespace)


restore = restore_helpers()


class RestoreStartupDiagnosticsTest(unittest.TestCase):
    def test_selected_local_and_remote_ports_resolve_socket_owners(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            (proc / "net").mkdir()
            # /proc/net/tcp uses hexadecimal ports and decimal socket inodes.
            (proc / "net/tcp").write_text(
                "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
                "0: 0100007F:2328 00000000:0000 0A 0:0 00:0 0 1000 0 101\n"
                "1: 0100007F:C001 0100007F:2329 01 0:0 00:0 0 1000 0 102\n"
                "2: 0100007F:C002 0100007F:232A 01 0:0 00:0 0 1000 0 103\n")
            (proc / "net/tcp6").write_text("sl local_address rem_address st\n")
            (proc / "4321/fd").mkdir(parents=True)
            (proc / "4321/comm").write_text("beam.smp\n")
            (proc / "4321/cmdline").write_text("private-cookie-and-credentials")
            os.symlink("socket:[101]", proc / "4321/fd/4")
            os.symlink("socket:[102]", proc / "4321/fd/5")
            result = restore.socket_snapshot({9000, 9001}, proc=proc)
            self.assertEqual([9000, 49153], [row["local_port"] for row in result["sockets"]])
            self.assertEqual([0, 9001], [row["remote_port"] for row in result["sockets"]])
            self.assertEqual(["0A", "01"], [row["state"] for row in result["sockets"]])
            for row in result["sockets"]:
                self.assertEqual([{"pid": 4321, "comm": "beam.smp"}], row["owners"])
            self.assertNotIn("private-cookie", json.dumps(result))

    def test_failed_start_keeps_port_receipt_and_original_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nodes = restore.Nodes("cluster", 1, root / "nodes", "fixture", root / "evidence",
                                  root / "control", "source")
            failure = AssertionError("original startup failure")
            with patch.object(nodes, "_start", side_effect=failure), \
                    patch.dict(restore.Nodes.start.__globals__, socket_snapshot=lambda ports: {"sockets": [], "unreadable": 0}):
                with self.assertRaises(AssertionError) as raised:
                    nodes.start("source", bootstrap=True)
            self.assertIs(failure, raised.exception)
            receipt = restore.read(root / "evidence/source-source-start-1.json")
            self.assertEqual(nodes.ports, receipt["s3_ports"])
            self.assertEqual(nodes.internal_ports, receipt["internal_ports"])
            self.assertEqual("AssertionError", receipt["failure_type"])
            self.assertIn("before", receipt)
            self.assertIn("failure", receipt)
            for secret in (restore.COOKIE, restore.MASTER_KEY, restore.INTERNAL_SECRET):
                self.assertNotIn(secret, json.dumps(receipt))

    def test_exited_node_records_pid_and_status_before_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nodes = restore.Nodes("standalone", 1, root / "nodes", "fixture", root / "evidence",
                                  root / "control", "source")
            process = SimpleNamespace(pid=4321, poll=lambda: 17, wait=lambda timeout: 17)
            try:
                with patch.object(restore.subprocess, "Popen", return_value=process), \
                        patch.dict(restore.Nodes.start.__globals__, socket_snapshot=lambda ports: {"sockets": [], "unreadable": 0}):
                    with self.assertRaisesRegex(AssertionError, "data-a exited during source"):
                        nodes.start("source")
                receipt = restore.read(root / "evidence/source-source-start-1.json")
                self.assertEqual({"data-a": {"pid": 4321, "exit_status": 17}}, receipt["processes"])
            finally:
                nodes.stop()
            self.assertEqual({}, nodes.processes)
            self.assertEqual([], nodes.handles)


if __name__ == "__main__":
    unittest.main()
