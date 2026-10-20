"""Fault fixtures must cut real declared bodies, then restore normal delivery."""
import http.client
import http.server
import threading
import unittest

from e2e.gateway_fault_proxy import FaultProxy


class ProxyInterruptionTest(unittest.TestCase):
    def test_partial_response_retains_full_length_and_clear_restores_bytes(self):
        payload = bytes(range(256)) * 1024

        class Upstream(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        proxy = FaultProxy(f"http://127.0.0.1:{server.server_port}")
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(proxy.close)
        rule = proxy.fault("interrupt-download", "GET", "/object")
        connection = http.client.HTTPConnection("127.0.0.1", proxy.server.server_port, timeout=5)
        self.addCleanup(connection.close)
        connection.request("GET", "/object")
        response = connection.getresponse()
        self.assertEqual(str(len(payload)), response.getheader("Content-Length"))
        with self.assertRaises(http.client.IncompleteRead) as failure:
            response.read()
        self.assertEqual(payload[:65_536], failure.exception.partial)
        self.assertTrue(rule["observed"].wait(1))
        proxy.clear()
        connection.close()
        connection = http.client.HTTPConnection("127.0.0.1", proxy.server.server_port, timeout=5)
        self.addCleanup(connection.close)
        connection.request("GET", "/object")
        self.assertEqual(payload, connection.getresponse().read())


if __name__ == "__main__":
    unittest.main()
