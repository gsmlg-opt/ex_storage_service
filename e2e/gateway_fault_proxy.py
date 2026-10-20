"""Bounded transparent fault proxy for disposable gateway acceptance tests."""
import collections
import http.client
import http.server
import threading
import time
import urllib.parse


class FaultProxy:
    def __init__(self, upstream):
        self.upstream = urllib.parse.urlsplit(upstream)
        self.lock = threading.Lock()
        self.counts = collections.Counter()
        self.rule = None
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass  # Signed headers and credentials never enter proxy logs.

            def do_GET(self):
                self.forward()

            do_HEAD = do_PUT = do_DELETE = do_POST = do_GET

            def forward(self):
                connection = None
                try:
                    with owner.lock:
                        owner.counts[(self.command, self.path)] += 1
                        rule = owner.rule
                    paths = rule["path"] if rule and isinstance(rule["path"], list) else [rule["path"]] if rule else []
                    matches = rule and rule["method"] == self.command and self.path in paths
                    if matches and rule["kind"] == "status":
                        return self.error_status(rule["status"])
                    if matches and rule["kind"] == "timeout":
                        rule["observed"].set()
                        rule["release"].wait(75)
                        return
                    if self.headers.get("Transfer-Encoding"):
                        return self.error_status(400)
                    connection = http.client.HTTPConnection(owner.upstream.hostname,
                                                           owner.upstream.port, timeout=75)
                    connection.putrequest(self.command, self.path, skip_host=True,
                                          skip_accept_encoding=True)
                    for name, value in self.headers.items():
                        if name.lower() not in {"connection", "expect"}:
                            connection.putheader(name, value)
                    connection.putheader("Connection", "close")
                    connection.endheaders()
                    remaining = int(self.headers.get("Content-Length", "0"))
                    while remaining:
                        chunk = self.rfile.read(min(65_536, remaining))
                        if not chunk:
                            raise ConnectionError("interrupted proxy upload")
                        connection.send(chunk)
                        if matches and rule["kind"] == "pace-upload":
                            time.sleep(len(chunk) / rule["rate"])
                        remaining -= len(chunk)
                    response = connection.getresponse()
                    if matches and rule["kind"] == "hold-success" and response.status < 300:
                        while response.read(65_536):
                            pass
                        rule["upstream_status"] = response.status
                        rule["observed"].set()
                        rule["release"].wait(75)
                        return  # The owned BEAM is terminated before releasing this response.
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in {"transfer-encoding", "connection", "server", "date"}:
                            self.send_header(name, value)
                    self.send_header("Connection", "close")
                    self.end_headers()
                    if self.command != "HEAD":
                        while chunk := response.read(65_536):
                            self.wfile.write(chunk)
                            if matches and rule["kind"] == "interrupt-download":
                                self.wfile.flush()
                                rule["observed"].set()
                                return  # Retain the declared full Content-Length.
                            if matches and rule["kind"] == "pace-download":
                                time.sleep(len(chunk) / rule["rate"])
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    if connection:
                        connection.close()
                    self.close_connection = True

            def error_status(self, status):
                body = b"<Error><Code>InjectedUpstreamFault</Code></Error>"
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def fault(self, kind, method, path, status=None, rate=None):
        self.clear()
        rule = {"kind": kind, "method": method, "path": path, "status": status,
                "observed": threading.Event(), "release": threading.Event(), "rate": rate}
        with self.lock:
            self.rule = rule
        return rule

    def clear(self):
        with self.lock:
            if self.rule:
                self.rule["release"].set()
            self.rule = None

    def count(self, method, path):
        with self.lock:
            return self.counts[(method, path)]

    def snapshot_counts(self):
        with self.lock:
            return dict(self.counts)

    def mutations(self):
        with self.lock:
            return {item: count for item, count in self.counts.items()
                    if item[0] in {"PUT", "POST", "DELETE"}}

    def close(self):
        self.clear()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
