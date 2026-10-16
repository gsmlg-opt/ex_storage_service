import contextlib
import hashlib
import http.server
import io
import json
from pathlib import Path
import tarfile
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

import verify_published


class RegistryFixture(http.server.BaseHTTPRequestHandler):
    responses = []
    requests = []

    def do_GET(self):
        type(self).requests.append(self.path)
        status, body = self.responses[0]
        if len(self.responses) > 1:
            self.responses.pop(0)
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class PublishedPackageVerificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RegistryFixture)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.payload, self.checksum = self.package(b"candidate")
        self.candidate = self.root / "candidate.tar"
        self.candidate.write_bytes(self.payload)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps({"version": "0.6.8", "hex_inner_checksum": self.checksum}))
        self.output = self.root / "published.tar"
        self.status = self.root / "status.json"
        RegistryFixture.requests = []
        RegistryFixture.responses = [(200, self.payload)]

    def package(self, contents):
        metadata = b"fixture metadata"
        checksum = hashlib.sha256(b"3" + metadata + contents).hexdigest()
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            for name, payload in (("VERSION", b"3"), ("CHECKSUM", checksum.encode()),
                                  ("metadata.config", metadata), ("contents.tar.gz", contents)):
                member = tarfile.TarInfo(name)
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))
        return output.getvalue(), checksum

    def arguments(self, *extra):
        return ["--version", "0.6.8", "--manifest", str(self.manifest),
                "--candidate-package", str(self.candidate), "--output", str(self.output),
                "--status-file", str(self.status), *extra]

    def verify(self, *extra):
        real_urlopen = urllib.request.urlopen

        def local_registry(url, timeout):
            self.assertEqual(30, timeout)
            self.assertTrue(url.startswith("https://repo.hex.pm/tarballs/"))
            local = f"http://127.0.0.1:{self.server.server_port}/" + url.rsplit("/", 1)[1]
            return real_urlopen(local, timeout=timeout)

        with patch("verify_published.urllib.request.urlopen", side_effect=local_registry), \
                patch("verify_published.time.sleep") as sleep, \
                contextlib.redirect_stdout(io.StringIO()):
            result = verify_published.main(self.arguments(*extra))
        return result, sleep

    def assert_no_output(self):
        self.assertFalse(self.output.exists())
        self.assertFalse(self.status.exists())

    def test_existing_identical_package_is_reused_without_changing_its_bytes(self):
        result, _sleep = self.verify("--allow-missing")
        self.assertEqual("reused", result["status"])
        self.assertEqual("0.6.8", result["version"])
        self.assertEqual(self.checksum, result["hex_inner_checksum"])
        self.assertEqual(self.payload, self.output.read_bytes())
        self.assertEqual(result, json.loads(self.status.read_text()))
        self.assertEqual(["/ex_storage_service_cli-0.6.8.tar"], RegistryFixture.requests)

    def test_core_package_and_existing_strict_argument_interface_are_supported(self):
        args = self.arguments("--package-name", "ex_storage_service")
        args[args.index("--candidate-package"):args.index("--candidate-package") + 2] = []
        with patch.object(self, "arguments", return_value=args):
            result, _sleep = self.verify()
        self.assertEqual("verified", result["status"])
        self.assertEqual(["/ex_storage_service-0.6.8.tar"], RegistryFixture.requests)
        self.assertEqual(self.payload, self.output.read_bytes())

    def test_only_missing_package_is_absent_and_has_no_retained_tar(self):
        RegistryFixture.responses = [(404, b"missing")]
        result, sleep = self.verify("--allow-missing")
        self.assertEqual("absent", result["status"])
        self.assertNotIn("hex_inner_checksum", result)
        self.assertFalse(self.output.exists())
        self.assertEqual(result, json.loads(self.status.read_text()))
        self.assertEqual(1, len(RegistryFixture.requests))
        sleep.assert_not_called()

    def test_strict_mode_retries_registry_propagation_then_verifies(self):
        RegistryFixture.responses = [(404, b"missing"), (200, self.payload)]
        result, sleep = self.verify()
        self.assertEqual("verified", result["status"])
        sleep.assert_called_once_with(5)
        self.assertEqual(2, len(RegistryFixture.requests))

    def test_strict_missing_package_exhausts_bounded_retries(self):
        RegistryFixture.responses = [(404, b"missing")]
        with self.assertRaises(urllib.error.HTTPError):
            self.verify()
        self.assertEqual(12, len(RegistryFixture.requests))
        self.assert_no_output()

    def test_non_404_http_errors_are_fatal_even_when_missing_is_allowed(self):
        for code in (401, 403, 429, 500):
            with self.subTest(code=code):
                RegistryFixture.responses = [(code, b"failure")]
                RegistryFixture.requests = []
                with self.assertRaises(urllib.error.HTTPError):
                    self.verify("--allow-missing")
                self.assertEqual(1, len(RegistryFixture.requests))
                self.assert_no_output()

    def test_different_valid_registry_payload_is_not_retained_or_reused(self):
        RegistryFixture.responses = [(200, self.package(b"other source")[0])]
        with self.assertRaisesRegex(RuntimeError, "differs from the validated release package"):
            self.verify("--allow-missing")
        self.assert_no_output()

    def test_corrupt_registry_package_is_not_retained(self):
        RegistryFixture.responses = [(200, self.payload.replace(b"candidate", b"corrupted"))]
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            self.verify("--allow-missing")
        self.assert_no_output()

    def test_registry_read_is_bounded_and_oversized_payload_is_not_retained(self):
        with patch("verify_published.PACKAGE_LIMIT", len(self.payload) - 1), \
                self.assertRaisesRegex(RuntimeError, "Registry tar exceeds its budget"):
            self.verify("--allow-missing")
        self.assert_no_output()

    def test_candidate_must_match_manifest_before_any_registry_request(self):
        self.candidate.write_bytes(self.package(b"different candidate")[0])
        with self.assertRaisesRegex(RuntimeError, "Candidate Hex source payload differs"):
            self.verify("--allow-missing")
        self.assertEqual([], RegistryFixture.requests)
        self.assert_no_output()

    def test_corrupt_candidate_fails_before_registry_request(self):
        self.candidate.write_bytes(self.payload.replace(b"candidate", b"corrupted"))
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            self.verify("--allow-missing")
        self.assertEqual([], RegistryFixture.requests)
        self.assert_no_output()

    def test_network_errors_do_not_become_absent(self):
        for error in (TimeoutError("fixture timeout"), urllib.error.URLError("fixture connection failure")):
            with self.subTest(error=type(error).__name__), \
                    patch("verify_published.urllib.request.urlopen", side_effect=error), \
                    self.assertRaises(type(error)):
                verify_published.main(self.arguments("--allow-missing"))
            self.assert_no_output()

    def test_existing_retained_file_is_never_overwritten(self):
        self.output.write_bytes(b"existing evidence")
        with self.assertRaises(FileExistsError):
            self.verify("--allow-missing")
        self.assertEqual(b"existing evidence", self.output.read_bytes())
        self.assertFalse(self.status.exists())

    def test_existing_evidence_cannot_be_reported_as_an_absent_package(self):
        self.output.write_bytes(b"existing evidence")
        RegistryFixture.responses = [(404, b"missing")]
        with self.assertRaises(FileExistsError):
            self.verify("--allow-missing")
        self.assertEqual([], RegistryFixture.requests)
        self.assertEqual(b"existing evidence", self.output.read_bytes())
        self.assertFalse(self.status.exists())


if __name__ == "__main__":
    unittest.main()
