import hashlib
import io
import json
import os
from pathlib import Path
import platform
import sys
import tarfile
import tempfile
import unittest
import zipfile

from package_probe import CLI, ESS_LICENSE, REQUIRED, extract_package, inspect_escript, run_pty


class EscriptApplicationBoundaryTest(unittest.TestCase):
    def archive(self, extra, exclude=(), license_bytes=ESS_LICENSE):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            for suffix in (
                "priv/compatibility/aws/service-2.json",
                "priv/compatibility/NOTICE-YAML.txt",
                "priv/compatibility/NOTICE-QUERY.txt",
                "priv/compatibility/LICENSE-ESS.txt",
                "priv/native/ess_signals.so",
                "priv/native/ess_signals.json",
                "priv/native/ess_pager",
                "priv/native/ess_pager.json",
            ):
                if suffix not in exclude:
                    contents = license_bytes if suffix.endswith("LICENSE-ESS.txt") else b"fixture"
                    archive.writestr("ex_storage_service_cli/" + suffix, contents)
            archive.writestr("http_fetch/ebin/http_fetch.app", b"client transport")
            for name in extra:
                archive.writestr(name, b"injected server application")
        payload.seek(0)
        return payload

    def test_standalone_cli_and_client_transport_are_allowed(self):
        self.assertEqual(9, inspect_escript(self.archive([])))

    def test_missing_and_changed_own_license_are_rejected(self):
        for archive in (
            self.archive([], exclude=["priv/compatibility/LICENSE-ESS.txt"]),
            self.archive([], license_bytes=ESS_LICENSE.replace(b"Copyright", b"Changed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "ESS license notice"):
                inspect_escript(archive)

    def test_missing_embedded_pager_assets_fail_before_runtime(self):
        for suffix in ("priv/native/ess_pager", "priv/native/ess_pager.json"):
            with self.subTest(asset=suffix), self.assertRaisesRegex(RuntimeError, "Missing escript asset"):
                inspect_escript(self.archive([], exclude=[suffix]))

    def test_actual_mix_unversioned_roots_reject_server_and_framework_leaks(self):
        for name in (
            "ex_storage_service/ebin/ex_storage_service.app",
            "ex_storage_service_cluster/ebin/ex_storage_service_cluster.app",
            "ex_storage_service_s3/ebin/ex_storage_service_s3.app",
            "ex_storage_service_web/ebin/ex_storage_service_web.app",
            "phoenix/ebin/phoenix.app",
            "concord/ebin/concord.app",
            "ecto/ebin/ecto.app",
        ):
            with self.subTest(entry=name), self.assertRaisesRegex(RuntimeError, "Server application leaked"):
                inspect_escript(self.archive([name]))

    def test_versioned_application_roots_remain_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Server application leaked"):
            inspect_escript(self.archive(["phoenix-1.8.0/ebin/phoenix.app"]))


@unittest.skipUnless(platform.system() == "Linux", "PTY pager acceptance is Linux-only")
class PagerTerminalRunnerTest(unittest.TestCase):
    def test_controlling_terminal_and_literal_arguments(self):
        logs = []
        output = run_pty([sys.executable, "-c",
            "import os,sys; f=open('/dev/tty'); assert os.tcgetpgrp(f.fileno())==os.getpgrp(); print(sys.argv[1])",
            "literal $(not-a-command); *"], Path.cwd(), dict(os.environ), logs)
        self.assertEqual(b"literal $(not-a-command); *\n", output)
        self.assertEqual(0, logs[0]["status"])

    def test_timeout_and_output_limit_reap_owned_process(self):
        for code, timeout, budget, message in [
            ("import time; time.sleep(30)", 0.1, 1024, "deadline"),
            ("import os,time; os.write(1,b'x'*65536); time.sleep(30)", 5, 1024, "budget"),
        ]:
            logs = []
            with self.subTest(reason=message), self.assertRaisesRegex(RuntimeError, message):
                run_pty([sys.executable, "-c", code], Path.cwd(), dict(os.environ), logs,
                        timeout=timeout, max_output=budget)
            self.assertLess(logs[0]["status"], 0)


class SourceLicenseDistributionTest(unittest.TestCase):
    def package(self, root, source_license=ESS_LICENSE, embedded_license=ESS_LICENSE):
        files = {name: b"fixture" for name in REQUIRED}
        files["LICENSE"] = source_license
        notice = "priv/compatibility/LICENSE-ESS.txt"
        if embedded_license is None:
            files.pop(notice, None)
        else:
            files[notice] = embedded_license
        files["priv/compatibility/aws/service-2.json"] = json.dumps(
            {"operations": {str(index): {} for index in range(108)}}
        ).encode()
        contents = io.BytesIO()
        with tarfile.open(fileobj=contents, mode="w:gz") as inner:
            for name, data in files.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                inner.addfile(member, io.BytesIO(data))
        data = contents.getvalue()
        metadata = b"fixture metadata"
        checksum = hashlib.sha256(b"3" + metadata + data).hexdigest().encode()
        archive = Path(root) / "source.tar"
        with tarfile.open(archive, "w") as outer:
            for name, payload in (
                ("VERSION", b"3"), ("metadata.config", metadata),
                ("contents.tar.gz", data), ("CHECKSUM", checksum),
            ):
                member = tarfile.TarInfo(name)
                member.size = len(payload)
                outer.addfile(member, io.BytesIO(payload))
        destination = Path(root) / "extracted"
        destination.mkdir()
        return archive, destination

    def test_committed_embedded_notice_is_an_exact_license_copy(self):
        self.assertEqual((CLI / "LICENSE").read_bytes(),
                         (CLI / "priv/compatibility/LICENSE-ESS.txt").read_bytes())

    def test_source_tar_includes_matching_own_and_embedded_notices(self):
        with tempfile.TemporaryDirectory() as root:
            archive, destination = self.package(root)
            extract_package(archive, destination)
            self.assertEqual((destination / "LICENSE").read_bytes(),
                             (destination / "priv/compatibility/LICENSE-ESS.txt").read_bytes())

    def test_missing_embedded_notice_in_source_tar_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            archive, destination = self.package(root, embedded_license=None)
            with self.assertRaisesRegex(RuntimeError, "Missing packaged asset.*LICENSE-ESS"):
                extract_package(archive, destination)

    def test_mismatched_and_jointly_changed_source_notices_are_rejected(self):
        changed = ESS_LICENSE.replace(b"Copyright", b"Changed")
        for source, embedded in ((ESS_LICENSE, changed), (changed, changed)):
            with self.subTest(source_changed=source != ESS_LICENSE), tempfile.TemporaryDirectory() as root:
                archive, destination = self.package(root, source, embedded)
                with self.assertRaisesRegex(RuntimeError, "ESS license notice"):
                    extract_package(archive, destination)


if __name__ == "__main__":
    unittest.main()
