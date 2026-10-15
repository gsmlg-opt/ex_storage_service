import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location("provider_probe", Path(__file__).with_name("provider_probe.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ProviderAdmissionTest(unittest.TestCase):
    def info(self, name="ess-cli-server", identity="a" * 64, port="12345"):
        return {"Name": "/" + name, "Id": identity, "Mounts": [],
                "NetworkSettings": {"Ports": {"9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": port}]}}}

    def inspect(self, info):
        return subprocess.CompletedProcess([], 0, json.dumps([info]).encode(), b"")

    def test_admission_rejects_bind_mounts_wrong_identity_and_every_public_mapping(self):
        mutations = [
            lambda info: info.update(Name="/unrelated"),
            lambda info: info.update(Id="short-id"),
            lambda info: info.update(Mounts=[{"Type": "bind", "Source": "/user/data"}]),
            lambda info: info["NetworkSettings"]["Ports"].update({"9001/tcp": [{"HostIp": "0.0.0.0", "HostPort": "3333"}]}),
            lambda info: info["NetworkSettings"]["Ports"].update({"9001/tcp": [{"HostIp": "::", "HostPort": "3333"}]}),
            lambda info: info["NetworkSettings"]["Ports"]["9000/tcp"][0].update(HostPort="wrong"),
        ]
        for mutation in mutations:
            info = copy.deepcopy(self.info())
            mutation(info)
            with self.subTest(info=info), patch.object(probe, "execute", return_value=self.inspect(info)) as execute:
                with self.assertRaises(RuntimeError):
                    probe.inspect_disposable("docker", "ess-cli-server", "http://127.0.0.1:12345")
                self.assertEqual(execute.call_args.args[0], ["docker", "inspect", "ess-cli-server"])

    def test_invalid_second_provider_prevents_all_credential_and_object_mutations(self):
        first = self.info()
        second = self.info("ess-cli-rustfs", "b" * 64, "12346")
        second["Mounts"] = [{"Type": "bind"}]
        with patch("sys.argv", self.arguments()), patch.object(probe, "Probe") as constructor:
            with patch.object(probe, "execute", side_effect=[self.inspect(first), self.inspect(second)]) as execute:
                with self.assertRaisesRegex(RuntimeError, "bind mounts"):
                    probe.main()
                constructor.assert_not_called()
                self.assertEqual([call.args[0][1] for call in execute.call_args_list], ["inspect", "inspect"])

    def test_seed_and_teardown_use_admitted_ids_even_when_names_are_reused(self):
        first = self.info()
        second = self.info("ess-cli-rustfs", "b" * 64, "12346")
        instance = Mock()
        instance.credentials = []
        instance.report = {"providers": []}
        instance.seed.return_value = {"access_key": "fixture", "secret_key": "fixture"}
        with tempfile.TemporaryDirectory(prefix="ess-provider-admission-test-") as directory:
            argv = self.arguments(str(Path(directory) / "report.json"))
            results = [self.inspect(first), self.inspect(second), subprocess.CompletedProcess([], 0, b"", b""), subprocess.CompletedProcess([], 0, b"", b"")]
            with patch("sys.argv", argv), patch.object(probe, "Probe", return_value=instance), patch.object(probe, "execute", side_effect=results) as execute:
                self.assertEqual(probe.main(), 0)
                instance.seed.assert_called_once_with("a" * 64)
                self.assertEqual([call.args[0] for call in execute.call_args_list[2:]], [
                    ["docker", "rm", "--force", "--volumes", "a" * 64],
                    ["docker", "rm", "--force", "--volumes", "b" * 64],
                ])

    def arguments(self, report="/unused/report.json"):
        return ["provider_probe.py", "--escript", "/unused/ess", "--aws", "/unused/aws", "--runtime", "docker",
                "--ess-endpoint", "http://127.0.0.1:12345", "--rustfs-endpoint", "http://127.0.0.1:12346",
                "--ess-container", "ess-cli-server", "--rustfs-container", "ess-cli-rustfs", "--report", report]


if __name__ == "__main__":
    unittest.main()
