"""Application-free real BEAM emission, including transient RPC-style ownership."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from resource_cadence import METRICS, measure_samples


class SamplerEmissionTest(unittest.TestCase):
    def test_cluster_rpc_embeds_the_complete_collector_source(self):
        repo = Path(__file__).resolve().parents[1]
        controller = (repo / "e2e/scripts/cluster_resource_control.exs").read_text()
        expression = re.search(r'Code\.eval_string\(#\{(inspect\(sampler[^\n]*\))\}\)', controller).group(1)
        script = '''
sampler = File.read!("e2e/scripts/resource_sampler.exs")
encoded = ENCODE_EXPRESSION
{decoded, []} = Code.eval_string(encoded)
true = decoded == sampler
{function, []} = Code.eval_string("fn -> Code.eval_string(" <> encoded <> ") end")
true = is_function(function, 0)
IO.puts(JSON.encode!(%{bytes: byte_size(decoded)}))
'''.replace("ENCODE_EXPRESSION", expression)
        completed = subprocess.run(["elixir", "-e", script], cwd=repo,
                                   env=dict(os.environ, PAGER="cat", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2"),
                                   capture_output=True, text=True, timeout=15)
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual((repo / "e2e/scripts/resource_sampler.exs").stat().st_size,
                         json.loads(completed.stdout.strip())["bytes"])

    def test_whole_collector_observes_new_files_while_shared_servers_are_held(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="ess-sampler-contention-") as directory:
            root = Path(directory)
            tmp, data = root / "tmp", root / "data"
            tmp.mkdir()
            cache = data / "bucket" / "cache" / "nested"
            cache.mkdir(parents=True)
            (tmp / "probe.tmp").write_bytes(b"staging")
            (cache / "probe.tmp").write_bytes(b"cache-bytes")
            (cache / "retained.blob").write_bytes(b"not temporary")
            (tmp / ".hidden").write_bytes(b"hidden")
            (tmp / ".hidden-dir").mkdir()
            (tmp / ".hidden-dir" / "hidden.tmp").write_bytes(b"hidden")
            (cache / ".hidden.tmp").write_bytes(b"hidden")
            (root / "linked").mkdir()
            (root / "linked" / "linked.tmp").write_bytes(b"link")
            (tmp / "linked-dir").symlink_to(root / "linked", target_is_directory=True)
            (tmp / "linked-file").symlink_to(cache / "probe.tmp")
            (tmp / "broken-link").symlink_to(root / "absent")
            environment = dict(os.environ, E2E_RESOURCE_ROOT=str(root), ESS_TMP_ROOT=str(tmp),
                               ESS_DATA_ROOT=str(data), E2E_RESOURCE_PYTHON=sys.executable, PAGER="cat",
                               ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
            script = '''
root = System.fetch_env!("E2E_RESOURCE_ROOT")
tmp = System.fetch_env!("ESS_TMP_ROOT")
data = System.fetch_env!("ESS_DATA_ROOT")
paths = Path.wildcard(Path.join([tmp, "**", "*"])) ++
  Path.wildcard(Path.join([data, "*", "cache", "**", "*.tmp"]))
expected = Enum.reduce(paths, 0, fn path, sum ->
  case File.stat(path) do
    {:ok, %{type: :regular, size: size}} -> sum + size
    _ -> sum
  end
end)
Code.eval_file("e2e/scripts/resource_sampler.exs")
Process.sleep(200)
server = Process.whereis(:file_server_2)
loader = Process.whereis(:erl_prim_loader)
:ok = :sys.suspend(server)
true = :erlang.suspend_process(loader)
held = try do
  start_ns = System.system_time(:nanosecond)
  Process.sleep(75)
  {:ok, file} = :file.open(Path.join(tmp, "new.tmp"), [:write, :raw, :binary])
  :ok = :file.write(file, "new-staging")
  :ok = :file.close(file)
  created_ns = System.system_time(:nanosecond)
  Process.sleep(200)
  %{start_ns: start_ns, created_ns: created_ns, end_ns: System.system_time(:nanosecond)}
after
  true = :erlang.resume_process(loader)
  :ok = :sys.resume(server)
end
Process.sleep(150)
File.rm_rf!(tmp)
File.rm_rf!(data)
Process.sleep(150)
IO.puts(JSON.encode!(Map.merge(held, %{expected: expected, added: byte_size("new-staging")})))
'''
            completed = subprocess.run(["elixir", "-e", script], cwd=repo, env=environment,
                                       capture_output=True, text=True, timeout=15)
            self.assertEqual(0, completed.returncode, completed.stderr)
            observed = json.loads(completed.stdout.strip())
            raw = (root / "samples.jsonl").read_bytes().splitlines()
            rows = [json.loads(line) for line in raw]
            self.assertTrue(all(len(line) + 1 <= 16_384 for line in raw))
            baseline = [row for row in rows if row["collection"]["end_ns"] < observed["start_ns"]]
            self.assertGreaterEqual(len(baseline), 2)
            self.assertTrue(all(row["temporary"] == observed["expected"] for row in baseline))
            during_new = [row for row in rows
                          if observed["created_ns"] <= row["probes"]["temporary"]["start_ns"]
                          and row["collection"]["end_ns"] <= observed["end_ns"]]
            measured = measure_samples(rows, baseline, observed["start_ns"], observed["end_ns"], {})
            # Use the unchanged original per-metric 100ms validator, including
            # both hold edges and post-idle temporary-file cleanup.
            self.assertEqual([], measured["failures"], measured["cadence"])
            self.assertGreaterEqual(len(during_new), 2)
            self.assertTrue(all(row["temporary"] == observed["expected"] + observed["added"]
                                for row in during_new))

    def test_rss_targets_explicit_beam_pid_and_reports_helper_separately(self):
        from scripts.resource_observer import rss, verify_stopped
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="ess-sampler-rss-") as directory:
            root = Path(directory)
            (root / "tmp").mkdir()
            (root / "data").mkdir()
            environment = dict(os.environ, E2E_RESOURCE_ROOT=str(root), ESS_TMP_ROOT=str(root / "tmp"),
                               ESS_DATA_ROOT=str(root / "data"), E2E_RESOURCE_PYTHON=sys.executable,
                               PAGER="cat", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
            script = '''
Code.eval_file("e2e/scripts/resource_sampler.exs")
Process.sleep(300)
IO.puts(System.pid())
receive do :never -> :ok after 5_000 -> :ok end
'''
            process = subprocess.Popen(["elixir", "-e", script], cwd=repo, env=environment,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                beam_pid = int(process.stdout.readline())
                identity = json.loads(next(root.glob("observer-*.json")).read_text())
                rows = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
                self.assertEqual(beam_pid, identity["beam"]["pid"])
                self.assertNotEqual(beam_pid, identity["observer"]["pid"])
                self.assertLess(abs(rows[-1]["rss"] - rss(beam_pid)), 16 * 1024 * 1024)
                self.assertGreater(rows[-1]["rss"], 2 * rows[-1]["observer"]["rss"])
                self.assertGreater(rows[-1]["observer"]["max_rss"], 0)
            finally:
                process.terminate()
                process.communicate(timeout=5)
            receipt = verify_stopped(root)
            self.assertEqual(1, len(receipt))
            self.assertTrue(receipt[0]["stopped"])

    def test_collector_outlives_eval_worker_and_exports_actual_probe_windows(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="ess-sampler-emission-") as directory:
            root = Path(directory)
            (root / "tmp").mkdir()
            (root / "data").mkdir()
            environment = dict(os.environ, E2E_RESOURCE_ROOT=str(root), ESS_TMP_ROOT=str(root / "tmp"),
                               ESS_DATA_ROOT=str(root / "data"), E2E_RESOURCE_PYTHON=sys.executable, PAGER="cat",
                               ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2")
            # RPC workers exit with a return-reason after evaluating their body.
            # A collector linked to that worker can die before its first export.
            script = '''
{worker, monitor} = spawn_monitor(fn ->
  Code.eval_file("e2e/scripts/resource_sampler.exs")
  exit({:rpc_return, :ok})
end)
receive do
  {:DOWN, ^monitor, :process, ^worker, {:rpc_return, :ok}} -> :ok
after
  5_000 -> raise "sampler eval worker did not finish"
end
Process.sleep(1_000)
'''
            completed = subprocess.run(["elixir", "-e", script], cwd=repo, env=environment,
                                       capture_output=True, text=True, timeout=15)
            self.assertEqual(0, completed.returncode, completed.stderr)
            raw = (root / "samples.jsonl").read_bytes().splitlines()
            self.assertGreaterEqual(len(raw), 20)
            self.assertTrue(all(len(line) + 1 <= 16_384 for line in raw))
            samples = [json.loads(line) for line in raw]
            self.assertEqual(1, len({row["sampler_id"] for row in samples}))
            for row in samples:
                self.assertEqual(set(METRICS), set(row["probes"]))
                collection = row["collection"]
                self.assertLessEqual(collection["start_ns"], collection["end_ns"])
                for metric in METRICS:
                    probe = row["probes"][metric]
                    self.assertLessEqual(collection["start_ns"], probe["start_ns"])
                    self.assertLessEqual(probe["start_ns"], probe["end_ns"])
                    self.assertLessEqual(probe["end_ns"], collection["end_ns"])
                    self.assertLessEqual(probe["start_monotonic_ns"], probe["end_monotonic_ns"])
                    self.assertIsInstance(row[metric], int)
            scheduled = [row["scheduled_monotonic_ns"] for row in samples]
            self.assertTrue(all((b - a) % 25_000_000 == 0 and b > a for a, b in zip(scheduled, scheduled[1:])))


if __name__ == "__main__":
    unittest.main()
