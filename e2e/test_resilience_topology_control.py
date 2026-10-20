"""Execute the actual RPC classifiers against election and fixture-guard outcomes."""
import os
from pathlib import Path
import subprocess
import unittest


class TopologyTransientTest(unittest.TestCase):
    def test_election_pending_is_bounded_to_topology_and_guard_errors_remain_fatal(self):
        source = r'''
        for path <- ["e2e/scripts/resilience_hardening_control.exs", "e2e/scripts/cluster_primary_lifecycle.exs"] do
          ast = Code.string_to_quoted!(File.read!(path))
          {_, cases} = Macro.prewalk(ast, [], fn
            {:case, _, [{:reply, _, _}, _]} = expression, acc -> {expression, [expression | acc]}
            expression, acc -> {expression, acc}
          end)
          [classifier] = cases
          evaluate = fn reply, topology ->
            Code.eval_quoted(classifier, [reply: reply, topology?: topology,
              target: :fixture, id: "fixture"]) |> elem(0)
          end
          for reason <- [:cluster_not_ready, :view_change] do
            pending = {:badrpc, {:EXIT, {{:badmatch, {:error, reason}}, []}}}
            result = evaluate.(pending, true)
            true = result in [%{node_id: "fixture", status: "unavailable"},
                              %{"node_id" => "fixture", "status" => "unavailable"}]
            if String.contains?(path, "resilience_hardening_control") do
              try do
                evaluate.(pending, false)
                raise "non-topology mutation error was hidden"
              rescue
                error in RuntimeError -> true = String.starts_with?(error.message, "resilience RPC failed")
              end
            end
          end
          for reply <- [{:badrpc, {:EXIT, {{:badmatch, false}, []}}},
                        {:badrpc, {:EXIT, {{:badmatch, {:error, :invalid_configuration}}, []}}}] do
            try do
              evaluate.(reply, true)
              raise "fixture guard failure was hidden"
            rescue
              error in RuntimeError ->
                true = String.starts_with?(error.message, "resilience RPC failed") or
                  String.starts_with?(error.message, "consensus status/fixture guard failed")
            end
          end
          IO.puts("PASS actual topology RPC classifier: " <> path)
        end
        '''
        result = subprocess.run(["elixir", "-e", source], cwd=Path(__file__).resolve().parents[1],
            env=dict(os.environ, PAGER="cat", ERL_FLAGS="+S 4:4 +SDcpu 2 +SDio 2"),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(result.stdout.count("PASS actual topology RPC classifier:"), 2)


if __name__ == "__main__":
    unittest.main()
