# Test-only consensus proof. Health responses do not establish election/rejoin.
defmodule ESSClusterPrimaryLifecycle do
  @nodes %{
    "meta" => :"ess_meta@127.0.0.1",
    "data-a" => :"ess_data_a@127.0.0.1",
    "data-b" => :"ess_data_b@127.0.0.1"
  }

  def run(mode) when mode in ["record", "elected", "rejoined"] do
    root =
      Path.expand(System.get_env("ESS_CLUSTER_E2E_ROOT", "/tmp/ex_storage_service/cluster-e2e"))

    true =
      root == "/tmp/ex_storage_service/cluster-e2e" or
        Regex.match?(~r|^/tmp/ess-s3-hardening-cluster-[A-Za-z0-9]{8}$|, root)

    %{type: :directory} = File.lstat!(root)
    path = Path.join(root, "primary-loss.json")

    state =
      if mode == "record" do
        false = File.exists?(path)
        %{"root" => root, "schema" => 1}
      else
        state = JSON.decode!(File.read!(path))
        true = state["root"] == root and state["schema"] == 1
        true = is_map_key(@nodes, state["before"]["primary"])
        state
      end

    ids =
      if mode == "elected",
        do: Map.keys(@nodes) -- [state["before"]["primary"]],
        else: Map.keys(@nodes)

    snapshot = await(mode, root, state, ids, System.monotonic_time(:millisecond) + 30_000)
    field = if mode == "record", do: "before", else: mode
    File.write!(path, JSON.encode!(Map.put(state, field, snapshot)))
    IO.puts(JSON.encode!(%{phase: mode, status: "passed", topology: snapshot}))
  end

  defp await(mode, root, state, ids, deadline) do
    statuses = Enum.map(ids, &status(root, &1))
    first = hd(statuses)

    coherent =
      first["status"] == "normal" and
        Enum.all?(statuses, fn status ->
          status["status"] == "normal" and status["commit"] == status["applied"] and
            Enum.all?(["view", "commit", "primary", "configuration"], &(status[&1] == first[&1]))
        end)

    ready =
      coherent and
        case mode do
          "record" ->
            first["primary"] in ids

          "elected" ->
            before = state["before"]

            first["primary"] in ids and first["view"] > before["view"] and
              first["commit"] >= before["commit"] and
              first["configuration"] == before["configuration"] and
              not Node.connect(Map.fetch!(@nodes, before["primary"]))

          "rejoined" ->
            elected = state["elected"]
            stopped = Enum.find(statuses, &(&1["node_id"] == state["before"]["primary"]))

            not stopped["bootstrap"] and first["view"] >= elected["view"] and
              first["commit"] >= elected["commit"] and
              first["configuration"] == elected["configuration"]
        end

    if ready do
      Map.take(first, ["view", "commit", "primary", "configuration"])
      |> Map.put("statuses", statuses)
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "consensus #{mode} did not converge: #{inspect(statuses)}"
      end

      Process.sleep(250)
      await(mode, root, state, ids, deadline)
    end
  end

  defp status(root, id) do
    target = Map.fetch!(@nodes, id)

    source = """
    if Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :ex_storage_service end) do
    {:ok, context} = ExStorageService.Context.default()
    true = context.config.mode == :cluster and context.config.node_id == #{inspect(id)}
    true = context.data_root == #{inspect(Path.join(root, id))}
    true = context.config.replication_factor == 2 and context.config.write_quorum == 2
    false = context.config.allow_degraded_writes
    {:ok, status} = ViewstampedReplication.status("ess-cluster-e2e", #{inspect(id)})
    {:ok, %{cluster: %{primary_id: primary}}} = Concord.status()
    %{"node_id" => #{inspect(id)}, "status" => to_string(status.status), "primary" => primary,
      "view" => status.view_number, "commit" => status.commit_number,
      "applied" => status.applied_number, "configuration" => Base.encode16(status.configuration_hash),
      "bootstrap" => context.config.cluster_bootstrap, "rf" => context.config.replication_factor,
      "w" => context.config.write_quorum, "degraded" => context.config.allow_degraded_writes}
    else
      %{"node_id" => #{inspect(id)}, "status" => "unavailable"}
    end
    """

    if Node.connect(target) do
      reply = :rpc.call(target, Code, :eval_string, [source], 5_000)

      case reply do
        {%{} = result, binding} when is_list(binding) ->
          result

        {:badrpc, reason} when reason in [:nodedown, :timeout] ->
          %{"node_id" => id, "status" => "unavailable"}

        {:badrpc, {:EXIT, {{:badmatch, {:error, reason}}, _stack}}}
        when reason in [:cluster_not_ready, :view_change] ->
          %{"node_id" => id, "status" => "unavailable"}

        _ ->
          raise "consensus status/fixture guard failed for #{id}"
      end
    else
      %{"node_id" => id, "status" => "unavailable"}
    end
  end
end
