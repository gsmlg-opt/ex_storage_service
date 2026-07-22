defmodule ExStorageService.Cluster.ThreeVoterClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 120_000

  @cookie :ess_phase4_cluster_test
  @majority_partition_cookie :ess_phase4_partition_majority
  @minority_partition_cookie :ess_phase4_partition_minority

  @tag :tmp_dir
  test "three voters preserve quorum and isolate the metadata-only role", %{tmp_dir: tmp_dir} do
    suffix = System.unique_integer([:positive, :monotonic])

    nodes =
      for {id, role, label} <- [
            {"node-a", :data, "a"},
            {"node-b", :data, "b"},
            {"node-c", :metadata, "c"}
          ] do
        peer = start_peer!("ess_phase4_#{label}_#{suffix}")
        on_exit(fn -> stop_peer(peer.pid) end)

        Map.merge(peer, %{
          id: id,
          role: role,
          root: Path.join(tmp_dir, id)
        })
      end

    members = Enum.map(nodes, &%{id: &1.id, endpoint: &1.node})

    Enum.each(nodes, &prepare_peer!/1)
    Enum.each(nodes, &configure_and_start!(&1, members, true))
    Enum.each(nodes, &await_ready!(&1, 20_000))

    [node_a, node_b, node_c] = nodes
    key = "phase4:cross-node:#{suffix}"

    assert :ok = call(node_a, Concord, :put, [key, "written-on-a", [timeout: 2_000]])

    assert {:ok, "written-on-a"} =
             call(node_b, Concord, :get, [key, [consistency: :strong, timeout: 2_000]])

    refute File.exists?(Path.join(node_c.root, "blob-should-not-exist"))
    refute :ex_storage_service_s3 in started_applications(node_c)
    refute :ex_storage_service_web in started_applications(node_c)

    assert nodes |> Enum.map(& &1.root) |> Enum.uniq() |> length() == 3
    assert Enum.all?(nodes, &File.dir?(&1.root))

    suspend_discovery(nodes)
    isolate(node_c, [node_a, node_b])
    await_partition!(node_c, [node_a, node_b])

    assert {:error, :timeout} =
             call(node_c, Concord, :put, ["phase4:minority:#{suffix}", "rejected", [timeout: 500]])

    majority_key = "phase4:majority:#{suffix}"
    assert :ok = call(node_a, Concord, :put, [majority_key, "committed", [timeout: 2_000]])

    heal(node_c, [node_a, node_b])
    resume_discovery(nodes)

    await_value!(node_c, majority_key, "committed", 10_000)

    stop_peer(node_c.pid)

    survivor_key = "phase4:survivor:#{suffix}"

    assert :ok =
             call(node_a, Concord, :put, [survivor_key, "two-voter-majority", [timeout: 3_000]])

    assert {:ok, "two-voter-majority"} =
             call(node_b, Concord, :get, [
               survivor_key,
               [consistency: :strong, timeout: 3_000]
             ])

    restarted_c = start_peer!("ess_phase4_c_#{suffix}")
    on_exit(fn -> stop_peer(restarted_c.pid) end)

    restarted_c =
      Map.merge(restarted_c, %{id: node_c.id, role: node_c.role, root: node_c.root})

    prepare_peer!(restarted_c)
    configure_and_start!(restarted_c, members, false)
    await_ready!(restarted_c, 20_000)
    await_value!(restarted_c, survivor_key, "two-voter-majority", 10_000)
  end

  defp start_peer!(name) do
    opts = %{
      name: String.to_atom(name),
      host: ~c"127.0.0.1",
      longnames: true,
      connection: :standard_io,
      args: [
        ~c"-setcookie",
        Atom.to_charlist(@cookie),
        ~c"-kernel",
        ~c"connect_all",
        ~c"false"
      ]
    }

    assert {:ok, pid, node} = :peer.start_link(opts)
    %{pid: pid, node: node}
  end

  defp prepare_peer!(peer) do
    :ok = call(peer, :code, :add_paths, [:code.get_path()])

    {:module, ViewstampedReplication.Replica} =
      call(peer, Code, :ensure_loaded, [ViewstampedReplication.Replica])

    assert {:ok, _started} =
             call(peer, Application, :ensure_all_started, [:viewstamped_replication])
  end

  defp configure_and_start!(peer, members, bootstrap?) do
    :ok = put_env(peer, :concord, :cluster_enabled, true)
    :ok = put_env(peer, :concord, :data_dir, peer.root)

    :ok =
      put_env(peer, :concord, :vsr,
        group_id: "phase4-cluster",
        replica_id: peer.id,
        members: members,
        transport: :distribution,
        storage: :file,
        storage_path: Path.join(peer.root, "vsr"),
        bootstrap: bootstrap?
      )

    config = [
      auto_start: false,
      mode: :cluster,
      node_role: peer.role,
      node_id: peer.id,
      cluster_name: "phase4-cluster",
      cluster_topology: :static,
      cluster_members: members,
      cluster_seeds: members |> Enum.map(& &1.endpoint) |> Enum.reject(&(&1 == peer.node)),
      cluster_bootstrap: bootstrap?,
      erlang_node: peer.node,
      erlang_cookie: @cookie,
      internal_advertised_url: "http://#{peer.id}.internal:9100",
      data_root: Path.join(peer.root, "data"),
      blob_root: Path.join(peer.root, "blob-should-not-exist"),
      tmp_root: Path.join(peer.root, "tmp-should-not-exist"),
      ra_root: Path.join(peer.root, "ra-legacy"),
      metadata_root: peer.root,
      web_enabled: false,
      public_s3_enabled: false,
      cluster_data_plane_enabled: false,
      replication_factor: 2,
      write_quorum: 2
    ]

    :ok = put_env(peer, :ex_storage_service, :instance_config, config)
    :ok = put_env(peer, :ex_storage_service, :node_role, peer.role)
    :ok = put_env(peer, :ex_storage_service_s3, :enabled, false)
    :ok = put_env(peer, :ex_storage_service_web, :enabled, false)

    assert {:ok, started} = call(peer, Application, :ensure_all_started, [:ex_storage_service])
    assert :concord in started or :concord in started_applications(peer)
    assert :ex_storage_service in started or :ex_storage_service in started_applications(peer)
  end

  defp put_env(peer, app, key, value),
    do: call(peer, Application, :put_env, [app, key, value])

  defp await_ready!(peer, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    eventually!(deadline, fn ->
      match?(
        {:ok, _status},
        call(peer, ExStorageService.Cluster.Readiness, :check, [[timeout: 500]])
      )
    end)
  end

  defp await_value!(peer, key, value, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    eventually!(deadline, fn ->
      call(peer, Concord, :get, [key, [consistency: :strong, timeout: 500]]) == {:ok, value}
    end)
  end

  defp eventually!(deadline, fun) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(100)
        eventually!(deadline, fun)
      else
        flunk("cluster condition did not become true before the deadline")
      end
    end
  end

  defp suspend_discovery(peers) do
    Enum.each(peers, fn peer ->
      :ok = call(peer, :sys, :suspend, [ExStorageService.Cluster.StaticDiscovery])
    end)
  end

  defp resume_discovery(peers) do
    Enum.each(peers, fn peer ->
      :ok = call(peer, :sys, :resume, [ExStorageService.Cluster.StaticDiscovery])
    end)
  end

  defp isolate(minority, majority) do
    Enum.each(majority, fn voter ->
      true = call(voter, Node, :set_cookie, [minority.node, @majority_partition_cookie])
      true = call(minority, Node, :set_cookie, [voter.node, @minority_partition_cookie])
      _ = call(voter, Node, :disconnect, [minority.node])
      _ = call(minority, Node, :disconnect, [voter.node])
    end)
  end

  defp heal(minority, majority) do
    Enum.each(majority, fn voter ->
      true = call(voter, Node, :set_cookie, [minority.node, @cookie])
      true = call(minority, Node, :set_cookie, [voter.node, @cookie])
      true = call(voter, Node, :connect, [minority.node])
    end)
  end

  defp await_partition!(minority, majority) do
    deadline = System.monotonic_time(:millisecond) + 5_000

    eventually!(deadline, fn ->
      minority_connections = call(minority, Node, :list, [])

      Enum.all?(majority, fn voter ->
        minority.node not in call(voter, Node, :list, []) and
          voter.node not in minority_connections
      end)
    end)
  end

  defp started_applications(peer) do
    peer
    |> call(Application, :started_applications, [])
    |> Enum.map(&elem(&1, 0))
  end

  defp call(peer, module, function, args, timeout \\ 30_000) do
    :peer.call(peer.pid, module, function, args, timeout)
  end

  defp stop_peer(pid) do
    try do
      :peer.stop(pid)
    catch
      :exit, _reason -> :ok
    end
  end
end
