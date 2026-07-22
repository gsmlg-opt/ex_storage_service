defmodule ExStorageService.Cluster.TopologyTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{StaticDiscovery, Topology}
  alias ExStorageService.InstanceConfig

  test "standalone mode starts no discovery" do
    assert {:ok, config} = InstanceConfig.new([])
    assert Topology.children(config) == []
  end

  test "static discovery connects only configured remote seeds" do
    parent = self()
    connector = fn seed -> send(parent, {:connect, seed}) end

    start_supervised!(
      {StaticDiscovery,
       seeds: [:"node-b@127.0.0.1", :"node-c@127.0.0.1"],
       connector: connector,
       interval: :timer.hours(1),
       name: nil}
    )

    assert_receive {:connect, :"node-b@127.0.0.1"}
    assert_receive {:connect, :"node-c@127.0.0.1"}
  end

  test "DNS topology uses DNSCluster without changing membership" do
    config = cluster_config(:dns, ["ess.internal"])

    assert [
             {DNSCluster, [query: ["ess.internal"], name: ExStorageService.Cluster.DNSDiscovery]}
           ] = Topology.children(config)

    assert Enum.map(config.cluster_members, & &1.id) == ["node-a", "node-b", "node-c"]
  end

  defp cluster_config(topology, seeds) do
    members = [
      %{id: "node-a", endpoint: :"ess-a@127.0.0.1"},
      %{id: "node-b", endpoint: :"ess-b@127.0.0.1"},
      %{id: "node-c", endpoint: :"ess-c@127.0.0.1"}
    ]

    {:ok, config} =
      InstanceConfig.new(
        mode: :cluster,
        node_id: "node-a",
        cluster_name: "ess-test",
        cluster_topology: topology,
        cluster_members: members,
        cluster_seeds: seeds,
        erlang_node: :"ess-a@127.0.0.1",
        erlang_cookie: :ess_test_cookie,
        internal_advertised_url: "http://ess-a.internal:9100",
        public_s3_enabled: false,
        web_enabled: false
      )

    config
  end
end
