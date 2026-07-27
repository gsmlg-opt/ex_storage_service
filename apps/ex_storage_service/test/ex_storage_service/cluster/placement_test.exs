defmodule ExStorageService.Cluster.PlacementTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Node, Placement}

  test "selection is deterministic across input order for many hashes" do
    nodes = Enum.map(["node-a", "node-b", "node-c"], &cluster_node/1)

    Enum.each(1..500, fn key ->
      hash = :crypto.hash(:sha256, "object-#{key}") |> Base.encode16(case: :lower)

      assert Placement.select(hash, nodes, 2) ==
               Placement.select(hash, Enum.reverse(nodes), 2)
    end)
  end

  test "adding a node only moves primary assignments to the added node" do
    old_nodes = Enum.map(["node-a", "node-b", "node-c"], &cluster_node/1)
    new_nodes = [cluster_node("node-d") | old_nodes]

    Enum.each(1..1_000, fn key ->
      hash = :crypto.hash(:sha256, "blob-#{key}") |> Base.encode16(case: :lower)
      assert {:ok, [old_primary]} = Placement.select(hash, old_nodes, 1)
      assert {:ok, [new_primary]} = Placement.select(hash, new_nodes, 1)

      if new_primary.node_id != "node-d" do
        assert new_primary.node_id == old_primary.node_id
      end
    end)
  end

  test "only enabled non-draining data nodes with transport endpoints are eligible" do
    eligible = cluster_node("node-a")
    disabled = %{cluster_node("node-b") | enabled: false}
    draining = %{cluster_node("node-c") | draining: true}
    metadata = %{cluster_node("node-d") | role: :metadata}
    missing_endpoint = %{cluster_node("node-e") | internal_endpoint: nil}

    assert {:ok, [^eligible]} =
             Placement.select(
               "hash",
               [disabled, draining, metadata, missing_endpoint, eligible],
               1
             )

    assert {:error, :insufficient_eligible_nodes} =
             Placement.select("hash", [disabled, draining, metadata], 1)
  end

  test "requires a positive replication factor and enough unique nodes" do
    node = cluster_node("node-a")

    assert {:error, :invalid_replication_factor} = Placement.select("hash", [node], 0)

    assert {:error, :duplicate_node_id} = Placement.select("hash", [node, node], 2)
  end

  defp cluster_node(node_id) do
    %Node{
      schema: 2,
      node_id: node_id,
      generation: 1,
      role: :data,
      erlang_endpoint: :"#{node_id}@127.0.0.1",
      internal_endpoint: "http://#{node_id}.internal:9100",
      enabled: true,
      draining: false,
      zone: nil,
      capacity: nil,
      updated_at: "2026-07-27T00:00:00Z"
    }
  end
end
