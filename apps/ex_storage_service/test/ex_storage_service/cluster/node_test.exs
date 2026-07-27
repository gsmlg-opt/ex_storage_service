defmodule ExStorageService.Cluster.NodeTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.Node
  alias ExStorageService.InstanceConfig

  test "builds the persistent control record from validated instance configuration" do
    assert {:ok, config} =
             InstanceConfig.new(
               cluster_options(
                 node_generation: 7,
                 node_enabled: true,
                 node_draining: false,
                 node_zone: "rack-a",
                 node_capacity: 1_000_000
               )
             )

    timestamp = "2026-07-27T00:00:00Z"
    node = Node.from_config(config, timestamp: timestamp)

    assert %Node{
             schema: 2,
             node_id: "node-a",
             generation: 7,
             role: :data,
             erlang_endpoint: :"ess-a@127.0.0.1",
             internal_endpoint: "http://ess-a.internal:9100",
             enabled: true,
             draining: false,
             zone: "rack-a",
             capacity: 1_000_000,
             updated_at: ^timestamp
           } = node

    assert Map.keys(Map.from_struct(node)) |> Enum.sort() ==
             [
               :capacity,
               :draining,
               :enabled,
               :erlang_endpoint,
               :generation,
               :internal_endpoint,
               :node_id,
               :role,
               :schema,
               :updated_at,
               :zone
             ]
  end

  test "casts persisted maps and rejects incomplete records" do
    node = cluster_node("node-a")

    assert {:ok, ^node} = node |> Map.from_struct() |> Node.cast()
    assert {:error, :invalid_cluster_node} = Node.cast(%{node_id: "node-a"})
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

  defp cluster_options(overrides) do
    [
      mode: :cluster,
      node_id: "node-a",
      cluster_name: "ess-test",
      cluster_topology: :static,
      cluster_members: [
        %{id: "node-a", endpoint: :"ess-a@127.0.0.1"},
        %{id: "node-b", endpoint: :"ess-b@127.0.0.1"},
        %{id: "node-c", endpoint: :"ess-c@127.0.0.1"}
      ],
      cluster_seeds: [:"ess-b@127.0.0.1", :"ess-c@127.0.0.1"],
      erlang_node: :"ess-a@127.0.0.1",
      erlang_cookie: :ess_test_cookie,
      internal_advertised_url: "http://ess-a.internal:9100",
      public_s3_enabled: false,
      web_enabled: false
    ]
    |> Keyword.merge(overrides)
  end
end
