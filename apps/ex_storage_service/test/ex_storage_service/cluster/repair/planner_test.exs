defmodule ExStorageService.Cluster.Repair.PlannerTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Node, Repair.Planner}
  alias ExStorageService.Metadata.Models.{Blob, BlobLocation}
  alias ExStorageService.{Context, InstanceConfig}

  defmodule MembershipDouble do
    def members(_config, opts), do: {:ok, Agent.get(opts[:engine], & &1.members)}
  end

  defmodule CatalogDouble do
    def list_page(_shard, _cursor, _limit, opts) do
      {:ok, %{records: Agent.get(opts[:engine], & &1.records), next_cursor: nil}}
    end
  end

  defmodule LocationsDouble do
    def list(_hash, opts), do: {:ok, Agent.get(opts[:engine], & &1.locations)}
  end

  defmodule OutboxDouble do
    def enqueue_legacy(events, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:planned_events, events})

      send(
        Agent.get(opts[:engine], & &1.owner),
        {:planned_operation, opts[:operation_id], events}
      )

      :ok
    end
  end

  defmodule SnapshotBackendDouble do
    def put(key, value, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:snapshot_put, key, value})
      :ok
    end
  end

  test "each repair shard has exactly one deterministic owner" do
    members = Enum.map(["node-a", "node-b", "node-c"], &member/1)

    ownership =
      Map.new(Planner.shards(), &{&1, Planner.owner(&1, Enum.reverse(members))})

    assert map_size(ownership) == 256
    assert Enum.all?(ownership, fn {_shard, owner} -> match?({:ok, _node_id}, owner) end)

    owned =
      Enum.flat_map(members, fn %{node: node} ->
        Enum.map(Planner.owned_shards(node.node_id, members), &{&1, node.node_id})
      end)

    assert length(owned) == 256
    assert owned |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 256
  end

  test "a draining node owns no maintenance shards" do
    members = [member("node-a", draining: true), member("node-b")]
    assert Planner.owned_shards("node-a", members) == []
    assert length(Planner.owned_shards("node-b", members)) == 256
  end

  test "adding a node moves replicas only to the added node" do
    old_members = Enum.map(["node-a", "node-b", "node-c"], &member/1)
    new_members = old_members ++ [member("node-d")]

    Enum.each(1..500, fn index ->
      hash = sha256("blob-#{index}")
      descriptor = blob(hash, 2)

      assert {:ok, old_plan} =
               Planner.plan_blob(descriptor, old_members, ready_locations(hash, old_members))

      assert {:ok, new_plan} =
               Planner.plan_blob(descriptor, new_members, ready_locations(hash, old_members))

      old_ids = MapSet.new(old_plan.desired, & &1.node.node_id)
      new_ids = MapSet.new(new_plan.desired, & &1.node.node_id)
      added = MapSet.difference(new_ids, old_ids)
      old_excess = MapSet.new(old_plan.excess, & &1.location.node_id)
      new_excess = MapSet.new(new_plan.excess, & &1.location.node_id)
      newly_excess = MapSet.difference(new_excess, old_excess)

      assert added == MapSet.new() or added == MapSet.new(["node-d"])
      assert length(new_plan.missing) <= 1
      assert MapSet.size(newly_excess) <= 1
    end)
  end

  test "drain plans replacement before the old location becomes excess-only cleanup" do
    hash = sha256("draining")
    data_a = member("node-a", draining: true)
    data_b = member("node-b")
    data_c = member("node-c")
    members = [data_a, data_b, data_c]
    descriptor = blob(hash, 2)

    initial = [location(hash, data_a), location(hash, data_b)]

    assert {:ok, before_copy} = Planner.plan_blob(descriptor, members, initial)
    assert Enum.map(before_copy.missing, & &1.node.node_id) == ["node-c"]
    assert Enum.map(before_copy.excess, & &1.location.node_id) == ["node-a"]
    assert length(before_copy.ready_desired) == 1

    assert {:ok, after_copy} =
             Planner.plan_blob(descriptor, members, [location(hash, data_c) | initial])

    assert after_copy.missing == []
    assert length(after_copy.ready_desired) == 2
    assert Enum.map(after_copy.excess, & &1.location.node_id) == ["node-a"]
  end

  test "stale and unhealthy records on a draining node remain cleanup candidates" do
    hash = sha256("stale-draining-location")
    data_a = member("node-a", draining: true, generation: 2)
    data_b = member("node-b")
    data_c = member("node-c")

    stale = %{
      key: "location:node-a",
      mod_revision: 7,
      location: %BlobLocation{
        hash: hash,
        node_id: "node-a",
        node_generation: 1,
        state: :suspect,
        size: 12,
        verified_at: 1
      }
    }

    assert {:ok, plan} =
             Planner.plan_blob(
               blob(hash, 2),
               [data_a, data_b, data_c],
               [stale, location(hash, data_b), location(hash, data_c)]
             )

    assert plan.missing == []
    assert Enum.map(plan.excess, & &1.location.node_id) == ["node-a"]
  end

  test "repeated drain topology revisions create a new repair occurrence" do
    hash = sha256("repeated-drain")
    shard = String.slice(hash, 0, 2)
    owner = self()
    draining = member("node-a", draining: true)
    data_b = member("node-b")
    data_c = member("node-c")
    first_members = [draining, data_b, data_c]
    {:ok, owner_node_id} = Planner.owner(shard, first_members)

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          members: first_members,
          records: [%{key: "blob:#{hash}", descriptor: blob(hash, 2), mod_revision: 1}],
          locations: [location(hash, draining), location(hash, data_b)]
        }
      end)

    {:ok, config} = InstanceConfig.new(auto_start: false)

    context =
      Context.new(%{
        config
        | mode: :cluster,
          node_id: owner_node_id,
          node_generation: 1,
          workers: Map.put(config.workers, :repair, true)
      })

    planner_opts = [
      engine: engine,
      membership: MembershipDouble,
      catalog: CatalogDouble,
      locations: LocationsDouble,
      outbox: OutboxDouble
    ]

    assert {:ok, %{planned: 1}} = Planner.run_page(context, shard, nil, 10, planner_opts)

    assert_receive {:planned_operation, first_operation_id, [%{id: first_event_id}]}

    Agent.update(engine, fn state ->
      members =
        Enum.map(state.members, fn member ->
          %{member | mod_revision: member.mod_revision + 10}
        end)

      %{state | members: members}
    end)

    assert {:ok, %{planned: 1}} = Planner.run_page(context, shard, nil, 10, planner_opts)

    assert_receive {:planned_operation, second_operation_id, [%{id: second_event_id}]}
    refute first_operation_id == second_operation_id
    refute first_event_id == second_event_id
  end

  test "scrub-only workers enqueue a bounded durable scrub job when verification is due" do
    hash = sha256("scrub-due")
    owner = self()
    local = member("node-a")

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          owner: owner,
          members: [local],
          records: [
            %{key: "blob:#{hash}", descriptor: blob(hash, 1), mod_revision: 1}
          ],
          locations: [location(hash, local)]
        }
      end)

    {:ok, config} = InstanceConfig.new(auto_start: false)

    context =
      Context.new(%{
        config
        | mode: :cluster,
          node_id: "node-a",
          node_generation: 1,
          workers: Map.merge(config.workers, %{repair: false, scrub: true})
      })

    assert {:ok, %{planned: 1, blobs: 1, next_cursor: nil}} =
             Planner.run_page(context, String.slice(hash, 0, 2), nil, 10,
               engine: engine,
               membership: MembershipDouble,
               catalog: CatalogDouble,
               locations: LocationsDouble,
               outbox: OutboxDouble,
               now_ms: 100_000,
               scrub_interval_ms: 1
             )

    assert_receive {:planned_events,
                    [
                      %{
                        kind: :scrub,
                        payload: %{
                          hash: ^hash,
                          target_node_id: "node-a",
                          target_node_generation: 1
                        }
                      }
                    ]}
  end

  test "publishes a complete topology-fenced snapshot after all owned shards finish" do
    owner = self()
    local = member("node-a")

    {:ok, engine} =
      Agent.start_link(fn ->
        %{owner: owner, members: [local], records: [], locations: []}
      end)

    {:ok, config} = InstanceConfig.new(auto_start: false)

    context =
      Context.new(%{
        config
        | mode: :cluster,
          node_id: "node-a",
          node_generation: 1
      })

    {:ok, planner} =
      Planner.start_link(
        context: context,
        scan_interval: 0,
        planner_opts: [
          engine: engine,
          membership: MembershipDouble,
          catalog: CatalogDouble,
          backend: SnapshotBackendDouble,
          status_now_ms: 100_000
        ]
      )

    Process.unlink(planner)
    on_exit(fn -> if Process.alive?(planner), do: GenServer.stop(planner) end)

    assert_receive {:snapshot_put, key, snapshot}, 5_000

    assert key == ExStorageService.Metadata.Keys.cluster_status_owner("node-a")
    assert snapshot.schema == 2
    assert snapshot.complete
    assert snapshot.node_id == "node-a"
    assert snapshot.node_generation == 1
    assert snapshot.topology_fingerprint == Planner.topology_fingerprint([local])
    assert snapshot.owned_shards == Planner.shards()
    assert snapshot.actual_replicas == 0
    assert snapshot.under_replicated_blobs == 0
    assert snapshot.unavailable_blobs == 0
    assert snapshot.scan_errors == 0
    assert snapshot.updated_at_ms == 100_000

    GenServer.stop(planner)
  end

  defp blob(hash, replication_factor) do
    %Blob{
      hash: hash,
      size: 12,
      created_at: "2026-07-29T00:00:00Z",
      desired_replication_factor: replication_factor
    }
  end

  defp ready_locations(hash, members), do: Enum.map(members, &location(hash, &1))

  defp location(hash, %{node: node}) do
    %{
      key: "location:#{node.node_id}",
      location: %BlobLocation{
        hash: hash,
        node_id: node.node_id,
        node_generation: node.generation,
        size: 12,
        verified_at: 1
      }
    }
  end

  defp member(node_id, opts \\ []) do
    %{
      mod_revision: :erlang.phash2(node_id),
      node: %Node{
        schema: 2,
        node_id: node_id,
        generation: Keyword.get(opts, :generation, 1),
        role: :data,
        erlang_endpoint: :"#{node_id}@127.0.0.1",
        internal_endpoint: "http://#{node_id}.internal:9100",
        enabled: true,
        draining: Keyword.get(opts, :draining, false),
        zone: nil,
        capacity: nil,
        updated_at: "2026-07-29T00:00:00Z"
      }
    }
  end

  defp sha256(value),
    do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower)
end
