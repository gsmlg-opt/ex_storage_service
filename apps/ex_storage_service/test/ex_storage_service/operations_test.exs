defmodule ExStorageService.OperationsTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Node, Repair.Planner}
  alias ExStorageService.Metadata.Models.{Blob, BlobLocation}
  alias ExStorageService.Operations.{Cluster, Repair}
  alias ExStorageService.Operations.Blob, as: BlobOperations
  alias ExStorageService.Operations.Node, as: NodeOperations
  alias ExStorageService.{Context, InstanceConfig}

  defmodule MembershipDouble do
    def register(config, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:registered, config.node_id})
      :ok
    end

    def members(_config, opts), do: {:ok, Agent.get(opts[:engine], & &1.members)}

    def member(_config, node_id, opts) do
      case Enum.find(Agent.get(opts[:engine], & &1.members), &(&1.node.node_id == node_id)) do
        nil -> {:error, :not_found}
        member -> {:ok, member}
      end
    end

    def set_draining(_config, node_id, draining, opts) do
      Agent.get_and_update(opts[:engine], fn state ->
        members =
          Enum.map(state.members, fn
            %{node: %{node_id: ^node_id} = node} = member ->
              %{member | node: %{node | draining: draining}}

            member ->
              member
          end)

        current = Enum.find(members, &(&1.node.node_id == node_id))
        {{:ok, current}, %{state | members: members}}
      end)
    end
  end

  defmodule ReadinessDouble do
    def await(_opts),
      do: {:ok, %{cluster: %{status: :normal, primary_id: "metadata-a"}}}

    def check(_opts),
      do: {:ok, %{cluster: %{status: :normal, primary_id: "metadata-a"}}}
  end

  defmodule MembershipFailureDouble do
    def members(_config, _opts), do: {:error, :no_leader}
  end

  defmodule StatusDouble do
    def snapshot(_opts),
      do: %{status: :ok, complete: true, under_replicated_blobs: 0}
  end

  defmodule CatalogDouble do
    def get(hash, opts) do
      record =
        Agent.get(opts[:engine], fn state ->
          Enum.find(state.records, &(&1.descriptor.hash == hash))
        end)

      if record, do: {:ok, record}, else: {:error, :not_found}
    end

    def list_page(_shard, _cursor, _limit, opts) do
      {:ok, %{records: Agent.get(opts[:engine], & &1.records), next_cursor: nil}}
    end
  end

  defmodule LocationsDouble do
    def list(hash, opts) do
      {:ok, Agent.get(opts[:engine], &Map.get(&1.locations, hash, []))}
    end
  end

  defmodule OutboxDouble do
    def enqueue_legacy(events, opts) do
      send(Agent.get(opts[:engine], & &1.owner), {:outbox_enqueued, events})
      :ok
    end
  end

  test "cluster bootstrap waits for metadata, registers the node, and returns sanitized status" do
    context = context("node-a")
    {:ok, engine} = state(context, [], %{})

    assert {:ok,
            %{
              bootstrap: :complete,
              metadata: %{status: :normal, primary_id: "metadata-a"},
              cluster: %{status: :ok, complete: true}
            }} =
             Cluster.bootstrap(context,
               engine: engine,
               membership: MembershipDouble,
               readiness: ReadinessDouble,
               status: StatusDouble
             )

    assert_receive {:registered, "node-a"}
  end

  test "cluster status remains diagnostic when membership is unavailable" do
    context = context("node-a")

    assert {:ok,
            %{
              membership_status: :unavailable,
              membership_error: :no_leader,
              members: [],
              metadata: %{status: :normal}
            }} =
             Cluster.status(context,
               membership: MembershipFailureDouble,
               readiness: ReadinessDouble,
               status: StatusDouble
             )
  end

  test "blob locate returns the immutable descriptor and location revisions" do
    hash = sha256("locate")
    descriptor = blob(hash)
    records = [%{key: "blob:#{hash}", descriptor: descriptor, mod_revision: 7}]
    locations = %{hash => [location(hash, "node-a", 11)]}
    context = context("node-a")
    {:ok, engine} = state(context, records, locations)

    assert {:ok,
            %{
              descriptor: ^descriptor,
              descriptor_revision: 7,
              locations: [
                %{
                  node_id: "node-a",
                  node_generation: 1,
                  state: :ready,
                  mod_revision: 11
                }
              ]
            }} =
             BlobOperations.locate(hash,
               engine: engine,
               catalog: CatalogDouble,
               locations: LocationsDouble
             )
  end

  test "blob audit reports under-replication without mutating metadata" do
    hash = sha256("audit")
    descriptor = blob(hash)
    context = context(owner_for(hash))
    ready_node = desired_node(descriptor)
    records = [%{key: "blob:#{hash}", descriptor: descriptor, mod_revision: 3}]
    locations = %{hash => [location(hash, ready_node, 4)]}
    {:ok, engine} = state(context, records, locations)

    assert {:ok,
            %{
              blobs: 1,
              healthy: 0,
              under_replicated: 1,
              unavailable: 0,
              issues: [%{hash: ^hash, missing: [_]}],
              next_cursor: nil
            }} =
             BlobOperations.audit_page(context, nil, 10,
               engine: engine,
               membership: MembershipDouble,
               catalog: CatalogDouble,
               locations: LocationsDouble
             )

    refute_receive {:outbox_enqueued, _events}
  end

  test "repair plan is read-only while repair run enters the durable outbox path" do
    hash = sha256("repair")
    shard = String.slice(hash, 0, 2)
    descriptor = blob(hash)
    context = context(owner_for(hash))
    ready_node = desired_node(descriptor)
    records = [%{key: "blob:#{hash}", descriptor: descriptor, mod_revision: 5}]
    locations = %{hash => [location(hash, ready_node, 6)]}
    {:ok, engine} = state(context, records, locations)

    opts = [
      engine: engine,
      membership: MembershipDouble,
      catalog: CatalogDouble,
      locations: LocationsDouble,
      outbox: OutboxDouble
    ]

    assert {:ok,
            %{
              shard: ^shard,
              blobs: 1,
              actions: 1,
              plans: [%{hash: ^hash, missing: [_]}],
              next_cursor: nil
            }} = Repair.plan_page(context, shard, nil, 10, opts)

    refute_receive {:outbox_enqueued, _events}

    assert {:ok, %{blobs: 1, planned: 1, next_cursor: nil}} =
             Repair.run_page(context, shard, nil, 10, opts)

    assert_receive {:outbox_enqueued, [%{kind: :repair_blob, payload: %{hash: ^hash}}]}
  end

  test "node drain and status use the fenced drain service" do
    hash = sha256("drain")
    context = context("node-a")
    records = [%{key: "blob:#{hash}", descriptor: blob(hash), mod_revision: 1}]

    locations = %{
      hash => [location(hash, "node-a", 2), location(hash, "node-b", 3)]
    }

    {:ok, engine} = state(context, records, locations)

    opts = [
      engine: engine,
      membership: MembershipDouble,
      catalog: CatalogDouble,
      locations: LocationsDouble
    ]

    assert {:ok, %{node: %{node_id: "node-a", draining: true}}} =
             NodeOperations.drain(context, "node-a", opts)

    assert {:ok, %{node_id: "node-a", blobs_remaining: 1, next_cursor: nil}} =
             NodeOperations.status(context, "node-a", nil, 10, opts)
  end

  test "operator mutations fail closed in standalone mode" do
    {:ok, config} = InstanceConfig.new(auto_start: false)
    context = Context.new(config)

    assert {:error, :standalone_mode} = Cluster.bootstrap(context)
    assert {:error, :standalone_mode} = Repair.run_page(context, "00")
    assert {:error, :standalone_mode} = NodeOperations.drain(context, "node-a")
  end

  defp state(context, records, locations) do
    owner = self()

    Agent.start_link(fn ->
      %{
        owner: owner,
        members: members(),
        context: context,
        records: records,
        locations: locations
      }
    end)
  end

  defp context(node_id) do
    {:ok, config} = InstanceConfig.new(auto_start: false)

    Context.new(%{
      config
      | mode: :cluster,
        node_id: node_id,
        node_generation: 1,
        replication_factor: 2,
        write_quorum: 2,
        workers: Map.merge(config.workers, %{repair: true, scrub: false})
    })
  end

  defp owner_for(hash) do
    {:ok, owner} = Planner.owner(String.slice(hash, 0, 2), members())
    owner
  end

  defp desired_node(descriptor) do
    {:ok, plan} = Planner.plan_blob(descriptor, members(), [])

    plan.desired
    |> hd()
    |> Map.fetch!(:node)
    |> Map.fetch!(:node_id)
  end

  defp blob(hash) do
    %Blob{
      hash: hash,
      size: 12,
      created_at: "2026-07-29T00:00:00Z",
      desired_replication_factor: 2
    }
  end

  defp location(hash, node_id, revision) do
    %{
      key: "location:#{node_id}",
      mod_revision: revision,
      location: %BlobLocation{
        hash: hash,
        node_id: node_id,
        node_generation: 1,
        state: :ready,
        size: 12,
        verified_at: 1
      }
    }
  end

  defp members do
    Enum.with_index(["node-a", "node-b", "node-c"], 1)
    |> Enum.map(fn {node_id, revision} ->
      %{
        mod_revision: revision,
        node: %Node{
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
          updated_at: "2026-07-29T00:00:00Z"
        }
      }
    end)
  end

  defp sha256(value),
    do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower)
end
