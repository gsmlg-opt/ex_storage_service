defmodule ExStorageService.Cluster.DrainTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Drain, Node}
  alias ExStorageService.Metadata.Models.{Blob, BlobLocation}
  alias ExStorageService.{Context, InstanceConfig}

  defmodule MembershipDouble do
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

    def member(_config, node_id, opts) do
      case Enum.find(Agent.get(opts[:engine], & &1.members), &(&1.node.node_id == node_id)) do
        nil -> {:error, :not_found}
        member -> {:ok, member}
      end
    end

    def members(_config, opts), do: {:ok, Agent.get(opts[:engine], & &1.members)}
  end

  defmodule CatalogDouble do
    def list_page(_shard, _cursor, _limit, opts) do
      {:ok, %{records: Agent.get(opts[:engine], & &1.records), next_cursor: nil}}
    end
  end

  defmodule LocationsDouble do
    def list(hash, opts) do
      {:ok, Agent.get(opts[:engine], &Map.fetch!(&1.locations, hash))}
    end
  end

  test "draining either data node reports repair before retirement and preserves RF" do
    for draining_id <- ["node-a", "node-b"] do
      hash = sha256("drain-#{draining_id}")
      other_id = if draining_id == "node-a", do: "node-b", else: "node-a"
      owner = self()

      {:ok, engine} =
        Agent.start_link(fn ->
          %{
            owner: owner,
            members: [member("node-a"), member("node-b"), member("node-c")],
            records: [
              %{
                key: "blob:#{hash}",
                descriptor: blob(hash),
                mod_revision: 1
              }
            ],
            locations: %{
              hash => [location(hash, draining_id), location(hash, other_id)]
            }
          }
        end)

      context = context()
      opts = opts(engine)

      assert {:ok, %{node: %{draining: true}}} = Drain.start(context, draining_id, opts)

      assert {:ok,
              %{
                blobs_remaining: 1,
                awaiting_repair: 1,
                ready_to_retire: 0,
                blockers: []
              }} = Drain.status_page(context, draining_id, nil, 100, opts)

      Agent.update(engine, fn state ->
        update_in(state.locations[hash], &[location(hash, "node-c") | &1])
      end)

      assert {:ok,
              %{
                blobs_remaining: 1,
                awaiting_repair: 0,
                ready_to_retire: 1,
                blockers: []
              }} = Drain.status_page(context, draining_id, nil, 100, opts)

      assert {:ok, %{node: %{draining: false}}} = Drain.cancel(context, draining_id, opts)
      Agent.stop(engine)
    end
  end

  defp opts(engine),
    do: [
      engine: engine,
      membership: MembershipDouble,
      catalog: CatalogDouble,
      locations: LocationsDouble
    ]

  defp context do
    {:ok, config} = InstanceConfig.new(auto_start: false)

    Context.new(%{
      config
      | mode: :cluster,
        node_id: "node-a",
        node_generation: 1,
        replication_factor: 2,
        write_quorum: 2
    })
  end

  defp blob(hash) do
    %Blob{
      hash: hash,
      size: 12,
      created_at: "2026-07-29T00:00:00Z",
      desired_replication_factor: 2
    }
  end

  defp location(hash, node_id) do
    %{
      key: "location:#{node_id}",
      location: %BlobLocation{
        hash: hash,
        node_id: node_id,
        node_generation: 1,
        size: 12,
        verified_at: 1
      }
    }
  end

  defp member(node_id) do
    %{
      mod_revision: :erlang.phash2(node_id),
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
  end

  defp sha256(value),
    do: :sha256 |> :crypto.hash(value) |> Base.encode16(case: :lower)
end
