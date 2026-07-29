defmodule ExStorageService.Cluster.StatusProviderTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{Node, Repair.Planner, Status, StatusProvider}
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.{Context, InstanceConfig}

  defmodule MembershipDouble do
    def members(_config, opts), do: {:ok, Agent.get(opts[:engine], & &1.members)}
  end

  defmodule BackendDouble do
    def get(key, opts) do
      case Agent.get(opts[:engine], &Map.get(&1.snapshots, key)) do
        nil -> {:ok, nil}
        snapshot -> {:ok, %{value: snapshot, mod_revision: 1}}
      end
    end
  end

  defmodule JobStoreDouble do
    def list_page(cursor, _limit, opts) do
      {:ok, Agent.get(opts[:engine], &Map.fetch!(&1.job_pages, cursor))}
    end
  end

  setup do
    now_ms = 100_000
    members = [member("node-a"), member("node-b")]
    context = context()
    snapshots = owner_snapshots(members, context, now_ms)

    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          members: members,
          snapshots: snapshots,
          job_pages: %{
            nil => %{
              jobs: [
                job("repair-pending", :repair_blob, :pending),
                job("scrub-running", :scrub, :running),
                job("cleanup-failed", :cleanup, :failed),
                job("replication-pending", :cross_cluster_put, :pending)
              ],
              next_cursor: "page-2"
            },
            "page-2" => %{
              jobs: [job("repair-completed", :repair_blob, :completed)],
              next_cursor: nil
            }
          }
        }
      end)

    %{context: context, engine: engine, members: members, now_ms: now_ms}
  end

  test "aggregates every current owner and durable maintenance job page", fixture do
    assert {:ok, snapshot} = StatusProvider.snapshot(provider_opts(fixture))

    assert snapshot.complete
    assert snapshot.owners_expected == 2
    assert snapshot.owners_current == 2
    assert snapshot.invalid_owners == []
    assert snapshot.actual_replicas == 21
    assert snapshot.under_replicated_blobs == 3
    assert snapshot.unavailable_blobs == 1

    assert snapshot.repair_backlog == %{
             pending: 1,
             running: 1,
             failed: 1,
             under_replicated: 3
           }

    assert snapshot.updated_at ==
             fixture.now_ms
             |> Kernel.-(2_000)
             |> DateTime.from_unix!(:millisecond)
             |> DateTime.to_iso8601()
  end

  test "rejects stale and topology-mismatched owner snapshots", fixture do
    [first_id, second_id] =
      fixture.members
      |> Enum.map(& &1.node.node_id)
      |> Enum.sort()

    Agent.update(fixture.engine, fn state ->
      snapshots =
        state.snapshots
        |> Map.update!(Keys.cluster_status_owner(first_id), fn snapshot ->
          %{snapshot | topology_fingerprint: "old-topology"}
        end)
        |> Map.update!(Keys.cluster_status_owner(second_id), fn snapshot ->
          %{snapshot | updated_at_ms: fixture.now_ms - 10_001}
        end)

      %{state | snapshots: snapshots}
    end)

    assert {:ok, snapshot} =
             StatusProvider.snapshot(Keyword.merge(provider_opts(fixture), freshness_ms: 10_000))

    refute snapshot.complete
    assert snapshot.owners_current == 0

    assert snapshot.invalid_owners == [
             %{node_id: first_id, reason: :topology_mismatch},
             %{node_id: second_id, reason: :stale}
           ]
  end

  test "marks aggregation partial when the bounded job scan has more pages", fixture do
    assert {:ok, snapshot} =
             StatusProvider.snapshot(Keyword.merge(provider_opts(fixture), max_job_pages: 1))

    refute snapshot.complete
    assert snapshot.invalid_owners == []
    assert snapshot.repair_backlog.pending == 1
    assert snapshot.repair_backlog.running == 1
    assert snapshot.repair_backlog.failed == 1
  end

  test "cluster status uses the durable cluster-wide provider by default", fixture do
    snapshot = Status.snapshot(provider_opts(fixture))

    assert snapshot.status == :ok
    assert snapshot.complete
    assert snapshot.owners_expected == 2
    assert snapshot.owners_current == 2
    assert snapshot.repair_backlog.pending == 1
  end

  defp provider_opts(fixture) do
    [
      context: fixture.context,
      engine: fixture.engine,
      membership: MembershipDouble,
      backend: BackendDouble,
      job_store: JobStoreDouble,
      now_ms: fixture.now_ms
    ]
  end

  defp owner_snapshots(members, context, now_ms) do
    topology = Planner.topology_fingerprint(members)

    members
    |> Enum.sort_by(& &1.node.node_id)
    |> Enum.with_index()
    |> Map.new(fn {%{node: node}, index} ->
      snapshot = %{
        schema: 2,
        complete: true,
        node_id: node.node_id,
        node_generation: node.generation,
        topology_fingerprint: topology,
        owned_shards: Planner.owned_shards(node.node_id, members),
        desired_replicas: context.config.replication_factor,
        actual_replicas: 10 + index,
        required_write_quorum: context.config.write_quorum,
        under_replicated_blobs: 1 + index,
        unavailable_blobs: index,
        planned_actions: 0,
        scan_errors: 0,
        updated_at_ms: now_ms - (index + 1) * 1_000
      }

      {Keys.cluster_status_owner(node.node_id), snapshot}
    end)
  end

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

  defp job(id, kind, state) do
    {:ok, job} =
      Job.new("operation-#{id}", %{id: id, kind: kind, payload: %{}}, 1)

    %{job | state: state}
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
end
