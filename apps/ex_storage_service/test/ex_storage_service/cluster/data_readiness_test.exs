defmodule ExStorageService.Cluster.DataReadinessTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{DataReadiness, Node}
  alias ExStorageService.{Context, InstanceConfig}

  defmodule MembershipStub do
    def members(_config, opts), do: {:ok, Keyword.fetch!(opts, :members)}
  end

  defmodule TransportStub do
    def health(_context, node, opts) do
      if node.node_id in Keyword.get(opts, :healthy_nodes, []), do: :ok, else: {:error, :down}
    end
  end

  defmodule BlockingTransportStub do
    def health(_context, node, opts) do
      send(Keyword.fetch!(opts, :owner), {:health_probe_started, node.node_id, self()})

      receive do
        :release -> :ok
      end
    end
  end

  test "strict W becomes unavailable when fewer than W eligible nodes are healthy" do
    context = context()
    members = [member("node-a"), member("node-b"), member("node-c", role: :metadata)]

    opts = [
      membership: MembershipStub,
      transport: TransportStub,
      local_storage_checker: fn _context -> :ok end,
      membership_opts: [members: members],
      transport_opts: [healthy_nodes: []]
    ]

    assert {:error, {:insufficient_healthy_nodes, %{healthy: 1, required: 2}}} =
             DataReadiness.check(context, opts)

    assert {:ok, %{healthy_nodes: 2, required_write_quorum: 2}} =
             DataReadiness.check(
               context,
               put_in(opts, [:transport_opts, :healthy_nodes], ["node-b"])
             )
  end

  test "draining and disabled nodes do not satisfy the write quorum" do
    context = context()

    members = [
      member("node-a"),
      member("node-b", draining: true),
      member("node-c", enabled: false)
    ]

    assert {:error, {:insufficient_healthy_nodes, %{healthy: 1, required: 2}}} =
             DataReadiness.check(context,
               membership: MembershipStub,
               transport: TransportStub,
               local_storage_checker: fn _context -> :ok end,
               membership_opts: [members: members],
               transport_opts: [healthy_nodes: ["node-b", "node-c"]]
             )
  end

  test "remote health probes run concurrently under the replica task supervisor" do
    context = context()
    members = [member("node-a"), member("node-b"), member("node-c")]
    owner = self()

    task =
      Task.async(fn ->
        DataReadiness.check(context,
          membership: MembershipStub,
          transport: BlockingTransportStub,
          local_storage_checker: fn _context -> :ok end,
          membership_opts: [members: members],
          transport_opts: [owner: owner],
          probe_timeout: 1_000
        )
      end)

    assert_receive {:health_probe_started, "node-b", node_b}
    assert_receive {:health_probe_started, "node-c", node_c}
    send(node_b, :release)
    send(node_c, :release)

    assert {:ok, %{healthy_nodes: 3}} = Task.await(task)
  end

  defp context do
    {:ok, config} = InstanceConfig.new(auto_start: false)

    context =
      Context.new(%{
        config
        | mode: :cluster,
          node_id: "node-a",
          node_generation: 1,
          replication_factor: 2,
          write_quorum: 2
      })

    %{context | replica_task_supervisor: start_supervised!(Task.Supervisor)}
  end

  defp member(node_id, opts \\ []) do
    %{
      mod_revision: :erlang.phash2(node_id),
      node: %Node{
        schema: 2,
        node_id: node_id,
        generation: 1,
        role: Keyword.get(opts, :role, :data),
        erlang_endpoint: :"#{node_id}@127.0.0.1",
        internal_endpoint: "http://#{node_id}.internal:9100",
        enabled: Keyword.get(opts, :enabled, true),
        draining: Keyword.get(opts, :draining, false),
        zone: nil,
        capacity: nil,
        updated_at: "2026-07-29T00:00:00Z"
      }
    }
  end
end
