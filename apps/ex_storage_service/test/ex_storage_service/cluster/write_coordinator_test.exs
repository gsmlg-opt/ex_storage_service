defmodule ExStorageService.Cluster.WriteCoordinatorTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Cluster.{Node, ReplicaAck, WriteCoordinator}
  alias ExStorageService.{Context, InstanceConfig}

  defmodule Transport do
    @behaviour ExStorageService.Cluster.Transport

    def head_blob(_context, node, hash, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:head, node.node_id, hash})

      case Keyword.get(opts, :head_result, :missing) do
        :missing ->
          {:error, :not_found}

        :present ->
          {:ok,
           %{
             hash: hash,
             size: Keyword.fetch!(opts, :size),
             node_id: node.node_id,
             node_generation: node.generation,
             verified_at: 1,
             fencing_or_request_id: Keyword.fetch!(opts, :request_id)
           }}
      end
    end

    def put_blob(_context, node, _source, descriptor, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:put, node.node_id, descriptor.hash})

      case Keyword.get(opts, :put_result, :ok) do
        :fail ->
          {:error, :unavailable}

        :wrong_generation ->
          {:ok, ack(node, descriptor, opts, node.generation + 1)}

        :ok ->
          {:ok, ack(node, descriptor, opts, node.generation)}
      end
    end

    def open_blob(_context, _node, _hash, _range, _opts), do: {:error, :unsupported}
    def delete_blob(_context, _node, _hash, _opts), do: {:error, :unsupported}
    def health(_context, _node, _opts), do: :ok

    defp ack(node, descriptor, opts, generation) do
      %ReplicaAck{
        node_id: node.node_id,
        node_generation: generation,
        hash: descriptor.hash,
        size: descriptor.size,
        verified_at: 1,
        fencing_or_request_id: Keyword.fetch!(opts, :request_id)
      }
    end
  end

  @tag :tmp_dir
  test "strict RF=2/W=2 returns two verified acknowledgements", %{tmp_dir: tmp_dir} do
    context = context(tmp_dir)
    body = "quorum-write"
    {:ok, staged} = LocalCAS.stage(body, Context.blob_store_options(context))

    assert {:ok,
            %{
              durability: :strict,
              configured_write_quorum: 2,
              required_write_quorum: 2,
              achieved_replica_count: 2,
              missing_node_ids: [],
              acknowledgements: acknowledgements,
              ready_blob: %{hash: hash}
            }} =
             WriteCoordinator.ensure_blob(context, staged,
               placement_records: records(),
               transport: Transport,
               operation_id: "strict-op",
               transport_opts: [test_pid: self(), size: byte_size(body)]
             )

    assert Enum.map(acknowledgements, & &1.node_id) == ["node-a", "node-b"]
    assert File.read!(LocalCAS.blob_path(hash, Context.blob_store_options(context))) == body
    assert_received {:put, "node-b", ^hash}
  end

  @tag :tmp_dir
  test "strict W=2 never succeeds with one durable replica", %{tmp_dir: tmp_dir} do
    context = context(tmp_dir)
    body = "strict-failure"
    {:ok, staged} = LocalCAS.stage(body, Context.blob_store_options(context))

    assert {:error, :blob_write_quorum_unavailable} =
             WriteCoordinator.ensure_blob(context, staged,
               placement_records: records(),
               transport: Transport,
               operation_id: "strict-failure-op",
               transport_opts: [test_pid: self(), size: byte_size(body), put_result: :fail]
             )

    assert File.exists?(LocalCAS.blob_path(staged.hash, Context.blob_store_options(context)))
  end

  @tag :tmp_dir
  test "verified HEAD deduplicates content without a body transfer", %{tmp_dir: tmp_dir} do
    context = context(tmp_dir)
    body = "deduplicated"
    {:ok, staged} = LocalCAS.stage(body, Context.blob_store_options(context))

    assert {:ok, %{achieved_replica_count: 2}} =
             WriteCoordinator.ensure_blob(context, staged,
               placement_records: records(),
               transport: Transport,
               operation_id: "dedupe-op",
               transport_opts: [
                 test_pid: self(),
                 size: byte_size(body),
                 head_result: :present
               ]
             )

    assert_received {:head, "node-b", _hash}
    refute_received {:put, "node-b", _hash}
  end

  @tag :tmp_dir
  test "degraded writes are explicit and retain mandatory repair evidence", %{tmp_dir: tmp_dir} do
    context = context(tmp_dir, true)
    body = "degraded"
    {:ok, staged} = LocalCAS.stage(body, Context.blob_store_options(context))

    assert {:ok,
            %{
              durability: :degraded,
              configured_write_quorum: 2,
              required_write_quorum: 1,
              achieved_replica_count: 1,
              missing_node_ids: ["node-b"]
            }} =
             WriteCoordinator.ensure_blob(context, staged,
               placement_records: records(),
               transport: Transport,
               operation_id: "degraded-op",
               transport_opts: [test_pid: self(), size: byte_size(body), put_result: :fail]
             )
  end

  @tag :tmp_dir
  test "stale node generations cannot satisfy quorum", %{tmp_dir: tmp_dir} do
    context = context(tmp_dir)
    body = "stale-generation"
    {:ok, staged} = LocalCAS.stage(body, Context.blob_store_options(context))

    assert {:error, :invalid_replica_ack} =
             WriteCoordinator.ensure_blob(context, staged,
               placement_records: records(),
               transport: Transport,
               operation_id: "stale-op",
               transport_opts: [
                 test_pid: self(),
                 size: byte_size(body),
                 put_result: :wrong_generation
               ]
             )
  end

  defp context(tmp_dir, allow_degraded \\ false) do
    members = [
      %{id: "node-a", endpoint: :"node-a@127.0.0.1"},
      %{id: "node-b", endpoint: :"node-b@127.0.0.1"},
      %{id: "node-c", endpoint: :"node-c@127.0.0.1"}
    ]

    {:ok, config} =
      InstanceConfig.new(
        mode: :cluster,
        node_role: :data,
        node_id: "node-a",
        node_generation: 7,
        cluster_name: "write-test",
        cluster_topology: :static,
        cluster_members: members,
        cluster_seeds: [:"node-b@127.0.0.1", :"node-c@127.0.0.1"],
        erlang_node: :"node-a@127.0.0.1",
        erlang_cookie: :write_test_cookie,
        internal_advertised_url: "http://node-a.internal:9100",
        public_s3_enabled: true,
        web_enabled: false,
        cluster_data_plane_enabled: true,
        replication_factor: 2,
        write_quorum: 2,
        allow_degraded_writes: allow_degraded,
        data_root: Path.join(tmp_dir, "data"),
        blob_root: Path.join(tmp_dir, "blob"),
        tmp_root: Path.join(tmp_dir, "tmp")
      )

    Context.new(config)
  end

  defp records do
    [
      %{node: node("node-a", 7, :data, "http://node-a.internal:9100"), mod_revision: 11},
      %{node: node("node-b", 9, :data, "http://node-b.internal:9100"), mod_revision: 12},
      %{node: node("node-c", 4, :metadata, nil), mod_revision: 13}
    ]
  end

  defp node(id, generation, role, endpoint) do
    %Node{
      schema: 2,
      node_id: id,
      generation: generation,
      role: role,
      erlang_endpoint: String.to_atom("#{id}@127.0.0.1"),
      internal_endpoint: endpoint,
      enabled: true,
      draining: false,
      zone: nil,
      capacity: nil,
      updated_at: "2026-07-27T00:00:00Z"
    }
  end
end
