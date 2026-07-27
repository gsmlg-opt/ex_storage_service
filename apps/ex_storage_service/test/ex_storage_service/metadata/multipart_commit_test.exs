defmodule ExStorageService.Metadata.MultipartCommitTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Cluster.{BlobDescriptor, Node, ReplicaAck}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.{Keys, MultipartCommit}

  test "part record, descriptor, locations, and operation outcome commit atomically" do
    suffix = System.unique_integer([:positive, :monotonic])
    upload_id = "upload-#{suffix}"
    operation_id = "part-operation-#{suffix}"
    bucket = "bucket"
    hash = :crypto.hash(:sha256, "multipart-part") |> Base.encode16(case: :lower)
    timestamp = "2026-07-27T00:00:00Z"

    upload_key = Keys.multipart_upload(upload_id)
    node_a_key = Keys.cluster_node("node-a")
    node_b_key = Keys.cluster_node("node-b")

    cleanup_keys = [
      upload_key,
      node_a_key,
      node_b_key,
      Keys.multipart_part(upload_id, 1),
      Keys.blob(hash),
      Keys.blob_location(hash, "node-a"),
      Keys.blob_location(hash, "node-b"),
      Keys.outbox(operation_id)
    ]

    on_exit(fn -> cleanup(cleanup_keys) end)

    :ok =
      put(
        upload_key,
        %{
          schema: 2,
          bucket: bucket,
          key: "object",
          upload_id: upload_id,
          status: :initiated,
          created_at: timestamp,
          updated_at: timestamp
        }
      )

    node_a = node("node-a", 7, timestamp)
    node_b = node("node-b", 9, timestamp)
    :ok = put(node_a_key, Map.from_struct(node_a))
    :ok = put(node_b_key, Map.from_struct(node_b))

    {:ok, %{mod_revision: revision_a}} = Backend.get(node_a_key)
    {:ok, %{mod_revision: revision_b}} = Backend.get(node_b_key)

    descriptor = %BlobDescriptor{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: 14,
      desired_replication_factor: 2,
      created_at: timestamp
    }

    acknowledgements = [
      ack("node-a", 7, hash, 14, operation_id),
      ack("node-b", 9, hash, 14, operation_id)
    ]

    durability = %{
      descriptor: descriptor,
      placement: [
        %{node: node_a, mod_revision: revision_a},
        %{node: node_b, mod_revision: revision_b}
      ],
      acknowledgements: acknowledgements,
      missing_node_ids: [],
      configured_write_quorum: 2,
      required_write_quorum: 2,
      achieved_replica_count: 2,
      durability: :strict
    }

    part = %{hash: hash, etag: "part-etag", size: 14, part_number: 1}

    assert {:error, :invalid_durability_evidence} =
             MultipartCommit.put_part(
               bucket,
               upload_id,
               1,
               part,
               %{
                 durability
                 | acknowledgements: [hd(acknowledgements)],
                   achieved_replica_count: 1,
                   missing_node_ids: ["node-b"]
               },
               operation_id: operation_id
             )

    assert {:ok, %{etag: "part-etag", hash: ^hash, part_number: 1, size: 14}} =
             MultipartCommit.put_part(bucket, upload_id, 1, part, durability,
               operation_id: operation_id,
               timestamp: timestamp
             )

    assert {:ok, %{hash: ^hash, durability: %{acknowledged_replica_count: 2}}} =
             Concord.get(Keys.multipart_part(upload_id, 1))

    assert {:ok, %{hash: ^hash, desired_replication_factor: 2}} =
             Concord.get(Keys.blob(hash))

    assert {:ok, %{node_id: "node-a", node_generation: 7, state: :ready}} =
             Concord.get(Keys.blob_location(hash, "node-a"))

    assert {:ok, %{node_id: "node-b", node_generation: 9, state: :ready}} =
             Concord.get(Keys.blob_location(hash, "node-b"))

    assert {:ok, %{result: %{etag: "part-etag"}, events: []}} =
             Concord.get(Keys.outbox(operation_id))

    assert {:ok, %{etag: "part-etag"}} =
             MultipartCommit.put_part(bucket, upload_id, 1, part, durability,
               operation_id: operation_id,
               timestamp: timestamp
             )
  end

  defp node(id, generation, timestamp) do
    %Node{
      schema: 2,
      node_id: id,
      generation: generation,
      role: :data,
      erlang_endpoint: String.to_atom("#{id}@127.0.0.1"),
      internal_endpoint: "http://#{id}.internal:9100",
      enabled: true,
      draining: false,
      zone: nil,
      capacity: nil,
      updated_at: timestamp
    }
  end

  defp ack(node_id, generation, hash, size, operation_id) do
    %ReplicaAck{
      node_id: node_id,
      node_generation: generation,
      hash: hash,
      size: size,
      verified_at: "2026-07-27T00:00:00Z",
      fencing_or_request_id: "#{operation_id}:replica:#{node_id}"
    }
  end

  defp put(key, value) do
    case Concord.put(key, value) do
      :ok -> :ok
      {:ok, _result} -> :ok
    end
  end

  defp cleanup(keys) do
    Enum.each(keys, fn key ->
      _ = Concord.delete(key)
    end)
  end
end
