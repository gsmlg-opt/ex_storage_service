defmodule ExStorageService.Metadata.MultipartCommitTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Cluster.{BlobDescriptor, Node, ReplicaAck}
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  alias ExStorageService.Metadata.{
    BucketLifecycle,
    Keys,
    ListIndex,
    MultipartCommit,
    OperationIntents
  }

  defmodule InterleavingBackend do
    def get(key, opts) do
      if String.contains?(key, ":uploads:node:") and opts[:engine] do
        interleaving =
          Agent.get_and_update(opts[:engine], fn state ->
            {state, %{state | remaining: max(state.remaining - 1, 0)}}
          end)

        if interleaving.remaining > 0 do
          part = %{
            hash: "interleaved-#{interleaving.remaining}",
            etag: "interleaved-#{interleaving.remaining}",
            size: 1,
            part_number: 1
          }

          {:ok, _} =
            MultipartCommit.put_local_part(
              interleaving.bucket,
              interleaving.upload_id,
              1,
              part
            )

          # A real part commit replaces the single upload's membership-tree node.
          {:ok, nil} = Backend.get(key)
        end
      end

      Backend.get(key, Keyword.delete(opts, :engine))
    end

    def transaction(spec, opts), do: Backend.transaction(spec, Keyword.delete(opts, :engine))

    def resolve_transaction(key, opts),
      do: Backend.resolve_transaction(key, Keyword.delete(opts, :engine))
  end

  test "standalone part planning conflicts use the remaining publication retry budget" do
    {bucket, upload_id} = upload_fixture()

    {:ok, engine} =
      Agent.start_link(fn -> %{bucket: bucket, upload_id: upload_id, remaining: 16} end)

    part = %{hash: "target-hash", etag: "target-etag", size: 1, part_number: 2}

    assert {:ok, "target-etag"} =
             MultipartCommit.put_local_part(bucket, upload_id, 2, part,
               backend: InterleavingBackend,
               engine: engine
             )

    assert Agent.get(engine, & &1.remaining) == 0

    assert {:ok, %{value: %{etag: "target-etag"}}} =
             Backend.get(Keys.multipart_part(upload_id, 2))
  end

  test "shared indexed transaction reports a planning conflict as an unsuccessful compare" do
    {bucket, upload_id} = upload_fixture()

    {:ok, engine} =
      Agent.start_link(fn -> %{bucket: bucket, upload_id: upload_id, remaining: 16} end)

    upload_key = Keys.multipart_upload(upload_id)
    assert {:ok, record} = Backend.get(upload_key, consistency: :linearizable)

    spec = %{
      compare: [{:mod_revision, upload_key, :==, record.mod_revision}],
      success: [{:put, upload_key, Map.put(record.value, :target_marker, true), %{}}],
      failure: []
    }

    assert {:ok, %{succeeded: false}} =
             ListIndex.transaction(InterleavingBackend, bucket, spec, engine: engine)

    assert Agent.get(engine, & &1.remaining) == 0
    assert {:ok, %{value: current}} = Backend.get(upload_key, consistency: :linearizable)
    refute Map.has_key?(current, :target_marker)
  end

  test "quorum part planning conflicts use the remaining publication retry budget" do
    {bucket, upload_id} = upload_fixture()

    {:ok, engine} =
      Agent.start_link(fn -> %{bucket: bucket, upload_id: upload_id, remaining: 16} end)

    operation_id = "index-part-#{upload_id}"
    hash = :crypto.hash(:sha256, "target") |> Base.encode16(case: :lower)
    timestamp = "2026-10-18T00:00:00Z"
    node_a = node("node-a", 7, timestamp)
    node_b = node("node-b", 9, timestamp)
    :ok = put(Keys.cluster_node("node-a"), Map.from_struct(node_a))
    :ok = put(Keys.cluster_node("node-b"), Map.from_struct(node_b))
    {:ok, %{mod_revision: revision_a}} = Backend.get(Keys.cluster_node("node-a"))
    {:ok, %{mod_revision: revision_b}} = Backend.get(Keys.cluster_node("node-b"))

    durability = %{
      descriptor: %BlobDescriptor{
        schema: 2,
        hash: hash,
        algorithm: :sha256,
        size: 6,
        desired_replication_factor: 2,
        created_at: timestamp
      },
      placement: [
        %{node: node_a, mod_revision: revision_a},
        %{node: node_b, mod_revision: revision_b}
      ],
      acknowledgements: [
        ack("node-a", 7, hash, 6, operation_id),
        ack("node-b", 9, hash, 6, operation_id)
      ],
      missing_node_ids: [],
      configured_write_quorum: 2,
      required_write_quorum: 2,
      achieved_replica_count: 2,
      durability: :strict
    }

    part = %{hash: hash, etag: "target-etag", size: 6, part_number: 2}

    assert {:ok, %{etag: "target-etag", part_number: 2}} =
             MultipartCommit.put_part(bucket, upload_id, 2, part, durability,
               backend: InterleavingBackend,
               engine: engine,
               operation_id: operation_id
             )

    assert Agent.get(engine, & &1.remaining) == 0
    assert {:ok, %{value: %{hash: ^hash}}} = Backend.get(Keys.multipart_part(upload_id, 2))
    assert {:ok, %{value: %{result: %{hash: ^hash}}}} = Backend.get(Keys.outbox(operation_id))
  end

  test "exhausted standalone part planning conflicts leave the target part unpublished" do
    {bucket, upload_id} = upload_fixture()

    {:ok, engine} =
      Agent.start_link(fn -> %{bucket: bucket, upload_id: upload_id, remaining: 128} end)

    part = %{hash: "target-hash", etag: "target-etag", size: 1, part_number: 2}

    assert {:error, :metadata_quorum_unavailable} =
             MultipartCommit.put_local_part(bucket, upload_id, 2, part,
               backend: InterleavingBackend,
               engine: engine
             )

    assert Agent.get(engine, & &1.remaining) == 0
    assert {:ok, nil} = Backend.get(Keys.multipart_part(upload_id, 2))
    assert {:ok, %{value: %{parts_count: 1}}} = Backend.get(Keys.multipart_upload(upload_id))
  end

  test "stable upload index corruption is preserved by part and shared transaction boundaries" do
    {bucket, upload_id} = upload_fixture()
    {:ok, %{value: bucket_metadata}} = Backend.get("bucket:#{bucket}", consistency: :linearizable)
    root_key = ListIndex.root_key(bucket, BucketLifecycle.generation(bucket_metadata))
    {:ok, %{value: root}} = Backend.get(root_key, consistency: :linearizable)

    node_key =
      ListIndex.namespace(bucket, root, :uploads) <>
        Base.url_encode64(root.uploads_root, padding: false)

    :ok = put(node_key, %{invalid: true})
    part = %{hash: "target-hash", etag: "target-etag", size: 1, part_number: 2}

    assert {:error, :corrupt_index} =
             MultipartCommit.put_local_part(bucket, upload_id, 2, part)

    upload_key = Keys.multipart_upload(upload_id)
    assert {:ok, %{value: upload}} = Backend.get(upload_key, consistency: :linearizable)

    assert {:error, :corrupt_index} =
             ListIndex.transaction(Backend, bucket, %{
               compare: [],
               success: [{:put, upload_key, Map.put(upload, :target_marker, true), %{}}],
               failure: []
             })

    assert {:ok, nil} = Backend.get(Keys.multipart_part(upload_id, 2))
    assert {:ok, %{value: ^upload}} = Backend.get(upload_key, consistency: :linearizable)
  end

  test "part record, descriptor, locations, and operation outcome commit atomically" do
    suffix = System.unique_integer([:positive, :monotonic])
    upload_id = "upload-#{suffix}"
    operation_id = "part-operation-#{suffix}"
    bucket = "multipart-commit-#{suffix}"
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
      Keys.multipart_part(upload_id, 2),
      Keys.blob(hash),
      Keys.blob_location(hash, "node-a"),
      Keys.blob_location(hash, "node-b"),
      Keys.outbox(operation_id),
      Keys.outbox("#{operation_id}-stale"),
      Keys.outbox("#{operation_id}-fresh"),
      Keys.operation_intent("#{operation_id}-stale"),
      Keys.operation_intent("#{operation_id}-fresh"),
      Keys.gc_guard(hash),
      "bucket:#{bucket}"
    ]

    on_exit(fn -> cleanup(cleanup_keys) end)

    :ok = Metadata.create_bucket(bucket)
    {:ok, %{value: bucket_metadata}} = Backend.get("bucket:#{bucket}", consistency: :linearizable)

    upload = %{
      schema: 2,
      bucket: bucket,
      key: "object",
      bucket_generation: BucketLifecycle.generation(bucket_metadata),
      upload_id: upload_id,
      status: :initiated,
      created_at: timestamp,
      updated_at: timestamp
    }

    {:ok, upload_spec} =
      ListIndex.augment(Backend, bucket, %{
        compare: [{:exists, upload_key, :==, false}],
        success: [{:put, upload_key, upload, %{}}],
        failure: []
      })

    {:ok, %{succeeded: true}} = Backend.transaction(upload_spec)

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

    :ok =
      put(Keys.blob_location(hash, "node-a"), %{
        schema: 2,
        hash: hash,
        node_id: "node-a",
        node_generation: 7,
        state: :absent,
        size: 14,
        verified_at: timestamp,
        retired_at_ms: 200
      })

    stale_operation_id = "#{operation_id}-stale"

    assert {:ok, _intent} =
             OperationIntents.open(stale_operation_id, hash, 14, "node-a", 7,
               now_ms: 100,
               protection_ms: 10_000
             )

    assert {:error, :operation_intent_before_blob_retirement} =
             MultipartCommit.put_part(bucket, upload_id, 2, part, durability,
               operation_id: stale_operation_id,
               operation_intent: true,
               now_ms: 300,
               timestamp: timestamp
             )

    fresh_operation_id = "#{operation_id}-fresh"

    assert {:ok, _intent} =
             OperationIntents.open(fresh_operation_id, hash, 14, "node-a", 7,
               now_ms: 201,
               protection_ms: 10_000
             )

    assert {:ok, %{part_number: 2}} =
             MultipartCommit.put_part(bucket, upload_id, 2, part, durability,
               operation_id: fresh_operation_id,
               operation_intent: true,
               now_ms: 300,
               timestamp: timestamp
             )

    assert {:ok, %{state: :ready}} = Concord.get(Keys.blob_location(hash, "node-a"))
  end

  defp upload_fixture do
    suffix = System.unique_integer([:positive, :monotonic])
    bucket = "multipart-index-conflict-#{suffix}"
    upload_id = "index-upload-#{suffix}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, %{value: bucket_metadata}} = Backend.get("bucket:#{bucket}", consistency: :linearizable)

    upload = %{
      bucket: bucket,
      key: "object",
      upload_id: upload_id,
      bucket_generation: BucketLifecycle.generation(bucket_metadata),
      status: :initiated,
      created_at: "2026-10-18T00:00:00Z",
      updated_at: "2026-10-18T00:00:00Z"
    }

    {:ok, spec} =
      ListIndex.augment(Backend, bucket, %{
        compare: [],
        success: [{:put, Keys.multipart_upload(upload_id), upload, %{}}],
        failure: []
      })

    {:ok, %{succeeded: true}} = Backend.transaction(spec)
    {bucket, upload_id}
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
