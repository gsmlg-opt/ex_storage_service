defmodule ExStorageService.Cluster.TransportTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck, Transport}

  test "transport exposes only content-addressed blob operations" do
    assert Transport.behaviour_info(:callbacks) |> MapSet.new() ==
             MapSet.new(
               delete_blob: 4,
               health: 3,
               head_blob: 4,
               open_blob: 5,
               put_blob: 5
             )
  end

  test "blob descriptors retain immutable content identity and durability intent" do
    now = DateTime.utc_now()

    assert %BlobDescriptor{
             schema: 2,
             hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             algorithm: :sha256,
             size: 42,
             desired_replication_factor: 2,
             created_at: ^now
           } = %BlobDescriptor{
             schema: 2,
             hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             algorithm: :sha256,
             size: 42,
             desired_replication_factor: 2,
             created_at: now
           }
  end

  test "replica acknowledgements retain durable verification evidence" do
    now = DateTime.utc_now()

    assert %ReplicaAck{
             node_id: "data-a",
             node_generation: "generation-a",
             hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             size: 42,
             verified_at: ^now,
             fencing_or_request_id: "request-1"
           } = %ReplicaAck{
             node_id: "data-a",
             node_generation: "generation-a",
             hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             size: 42,
             verified_at: now,
             fencing_or_request_id: "request-1"
           }
  end
end
