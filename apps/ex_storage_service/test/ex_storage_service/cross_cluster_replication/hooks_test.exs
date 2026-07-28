defmodule ExStorageService.CrossClusterReplication.HooksTest do
  use ExUnit.Case, async: true

  alias ExStorageService.CrossClusterReplication.Hooks

  defmodule ConfigStub do
    def get_bucket_replicas("bucket") do
      {:ok,
       [
         %{
           endpoint: "https://dr-b.example",
           access_key: "b",
           secret_key_enc: "secret-b",
           bucket: "backup-b"
         },
         %{
           endpoint: "https://dr-a.example",
           access_key: "a",
           secret_key_enc: "secret-a",
           bucket: nil
         }
       ]}
    end
  end

  test "builds deterministic cross-cluster PUT events with a pinned object" do
    object = %{
      version_id: "v1",
      content_hash: "sha256",
      etag: "etag",
      size: 12,
      content_type: "text/plain",
      ignored: "not part of the pinned identity"
    }

    opts = [config: ConfigStub, operation_id: "put-op"]
    assert {:ok, first} = Hooks.events_for_put("bucket", "key", object, opts)
    assert {:ok, ^first} = Hooks.events_for_put("bucket", "key", object, opts)

    assert [
             %{kind: :cross_cluster_put, state: :pending, payload: first_payload},
             %{kind: :cross_cluster_put, state: :pending, payload: second_payload}
           ] = first

    assert Enum.sort([
             first_payload.replica.endpoint,
             second_payload.replica.endpoint
           ]) == ["https://dr-a.example", "https://dr-b.example"]

    assert first_payload.object == object
    assert second_payload.object == object
  end

  test "builds deterministic idempotent DELETE events" do
    opts = [config: ConfigStub, operation_id: "delete-op"]
    assert {:ok, first} = Hooks.events_for_delete("bucket", "key", opts)
    assert {:ok, ^first} = Hooks.events_for_delete("bucket", "key", opts)

    assert Enum.all?(first, fn event ->
             event.kind == :cross_cluster_delete and event.state == :pending and
               event.payload.object == nil
           end)
  end
end
