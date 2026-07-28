defmodule ExStorageService.CrossClusterReplication.HandlerTest do
  use ExUnit.Case, async: true

  alias ExStorageService.CrossClusterReplication.Handler
  alias ExStorageService.{Context, InstanceConfig}

  setup do
    {:ok, config} = InstanceConfig.new(auto_start: false)
    %{context: Context.new(config)}
  end

  test "applies a current pinned PUT and skips a superseded duplicate", %{context: context} do
    parent = self()
    job = put_job()

    current = fn _bucket, _key, _context ->
      {:ok,
       %{
         delete_marker: false,
         metadata: %{content_hash: "hash", version_id: "v1"}
       }}
    end

    put = fn bucket, key, replica, object, _opts ->
      send(parent, {:put, bucket, key, replica, object})
      :ok
    end

    assert :ok = Handler.perform(job, context, head: current, put: put)

    assert_received {:put, "bucket", "key", %{endpoint: "https://dr.example"},
                     %{version_id: "v1"}}

    newer = fn _bucket, _key, _context ->
      {:ok,
       %{
         delete_marker: false,
         metadata: %{content_hash: "new-hash", version_id: "v2"}
       }}
    end

    assert :ok = Handler.perform(job, context, head: newer, put: put)
    refute_received {:put, _, _, _, _}
  end

  test "DELETE reconciles the destination to the current local head", %{context: context} do
    parent = self()
    job = delete_job()

    put = fn bucket, key, replica, object, _opts ->
      send(parent, {:put, bucket, key, replica, object})
      :ok
    end

    delete = fn bucket, key, replica ->
      send(parent, {:delete, bucket, key, replica})
      :ok
    end

    deleted = fn _bucket, _key, _context -> {:error, :object_not_found} end
    assert :ok = Handler.perform(job, context, head: deleted, delete: delete)
    assert_received {:delete, "bucket", "key", %{endpoint: "https://dr.example"}}

    recreated = fn _bucket, _key, _context ->
      {:ok, %{delete_marker: false, metadata: %{content_hash: "new"}}}
    end

    assert :ok = Handler.perform(job, context, head: recreated, delete: delete, put: put)
    refute_received {:delete, _, _, _}

    assert_received {:put, "bucket", "key", %{endpoint: "https://dr.example"},
                     %{content_hash: "new"}}
  end

  defp put_job do
    %{
      kind: :cross_cluster_put,
      payload: %{
        bucket: "bucket",
        key: "key",
        replica: %{endpoint: "https://dr.example"},
        object: %{content_hash: "hash", version_id: "v1"}
      }
    }
  end

  defp delete_job do
    %{
      kind: :cross_cluster_delete,
      payload: %{
        bucket: "bucket",
        key: "key",
        replica: %{endpoint: "https://dr.example"}
      }
    }
  end
end
