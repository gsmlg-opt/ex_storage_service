defmodule ExStorageService.CrossClusterReplication.ConfigTest do
  use ExUnit.Case, async: false

  alias ExStorageService.CrossClusterReplication.{Config, Handler, Hooks}
  alias ExStorageService.{Context, InstanceConfig, Metadata}
  alias ExStorageService.Metadata.BucketLifecycle
  alias ExStorageService.Metadata.Backend.Concord, as: Backend

  setup do
    bucket = "replication-config-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    on_exit(fn -> Metadata.delete_bucket(bucket) end)
    {:ok, bucket: bucket}
  end

  test "deleting and recreating a bucket drops its previous replication targets", %{
    bucket: bucket
  } do
    assert :ok = Config.set_bucket_replicas(bucket, [replica()])
    assert :ok = Metadata.delete_bucket(bucket)
    assert {:ok, nil} = Backend.get("replication:#{bucket}")
    assert :ok = Metadata.create_bucket(bucket)
    assert {:ok, []} = Config.get_bucket_replicas(bucket)

    assert {:ok, []} =
             Hooks.events_for_put(bucket, "new", %{content_hash: "new"}, operation_id: "new")
  end

  test "creating a bucket clears orphan targets retained by historical deletion", %{
    bucket: bucket
  } do
    assert :ok = Metadata.delete_bucket(bucket)
    assert :ok = Concord.put("replication:#{bucket}", [replica()])
    assert :ok = Metadata.create_bucket(bucket)
    assert {:ok, []} = Config.get_bucket_replicas(bucket)
  end

  test "replication configuration writes retain the observed bucket generation", %{bucket: bucket} do
    {:ok, original} = Metadata.get_bucket(bucket)
    assert :ok = Metadata.delete_bucket(bucket)
    assert :ok = Metadata.create_bucket(bucket)
    opts = [bucket_generation: BucketLifecycle.generation(original)]

    assert {:error, :bucket_generation_changed} =
             Config.set_bucket_replicas(bucket, [replica()], opts)

    assert {:error, :bucket_generation_changed} = Config.remove_bucket_replicas(bucket, opts)
    assert {:ok, []} = Config.get_bucket_replicas(bucket)
  end

  test "deleted buckets cannot acquire orphan replication targets", %{bucket: bucket} do
    assert :ok = Metadata.delete_bucket(bucket)
    assert {:error, :bucket_not_found} = Config.set_bucket_replicas(bucket, [replica()])
    assert {:error, :bucket_not_found} = Config.remove_bucket_replicas(bucket)
    assert {:ok, nil} = Backend.get("replication:#{bucket}")
  end

  test "historical hook jobs cannot send a later object to a removed target", %{bucket: bucket} do
    assert :ok = Config.set_bucket_replicas(bucket, [replica()])
    old = %{content_hash: "old", version_id: "old"}
    assert {:ok, [event]} = Hooks.events_for_put(bucket, "key", old, operation_id: "old")
    assert :ok = Config.remove_bucket_replicas(bucket)
    {:ok, config} = InstanceConfig.new(auto_start: false)
    latest = %{content_hash: "new", version_id: "new"}

    assert :ok =
             Handler.perform(event, Context.new(config),
               head: fn _, _, _ -> {:ok, %{delete_marker: false, metadata: latest}} end,
               put: fn _, _, _, _, _ -> flunk("removed target received later content") end
             )
  end

  defp replica, do: %{endpoint: "https://old-target.example", bucket: nil}
end
