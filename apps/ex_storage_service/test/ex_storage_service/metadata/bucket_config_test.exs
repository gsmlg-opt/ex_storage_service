defmodule ExStorageService.Metadata.BucketConfigTest do
  use ExUnit.Case, async: false
  alias ExStorageService.{Metadata, Notifications}
  alias ExStorageService.Metadata.{Backend.Concord, BucketLifecycle}
  alias ExStorageService.Storage.{Lifecycle, Versioning}
  alias ExStorageService.CloudCache.Config, as: CloudConfig

  defmodule FailedBackend do
    def get(key, opts), do: Concord.get(key, opts)
    def transaction(_spec, _opts), do: {:error, :timeout}
  end

  defmodule RecreatedBackend do
    def get(key, opts), do: Concord.get(key, opts)

    def transaction(spec, _opts) do
      [{:mod_revision, "bucket:" <> bucket, :==, _}] = spec.compare
      :ok = Metadata.delete_bucket(bucket)
      :ok = Metadata.create_bucket(bucket)
      {:ok, %{succeeded: false}}
    end
  end

  setup do
    bucket = "bucket-config-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, meta} = Metadata.get_bucket(bucket)
    on_exit(fn -> Metadata.delete_bucket(bucket) end)
    {:ok, bucket: bucket, generation: BucketLifecycle.generation(meta)}
  end

  test "config mutation failures propagate without publishing a value", %{bucket: bucket} do
    opts = [backend: FailedBackend]
    assert {:error, :timeout} = Versioning.set_versioning(bucket, :enabled, opts)
    assert {:error, :timeout} = Lifecycle.put_rules(bucket, [], opts)
    assert {:error, :timeout} = Notifications.put_config(bucket, [], opts)
    assert {:error, :timeout} = Lifecycle.delete_rules(bucket, opts)
    assert {:error, :timeout} = Notifications.delete_config(bucket, opts)
    assert {:error, :timeout} = CloudConfig.set_config(bucket, %{enabled: true}, opts)
    assert {:error, :timeout} = CloudConfig.delete_config(bucket, opts)

    for prefix <- ~w(bucket_versioning lifecycle notification cloud_cache) do
      assert {:ok, nil} = Concord.get("#{prefix}:#{bucket}")
    end
  end

  test "stale requests cannot change configuration in a recreated bucket", context do
    :ok = Metadata.delete_bucket(context.bucket)
    :ok = Metadata.create_bucket(context.bucket)
    opts = [bucket_generation: context.generation]

    assert {:error, :bucket_generation_changed} =
             Versioning.set_versioning(context.bucket, :enabled, opts)

    assert {:error, :bucket_generation_changed} = Lifecycle.put_rules(context.bucket, [], opts)

    assert {:error, :bucket_generation_changed} =
             Notifications.put_config(context.bucket, [], opts)

    assert {:error, :bucket_generation_changed} = Lifecycle.delete_rules(context.bucket, opts)

    assert {:error, :bucket_generation_changed} =
             Notifications.delete_config(context.bucket, opts)

    assert {:error, :bucket_generation_changed} =
             CloudConfig.set_config(context.bucket, %{enabled: true}, opts)

    assert {:error, :bucket_generation_changed} = CloudConfig.delete_config(context.bucket, opts)
  end

  test "transaction conflict retains its original bucket identity across retries", %{
    bucket: bucket
  } do
    assert {:error, :bucket_generation_changed} =
             Versioning.set_versioning(bucket, :enabled, backend: RecreatedBackend)

    assert {:ok, nil} = Concord.get("bucket_versioning:#{bucket}")

    assert {:error, :bucket_generation_changed} =
             CloudConfig.set_config(bucket, %{enabled: true}, backend: RecreatedBackend)

    assert {:ok, nil} = Concord.get("cloud_cache:#{bucket}")
  end

  test "nonexistent buckets cannot acquire orphan configuration" do
    bucket = "missing-config-#{System.unique_integer([:positive])}"
    assert {:error, :bucket_not_found} = Versioning.set_versioning(bucket, :enabled)
    assert {:error, :bucket_not_found} = Lifecycle.put_rules(bucket, [])
    assert {:error, :bucket_not_found} = Notifications.put_config(bucket, [])
    assert {:error, :bucket_not_found} = Lifecycle.delete_rules(bucket)
    assert {:error, :bucket_not_found} = Notifications.delete_config(bucket)
    assert {:error, :bucket_not_found} = CloudConfig.set_config(bucket, %{enabled: true})
    assert {:error, :bucket_not_found} = CloudConfig.delete_config(bucket)
  end

  test "cloud config retains encrypted credentials during fenced updates", %{bucket: bucket} do
    assert :ok =
             CloudConfig.set_config(bucket, %{enabled: true, secret_access_key: "test-secret"})

    assert {:ok, before} = CloudConfig.get_config(bucket)
    assert before.encrypted_secret != "test-secret"
    assert :ok = CloudConfig.set_config(bucket, %{enabled: false})
    assert {:ok, after_update} = CloudConfig.get_config(bucket)
    assert after_update.encrypted_secret == before.encrypted_secret
    assert CloudConfig.plaintext_secret(after_update) == "test-secret"
    assert :ok = CloudConfig.delete_config(bucket)
    assert {:error, :not_found} = CloudConfig.get_config(bucket)
  end
end
