defmodule ExStorageServiceS3.BucketLifecycleTest do
  use ExUnit.Case, async: false
  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.Storage.{Multipart, Versioning}

  defmodule FailedDeleteServer do
    def init(opts), do: opts

    def call(conn, _opts) do
      [bucket] = conn.path_info

      ExStorageServiceS3.Handlers.Bucket.delete_bucket(conn, bucket,
        delete_bucket: fn ^bucket -> {:error, {:metadata, :timeout}} end
      )
    end
  end

  test "injected bucket deletion failure returns a sanitized HTTP error and preserves the bucket" do
    bucket = "failed-bucket-delete-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    assert {:ok, original} = Metadata.get_bucket(bucket)
    on_exit(fn -> Metadata.delete_bucket(bucket) end)
    server = start_supervised!({Bandit, plug: FailedDeleteServer, port: 0})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)

    assert {:ok, response} = Req.delete("http://localhost:#{port}/#{bucket}", retry: false)
    assert response.status == 503
    assert response.body =~ "<Code>ServiceUnavailable</Code>"
    refute response.body =~ "metadata"
    refute response.body =~ "timeout"
    assert {:ok, ^original} = Metadata.get_bucket(bucket)
  end

  test "historical versions and delete markers prevent deletion" do
    bucket = "bucket-lifecycle-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    assert :ok = Versioning.set_versioning(bucket, :enabled)

    assert {:ok, %{version_id: version}} =
             ObjectService.put(bucket, "key", "value", "text/plain", %{})

    assert {:ok, %{version_id: marker}} = ObjectService.delete(bucket, "key", nil)
    assert {:error, :bucket_not_empty} = Metadata.delete_bucket(bucket)
    assert {:ok, _} = ObjectService.delete(bucket, "key", version)
    assert {:error, :bucket_not_empty} = Metadata.delete_bucket(bucket)
    assert {:ok, _} = ObjectService.delete(bucket, "key", marker)
    assert :ok = Metadata.delete_bucket(bucket)
    assert :ok = Metadata.create_bucket(bucket)
    assert Versioning.get_versioning(bucket) == :disabled
  end

  test "incomplete uploads block bucket deletion until aborted" do
    bucket = "bucket-upload-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    assert {:ok, upload} = Multipart.init_upload(bucket, "key")
    assert {:error, :bucket_not_empty} = Metadata.delete_bucket(bucket)
    assert :ok = Multipart.abort_upload(bucket, upload)
    assert :ok = Metadata.delete_bucket(bucket)
  end

  test "staged work cannot commit into a recreated bucket generation" do
    bucket = "bucket-generation-#{System.unique_integer([:positive])}"
    assert :ok = Metadata.create_bucket(bucket)
    assert {:ok, metadata} = Metadata.get_bucket(bucket)
    generation = ExStorageService.Metadata.BucketLifecycle.generation(metadata)
    assert :ok = Metadata.delete_bucket(bucket)
    assert :ok = Metadata.create_bucket(bucket)

    assert {:error, :bucket_generation_changed} =
             ObjectService.put(bucket, "key", "old", "text/plain", %{},
               metadata_opts: [bucket_generation: generation]
             )

    assert {:error, :object_not_found} = ObjectService.head(bucket, "key")
  end
end
