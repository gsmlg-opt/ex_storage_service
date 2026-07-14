defmodule ExStorageService.Storage.LifecycleTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine, Lifecycle}

  defp put_referenced_object(bucket, key, data, created_at) do
    {:ok, {hash, etag, size}} = Engine.put_object(bucket, key, data)

    Metadata.put_object_meta(bucket, key, %{
      content_hash: hash,
      size: size,
      etag: etag,
      created_at: created_at,
      updated_at: created_at
    })

    hash
  end

  test "transition rules pack old matching objects without deleting metadata" do
    bucket = "lifecycle-transition-#{:erlang.unique_integer([:positive])}"
    key = "archive/report.txt"
    data = "transition-me-#{System.unique_integer()}"

    old_date =
      DateTime.utc_now()
      |> DateTime.add(-2 * 86_400, :second)
      |> DateTime.to_iso8601()

    hash = put_referenced_object(bucket, key, data, old_date)

    :ok =
      Lifecycle.put_rules(bucket, [
        %{
          id: "pack-archive",
          prefix: "archive/",
          status: "Enabled",
          transition_days: 1,
          transition_storage_class: "PACKED"
        }
      ])

    assert {:ok, 1} = Lifecycle.evaluate_bucket(bucket)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{state: :packed}} = Metadata.get_blob_meta(hash)
    assert {:ok, %{content_hash: ^hash}} = Metadata.get_object_meta(bucket, key)
    assert {:ok, ^data} = Engine.read_object(bucket, hash)
  end
end
