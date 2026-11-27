defmodule ExStorageService.Storage.LifecycleTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.ObjectCommit
  alias ExStorageService.Storage.{CAS, Engine, Lifecycle, Versioning}

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

  test "a replacement with the same bytes survives expiration of a selected null version" do
    for state <- [:disabled, :suspended] do
      bucket = "lc-race-#{state}-#{System.unique_integer([:positive])}"
      :ok = Metadata.create_bucket(bucket)
      if state == :suspended, do: :ok = Versioning.set_versioning(bucket, state)
      now = DateTime.utc_now()

      old = %{
        etag: "same",
        size: 0,
        created_at: DateTime.add(now, -172_800) |> DateTime.to_iso8601()
      }

      fresh = %{old | created_at: DateTime.to_iso8601(now)}
      :ok = Metadata.put_object_meta(bucket, "key", old)

      selected = fn metadata ->
        assert metadata.version_id == "null"
        assert Lifecycle.should_expire?("key", metadata, [%{expiration_days: 1}], now)
        :ok = Metadata.put_object_meta(bucket, "key", fresh)
        true
      end

      assert {:ok, :skipped} = ObjectCommit.delete_current_if(bucket, "key", selected)
      assert {:ok, %{created_at: created_at}} = Metadata.get_object_meta(bucket, "key")
      assert created_at == fresh.created_at
    end
  end

  test "expiration in a versioned bucket creates one marker and preserves the original version" do
    bucket = "lc-versions-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Versioning.set_versioning(bucket, :enabled)
    :ok = Metadata.put_object_meta(bucket, "key", %{created_at: "2000-01-01T00:00:00Z", size: 0})
    {:ok, %{version_id: original}} = ObjectCommit.get_head(bucket, "key")
    :ok = Lifecycle.put_rules(bucket, [%{expiration_days: 1}])

    assert {:ok, 1} = Lifecycle.evaluate_bucket(bucket)
    assert {:ok, 0} = Lifecycle.evaluate_bucket(bucket)
    assert {:ok, %{is_delete_marker: true}} = ObjectCommit.get_head(bucket, "key")
    assert {:ok, %{version_id: ^original}} = ObjectCommit.get_version(bucket, "key", original)
  end

  test "a rejected expiration is reported and leaves the current object readable" do
    bucket = "lc-read-only-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    :ok = Metadata.put_object_meta(bucket, "key", %{created_at: "2000-01-01T00:00:00Z", size: 0})
    :ok = Lifecycle.put_rules(bucket, [%{expiration_days: 1}])
    config = Application.fetch_env!(:ex_storage_service, :instance_config)
    on_exit(fn -> Application.put_env(:ex_storage_service, :instance_config, config) end)

    Application.put_env(
      :ex_storage_service,
      :instance_config,
      Keyword.put(config, :metadata_schema, :v1)
    )

    assert {:error, :v2_metadata_writes_disabled} = Lifecycle.evaluate_bucket(bucket)
    assert {:ok, _} = Metadata.get_object_meta(bucket, "key")
  end

  test "a lifecycle page cannot continue into a replacement bucket" do
    bucket = "lc-recreate-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, %{generation: generation}} = Metadata.get_bucket(bucket)
    :ok = Metadata.delete_bucket(bucket)
    :ok = Metadata.create_bucket(bucket)

    assert {:error, :bucket_generation_changed} =
             Metadata.list_objects(bucket,
               continuation_token: "previous-key",
               bucket_generation: generation
             )
  end

  @tag timeout: 180_000
  test "expiration reaches the next page after removing the first page's last key" do
    bucket = "lc-pages-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    recent = DateTime.utc_now() |> DateTime.to_iso8601()

    for i <- 1..1001 do
      key = Integer.to_string(i) |> String.pad_leading(4, "0")
      created_at = if i >= 1000, do: "2000-01-01T00:00:00Z", else: recent
      :ok = Metadata.put_object_meta(bucket, key, %{size: 0, created_at: created_at})
    end

    :ok = Lifecycle.put_rules(bucket, [%{expiration_days: 1}])
    assert {:ok, 2} = Lifecycle.evaluate_bucket(bucket)
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "1000")
    assert {:error, :not_found} = Metadata.get_object_meta(bucket, "1001")
    assert {:ok, _} = Metadata.get_object_meta(bucket, "0999")
  end

  test "transition rules pack old matching objects without deleting metadata" do
    bucket = "lifecycle-transition-#{:erlang.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
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
