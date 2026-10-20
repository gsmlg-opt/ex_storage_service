defmodule ExStorageService.Metadata.LegacyObjectTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.{Keys, LegacyObject, ObjectCommit}
  alias ExStorageService.Metadata.ListIndex
  alias ExStorageService.Metadata.ListIndex.Backfill
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Storage.Versioning

  # Isolated fixture proof only. Actual source-process/backup preflight is
  # exercised separately; these tests never stop the shared Concord fixture.
  defmodule FixtureBackup do
    def assert_quiesced(:isolated_fixture), do: :ok
    def verify("fixture-backup", _opts), do: {:ok, %{backup_id: "fixture"}}
  end

  setup do
    bucket = "legacy-normalization-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      {:ok, records} = Concord.get_all()
      encoded = Keys.encode_component(bucket)

      prefixes = [
        "obj:#{bucket}:",
        "obj_ver:#{bucket}:",
        "obj_ver_list:#{bucket}:",
        "ess:v2:object_head:#{encoded}:",
        "ess:v2:object_version:#{encoded}:",
        "ess:v2:list_index:#{encoded}:"
      ]

      for {key, _} <- records,
          Enum.any?(prefixes, &String.starts_with?(key, &1)),
          do: Concord.delete(key)

      Concord.delete("bucket:#{bucket}")
      Concord.delete("bucket_versioning:#{bucket}")
    end)

    %{bucket: bucket}
  end

  test "explicit backfill retains indexed and unindexed history before the first hosted mutation",
       %{bucket: bucket} do
    # Blob descriptors are global. Reproduce coexistence with an independently
    # stored copy of each payload instead of depending on test execution order.
    shared_bucket = bucket <> "-shared"
    assert :ok = Metadata.create_bucket(shared_bucket)

    on_exit(fn ->
      for payload <- ["current", "indexed", "unindexed"],
          do: ExStorageService.ObjectService.delete(shared_bucket, payload, nil)

      Metadata.delete_bucket(shared_bucket)
    end)

    for payload <- ["current", "indexed", "unindexed"] do
      expected_hash = Base.encode16(:crypto.hash(:sha256, payload), case: :lower)
      expected_size = byte_size(payload)

      assert {:ok, %{metadata: %{content_hash: ^expected_hash, size: ^expected_size}}} =
               ExStorageService.ObjectService.put(
                 shared_bucket,
                 payload,
                 payload,
                 "text/plain",
                 %{}
               )
    end

    current = %{
      etag: "current",
      content_hash: Base.encode16(:crypto.hash(:sha256, "current"), case: :lower),
      size: byte_size("current"),
      created_at: "2020-01-03T00:00:00Z"
    }

    indexed = %{
      etag: "indexed",
      content_hash: Base.encode16(:crypto.hash(:sha256, "indexed"), case: :lower),
      size: byte_size("indexed"),
      created_at: "2020-01-02T00:00:00Z"
    }

    unindexed = %{
      etag: "unindexed",
      content_hash: Base.encode16(:crypto.hash(:sha256, "unindexed"), case: :lower),
      size: byte_size("unindexed"),
      created_at: "2020-01-01T00:00:00Z"
    }

    sources = %{
      "obj:#{bucket}:key:colon" => current,
      "obj_ver:#{bucket}:key:colon:indexed" => indexed,
      "obj_ver:#{bucket}:key:colon:unindexed" => unindexed,
      "obj_ver_list:#{bucket}:key:colon" => ["indexed"]
    }

    for {key, value} <- sources, do: assert(:ok = Concord.put(key, value))
    migrate(bucket)
    assert :ok = Versioning.set_versioning(bucket, :enabled)
    assert {:ok, id} = Versioning.put_version(bucket, "key:colon", %{etag: "new", size: 0})
    assert {:ok, versions} = Versioning.list_versions(bucket, "key:colon")

    assert MapSet.new(Enum.map(versions, & &1.version_id)) ==
             MapSet.new([id, "null", "indexed", "unindexed"])

    for {key, value} <- sources, do: assert({:ok, ^value} = Concord.get(key))
    assert {:ok, %{etag: "current"}} = Versioning.get_version(bucket, "key:colon", "null")
    assert {:ok, %{etag: "unindexed"}} = Versioning.get_version(bucket, "key:colon", "unindexed")
  end

  test "stale bucket generation cannot normalize or publish legacy data", %{bucket: bucket} do
    :ok = Concord.put("obj:#{bucket}:key", %{etag: "retained", size: 0})

    assert {:error, :bucket_generation_changed} =
             LegacyObject.prepare(Backend, bucket, "key",
               bucket_versioning: true,
               bucket_generation: "stale"
             )

    assert {:ok, nil} = Backend.get(Keys.object_head(bucket, "key"), [])
    assert {:ok, nil} = Backend.get(Keys.object_version(bucket, "key", "null"), [])
    assert {:ok, %{etag: "retained"}} = Concord.get("obj:#{bucket}:key")
  end

  test "large legacy history requires explicit backfill without truncation or mutation", %{
    bucket: bucket
  } do
    for index <- 1..257,
        do:
          assert(
            :ok =
              Concord.put("obj_ver:#{bucket}:key:v#{index}", %{
                etag: "v#{index}",
                size: 0,
                content_hash:
                  Base.encode16(:crypto.hash(:sha256, "legacy-#{bucket}-#{index}"), case: :lower)
              })
          )

    {:ok, bucket_record} = Metadata.get_bucket(bucket)

    :ok =
      Concord.delete(
        ListIndex.root_key(
          bucket,
          ExStorageService.Metadata.BucketLifecycle.generation(bucket_record)
        )
      )

    assert {:error, :listing_index_migration_required} =
             Versioning.put_version(bucket, "key", %{etag: "new", size: 0})

    assert {:ok, nil} = Backend.get(Keys.object_head(bucket, "key"), [])
    assert {:ok, %{etag: "v257"}} = Concord.get("obj_ver:#{bucket}:key:v257")
    migrate(bucket)
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert length(versions) == 257
    assert {:ok, _id} = Versioning.put_version(bucket, "key", %{etag: "new", size: 0})
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert length(versions) == 258
  end

  test "read-only v1 schema rejects a mutation before normalization", %{bucket: bucket} do
    :ok = Concord.put("obj:#{bucket}:key", %{etag: "retained", size: 0})

    assert {:error, :v2_metadata_writes_disabled} =
             ObjectCommit.put(bucket, "key", %{etag: "new"},
               bucket_versioning: true,
               metadata_schema: :v1
             )

    assert {:ok, nil} = Backend.get(Keys.object_head(bucket, "key"), [])
    assert {:ok, %{etag: "retained"}} = Concord.get("obj:#{bucket}:key")
  end

  defp migrate(bucket) do
    directory =
      Path.join(System.tmp_dir!(), "ess-legacy-backfill-#{System.unique_integer([:positive])}")

    File.mkdir!(directory)
    path = Path.join(directory, "manifest")

    opts = [
      backup_module: FixtureBackup,
      sources: [:isolated_fixture],
      backup: "fixture-backup",
      local_blob_probe: fn _hash, _size ->
        {:ok, %{node_id: "legacy-fixture", node_generation: 1, verified_at: "2020-01-01"}}
      end
    ]

    try do
      assert {:ok, build} = Backfill.begin(bucket, opts)
      assert {:ok, _} = Backfill.capture(bucket, build.owner_token, path, opts)
      advance(bucket, build.owner_token, path, opts, :building)
      advance(bucket, build.owner_token, path, opts, :validating)
      assert {:ok, %{phase: :ready}} = Backfill.cutover(bucket, build.owner_token, opts)
    after
      File.rm_rf!(directory)
    end
  end

  defp advance(bucket, token, path, opts, :building) do
    assert {:ok, status} = Backfill.resume(bucket, token, path, opts)
    if status.phase == :building, do: advance(bucket, token, path, opts, :building)
  end

  defp advance(bucket, token, path, opts, :validating) do
    assert {:ok, status} = Backfill.validate(bucket, token, path, opts)
    if status.phase != :validated, do: advance(bucket, token, path, opts, :validating)
  end
end
