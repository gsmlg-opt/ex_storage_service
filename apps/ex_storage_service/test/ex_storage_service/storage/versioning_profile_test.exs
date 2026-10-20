defmodule ExStorageService.Storage.VersioningProfileTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.ListIndex.Backfill
  alias ExStorageService.Storage.Versioning

  # Isolated test fixtures only; real source quiescence and backup verification
  # have separate acceptance coverage. Never stop the shared Concord fixture.
  defmodule FixtureBackup do
    def assert_quiesced(:isolated_fixture), do: :ok
    def verify("fixture-backup", _opts), do: {:ok, %{backup_id: "fixture"}}
  end

  setup do
    bucket = "version-profile-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    %{bucket: bucket}
  end

  test "never-enabled buckets replace one readable null version", %{bucket: bucket} do
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("first"))
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("second"))

    assert {:ok, %{version_id: "null", etag: "second"}} =
             Versioning.get_version(bucket, "key", "null")

    assert {:ok, [%{version_id: "null", etag: "second"}]} =
             Versioning.list_versions(bucket, "key")
  end

  test "unversioned delete leaves no historical object or delete marker", %{bucket: bucket} do
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("first"))
    assert {:ok, "null", :deleted} = Versioning.delete_version(bucket, "key")
    assert {:ok, []} = Versioning.list_versions(bucket, "key")
    assert {:error, :not_found} = Versioning.get_version(bucket, "key", "null")
    assert :ok = Metadata.delete_bucket(bucket)
  end

  test "enabling retains an earlier null version", %{bucket: bucket} do
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("original"))
    assert :ok = Versioning.set_versioning(bucket, :enabled)
    assert {:ok, enabled_id} = Versioning.put_version(bucket, "key", metadata("enabled"))
    assert enabled_id != "null"
    assert {:ok, %{etag: "original"}} = Versioning.get_version(bucket, "key", "null")

    assert {:ok, %{etag: "enabled", version_id: ^enabled_id}} =
             Versioning.get_version(bucket, "key", nil)
  end

  test "suspended writes replace only null and preserve numbered history", %{bucket: bucket} do
    assert :ok = Versioning.set_versioning(bucket, :enabled)
    assert {:ok, enabled_id} = Versioning.put_version(bucket, "key", metadata("enabled"))
    assert :ok = Versioning.set_versioning(bucket, :suspended)
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("first-null"))
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("second-null"))
    assert {:ok, versions} = Versioning.list_versions(bucket, "key")
    assert Enum.sort(Enum.map(versions, & &1.version_id)) == Enum.sort(["null", enabled_id])
    assert {:ok, %{etag: "enabled"}} = Versioning.get_version(bucket, "key", enabled_id)
    assert {:ok, %{etag: "second-null"}} = Versioning.get_version(bucket, "key", "null")
  end

  test "suspended deletion replaces null with one marker and deleting it reveals history", %{
    bucket: bucket
  } do
    assert :ok = Versioning.set_versioning(bucket, :enabled)
    assert {:ok, enabled_id} = Versioning.put_version(bucket, "key", metadata("enabled"))
    assert :ok = Versioning.set_versioning(bucket, :suspended)
    assert {:ok, "null"} = Versioning.put_version(bucket, "key", metadata("null"))
    assert {:ok, "null", :delete_marker} = Versioning.delete_version(bucket, "key")

    assert {:ok, %{is_delete_marker: true, version_id: "null"}} =
             Versioning.get_version(bucket, "key", nil)

    assert {:ok, "null", :deleted} = Versioning.delete_version(bucket, "key", "null")

    assert {:ok, %{version_id: ^enabled_id, etag: "enabled"}} =
             Versioning.get_version(bucket, "key", nil)
  end

  test "deleting v2 history never resurrects legacy object records in reads or listings", %{
    bucket: bucket
  } do
    :ok = Concord.put("obj:#{bucket}:key", metadata("legacy"))
    assert :ok = Versioning.set_versioning(bucket, :enabled)
    assert {:ok, id} = Versioning.put_version(bucket, "key", metadata("current"))
    assert {:ok, ^id, :deleted} = Versioning.delete_version(bucket, "key", id)
    assert {:ok, "null", :deleted} = Versioning.delete_version(bucket, "key", "null")
    assert {:error, :not_found} = Versioning.get_version(bucket, "key", nil)
    assert {:ok, %{keys: [], common_prefixes: []}} = Metadata.list_objects(bucket)
    assert {:ok, %{etag: "legacy"}} = Concord.get("obj:#{bucket}:key")
  end

  test "explicitly migrated legacy null reads and deletions match the exposed version identity",
       %{
         bucket: bucket
       } do
    :ok = Concord.put("obj:#{bucket}:legacy", legacy_metadata(bucket, "legacy-null"))
    assert {:error, :not_found} = Versioning.get_version(bucket, "legacy", "null")
    migrate(bucket)
    assert {:ok, %{etag: "legacy-null"}} = Versioning.get_version(bucket, "legacy", "null")
    assert {:ok, "null", :deleted} = Versioning.delete_version(bucket, "legacy", "null")
    assert {:error, :not_found} = Versioning.get_version(bucket, "legacy", nil)
    assert {:error, :not_found} = Versioning.get_version(bucket, "legacy", "null")
    assert {:ok, %{keys: []}} = Metadata.list_objects(bucket)
  end

  test "deleting an explicitly migrated historical version does not silently no-op", %{
    bucket: bucket
  } do
    :ok = Concord.put("obj_ver_list:#{bucket}:legacy", ["old"])
    :ok = Concord.put("obj_ver:#{bucket}:legacy:old", legacy_metadata(bucket, "old"))
    assert {:error, :not_found} = Versioning.get_version(bucket, "legacy", "old")
    migrate(bucket)
    assert {:ok, %{etag: "old"}} = Versioning.get_version(bucket, "legacy", "old")
    assert {:ok, "old", :deleted} = Versioning.delete_version(bucket, "legacy", "old")
    assert {:error, :not_found} = Versioning.get_version(bucket, "legacy", "old")
    assert {:ok, []} = Versioning.list_versions(bucket, "legacy")
  end

  defp legacy_metadata(bucket, etag) do
    metadata("#{bucket}:#{etag}") |> Map.put(:etag, etag)
  end

  defp migrate(bucket) do
    directory =
      Path.join(
        System.tmp_dir!(),
        "ess-version-profile-backfill-#{System.unique_integer([:positive])}"
      )

    File.mkdir!(directory)
    path = Path.join(directory, "manifest")

    opts = [
      backup_module: FixtureBackup,
      sources: [:isolated_fixture],
      backup: "fixture-backup",
      local_blob_probe: fn _hash, _size ->
        {:ok,
         %{node_id: "version-profile-fixture", node_generation: 1, verified_at: "2020-01-01"}}
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

  defp metadata(etag) do
    %{
      content_hash: :crypto.hash(:sha256, etag) |> Base.encode16(case: :lower),
      size: byte_size(etag),
      etag: etag,
      metadata: %{},
      content_type: "text/plain"
    }
  end
end
