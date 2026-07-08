defmodule ExStorageService.Storage.MigrationTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine, Migration}

  defp unique_bucket, do: "mig-#{:erlang.unique_integer([:positive])}"
  defp sha256_hex(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)

  defp seed_legacy_object(bucket, key, data) do
    hash = sha256_hex(data)
    legacy = Engine.legacy_content_path(Engine.data_root(), bucket, hash)
    File.mkdir_p!(Path.dirname(legacy))
    File.write!(legacy, data)

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    Metadata.put_object_meta(bucket, key, %{
      content_hash: hash,
      size: byte_size(data),
      etag: Base.encode16(:crypto.hash(:md5, data), case: :lower),
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    hash
  end

  test "moves legacy files into the CAS and reports counts" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    h1 = seed_legacy_object(bucket, "a.bin", "mig-data-1-#{System.unique_integer()}")
    h2 = seed_legacy_object(bucket, "b.bin", "mig-data-2-#{System.unique_integer()}")

    assert {:ok, report} = Migration.migrate_to_global_cas()

    assert report.migrated >= 2
    assert CAS.has_blob?(h1) and CAS.has_blob?(h2)
    refute File.exists?(Engine.legacy_content_path(Engine.data_root(), bucket, h1))
    assert {:ok, _} = Metadata.get_blob_meta(h1)
    # objects remain readable through the engine
    assert {:ok, _path} = Engine.get_object(bucket, h1)
  end

  test "is idempotent and counts already-global blobs" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    h = seed_legacy_object(bucket, "c.bin", "mig-data-3-#{System.unique_integer()}")

    assert {:ok, _} = Migration.migrate_to_global_cas()
    assert {:ok, report2} = Migration.migrate_to_global_cas()

    assert report2.already_global >= 1
    assert CAS.has_blob?(h)
  end

  test "reports metadata pointing at missing files" do
    bucket = unique_bucket()
    Metadata.create_bucket(bucket)
    ghost_hash = sha256_hex("ghost-#{System.unique_integer()}")

    now = DateTime.utc_now() |> DateTime.to_iso8601()

    Metadata.put_object_meta(bucket, "ghost.bin", %{
      content_hash: ghost_hash,
      size: 5,
      etag: "0",
      content_type: "application/octet-stream",
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    assert {:ok, report} = Migration.migrate_to_global_cas()
    assert {bucket, ghost_hash} in report.missing
  end
end
