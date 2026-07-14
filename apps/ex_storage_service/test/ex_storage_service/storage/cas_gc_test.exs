defmodule ExStorageService.Storage.CasGCTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, CasGC, Pack, Packer}

  # Every stage immediate: orphan grace 0, candidate grace 0, quarantine grace 0.
  @instant [orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0]

  defp seed_blob(data) do
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
    # backdate so mtime-grace tests with nonzero grace also work
    File.touch!(path, System.os_time(:second) - 7200)
    Metadata.ensure_blob_meta(hash, byte_size(data))
    hash
  end

  defp quarantine_path(hash) do
    Path.join([CAS.data_root(), CAS.reserved_root(), "gc", "quarantine", "sha256-#{hash}"])
  end

  defp reference(hash) do
    bucket = "gcref-#{:erlang.unique_integer([:positive])}"
    key = "k"

    Metadata.put_object_meta(bucket, key, %{
      content_hash: hash,
      size: 1,
      etag: "e",
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    {bucket, key}
  end

  test "full lifecycle: candidate -> quarantine -> delete for unreferenced blobs" do
    hash = seed_blob("gc-lifecycle-#{System.unique_integer()}")

    # sweep 1: candidate created, file untouched
    {:ok, r1} = CasGC.run_now(@instant)
    assert r1.candidates_created >= 1
    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{stage: :candidate}} = get_candidate(hash)

    # sweep 2: quarantined — file moved, blob meta updated
    {:ok, r2} = CasGC.run_now(@instant)
    assert r2.quarantined >= 1
    refute File.exists?(CAS.blob_path(hash))
    assert File.exists?(quarantine_path(hash))
    assert {:ok, %{state: :quarantined}} = Metadata.get_blob_meta(hash)

    # sweep 3: deleted — file, blob meta, candidate all gone
    {:ok, r3} = CasGC.run_now(@instant)
    assert r3.deleted >= 1
    refute File.exists?(quarantine_path(hash))
    assert {:error, :not_found} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "referenced blobs are never selected" do
    hash = seed_blob("gc-referenced-#{System.unique_integer()}")
    reference(hash)

    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)

    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "active multipart part blobs are rooted" do
    hash = seed_blob("gc-part-#{System.unique_integer()}")

    Concord.put("mpu_part:gcbucket:upload1:1", %{
      part_number: 1,
      hash: hash,
      size: 1,
      etag: "e",
      uploaded_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    {:ok, _} = CasGC.run_now(@instant)
    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)

    Concord.delete("mpu_part:gcbucket:upload1:1")
  end

  test "retained loose fallbacks for packed blobs are never garbage collected" do
    hash = seed_blob("gc-packed-fallback-#{System.unique_integer()}")

    assert {:ok, %{packed: 1}} = Pack.pack_blobs([hash])
    assert File.exists?(CAS.blob_path(hash))

    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{state: :packed}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "direct Pack calls skip blobs with a live GC candidate" do
    hash = seed_blob("gc-direct-pack-candidate-#{System.unique_integer()}")

    assert {:ok, _report} = CasGC.run_now(@instant)
    assert {:ok, %{stage: :candidate}} = get_candidate(hash)

    assert {:ok, %{pack_hash: nil, packed: 0}} = Pack.pack_blobs([hash])
    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{state: :active}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = Pack.locate(hash)
  end

  test "Packer skips reachable blobs that still have a GC candidate" do
    hash = seed_blob("gc-packer-candidate-#{System.unique_integer()}")

    assert {:ok, _report} = CasGC.run_now(@instant)
    assert {:ok, %{stage: :candidate}} = get_candidate(hash)
    reference(hash)

    assert {:ok, _report} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{state: :active}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = Pack.locate(hash)
    assert {:ok, %{stage: :candidate}} = get_candidate(hash)
  end

  test "packed blobs with quarantined candidates restore their loose fallback" do
    data = "gc-packed-quarantined-#{System.unique_integer()}"
    hash = seed_blob(data)

    assert {:ok, %{packed: 1}} = Pack.pack_blobs([hash])

    qpath = quarantine_path(hash)
    File.mkdir_p!(Path.dirname(qpath))
    File.rename!(CAS.blob_path(hash), qpath)

    Concord.put("gc:candidate:#{hash}", %{
      hash: "sha256:#{hash}",
      reason: :unreferenced,
      stage: :quarantined,
      first_seen_at: 0,
      eligible_after: 0
    })

    assert {:ok, _report} = CasGC.run_now(@instant)
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(qpath)
    assert {:ok, %{state: :packed}} = Metadata.get_blob_meta(hash)
    assert {:ok, ^data} = Pack.read(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "quarantined blob is restored when its hash becomes reachable again" do
    hash = seed_blob("gc-restore-#{System.unique_integer()}")

    {:ok, _} = CasGC.run_now(@instant)
    {:ok, _} = CasGC.run_now(@instant)
    assert File.exists?(quarantine_path(hash))

    # a new object now references the quarantined content
    reference(hash)

    {:ok, r} = CasGC.run_now(@instant)
    assert r.restored >= 1
    assert File.exists?(CAS.blob_path(hash))
    refute File.exists?(quarantine_path(hash))
    assert {:ok, %{state: :active}} = Metadata.get_blob_meta(hash)
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "dry_run reports without modifying anything" do
    hash = seed_blob("gc-dry-#{System.unique_integer()}")

    {:ok, report} = CasGC.run_now(Keyword.put(@instant, :dry_run, true))
    assert report.candidates_created >= 1

    assert File.exists?(CAS.blob_path(hash))
    assert {:error, :not_found} = get_candidate(hash)
  end

  test "fresh unreferenced blobs are protected by the mtime grace" do
    data = "gc-fresh-#{System.unique_integer()}"
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)

    {:ok, _} = CasGC.run_now(orphan_mtime_grace: 600, candidate_grace: 0, quarantine_grace: 0)
    assert File.exists?(path)
    assert {:error, :not_found} = get_candidate(hash)
  end

  defp get_candidate(hash) do
    case Concord.get("gc:candidate:#{hash}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, v} -> {:ok, v}
      other -> other
    end
  end
end
