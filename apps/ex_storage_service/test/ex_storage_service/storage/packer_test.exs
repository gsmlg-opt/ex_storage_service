defmodule ExStorageService.Storage.PackerTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, CasGC, Engine, Packer}

  defp put_and_reference(data) do
    bucket = "packer-#{:erlang.unique_integer([:positive])}"
    {:ok, {hash, etag, size}} = Engine.put_object(bucket, "k", data)

    Metadata.put_object_meta(bucket, "k", %{
      content_hash: hash,
      size: size,
      etag: etag,
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    # backdate the loose file so it is "cold"
    File.touch!(CAS.blob_path(hash), System.os_time(:second) - 90 * 86_400)
    {bucket, hash, data}
  end

  test "packs cold reachable blobs; reads keep working through the Engine" do
    {bucket, hash, data} = put_and_reference("cold-data-#{System.unique_integer()}")

    assert {:ok, %{packed: packed}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert packed >= 1
    refute File.exists?(CAS.blob_path(hash))

    assert {:ok, {:pack, pack_path, offset, size}} = Engine.get_object_location(bucket, hash)
    assert File.exists?(pack_path)
    assert size == byte_size(data)
    assert is_integer(offset)

    assert {:ok, ^data} = Engine.read_object(bucket, hash)
    assert :ok = Engine.promote_to_global(bucket, hash)
  end

  test "does not pack fresh, unreachable, or already-packed blobs" do
    # fresh + reachable
    {_bucket, fresh_hash, _} = put_and_reference("fresh-#{System.unique_integer()}")
    File.touch!(CAS.blob_path(fresh_hash), System.os_time(:second))

    # cold but unreachable (no obj/obj_ver references)
    orphan_data = "orphan-#{System.unique_integer()}"
    {:ok, {orphan_hash, _, _}} = Engine.put_object("packer-orphan", "k", orphan_data)
    File.touch!(CAS.blob_path(orphan_hash), System.os_time(:second) - 90 * 86_400)

    {:ok, _} = Packer.pack_now(cold_after: 3600, min_blobs: 1)

    assert File.exists?(CAS.blob_path(fresh_hash)), "fresh blob must stay loose"

    assert File.exists?(CAS.blob_path(orphan_hash)),
           "unreachable blob is GC's business, not the packer's"
  end

  test "respects the min_blobs threshold" do
    {_bucket, hash, _} = put_and_reference("threshold-#{System.unique_integer()}")

    {:ok, %{packed: 0}} = Packer.pack_now(cold_after: 0, min_blobs: 1_000_000)
    assert File.exists?(CAS.blob_path(hash))
  end

  test "CasGC ignores packed blobs" do
    {_bucket, hash, _} = put_and_reference("gc-packed-#{System.unique_integer()}")
    {:ok, %{packed: p}} = Packer.pack_now(cold_after: 0, min_blobs: 1)
    assert p >= 1

    {:ok, _} = CasGC.run_now(orphan_mtime_grace: 0, candidate_grace: 0, quarantine_grace: 0)
    assert {:ok, %{state: :packed}} = Metadata.get_blob_meta(hash)
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
