defmodule ExStorageService.Storage.PackTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Pack}

  defp seed_loose_blob(data) do
    hash = Base.encode16(:crypto.hash(:sha256, data), case: :lower)
    path = CAS.blob_path(hash)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
    Metadata.ensure_blob_meta(hash, byte_size(data))
    hash
  end

  test "pack_blobs consolidates loose blobs and preserves CAS identity" do
    d1 = "pack-me-1-#{System.unique_integer()}"
    d2 = "pack-me-2-#{System.unique_integer()}"
    h1 = seed_loose_blob(d1)
    h2 = seed_loose_blob(d2)

    assert {:ok, %{pack_hash: pack_hash, packed: 2}} = Pack.pack_blobs([h1, h2])

    # pack file is content-addressed by its own bytes
    pack_path = Pack.pack_path(pack_hash)
    assert File.exists?(pack_path)
    assert Base.encode16(:crypto.hash(:sha256, File.read!(pack_path)), case: :lower) == pack_hash
    assert File.exists?(pack_path <> ".idx")

    # loose files are gone; blob metadata points into the pack
    refute File.exists?(CAS.blob_path(h1))
    assert {:ok, %{state: :packed, pack: %{hash: ^pack_hash}}} = Metadata.get_blob_meta(h1)

    # reads return the original bytes
    assert {:ok, ^d1} = Pack.read(h1)
    assert {:ok, ^d2} = Pack.read(h2)

    assert {:ok, {^pack_path, offset, size}} = Pack.locate(h2)
    assert size == byte_size(d2)
    assert offset == byte_size(d1)
  end

  test "pack_blobs skips missing and already-packed blobs" do
    d = "pack-skip-#{System.unique_integer()}"
    h = seed_loose_blob(d)

    missing =
      Base.encode16(:crypto.hash(:sha256, "ghost-#{System.unique_integer()}"), case: :lower)

    assert {:ok, %{packed: 1}} = Pack.pack_blobs([h, missing])
    # re-packing the same blob is a no-op
    assert {:ok, %{packed: 0, pack_hash: nil}} = Pack.pack_blobs([h])
    assert {:ok, ^d} = Pack.read(h)
  end

  test "locate on a loose or unknown blob returns not_found" do
    h = seed_loose_blob("still-loose-#{System.unique_integer()}")
    assert {:error, :not_found} = Pack.locate(h)
  end
end
