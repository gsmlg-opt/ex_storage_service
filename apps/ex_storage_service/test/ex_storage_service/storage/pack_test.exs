defmodule ExStorageService.Storage.PackTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine, Pack, Packer}

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

    # blob metadata points into the pack while the loose file remains as a
    # fallback for readers that resolved it before the metadata changed
    assert File.exists?(CAS.blob_path(h1))

    assert {:ok, %{state: :packed, pack: %{hash: ^pack_hash}, packed_at: packed_at}} =
             Metadata.get_blob_meta(h1)

    assert is_integer(packed_at)

    # reads return the original bytes
    assert {:ok, ^d1} = Pack.read(h1)
    assert {:ok, ^d2} = Pack.read(h2)

    assert {:ok, {^pack_path, offset, size}} = Pack.locate(h2)
    assert size == byte_size(d2)
    assert offset == byte_size(d1)

    missing_path = pack_path <> ".temporarily-missing"
    File.rename!(pack_path, missing_path)

    try do
      assert {:error, :not_found} = Pack.locate(h1)
      assert {:ok, {:file, loose_path}} = Engine.get_object_location("unused", h1)
      assert loose_path == CAS.blob_path(h1)
      assert {:ok, ^d1} = Engine.read_object("unused", h1)
    after
      File.rename!(missing_path, pack_path)
    end
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

  test "direct pack calls are disabled in cluster mode" do
    data = "cluster-pack-disabled-#{System.unique_integer()}"
    hash = seed_loose_blob(data)

    assert {:ok, %{packed: 0, pack_hash: nil, disabled: :cluster_mode}} =
             Pack.pack_blobs([hash], mode: :cluster)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, %{state: :active}} = Metadata.get_blob_meta(hash)
  end

  test "pack creation preserves content across multiple bounded copy chunks" do
    data = :binary.copy(<<0, 1, 2, 3>>, 200_000)
    hash = seed_loose_blob(data)

    assert {:ok, %{packed: 1}} = Pack.pack_blobs([hash])
    assert {:ok, ^data} = Pack.read(hash)
  end

  test "an open loose reader survives packing and physical loose-file cleanup" do
    data = :binary.copy("reader-#{System.unique_integer([:positive])}-", 80_000)
    hash = seed_loose_blob(data)
    path = CAS.blob_path(hash)
    source = ExStorageService.BlobStore.Source.file(path, 0, byte_size(data))
    owner = self()

    reader =
      Task.async(fn ->
        ExStorageService.BlobStore.Source.reduce(
          source,
          {:crypto.hash_init(:sha256), 0},
          fn chunk, {digest, bytes} ->
            if bytes == 0 do
              send(owner, {:loose_reader_open, self()})

              receive do
                :resume_reader -> :ok
              after
                10_000 -> raise "packed reader resume timed out"
              end
            end

            {:cont, {:crypto.hash_update(digest, chunk), bytes + byte_size(chunk)}}
          end
        )
      end)

    assert_receive {:loose_reader_open, reader_pid}, 5_000
    assert reader_pid == reader.pid

    try do
      assert {:ok, %{packed: 1}} = Pack.pack_blobs([hash])
      assert File.exists?(path)

      assert {:ok, _report} =
               Packer.pack_now(cold_after: 0, min_blobs: 1_000_000, cleanup_after: 0)

      refute File.exists?(path)
      assert {:ok, ^data} = Pack.read(hash)
    after
      send(reader.pid, :resume_reader)
    end

    assert {:ok, {digest, bytes}} = Task.await(reader, 5_000)
    assert bytes == byte_size(data)
    assert Base.encode16(:crypto.hash_final(digest), case: :lower) == hash
  end

  test "cleanup retains the loose fallback when the pack is truncated" do
    data = "truncated-pack-fallback-#{System.unique_integer()}"
    hash = seed_loose_blob(data)

    assert {:ok, %{pack_hash: pack_hash, packed: 1}} = Pack.pack_blobs([hash])
    pack_path = Pack.pack_path(pack_hash)
    File.write!(pack_path, binary_part(data, 0, byte_size(data) - 1))

    assert {:error, :not_found} = Pack.locate(hash)

    assert {:ok, _report} =
             Packer.pack_now(cold_after: 0, min_blobs: 1_000_000, cleanup_after: 0)

    assert File.exists?(CAS.blob_path(hash))
    assert {:ok, {:file, loose_path}} = Engine.get_object_location("unused", hash)
    assert loose_path == CAS.blob_path(hash)
    assert {:ok, ^data} = Engine.read_object("unused", hash)
  end

  test "locate on a loose or unknown blob returns not_found" do
    h = seed_loose_blob("still-loose-#{System.unique_integer()}")
    assert {:error, :not_found} = Pack.locate(h)
  end
end
