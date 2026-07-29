defmodule ExStorageService.Cluster.ScrubberTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Cluster.Scrubber

  @chunk_size 262_144

  @tag :tmp_dir
  test "streams a loose blob through bounded checksum verification", %{tmp_dir: tmp_dir} do
    opts = blob_opts(tmp_dir)
    chunk = :binary.copy(<<17>>, @chunk_size)

    assert {:ok, staged} = LocalCAS.stage(Stream.map(1..3, fn _ -> chunk end), opts)
    assert {:ok, ready} = LocalCAS.commit(staged, opts)

    assert {:ok, %{hash: hash, size: size, bytes: size}} =
             Scrubber.scrub(ready.hash, blob_store_opts: opts)

    assert hash == ready.hash
    assert size == 3 * @chunk_size
  end

  @tag :tmp_dir
  test "reports checksum corruption without changing the physical blob", %{tmp_dir: tmp_dir} do
    opts = blob_opts(tmp_dir)
    assert {:ok, staged} = LocalCAS.stage("scrub-corruption", opts)
    assert {:ok, ready} = LocalCAS.commit(staged, opts)
    File.write!(ready.path, "same-size-damage")

    assert byte_size("same-size-damage") == ready.size

    assert {:error, :checksum_mismatch} =
             Scrubber.scrub(ready.hash, blob_store_opts: opts)

    assert File.read!(ready.path) == "same-size-damage"
  end

  @tag :tmp_dir
  test "applies the configured byte rate one source chunk at a time", %{tmp_dir: tmp_dir} do
    opts = blob_opts(tmp_dir)
    chunk = :binary.copy(<<23>>, @chunk_size)
    assert {:ok, staged} = LocalCAS.stage(Stream.map(1..2, fn _ -> chunk end), opts)
    assert {:ok, ready} = LocalCAS.commit(staged, opts)
    parent = self()

    assert {:ok, %{bytes: bytes}} =
             Scrubber.scrub(ready.hash,
               blob_store_opts: opts,
               bytes_per_second: @chunk_size,
               sleep: fn delay -> send(parent, {:sleep, delay}) end
             )

    assert bytes == 2 * @chunk_size
    assert_receive {:sleep, 1_000}
    assert_receive {:sleep, 1_000}
    refute_receive {:sleep, _delay}
  end

  test "rejects an invalid rate limit before opening storage" do
    assert {:error, :invalid_rate_limit} =
             Scrubber.scrub(String.duplicate("a", 64), bytes_per_second: 0)
  end

  defp blob_opts(tmp_dir) do
    root = Path.join(tmp_dir, "cas")
    [root: root, tmp_dir: Path.join(root, "tmp"), pack_module: nil]
  end
end
