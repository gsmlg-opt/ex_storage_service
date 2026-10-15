defmodule ExStorageServiceCli.Transfer.SourceTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Transfer.Source}
  @moduletag :tmp_dir

  test "bounded range is exact, replayable, hashed incrementally, and supports checksum folds", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "source")
    bytes = :binary.copy(<<0, 255, 13, 10>>, 50_001)
    File.write!(path, bytes)
    assert {:ok, source} = Source.file(path, offset: 3, length: 140_003)
    expected = binary_part(bytes, 3, 140_003)
    assert source.payload_hash == sha(expected)

    for _ <- 1..2 do
      chunks = Enum.to_list(Source.enumerable(source))
      assert Enum.all?(chunks, &(byte_size(&1) <= 65_536))
      assert IO.iodata_to_binary(chunks) == expected
      assert open_count(path) == 0
    end

    assert {:ok, crc} = Source.reduce(source, 0, &:erlang.crc32(&2, &1))
    assert crc == :erlang.crc32(expected)
    assert :ok = Source.validate(source)
    assert {:ok, subrange} = Source.range(source, 65_538, 5)
    assert subrange.offset == 65_538
    assert subrange.payload_hash == sha(binary_part(bytes, 65_538, 5))
    assert {:error, %Error{code: :invalid_source_range}} = Source.range(source, 0, 5)
  end

  test "empty ranges hash and close without producing chunks", %{tmp_dir: dir} do
    path = Path.join(dir, "empty")
    File.write!(path, "")
    assert {:ok, source} = Source.file(path)
    assert source.payload_hash == sha("")
    assert Enum.to_list(Source.enumerable(source)) == []
    assert open_count(path) == 0
  end

  test "halt and consumer exception release the owned raw file", %{tmp_dir: dir} do
    path = Path.join(dir, "source")
    File.write!(path, :binary.copy("a", 200_000))
    {:ok, source} = Source.file(path)
    assert Enum.take(Source.enumerable(source), 1) == [:binary.copy("a", 65_536)]
    assert open_count(path) == 0

    assert_raise RuntimeError, "consumer failure", fn ->
      Enum.each(Source.enumerable(source), fn _ ->
        assert open_count(path) == 1
        raise "consumer failure"
      end)
    end

    assert open_count(path) == 0
  end

  test "mutation during enumeration fails and releases the file", %{tmp_dir: dir} do
    path = Path.join(dir, "source")
    File.write!(path, :binary.copy("a", 200_000))
    {:ok, source} = Source.file(path)

    assert {:error, %Error{code: :source_changed}} =
             Source.reduce(source, 0, fn chunk, count ->
               if count == 0, do: File.write!(path, "truncated")
               count + byte_size(chunk)
             end)

    assert open_count(path) == 0
  end

  test "full validation catches metadata-identical content rewrites", %{tmp_dir: dir} do
    path = Path.join(dir, "source")
    # OTP stat timestamps have second resolution. Find an actual rewrite in
    # the same second, retaining inode/size/mtime/ctime, without timing sleeps.
    source =
      Enum.reduce_while(1..10, nil, fn _, _ ->
        File.write!(path, "original")
        {:ok, source} = Source.file(path)
        File.write!(path, "modified")
        {:ok, current} = Source.file(path, hash: false)
        if current.identity == source.identity, do: {:halt, source}, else: {:cont, nil}
      end)

    assert %Source{} = source
    assert :ok = Source.validate_identity(source)
    assert {:error, %Error{code: :source_changed}} = Source.validate(source)
    assert open_count(path) == 0
  end

  test "replacement and invalid ranges fail before reading", %{tmp_dir: dir} do
    path = Path.join(dir, "source")
    File.write!(path, "source")
    {:ok, source} = Source.file(path)
    File.rename!(path, path <> ".old")
    File.write!(path, "source")
    assert {:error, %Error{code: :source_changed}} = Source.validate(source)
    assert {:error, %Error{code: :source_changed}} = Source.hash(source)

    for opts <- [[offset: -1], [offset: 7], [length: 7], [chunk_size: 65_537], [chunk_size: 0]] do
      assert {:error, %Error{code: :invalid_source_range}} = Source.file(path, opts)
    end

    assert {:error, %Error{code: :invalid_source}} = Source.file(dir)
    assert {:error, %Error{code: :source_unavailable}} = Source.file(path <> ".missing")
    assert open_count(path) == 0
  end

  test "a sparse 64 MiB source stays in the same chunk memory bound", %{tmp_dir: dir} do
    path = Path.join(dir, "large")
    {:ok, file} = File.open(path, [:write, :binary, :raw])
    :file.position(file, 64 * 1024 * 1024 - 1)
    :file.write(file, <<0>>)
    File.close(file)
    {:ok, source} = Source.file(path)

    assert {:ok, {bytes, largest}} =
             Source.reduce(source, {0, 0}, fn chunk, {bytes, largest} ->
               {bytes + byte_size(chunk), max(largest, byte_size(chunk))}
             end)

    assert bytes == 64 * 1024 * 1024
    assert largest == 65_536
    assert open_count(path) == 0
  end

  defp sha(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp open_count(path) do
    "/proc/self/fd"
    |> File.ls!()
    |> Enum.count(fn fd ->
      File.read_link(Path.join("/proc/self/fd", fd)) == {:ok, path}
    end)
  end
end
