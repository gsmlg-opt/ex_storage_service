defmodule ExStorageService.BlobStore.LocalCASTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.{LocalCAS, ReadyBlob, StagedBlob}

  defmodule NoPack do
    def locate(_hash), do: {:error, :not_found}
  end

  defmodule BoundedReadFileSystem do
    @max_read 262_144

    defdelegate mkdir_p(path), to: LocalCAS.FileSystem
    defdelegate open(path, modes), to: LocalCAS.FileSystem
    defdelegate write(io, data), to: LocalCAS.FileSystem
    defdelegate sync(io), to: LocalCAS.FileSystem
    defdelegate close(io), to: LocalCAS.FileSystem
    defdelegate rename(source, destination), to: LocalCAS.FileSystem
    defdelegate rm(path), to: LocalCAS.FileSystem
    defdelegate stat(path), to: LocalCAS.FileSystem
    defdelegate open_directory(path), to: LocalCAS.FileSystem

    def pread(io, offset, length) when length <= @max_read,
      do: LocalCAS.FileSystem.pread(io, offset, length)

    def pread(_io, _offset, length), do: {:error, {:unbounded_read, length}}
  end

  @tag :tmp_dir
  test "stage, commit, stat, open, discard, and delete preserve source shapes", %{
    tmp_dir: tmp_dir
  } do
    opts = blob_opts(tmp_dir)
    data = "local-cas-lifecycle"
    hash = sha256(data)
    etag = md5(data)

    assert {:ok, %StagedBlob{path: staged_path, hash: ^hash, etag: ^etag, size: 19} = staged} =
             LocalCAS.stage(["local-", "cas-", "lifecycle"], opts)

    assert File.read!(staged_path) == data
    assert Path.dirname(staged_path) == Keyword.fetch!(opts, :tmp_dir)

    assert {:ok,
            %ReadyBlob{
              path: ready_path,
              hash: ^hash,
              etag: ^etag,
              size: 19,
              source: {:file, ready_path, 0, 19}
            }} = LocalCAS.commit(staged, opts)

    refute File.exists?(staged_path)
    assert File.read!(ready_path) == data
    assert Path.relative_to(ready_path, Keyword.fetch!(opts, :root)) != ready_path

    assert {:ok, %{hash: ^hash, size: 19, storage: :loose, source: {:file, ^ready_path, 0, 19}}} =
             LocalCAS.stat(hash, opts)

    assert {:ok, {:file, ^ready_path, 0, 19}} = LocalCAS.open(hash, nil, opts)
    assert {:ok, {:file, ^ready_path, 6, 3}} = LocalCAS.open(hash, {6, 3}, opts)
    assert {:error, :invalid_range} = LocalCAS.open(hash, {18, 2}, opts)

    assert {:ok, disposable} = LocalCAS.stage("discard-me", opts)
    assert :ok = LocalCAS.discard(disposable, opts)
    refute File.exists?(disposable.path)
    assert :ok = LocalCAS.discard(disposable, opts)

    assert :ok = LocalCAS.delete(hash, opts)
    assert {:error, :not_found} = LocalCAS.stat(hash, opts)
    assert :ok = LocalCAS.delete(hash, opts)
  end

  @tag :tmp_dir
  test "commit crosses stage, sync, rename, and directory sync in order", %{tmp_dir: tmp_dir} do
    parent = self()

    faults =
      for phase <- [:stage, :sync, :rename, :directory_sync], into: %{} do
        {phase,
         fn ->
           send(parent, {:boundary, phase})
           :ok
         end}
      end

    opts = blob_opts(tmp_dir, faults: faults)

    assert {:ok, staged} = LocalCAS.stage("ordered", opts)
    assert {:ok, %ReadyBlob{}} = LocalCAS.commit(staged, opts)

    assert_received {:boundary, :stage}
    assert_received {:boundary, :sync}
    assert_received {:boundary, :rename}
    assert_received {:boundary, :directory_sync}
  end

  @tag :tmp_dir
  test "stage failure publishes no temporary or ready file", %{tmp_dir: tmp_dir} do
    opts = blob_opts(tmp_dir, faults: %{stage: {:error, :injected}})

    assert {:error, {:stage, :injected}} = LocalCAS.stage("never-written", opts)
    assert Path.wildcard(Path.join([Keyword.fetch!(opts, :root), "**", "*"])) == []
  end

  @tag :tmp_dir
  test "sync and rename failures leave a discardable staged blob and no ready blob", %{
    tmp_dir: tmp_dir
  } do
    base_opts = blob_opts(tmp_dir)

    for phase <- [:sync, :rename] do
      data = "fail-#{phase}"
      assert {:ok, staged} = LocalCAS.stage(data, base_opts)
      failing_opts = Keyword.put(base_opts, :faults, %{phase => {:error, :injected}})

      assert {:error, {^phase, :injected}} = LocalCAS.commit(staged, failing_opts)
      assert File.exists?(staged.path)
      refute File.exists?(LocalCAS.blob_path(staged.hash, base_opts))
      assert :ok = LocalCAS.discard(staged, base_opts)
    end
  end

  @tag :tmp_dir
  test "directory sync failure reports an ambiguous publish that is idempotently recoverable", %{
    tmp_dir: tmp_dir
  } do
    opts = blob_opts(tmp_dir)
    assert {:ok, staged} = LocalCAS.stage("directory-sync", opts)

    assert {:error, {:directory_sync, :injected}} =
             LocalCAS.commit(
               staged,
               Keyword.put(opts, :faults, %{directory_sync: {:error, :injected}})
             )

    ready_path = LocalCAS.blob_path(staged.hash, opts)
    assert File.exists?(ready_path)
    refute File.exists?(staged.path)

    assert {:ok, %ReadyBlob{path: ^ready_path, hash: hash}} = LocalCAS.commit(staged, opts)
    assert hash == staged.hash
  end

  @tag :tmp_dir
  test "verify hashes a large blob through bounded reads and detects corruption", %{
    tmp_dir: tmp_dir
  } do
    opts = blob_opts(tmp_dir, fs_module: BoundedReadFileSystem)
    chunk = :binary.copy(<<17>>, 262_144)
    stream = Stream.map(1..3, fn _ -> chunk end)

    assert {:ok, staged} = LocalCAS.stage(stream, opts)
    assert staged.size == 3 * 262_144
    assert {:ok, ready} = LocalCAS.commit(staged, opts)
    assert :ok = LocalCAS.verify(ready.hash, opts)

    {:ok, io} = :file.open(ready.path, [:read, :write, :raw, :binary])
    :ok = :file.pwrite(io, 262_144 + 7, <<99>>)
    :ok = :file.close(io)

    assert {:error, :checksum_mismatch} = LocalCAS.verify(ready.hash, opts)
    assert {:error, :not_found} = LocalCAS.verify(sha256("missing"), opts)
  end

  defp blob_opts(tmp_dir, extra \\ []) do
    root = Path.join(tmp_dir, "cas")

    [
      root: root,
      tmp_dir: Path.join([root, "tmp", "uploads"]),
      pack_module: NoPack
    ]
    |> Keyword.merge(extra)
  end

  defp sha256(data),
    do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)

  defp md5(data),
    do: :md5 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
