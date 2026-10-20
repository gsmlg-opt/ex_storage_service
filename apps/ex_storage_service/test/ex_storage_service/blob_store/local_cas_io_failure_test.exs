defmodule ExStorageService.BlobStore.LocalCASIOFailureTest do
  use ExUnit.Case, async: true
  alias ExStorageService.BlobStore.LocalCAS
  @moduletag :tmp_dir

  defmodule ClosedWriteFileSystem do
    defdelegate mkdir_p(path), to: LocalCAS.FileSystem
    defdelegate open(path, modes), to: LocalCAS.FileSystem
    defdelegate rm(path), to: LocalCAS.FileSystem

    def write(io, bytes) do
      :ok = LocalCAS.FileSystem.close(io)
      LocalCAS.FileSystem.write(io, bytes)
    end

    def close(io) do
      send(self(), :stage_close_attempted)
      LocalCAS.FileSystem.close(io)
    end
  end

  test "filesystem write returns file errors instead of raising", %{tmp_dir: root} do
    {:ok, io} = LocalCAS.FileSystem.open(Path.join(root, "closed"), [:write, :raw, :binary])
    :ok = LocalCAS.FileSystem.close(io)
    assert {:error, reason} = LocalCAS.FileSystem.write(io, "bytes")
    assert is_atom(reason)
  end

  test "reader write failure retains final reader state and closes and removes its stage", %{
    tmp_dir: root
  } do
    reader = fn :initial -> {:ok, "bytes", :consumed} end
    temporary = Path.join(root, "stages")

    assert {:error, {:stage, reason}, :consumed} =
             LocalCAS.stage_from_reader(reader, :initial,
               root: root,
               tmp_dir: temporary,
               fs_module: ClosedWriteFileSystem
             )

    assert is_atom(reason)
    assert_received :stage_close_attempted
    assert File.ls!(temporary) == []

    refute File.exists?(
             LocalCAS.blob_path(Base.encode16(:crypto.hash(:sha256, "bytes"), case: :lower),
               root: root
             )
           )
  end
end
