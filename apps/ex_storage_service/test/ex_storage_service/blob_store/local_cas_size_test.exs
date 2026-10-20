defmodule ExStorageService.BlobStore.LocalCASSizeTest do
  use ExUnit.Case, async: true
  alias ExStorageService.BlobStore.LocalCAS
  @moduletag :tmp_dir

  defmodule RecordingWrites do
    defdelegate mkdir_p(path), to: LocalCAS.FileSystem
    defdelegate open(path, modes), to: LocalCAS.FileSystem
    defdelegate close(io), to: LocalCAS.FileSystem
    defdelegate rm(path), to: LocalCAS.FileSystem

    def write(io, bytes) do
      send(self(), {:staged_write, bytes})
      LocalCAS.FileSystem.write(io, bytes)
    end
  end

  test "binary staging rejects bytes above its limit and removes the partial file", %{
    tmp_dir: root
  } do
    opts = [
      root: root,
      tmp_dir: Path.join(root, "stages"),
      fs_module: RecordingWrites,
      max_size: 4
    ]

    assert {:error, {:stage, :entity_too_large}} = LocalCAS.stage("12345", opts)
    refute_receive {:staged_write, _}
    assert File.ls!(opts[:tmp_dir]) == []
    assert {:ok, staged} = LocalCAS.stage("1234", opts)
    assert staged.size == 4
    assert_receive {:staged_write, "1234"}
    assert :ok = LocalCAS.discard(staged)
  end

  test "enumerable staging stops before writing the over-limit chunk", %{tmp_dir: root} do
    chunks =
      Stream.map(["12", "34", "5", "not consumed"], fn bytes ->
        send(self(), {:consumed, bytes})
        bytes
      end)

    opts = [
      root: root,
      tmp_dir: Path.join(root, "stages"),
      fs_module: RecordingWrites,
      max_size: 4
    ]

    assert {:error, {:stage, :entity_too_large}} = LocalCAS.stage(chunks, opts)
    assert_receive {:consumed, "12"}
    assert_receive {:consumed, "34"}
    assert_receive {:consumed, "5"}
    refute_receive {:consumed, "not consumed"}
    assert_receive {:staged_write, "12"}
    assert_receive {:staged_write, "34"}
    refute_receive {:staged_write, "5"}
    assert File.ls!(opts[:tmp_dir]) == []
  end

  test "invalid staging limits fail before creating temporary roots", %{tmp_dir: root} do
    temporary = Path.join(root, "stages")

    assert {:error, :invalid_max_size} =
             LocalCAS.stage("", root: root, tmp_dir: temporary, max_size: -1)

    refute File.exists?(temporary)
  end
end
