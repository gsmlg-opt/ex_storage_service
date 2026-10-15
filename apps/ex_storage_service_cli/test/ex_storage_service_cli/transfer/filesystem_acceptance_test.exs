defmodule ExStorageServiceCli.Transfer.FilesystemAcceptanceTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.Error
  alias ExStorageServiceCli.Transfer.{Inventory, Manifest, Sink}
  @moduletag :tmp_dir

  test "a traversal failure is emitted once and terminates cleanly", %{tmp_dir: root} do
    File.ln_s!(Path.join(root, "missing"), Path.join(root, "broken"))

    assert [{:error, %Error{code: :local_inventory}}] =
             root |> Inventory.local_entries() |> Enum.to_list()
  end

  test "actual distinct case and Unicode names fail portable destination preflight", %{
    tmp_dir: root
  } do
    for {names, index} <- Enum.with_index([["A", "a"], ["é", "é"]]) do
      directory = Path.join(root, to_string(index))
      File.mkdir!(directory)
      Enum.each(names, &File.write!(Path.join(directory, &1), &1))
      assert Enum.sort(File.ls!(directory)) == Enum.sort(names)
      assert {:ok, manifest} = Inventory.local(directory)

      try do
        assert {:error, %Error{code: :inventory_collision}} = Inventory.collision_check(manifest)
        assert Enum.sort(File.ls!(directory)) == Enum.sort(names)
      after
        Manifest.cleanup(manifest)
      end
    end
  end

  test "a real file-directory conflict cannot create or replace a destination", %{tmp_dir: root} do
    path = Path.join(root, "file")
    File.write!(path, "original")
    assert {:error, %Error{code: :unsafe_destination}} = Sink.open(Path.join(path, "child"))
    assert File.read!(path) == "original"
    assert File.ls!(root) == ["file"]
  end

  test "destination and parent symlink replacements fail publication", %{tmp_dir: root} do
    outside = Path.join(root, "outside")
    File.mkdir!(outside)
    File.write!(Path.join(outside, "foreign"), "foreign")
    parent = Path.join(root, "parent")
    File.mkdir!(parent)
    path = Path.join(parent, "destination")
    assert {:ok, sink} = Sink.open(path)
    assert :ok = Sink.write(sink, "new")
    File.ln_s!(Path.join(outside, "foreign"), path)
    assert {:error, %Error{code: :unsafe_destination}} = Sink.publish(sink)
    Sink.abort(sink)
    assert File.read!(Path.join(outside, "foreign")) == "foreign"
    File.rm!(path)

    assert {:ok, sink} = Sink.open(path)
    File.rename!(parent, parent <> "-old")
    File.ln_s!(outside, parent)
    assert {:error, %Error{code: :unsafe_destination}} = Sink.publish(sink)
    Sink.abort(sink)
    refute File.exists?(Path.join(outside, "destination"))
    assert File.read!(Path.join(outside, "foreign")) == "foreign"
  end

  test "a foreign staging symlink is never published or removed", %{tmp_dir: root} do
    destination = Path.join(root, "destination")
    foreign = Path.join(root, "foreign")
    File.write!(destination, "original")
    File.write!(foreign, "foreign")
    assert {:ok, sink} = Sink.open(destination)
    assert :ok = Sink.write(sink, "new")
    File.rm!(sink.temporary)
    File.ln_s!(foreign, sink.temporary)
    assert {:error, %Error{code: :unsafe_destination}} = Sink.publish(sink)
    assert :ok = Sink.abort(sink)
    assert File.read!(destination) == "original"
    assert File.read!(foreign) == "foreign"
    assert {:ok, %{type: :symlink}} = File.lstat(sink.temporary)
  end
end
