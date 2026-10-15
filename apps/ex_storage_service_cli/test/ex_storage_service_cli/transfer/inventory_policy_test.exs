defmodule ExStorageServiceCli.Transfer.InventoryPolicyTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Target}

  alias ExStorageServiceCli.Transfer.{
    Filter,
    Inventory,
    Manifest,
    Mime,
    Progress,
    Source,
    Spool,
    SyncPolicy
  }

  @moduletag :tmp_dir

  test "ordered filters use last match and wildcards span path components" do
    assert {:ok, rules} =
             Filter.compile(
               exclude: "*",
               include: "*.txt",
               exclude: "secret/*",
               include: "secret/keep?.txt"
             )

    assert Filter.include?(rules, "nested/雪.txt")
    refute Filter.include?(rules, "secret/drop.txt")
    assert Filter.include?(rules, "secret/keep1.txt")
    refute Filter.include?(rules, "image.png")
    assert {:ok, classes} = Filter.compile(exclude: "*", include: "[!0-9][]a-c]?.txt")
    assert Filter.include?(classes, "x]雪.txt")
    refute Filter.include?(classes, "1]雪.txt")
    assert {:ok, literals} = Filter.compile(exclude: "literal[invalid")
    refute Filter.include?(literals, "literal[invalid")

    assert {:error, %Error{code: :filter_budget}} =
             Filter.compile(List.duplicate({:include, "*"}, 129))
  end

  test "external manifests sort arbitrarily ordered runs and merge inventories", %{tmp_dir: root} do
    input =
      1200..1//-1
      |> Stream.map(&%{"name" => String.pad_leading(to_string(&1), 4, "0"), "size" => &1})

    assert {:ok, a} = Manifest.build(input, tmp_dir: root)
    assert a.count == 1200
    assert a.bytes == div(1200 * 1201, 2)

    assert Enum.map(Manifest.stream(a), & &1["name"]) ==
             Enum.map(1..1200, &String.pad_leading(to_string(&1), 4, "0"))

    assert Bitwise.band(File.stat!(a.root).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(a.path).mode, 0o777) == 0o600
    assert {:ok, b} = Manifest.build([%{"name" => "0002"}, %{"name" => "9999"}], tmp_dir: root)
    pairs = Enum.to_list(Manifest.pairs(a, b))
    assert {nil, %{"name" => "9999"}} = List.last(pairs)
    assert {%{"name" => "0002"}, %{"name" => "0002"}} = Enum.at(pairs, 1)
    Manifest.cleanup(a)
    Manifest.cleanup(b)
    assert File.ls!(root) == []
  end

  test "duplicate keys across runs and input exceptions clean every private run", %{tmp_dir: root} do
    entries = Enum.map(1..600, &%{"name" => to_string(&1)}) ++ [%{"name" => "1"}]
    assert {:error, %Error{code: :inventory_collision}} = Manifest.build(entries, tmp_dir: root)
    assert File.ls!(root) == []

    assert {:error, %Error{code: :inventory_io}} =
             Manifest.build(
               Stream.map(1..900, fn n ->
                 if n == 700, do: raise("failed"), else: %{"name" => to_string(n)}
               end),
               tmp_dir: root
             )

    assert File.ls!(root) == []
  end

  test "inventory failure on later page preserves completion failure", %{tmp_dir: root} do
    fetch = fn _, input ->
      if input["ContinuationToken"] do
        {:error, Error.new(:transport, "late listing failure")}
      else
        {:ok,
         %{
           "Contents" => [%{"Key" => "p/a", "Size" => 1, "ETag" => "a"}],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}
      end
    end

    assert {:error, %Error{category: :transport}} =
             Inventory.remote(nil, %Target{bucket: "b", key: "p"}, tmp_dir: root, fetch: fetch)

    assert File.ls!(root) == []
  end

  test "local traversal excludes symlinks explicitly and detects followed loops", %{tmp_dir: root} do
    File.write!(Path.join(root, "a"), "hello")
    File.mkdir!(Path.join(root, "sub"))
    File.ln_s!(root, Path.join(root, "sub/loop"))
    assert {:ok, inventory} = Inventory.local(root, follow_symlinks: false)
    assert [%{"name" => "a", "size" => 5}] = Enum.to_list(Manifest.stream(inventory))
    Manifest.cleanup(inventory)
    assert {:error, %Error{code: :symlink_loop}} = Inventory.local(root, follow_symlinks: true)
  end

  test "case, Unicode and file-directory mapping collisions fail", %{tmp_dir: root} do
    for names <- [["A", "a"], ["é", "é"], ["dir", "dir/file"]] do
      assert {:ok, inventory} = Manifest.build(Enum.map(names, &%{"name" => &1}), tmp_dir: root)

      assert {:error, %Error{code: :inventory_collision}} =
               Inventory.collision_check(inventory, tmp_dir: root)

      Manifest.cleanup(inventory)
    end
  end

  test "AWS direction-specific timestamps differ from mirror overwrite policy" do
    older = %{"size" => 4, "modified" => 1}
    newer = %{"size" => 4, "modified" => 2}
    assert {:skip, _} = SyncPolicy.compare(:aws, :download, newer, older, %{})
    assert {:copy, _} = SyncPolicy.compare(:aws, :download, older, newer, %{})
    assert {:copy, _} = SyncPolicy.compare(:aws, :upload, newer, older, %{})
    assert {:skip, _} = SyncPolicy.compare(:aws, :upload, older, newer, %{})

    assert {:copy, _} =
             SyncPolicy.compare(:aws, :download, newer, older, %{exact_timestamps: true})

    assert {:skip, _} = SyncPolicy.compare(:aws, :download, older, newer, %{size_only: true})
    assert {:conflict, _} = SyncPolicy.compare(:mirror, :copy, newer, older, %{})
    assert {:copy, _} = SyncPolicy.compare(:mirror, :copy, newer, older, %{overwrite: true})
  end

  test "stdin spool is bounded, replayable, validates expected size and cleans up", %{
    tmp_dir: root
  } do
    {:ok, input} = StringIO.open(String.duplicate("abc", 50_000))
    context = %ExStorageServiceCli.Context{}

    assert {:ok, 150_000} =
             Spool.with_source(
               context,
               [stdin: input, tmp_dir: root, expected_size: "150000"],
               fn source ->
                 assert :ok = Source.validate(source)

                 assert Source.enumerable(source) |> Enum.map(&byte_size/1) |> Enum.max() <=
                          65_536

                 {:ok, source.length}
               end
             )

    assert File.ls!(root) == []
    {:ok, input} = StringIO.open("x")

    assert {:error, %Error{code: :stdin_size}} =
             Spool.with_source(context, [stdin: input, tmp_dir: root, expected_size: 2], fn _ ->
               flunk("must not upload")
             end)

    assert File.ls!(root) == []
  end

  test "progress has a real completion effect and respects suppression" do
    owner = self()

    assert {:ok, :done} =
             Progress.with(
               %{progress_multiline: true},
               [progress_write: &send(owner, {:progress, &1})],
               fn opts ->
                 opts[:on_progress].(123)
                 {:ok, :done}
               end
             )

    assert_receive {:progress, "Completed 123 Bytes\n"}

    assert :ok =
             Progress.with(
               %{progress: false},
               [progress_write: &send(owner, {:progress, &1})],
               fn opts ->
                 assert opts[:on_progress] == nil
                 :ok
               end
             )

    refute_receive {:progress, _}
  end

  test "MIME inference covers encoded and compound extensions without host configuration" do
    assert Mime.guess("report.pdf") == "application/pdf"
    assert Mime.guess("bundle.tgz") == "application/x-tar"
    assert Mime.guess("drawing.svgz") == "image/svg+xml"
    assert Mime.guess("name.unknown") == nil
  end
end
