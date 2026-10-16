Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeDiffTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation}
  @moduletag :tmp_dir
  @earlier 1_767_225_600
  @external_resource Path.expand("../../fixtures/differential/native-diff-mc.json", __DIR__)
  @reference @external_resource |> File.read!() |> JSON.decode!()

  for captured <- @reference["cases"] do
    test "matches pinned mc comparison selection: #{captured["name"]}", %{tmp_dir: root} do
      captured = unquote(Macro.escape(captured))
      source = Path.join(root, "source")
      destination = Path.join(root, "target")
      for path <- [source, destination], do: File.mkdir_p!(path)

      if captured["source"],
        do: write(source, "item", captured["source"], captured["source_time"])

      if captured["target"],
        do: write(destination, "item", captured["target"], captured["target_time"])

      assert captured["exit"] == 0

      assert :ok =
               HighLevel.run(
                 invocation(),
                 [local(source), local(destination)],
                 [nil, nil],
                 opts(root)
               )

      if captured["rows"] != [], do: assert_receive({:difference, "different: item"})
      refute_receive {:difference, _}

      if captured["source"],
        do: assert(File.read!(Path.join(source, "item")) == captured["source"])

      if captured["target"],
        do: assert(File.read!(Path.join(destination, "item")) == captured["target"])
    end
  end

  defp invocation(filters \\ []),
    do: %Invocation{
      frontend: :native,
      command: ["diff"],
      options: %{progress: false},
      filters: filters
    }

  defp local(path), do: %Target{kind: :local, path: path}

  defp opts(root) do
    owner = self()
    File.mkdir_p!(Path.join(root, "staging"))
    [tmp_dir: Path.join(root, "staging"), max_attempts: 1, emit: &send(owner, {:difference, &1})]
  end

  defp write(directory, name, bytes, time \\ @earlier) do
    path = Path.join(directory, name)
    File.write!(path, bytes)
    File.touch!(path, time)
  end

  test "diff reports both missing sides and size changes without writing files", %{tmp_dir: root} do
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    for path <- [source, destination], do: File.mkdir_p!(path)
    write(source, "source-only", "source")
    write(destination, "destination-only", "destination")
    write(source, "changed", "long")
    write(destination, "changed", "old")
    write(source, "same-size", "abc")
    write(destination, "same-size", "xyz")

    assert :ok =
             HighLevel.run(
               invocation(),
               [local(source), local(destination)],
               [nil, nil],
               opts(root)
             )

    assert_receive {:difference, "different: changed"}
    assert_receive {:difference, "different: destination-only"}
    assert_receive {:difference, "different: source-only"}
    refute_receive {:difference, _}
    assert File.read!(Path.join(destination, "changed")) == "old"
    assert File.read!(Path.join(destination, "destination-only")) == "destination"
    refute File.exists?(Path.join(source, "destination-only"))
    refute File.exists?(Path.join(destination, "source-only"))
  end

  for {label, remote_time, expected} <- [
        {"newer", @earlier + 60, true},
        {"older", @earlier - 60, false},
        {"missing", nil, false}
      ] do
    test "diff compares a #{label} remote source time in the source-to-target direction", %{
      tmp_dir: root
    } do
      destination = Path.join(root, "destination")
      File.mkdir_p!(destination)
      write(destination, "item", "old")

      stamp =
        if unquote(remote_time),
          do:
            "<LastModified>#{DateTime.from_unix!(unquote(remote_time)) |> DateTime.to_iso8601()}</LastModified>",
          else: ""

      body =
        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>p/item</Key><Size>3</Size>#{stamp}<ETag>&quot;remote&quot;</ETag></Contents></ListBucketResult>"

      {endpoint, _} = WireServer.fixture([{200, [], body}])
      source = %Target{kind: :remote, bucket: "bucket", key: "p/"}

      assert :ok =
               HighLevel.run(
                 invocation(),
                 [source, local(destination)],
                 [%Context{endpoint: endpoint, unsigned: true}, nil],
                 opts(root)
               )

      if unquote(expected),
        do: assert_receive({:difference, "different: item"}),
        else: refute_receive({:difference, _})

      assert File.read!(Path.join(destination, "item")) == "old"
      assert_receive {:wire_request, %{method: "GET", target: request}}
      assert URI.parse(request).path == "/bucket"
      refute_receive {:wire_request, _}
    end
  end

  test "legacy size-only and exact-timestamps extensions still change comparisons", %{
    tmp_dir: root
  } do
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    for path <- [source, destination], do: File.mkdir_p!(path)
    write(source, "item", "abc", @earlier - 60)
    write(destination, "item", "xyz")

    assert :ok =
             HighLevel.run(
               %{invocation() | options: %{size_only: true}},
               [local(source), local(destination)],
               [nil, nil],
               opts(root)
             )

    refute_receive {:difference, _}

    assert :ok =
             HighLevel.run(
               %{invocation() | options: %{exact_timestamps: true}},
               [local(source), local(destination)],
               [nil, nil],
               opts(root)
             )

    assert_receive {:difference, "different: item"}
    assert File.read!(Path.join(destination, "item")) == "xyz"
  end

  test "exclusions protect destination-only names in the read-only comparison", %{tmp_dir: root} do
    source = Path.join(root, "source")
    destination = Path.join(root, "destination")
    for path <- [source, destination], do: File.mkdir_p!(path)
    write(destination, "visible", "show")
    write(destination, "hidden.txt", "hide")

    assert :ok =
             HighLevel.run(
               invocation(exclude: "*.txt"),
               [local(source), local(destination)],
               [nil, nil],
               opts(root)
             )

    assert_receive {:difference, "different: visible"}
    refute_receive {:difference, _}
    assert File.read!(Path.join(destination, "hidden.txt")) == "hide"
  end
end
