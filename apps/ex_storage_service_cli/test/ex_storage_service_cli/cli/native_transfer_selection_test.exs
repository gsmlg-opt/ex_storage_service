Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeTransferSelectionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, NativeTransferSelection, Parser}
  @moduletag :tmp_dir
  @now ~U[2026-01-03 00:00:00Z]
  @seconds DateTime.to_unix(@now)
  @nanos @seconds * 1_000_000_000
  defp remote(bucket, key), do: %Target{kind: :remote, bucket: bucket, key: key}
  defp local(path), do: %Target{kind: :local, path: path}

  defp invocation(command, options),
    do: %Invocation{
      frontend: :native,
      command: [command],
      options: Map.put(options, :progress, false)
    }

  defp opts(root), do: [now: @now, tmp_dir: root, max_attempts: 1, emit: fn _ -> :ok end]
  defp entry(seconds), do: %{"modified" => seconds * 1_000_000, "storage_class" => "STANDARD"}

  defp listing(records) do
    items =
      Enum.map_join(records, "", fn {key, modified, storage} ->
        date = if modified, do: "<LastModified>#{modified}</LastModified>", else: ""

        "<Contents><Key>#{key}</Key><Size>3</Size><ETag>&quot;e&quot;</ETag>#{date}<StorageClass>#{storage}</StorageClass></Contents>"
      end)

    "<ListBucketResult><IsTruncated>false</IsTruncated>#{items}</ListBucketResult>"
  end

  test "only native cp mv mirror accept age options and mirror accepts repeated exact storage classes" do
    for command <- ["cp", "mv", "mirror"] do
      assert {:ok, %{options: %{older_than: "1d", newer_than: "2d"}}} =
               Parser.parse([
                 command,
                 "source",
                 "dest",
                 "--older-than",
                 "1d",
                 "--newer-than",
                 "2d"
               ])
    end

    assert {:ok, %{options: %{exclude_storageclass: ["GLACIER", "DEEP_ARCHIVE"]}}} =
             Parser.parse([
               "mirror",
               "source",
               "dest",
               "--exclude-storageclass",
               "GLACIER",
               "--exclude-storageclass",
               "DEEP_ARCHIVE"
             ])

    assert {:ok, %{options: %{exclude_storageclass: ["GLACIER", "DEEP_ARCHIVE"]}}} =
             Parser.parse([
               "mirror",
               "source",
               "dest",
               "--exclude-storageclass=GLACIER",
               "--exclude-storageclass=DEEP_ARCHIVE"
             ])

    for command <- ["cp", "mv", "get", "put", "sync"] do
      assert {:error, %Error{}} =
               Parser.parse([command, "source", "dest", "--exclude-storageclass", "GLACIER"])
    end

    for command <- ["cp", "mv", "sync"] do
      assert {:error, %Error{}} =
               Parser.parse(["s3", command, "a", "s3://bucket/key", "--older-than", "1d"])
    end
  end

  test "pinned mc relative units signs compounds and inclusive older strict newer boundaries" do
    for {text, duration} <- [
          {"1ns", 1},
          {"1us", 1000},
          {"1µs", 1000},
          {"1μs", 1000},
          {"1ms", 1_000_000},
          {"1s", 1_000_000_000},
          {"1m", 60_000_000_000},
          {"1h", 3_600_000_000_000},
          {"1d", 86_400_000_000_000},
          {"1w", 604_800_000_000_000},
          {"1y", 31_536_000_000_000_000},
          {"+1d.5h", 88_200_000_000_000},
          {"-1s", -1_000_000_000},
          {"0", 0}
        ] do
      assert {:ok, %{older: %{cutoff: cutoff, absolute: false}}} =
               NativeTransferSelection.compile(invocation("cp", %{older_than: text}), now: @now)

      assert cutoff == @nanos - duration
    end

    assert {:ok, older} =
             NativeTransferSelection.compile(invocation("cp", %{older_than: "1d"}), now: @now)

    assert {:ok, newer} =
             NativeTransferSelection.compile(invocation("cp", %{newer_than: "1d"}), now: @now)

    assert {:ok, true} = NativeTransferSelection.include?(older, entry(@seconds - 86_400))
    assert {:ok, false} = NativeTransferSelection.include?(newer, entry(@seconds - 86_400))
    assert {:ok, false} = NativeTransferSelection.include?(older, entry(@seconds - 86_399))
    assert {:ok, true} = NativeTransferSelection.include?(newer, entry(@seconds - 86_399))
  end

  test "absolute cutoffs match the pinned executable equality observations" do
    for cutoff <- [
          "2026.01.02",
          "2026.01.02T00:00",
          "2026.01.02T00:00:00",
          "2026-01-02T00:00:00Z",
          "2026-01-02 00:00:00 UTC"
        ] do
      assert {:ok, older} =
               NativeTransferSelection.compile(invocation("cp", %{older_than: cutoff}), now: @now)

      assert {:ok, newer} =
               NativeTransferSelection.compile(invocation("cp", %{newer_than: cutoff}), now: @now)

      assert {:ok, false} = NativeTransferSelection.include?(older, entry(@seconds - 86_400))
      assert {:ok, true} = NativeTransferSelection.include?(newer, entry(@seconds - 86_400))
    end
  end

  test "single local cp applies relative and absolute selection to actual uploads", %{
    tmp_dir: root
  } do
    path = Path.join(root, "source")
    File.write!(path, "raw")
    File.touch!(path, @seconds - 86_400)

    {endpoint, _} =
      WireServer.fixture([{200, [{"ETag", "\"e\""}], ""}, {200, [{"ETag", "\"e\""}], ""}])

    context = %Context{endpoint: endpoint, unsigned: true}

    for options <- [
          %{older_than: "1d"},
          %{newer_than: "1d"},
          %{older_than: "2026-01-02T00:00:00Z"},
          %{newer_than: "2026-01-02T00:00:00Z"}
        ] do
      assert :ok =
               HighLevel.run(
                 invocation("cp", options),
                 [local(path), remote("bucket", "key")],
                 [nil, context],
                 opts(root)
               )
    end

    assert_receive {:wire_request, %{method: "PUT", body: "raw"}}
    assert_receive {:wire_request, %{method: "PUT", body: "raw"}}
    refute_receive {:wire_request, _}
  end

  test "single remote age selection inspects HEAD even in dryrun without downloading", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200,
         [
           {"ETag", "\"e\""},
           {"Content-Length", "3"},
           {"Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT"}
         ], ""}
      ])

    owner = self()

    assert :ok =
             HighLevel.run(
               invocation("cp", %{older_than: "1d", dryrun: true}),
               [remote("bucket", "key"), local(Path.join(root, "destination"))],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               Keyword.put(opts(root), :emit, &send(owner, {:line, &1}))
             )

    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/key"}}
    assert_receive {:line, line}
    assert line =~ "(dryrun) download"
    refute_receive {:wire_request, _}
    refute File.exists?(Path.join(root, "destination"))
  end

  test "storage classes are exact repeated values and local files have no class" do
    assert {:ok, selection} =
             NativeTransferSelection.compile(
               invocation("mirror", %{exclude_storageclass: ["GLACIER", "STANDARD"]}),
               now: @now
             )

    assert {:ok, false} =
             NativeTransferSelection.include?(selection, %{
               "key" => "k",
               "storage_class" => "GLACIER"
             })

    assert {:ok, false} = NativeTransferSelection.include?(selection, %{"key" => "k"})

    assert {:ok, true} =
             NativeTransferSelection.include?(selection, %{
               "key" => "k",
               "storage_class" => "glacier"
             })

    assert {:ok, true} = NativeTransferSelection.include?(selection, %{"path" => "local"})
  end

  test "stdio age selection fails before consuming stdin", %{tmp_dir: root} do
    owner = self()

    assert {:error, %Error{code: :native_transfer_timestamp}} =
             HighLevel.run(
               invocation("cp", %{older_than: "1d"}),
               [%Target{kind: :stdio, path: "-"}, remote("bucket", "key")],
               [nil, %Context{}],
               Keyword.put(opts(root), :read, fn _ ->
                 send(owner, :stdin_read)
                 :eof
               end)
             )

    refute_receive :stdin_read
    assert File.ls!(root) == []
  end

  test "recursive move leaves excluded source intact while retaining copy then delete fences", %{
    tmp_dir: root
  } do
    source = Path.join(root, "source")
    File.mkdir!(source)

    for {name, stamp} <- [{"old", @seconds - 172_800}, {"new", @seconds - 3600}] do
      File.write!(Path.join(source, name), "raw")
      File.touch!(Path.join(source, name), stamp)
    end

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"e\""}], ""},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "3"}], ""},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "3"}], ""}
      ])

    assert :ok =
             HighLevel.run(
               invocation("mv", %{recursive: true, older_than: "1d"}),
               [local(source), remote("bucket", "dest/")],
               [nil, %Context{endpoint: endpoint, unsigned: true}],
               opts(root)
             )

    refute File.exists?(Path.join(source, "old"))
    assert File.read!(Path.join(source, "new")) == "raw"
    assert_receive {:wire_request, %{method: "PUT", target: "/bucket/dest/old"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
  end

  test "mirror age class and name exclusions protect matching and destination-only local files",
       %{tmp_dir: root} do
    destination = Path.join(root, "destination")
    File.mkdir!(destination)

    for {name, stamp} <- [
          {"old", @seconds - 172_800},
          {"cold", @seconds - 3600},
          {"old-only", @seconds - 172_800},
          {"name-only", @seconds - 3600},
          {"remove", @seconds - 3600}
        ] do
      File.write!(Path.join(destination, name), "keep")
      File.touch!(Path.join(destination, name), stamp)
    end

    {endpoint, _} =
      WireServer.fixture([
        {200, [],
         listing([
           {"src/old", "2026-01-01T00:00:00Z", "STANDARD"},
           {"src/cold", "2026-01-02T23:00:00Z", "GLACIER"}
         ])},
        {404, [], ""}
      ])

    invocation = %{
      invocation("mirror", %{
        newer_than: "1d",
        exclude_storageclass: ["GLACIER"],
        remove: true,
        overwrite: true
      })
      | filters: [exclude: "name-only"]
    }

    assert :ok =
             HighLevel.run(
               invocation,
               [remote("bucket", "src/"), local(destination)],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               opts(root)
             )

    for name <- ["old", "cold", "old-only", "name-only"],
        do: assert(File.read!(Path.join(destination, name)) == "keep")

    refute File.exists?(Path.join(destination, "remove"))
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/src/remove"}}
    refute_receive {:wire_request, _}
  end

  test "mirror storage-class exclusions protect remote destination-only objects from deletion", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing([])},
        {200, [],
         listing([
           {"dest/cold", "2026-01-02T23:00:00Z", "GLACIER"},
           {"dest/remove", "2026-01-02T23:00:00Z", "STANDARD"}
         ])},
        {404, [], ""},
        {200, [{"ETag", "\"e\""}, {"Content-Length", "3"}], ""},
        {204, [], ""}
      ])

    context = %Context{
      endpoint: endpoint,
      unsigned: true,
      capabilities: %{conditional_delete: true}
    }

    assert :ok =
             HighLevel.run(
               invocation("mirror", %{
                 exclude_storageclass: ["GLACIER", "DEEP_ARCHIVE"],
                 remove: true
               }),
               [remote("bucket", "src/"), remote("bucket", "dest/")],
               [context, context],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/src/remove"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/dest/remove"}}

    assert_receive {:wire_request,
                    %{
                      method: "DELETE",
                      target: "/bucket/dest/remove",
                      headers: %{"if-match" => "\"e\""}
                    }}

    refute_receive {:wire_request, _}
  end

  test "recursive dryrun emits only selected entries and never uploads", %{tmp_dir: root} do
    source = Path.join(root, "source")
    File.mkdir!(source)

    for {name, stamp} <- [{"old", @seconds - 172_800}, {"new", @seconds - 3600}] do
      File.write!(Path.join(source, name), "raw")
      File.touch!(Path.join(source, name), stamp)
    end

    owner = self()

    assert :ok =
             HighLevel.run(
               invocation("cp", %{recursive: true, older_than: "1d", dryrun: true}),
               [local(source), remote("bucket", "dest/")],
               [nil, %Context{endpoint: "http://unused.invalid"}],
               Keyword.put(opts(root), :emit, &send(owner, {:line, &1}))
             )

    assert_receive {:line, line}
    assert line =~ "dest/old"
    refute_receive {:line, _}
    for name <- ["old", "new"], do: assert(File.exists?(Path.join(source, name)))
    refute_receive {:wire_request, _}
  end

  test "invalid age filters fail before local state or requests and missing timestamps prevent deletion",
       %{tmp_dir: root} do
    for value <- [
          "1M",
          "1mo",
          "1Y",
          "1 day",
          "yesterday",
          "999999999999y",
          String.duplicate("1", 257),
          <<255>>
        ] do
      assert {:error, %Error{code: :native_transfer_time}} =
               HighLevel.run(
                 invocation("cp", %{older_than: value}),
                 [local(Path.join(root, "missing")), remote("bucket", "key")],
                 [nil, %Context{}],
                 opts(root)
               )
    end

    assert File.ls!(root) == []

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing([{"src/unknown", nil, "STANDARD"}])},
        {200, [], listing([])}
      ])

    assert {:error, %Error{code: :native_transfer_timestamp}} =
             HighLevel.run(
               invocation("mirror", %{older_than: "1d", remove: true}),
               [remote("bucket", "src/"), remote("bucket", "dest/")],
               [
                 %Context{endpoint: endpoint, unsigned: true},
                 %Context{endpoint: endpoint, unsigned: true}
               ],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
    assert Path.wildcard(Path.join(root, ".ess-inventory-*")) == []
  end
end
