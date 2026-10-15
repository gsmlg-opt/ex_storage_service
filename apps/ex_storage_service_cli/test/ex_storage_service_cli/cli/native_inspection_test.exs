Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeInspectionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, NativeInspection, Parser, Runner}
  alias ExStorageServiceCli.Transfer.Coordinator
  @moduletag :tmp_dir
  @files [{"top.txt", 5}, {"a/one.txt", 7}, {"a/two.bin", 11}, {"a/b/deep.txt", 13}]

  test "native parser exposes selection flags and head/version aliases" do
    for args <- [
          ["head", "-n", "2", "--vid", "v1", "s3://bucket/key"],
          ["head", "--n", "2", "--version-id", "v1", "s3://bucket/key"]
        ] do
      assert {:ok, %{options: %{lines: 2, version_id: "v1"}}} = Parser.parse(args)
    end

    for command <- ["cat", "stat"],
        do:
          assert(
            {:ok, %{options: %{version_id: "v1"}}} =
              Parser.parse([command, "--vid", "v1", "s3://bucket/key"])
          )

    assert {:ok, _} = Parser.parse(["cat", "--offset", "4", "s3://bucket/key"])
    assert {:ok, _} = Parser.parse(["tree", "--depth", "1", "--files", "s3://bucket/"])

    assert {:ok, _} =
             Parser.parse(["du", "--depth", "2", "--recursive", "--versions", "s3://bucket/"])

    assert {:error, _} = Parser.parse(["head", "--n", "2", "--lines", "3", "s3://bucket/key"])
  end

  test "offset and tail deliver only pinned raw ranges", %{tmp_dir: root} do
    for {options, start, body} <- [
          {%{offset: 4}, 4, "456789abcdef"},
          {%{tail: 4}, 12, "cdef"},
          {%{tail: 100}, 0, "0123456789abcdef"}
        ] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [{"Content-Length", "16"}, {"ETag", "\"e\""}], ""},
          {206, [{"Content-Range", "bytes #{start}-15/16"}, {"ETag", "\"e\""}], body}
        ])

      parent = self()

      assert :ok =
               run("cat", target("key"), %{endpoint: endpoint}, options,
                 tmp_dir: root,
                 write: fn bytes ->
                   send(parent, {:raw, bytes})
                   :ok
                 end
               )

      assert_receive {:raw, ^body}
      assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/key"}}
      assert_receive {:wire_request, %{method: "GET", headers: headers}}
      assert headers["range"] == "bytes=#{start}-"
      assert headers["if-match"] == "\"e\""
    end

    assert File.ls!(root) == []
  end

  test "part reads confirm range count length and immutable identity before raw output" do
    fields = [
      {"Content-Length", "8"},
      {"Content-Range", "bytes 8-15/16"},
      {"x-amz-mp-parts-count", "2"},
      {"ETag", "\"e\""},
      {"x-amz-version-id", "v+雪"},
      {"x-amz-checksum-sha256", Base.encode64(:crypto.hash(:sha256, "whole object"))},
      {"x-amz-checksum-type", "COMPOSITE"}
    ]

    {endpoint, _} = WireServer.fixture([{206, fields, ""}, {206, fields, "89abcdef"}])
    parent = self()

    assert :ok =
             run(
               "cat",
               target("key"),
               %{endpoint: endpoint},
               %{part_number: 2, version_id: "v+雪"},
               write: fn bytes ->
                 send(parent, {:raw, bytes})
                 :ok
               end
             )

    assert_receive {:raw, "89abcdef"}
    assert_receive {:wire_request, %{method: "HEAD", target: head}}
    assert_receive {:wire_request, %{method: "GET", target: get}}
    assert head =~ "partNumber=2"
    assert get =~ "partNumber=2"
    assert get =~ "versionId=v%2B%E9%9B%AA"
  end

  test "ignored or changed part selection fails before writing any bytes" do
    for {status, content_range} <- [{200, "bytes 8-15/16"}, {206, "bytes 0-7/16"}] do
      head = [
        {"Content-Length", "8"},
        {"Content-Range", "bytes 8-15/16"},
        {"x-amz-mp-parts-count", "2"},
        {"ETag", "\"e\""}
      ]

      response =
        Keyword.new() ++
          [{"Content-Range", content_range}, {"x-amz-mp-parts-count", "2"}, {"ETag", "\"e\""}]

      {endpoint, _} = WireServer.fixture([{206, head, ""}, {status, response, "89abcdef"}])
      parent = self()

      assert {:error, %Error{code: :part_mismatch}} =
               run("cat", target("key"), %{endpoint: endpoint}, %{part_number: 2},
                 write: fn bytes ->
                   send(parent, {:raw, bytes})
                   :ok
                 end
               )

      refute_receive {:raw, _}
    end
  end

  test "ignored ranges and changed identities fail before raw output" do
    for {status, tag} <- [{200, "\"e\""}, {206, "\"changed\""}] do
      {endpoint, _} =
        WireServer.fixture([
          {200, [{"Content-Length", "16"}, {"ETag", "\"e\""}], ""},
          {status, [{"Content-Range", "bytes 4-15/16"}, {"ETag", tag}], "456789abcdef"}
        ])

      parent = self()

      assert {:error, %Error{}} =
               run("cat", target("key"), %{endpoint: endpoint}, %{offset: 4},
                 write: fn bytes ->
                   send(parent, {:raw, bytes})
                   :ok
                 end
               )

      refute_receive {:raw, _}
    end
  end

  test "invalid byte selections and unconfirmed part metadata refuse further I/O" do
    context = %Context{unsigned: true, endpoint: "http://unused.invalid"}

    for options <- [
          %{offset: -1},
          %{tail: -1},
          %{offset: 1, tail: 1},
          %{part_number: 0},
          %{part_number: 10_001},
          %{part_number: 1, tail: 1}
        ] do
      assert {:error, %Error{code: :cat_selection}} =
               NativeInspection.cat(target("key"), context, options, [])
    end

    assert {:error, %Error{code: :part_identity}} =
             NativeInspection.cat(target("key"), context, %{part_number: 2},
               call: fn "HeadObject", _ -> {:ok, %{"ETag" => "e", "ContentLength" => 8}} end
             )
  end

  test "head aliases retain bounded raw line output without added newline" do
    {endpoint, _} = WireServer.fixture([{200, [], "one\ntwo\nthree"}])
    assert {:ok, invocation} = Parser.parse(["head", "-n", "2", "s3://bucket/key"])
    parent = self()

    assert :ok =
             HighLevel.run(
               invocation,
               [target("key")],
               [%Context{unsigned: true, endpoint: endpoint}],
               write: fn bytes ->
                 send(parent, {:raw, bytes})
                 :ok
               end
             )

    assert_receive {:raw, "one\ntwo\n"}
  end

  test "tree projection obeys directory-only files and depth semantics", %{tmp_dir: root} do
    opts = inventory_opts(root)
    assert {:ok, dirs} = NativeInspection.tree(target(), %Context{}, %{depth: 1}, opts)
    assert Enum.map(dirs["Entries"], & &1["Path"]) == ["a/", "a/b/"]

    assert {:ok, files} =
             NativeInspection.tree(target(), %Context{}, %{depth: 1, files: true}, opts)

    assert Enum.map(files["Entries"], & &1["Path"]) == [
             "a/",
             "a/b/",
             "a/one.txt",
             "a/two.bin",
             "top.txt"
           ]

    refute files["Partial"]
    assert {:ok, all} = NativeInspection.tree(target(), %Context{}, %{files: true}, opts)
    assert Enum.any?(all["Entries"], &(&1["Path"] == "a/b/deep.txt"))
    assert File.ls!(root) == []
  end

  test "real Runner routes tree projection flags through bounded inspection", %{tmp_dir: root} do
    body =
      "<ListBucketResult><IsTruncated>false</IsTruncated>" <>
        Enum.map_join(@files, fn {key, size} ->
          "<Contents><Key>#{key}</Key><Size>#{size}</Size></Contents>"
        end) <>
        "</ListBucketResult>"

    {endpoint, _} = WireServer.fixture([{200, [], body}])

    output =
      ExUnit.CaptureIO.capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "tree",
                   "--depth",
                   "1",
                   "--files",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "--config-dir",
                   root,
                   "--output",
                   "json",
                   "s3://bucket/"
                 ])
      end)

    data = JSON.decode!(output)
    assert data["Returned"] == 5
    assert Enum.any?(data["Entries"], &(&1["Path"] == "a/b/"))
    refute Enum.any?(data["Entries"], &(&1["Path"] == "a/b/deep.txt"))
    assert_receive {:wire_request, %{method: "GET", target: path}}
    assert path =~ "list-type=2"
  end

  test "real bucket stat wire preserves delimiter prefixes and metadata HEADs", %{tmp_dir: root} do
    body =
      "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>top.txt</Key><Size>5</Size></Contents><CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes></ListBucketResult>"

    {endpoint, _} =
      WireServer.fixture([{200, [], body}, {200, [{"Content-Length", "5"}, {"ETag", "e"}], ""}])

    assert {:ok, data} = run("stat", target(), %{endpoint: endpoint}, %{}, tmp_dir: root)
    assert data["Returned"] == 2
    assert_receive {:wire_request, %{method: "GET", target: path}}
    assert path =~ "delimiter=%2F"
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/top.txt"}}
    assert File.ls!(root) == []
  end

  test "tree limits cap deduplicated nodes and still report exhaustive matched prefixes", %{
    tmp_dir: root
  } do
    assert {:ok, data} =
             NativeInspection.tree(
               target(),
               %Context{},
               %{files: true, limit: 1},
               inventory_opts(root)
             )

    assert data["Returned"] == 1
    assert data["MatchedPrefixes"] == 6
    assert data["Partial"]

    assert {:error, %Error{code: :tree_depth}} =
             NativeInspection.tree(target(), %Context{}, %{depth: 0}, [])

    assert File.ls!(root) == []
  end

  test "recursive summaries retain complete aggregate bytes when prefix output is capped", %{
    tmp_dir: root
  } do
    assert {:ok, data} =
             NativeInspection.du(target(), %Context{}, %{recursive: true}, inventory_opts(root))

    assert {data["Objects"], data["Bytes"]} == {4, 36}

    assert data["Entries"] == [
             %{"Prefix" => "", "Objects" => 4, "Bytes" => 36},
             %{"Prefix" => "a", "Objects" => 3, "Bytes" => 31},
             %{"Prefix" => "a/b", "Objects" => 1, "Bytes" => 13}
           ]

    assert {:ok, capped} =
             NativeInspection.du(
               target(),
               %Context{},
               %{recursive: true, limit: 1},
               inventory_opts(root)
             )

    assert capped["Bytes"] == 36
    assert capped["Partial"]
    assert capped["Returned"] == 1
    assert File.ls!(root) == []
  end

  test "summary depth is root-inclusive and versions retain immutable data rows", %{tmp_dir: root} do
    assert {:ok, data} =
             NativeInspection.du(
               target(),
               %Context{},
               %{recursive: true, depth: 1},
               inventory_opts(root)
             )

    assert length(data["Entries"]) == 1

    assert {:ok, versions} =
             NativeInspection.du(target(), %Context{}, %{versions: true}, inventory_opts(root))

    assert versions["Objects"] == 8
    assert versions["Bytes"] == 72
    assert versions["Versions"]
    assert File.ls!(root) == []
  end

  test "bucket and prefix stat list immediate files and folders then HEAD only files", %{
    tmp_dir: root
  } do
    parent = self()

    fetch = fn
      "ListObjectsV2", input ->
        send(parent, {:list, input})

        {:ok,
         %{
           "IsTruncated" => false,
           "Contents" => [record("a/one.txt", 7)],
           "CommonPrefixes" => [%{"Prefix" => "a/b/"}]
         }}

      "HeadObject", input ->
        send(parent, {:head, input})
        {:ok, %{"ETag" => "e", "ContentLength" => 7}}
    end

    assert {:ok, data} =
             NativeInspection.stat(target("a/"), %Context{}, %{}, tmp_dir: root, call: fetch)

    assert_receive {:list, %{"Prefix" => "a/", "Delimiter" => "/"}}
    assert_receive {:head, %{"Key" => "a/one.txt"}}
    refute_receive {:head, %{"Key" => "a/b/"}}
    assert Enum.any?(data["Entries"], &(&1["type"] == "folder"))
    assert data["MatchedObjects"] == 2
    assert File.ls!(root) == []
  end

  test "recursive stat caps HEAD requests while preserving full inventory count", %{tmp_dir: root} do
    parent = self()

    fetch = fn
      "ListObjectsV2", input ->
        assert input["Delimiter"] == nil
        {:ok, listing()}

      "HeadObject", input ->
        send(parent, {:head, input})
        {:ok, %{"ContentLength" => 1}}
    end

    assert {:ok, data} =
             NativeInspection.stat(target(), %Context{}, %{recursive: true, limit: 1},
               tmp_dir: root,
               call: fetch
             )

    assert data["Partial"]
    assert data["MatchedObjects"] == 4
    assert_receive {:head, _}
    refute_receive {:head, _}
    assert File.ls!(root) == []
  end

  test "version stat HEADs exact immutable versions and retains delete markers", %{tmp_dir: root} do
    parent = self()

    fetch = fn
      "ListObjectVersions", input ->
        assert input["Prefix"] == "top.txt"

        {:ok,
         %{
           "IsTruncated" => false,
           "Versions" => [
             Map.put(record("top.txt", 5), "VersionId", "v1"),
             Map.put(record("top.txt.other", 9), "VersionId", "v2")
           ],
           "DeleteMarkers" => [%{"Key" => "top.txt", "VersionId" => "deleted"}]
         }}

      "HeadObject", input ->
        send(parent, {:head, input})
        {:ok, %{"VersionId" => input["VersionId"], "ContentLength" => 5}}
    end

    assert {:ok, data} =
             NativeInspection.stat(target("top.txt"), %Context{}, %{versions: true},
               tmp_dir: root,
               call: fetch
             )

    assert data["Returned"] == 2
    assert_receive {:head, %{"VersionId" => "v1"}}
    refute_receive {:head, _}
    assert Enum.any?(data["Entries"], &(&1["type"] == "delete-marker"))
    assert File.ls!(root) == []
  end

  test "later inventory failures prevent stat HEADs and clean private manifests", %{tmp_dir: root} do
    parent = self()
    error = Error.new(:service, "Unavailable", code: "AccessDenied")

    fetch = fn
      "ListObjectsV2", %{"ContinuationToken" => _} ->
        {:error, error}

      "ListObjectsV2", _ ->
        {:ok, Map.merge(listing(), %{"IsTruncated" => true, "NextContinuationToken" => "next"})}

      "HeadObject", input ->
        send(parent, {:head, input})
        {:ok, %{}}
    end

    assert {:error, ^error} =
             NativeInspection.stat(target(), %Context{}, %{recursive: true},
               tmp_dir: root,
               call: fetch
             )

    refute_receive {:head, _}
    assert File.ls!(root) == []
  end

  test "explicit selected versions reject ignored responses and conflicting inventory modes" do
    assert {:error, %Error{code: :source_version_mismatch}} =
             NativeInspection.stat(target("key"), %Context{}, %{version_id: "selected"},
               input: %{"VersionId" => "selected"},
               call: fn "HeadObject", _ -> {:ok, %{"VersionId" => "current"}} end
             )

    for options <- [%{version_id: "v", versions: true}, %{version_id: "v", recursive: true}] do
      assert {:error, %Error{code: :version_selection}} =
               NativeInspection.stat(target("key"), %Context{}, options, [])
    end
  end

  test "coordinator cancellation stops inspection before any provider request", %{tmp_dir: root} do
    coordinator = start_supervised!({Coordinator, []})
    Coordinator.cancel(coordinator)

    assert {:error, %Error{category: :interrupted}} =
             NativeInspection.tree(target(), %Context{coordinator: coordinator}, %{files: true},
               tmp_dir: root,
               call: fn _, _ -> flunk("Cancelled inspection issued a request") end
             )

    assert File.ls!(root) == []
  end

  test "explicit depth implies descendant summaries without recursive flag", %{tmp_dir: root} do
    assert {:ok, data} =
             NativeInspection.du(target(), %Context{}, %{depth: 2}, inventory_opts(root))

    assert Enum.map(data["Entries"], & &1["Prefix"]) == ["", "a"]
  end

  test "version summary data count excludes tombstones", %{tmp_dir: root} do
    assert {:ok, data} =
             NativeInspection.du(target(), %Context{}, %{versions: true},
               tmp_dir: root,
               call: history_fetch()
             )

    assert {data["Objects"], data["Bytes"]} == {5, 28}
    assert hd(data["Entries"])["Objects"] == 5
    assert File.ls!(root) == []
  end

  test "all inspection rewind flags parse while Find does not expose rewind" do
    for command <- ~w(cat head ls stat du tree) do
      assert {:ok, %{options: %{rewind: "2026-10-15"}}} =
               Parser.parse([command, "--rewind", "2026-10-15", "s3://bucket/"])
    end

    assert {:error, _} = Parser.parse(["find", "--rewind", "2026-10-15", "s3://bucket/"])
  end

  test "historical snapshot retains tombstones while all versions retain older data", %{
    tmp_dir: root
  } do
    opts = [tmp_dir: root, call: history_fetch()]

    assert {:ok, snapshot} =
             NativeInspection.ls(target(), %Context{}, %{rewind: "2026-10-15T00:00:00Z"}, opts)

    assert snapshot["MatchedObjects"] == 3
    assert Enum.find(snapshot["Entries"], &(&1["key"] == "top.txt"))["version_id"] == "older"
    assert Enum.find(snapshot["Entries"], &(&1["key"] == "gone"))["type"] == "delete-marker"

    assert {:ok, all} =
             NativeInspection.ls(
               target(),
               %Context{},
               %{rewind: "2026-10-15T00:00:00Z", versions: true},
               opts
             )

    assert all["MatchedObjects"] == 4

    assert {:ok, data} =
             NativeInspection.du(target(), %Context{}, %{rewind: "2026-10-15T00:00:00Z"}, opts)

    assert {data["Objects"], data["Bytes"]} == {3, 18}

    assert {:ok, tree} =
             NativeInspection.tree(
               target(),
               %Context{},
               %{rewind: "2026-10-15T00:00:00Z", files: true},
               opts
             )

    assert Enum.map(tree["Entries"], & &1["Path"]) == ["a/", "a/leaf", "top.txt"]
    assert File.ls!(root) == []
  end

  test "malformed historical metadata fails the complete inventory and cleans private files", %{
    tmp_dir: root
  } do
    for field <- ["LastModified", "ETag", "Size"] do
      fetch = fn "ListObjectVersions", _ ->
        value =
          Map.put(record("key", 5), "VersionId", "v1")
          |> Map.put(field, if(field == "Size", do: -1, else: nil))

        {:ok, %{"IsTruncated" => false, "Versions" => [value]}}
      end

      assert {:error, %Error{code: :rewind_inventory}} =
               NativeInspection.du(target(), %Context{}, %{rewind: "2026-10-15"},
                 tmp_dir: root,
                 call: fetch
               )

      assert File.ls!(root) == []
    end
  end

  test "retained pinned mc oracle records strict equality and version summary semantics" do
    oracle =
      Path.expand("../../fixtures/differential/native-rewind-wire.json", __DIR__)
      |> File.read!()
      |> JSON.decode!()

    assert oracle["mc_commit"] == "77f82e18b5401a65958f1619df6ebb994634bd88"

    assert oracle["mc_sha256"] ==
             "a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2"

    assert [cat, equality, head, ls, versions, _stat, du, du_versions, _tree] = oracle["cases"]
    assert cat["exit"] == 0 and cat["stdout"] == "older"
    assert Enum.any?(cat["requests"], &(&1 == ["GET", "/bucket/top.txt?versionId=older"]))
    assert equality["exit"] == 1
    refute Enum.any?(equality["requests"], fn [_, path] -> path =~ "versionId=" end)
    assert head["stdout"] == "older\n"
    assert length(String.split(ls["stdout"], "\n", trim: true)) == 3
    assert length(String.split(versions["stdout"], "\n", trim: true)) == 4

    for result <- [du, du_versions] do
      assert %{"size" => 18, "objects" => 3} = JSON.decode!(result["stdout"])
    end
  end

  test "real Runner selects rewind listing before the legacy path", %{tmp_dir: root} do
    versions =
      "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>top.txt</Key><VersionId>older</VersionId><Size>5</Size><ETag>e</ETag><LastModified>2026-10-14T00:00:00Z</LastModified></Version></ListVersionsResult>"

    {endpoint, _} = WireServer.fixture([{200, [], versions}])

    output =
      ExUnit.CaptureIO.capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "ls",
                   "--rewind",
                   "2026-10-15",
                   "--service",
                   "s3",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "--config-dir",
                   root,
                   "--output",
                   "json",
                   "s3://bucket/"
                 ])
      end)

    assert %{"Entries" => [%{"version_id" => "older"}], "Returned" => 1} = JSON.decode!(output)
    assert_receive {:wire_request, %{target: path}}
    assert path =~ "versions="
  end

  test "historical exact equality and deleted objects fail before metadata or content reads", %{
    tmp_dir: root
  } do
    fetch = history_fetch()

    for {key, time} <- [{"top.txt", "2026-10-14T00:00:00Z"}, {"gone", "2026-10-15T00:00:00Z"}] do
      assert {:error, %Error{code: :rewind_not_found}} =
               NativeInspection.source(target(key), %Context{}, %{rewind: time},
                 tmp_dir: root,
                 call: fetch
               )
    end

    assert {:error, %Error{code: :version_selection}} =
             NativeInspection.source(
               target("top.txt"),
               %Context{},
               %{rewind: "1d", version_id: "v1"},
               []
             )

    assert File.ls!(root) == []
  end

  test "historical listed identity drift fails preflight", %{tmp_dir: root} do
    listing = history_fetch()

    for changed <- [
          %{"ETag" => "changed"},
          %{"ContentLength" => 9},
          %{"LastModified" => ~U[2026-10-14 00:00:01Z]},
          %{"VersionId" => "other"}
        ] do
      fetch = fn
        "HeadObject", _ ->
          {:ok,
           Map.merge(
             %{
               "ETag" => "e",
               "ContentLength" => 5,
               "LastModified" => ~U[2026-10-14 00:00:00Z],
               "VersionId" => "older"
             },
             changed
           )}

        operation, input ->
          listing.(operation, input)
      end

      assert {:error, %Error{code: :source_version_mismatch}} =
               NativeInspection.source(target("top.txt"), %Context{}, %{rewind: "2026-10-15"},
                 tmp_dir: root,
                 call: fetch
               )
    end
  end

  test "historical cat and head use version and If-Match on actual wire", %{tmp_dir: root} do
    versions =
      "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>top.txt</Key><VersionId>null</VersionId><Size>5</Size><ETag>e</ETag><LastModified>2026-10-14T00:00:00Z</LastModified></Version></ListVersionsResult>"

    metadata = [
      {"Content-Length", "5"},
      {"ETag", "e"},
      {"x-amz-version-id", "null"},
      {"Last-Modified", "Wed, 14 Oct 2026 00:00:00 GMT"}
    ]

    parent = self()

    for command <- ["cat", "head"] do
      {endpoint, _} =
        WireServer.fixture([{200, [], versions}, {200, metadata, ""}, {200, metadata, "hello"}])

      assert :ok =
               run(command, target("top.txt"), %{endpoint: endpoint}, %{rewind: "2026-10-15"},
                 tmp_dir: root,
                 write: fn bytes ->
                   send(parent, {:historical_raw, bytes})
                   :ok
                 end
               )

      assert_receive {:historical_raw, "hello"}
      assert_receive {:wire_request, %{method: "GET", target: list}}
      assert list =~ "versions="
      assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/top.txt?versionId=null"}}

      assert_receive {:wire_request,
                      %{method: "GET", target: "/bucket/top.txt?versionId=null", headers: headers}}

      assert headers["if-match"] == "e"
    end

    assert File.ls!(root) == []
  end

  test "historical full GET confirms identity before the raw sink", %{tmp_dir: root} do
    versions =
      "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>top.txt</Key><VersionId>older</VersionId><Size>5</Size><ETag>e</ETag><LastModified>2026-10-14T00:00:00Z</LastModified></Version></ListVersionsResult>"

    metadata = [
      {"Content-Length", "5"},
      {"ETag", "e"},
      {"x-amz-version-id", "older"},
      {"Last-Modified", "Wed, 14 Oct 2026 00:00:00 GMT"}
    ]

    {endpoint, _} =
      WireServer.fixture([
        {200, [], versions},
        {200, metadata, ""},
        {200, [{"ETag", "changed"}, {"x-amz-version-id", "older"}], "hello"}
      ])

    parent = self()

    assert {:error, %Error{code: :source_version_mismatch}} =
             run("cat", target("top.txt"), %{endpoint: endpoint}, %{rewind: "2026-10-15"},
               tmp_dir: root,
               write: fn bytes ->
                 send(parent, {:unexpected_raw, bytes})
                 :ok
               end
             )

    refute_receive {:unexpected_raw, _}
  end

  test "historical recursive stat pins selected versions and verifies listed identity", %{
    tmp_dir: root
  } do
    listing = history_fetch()
    parent = self()

    fetch = fn
      "HeadObject", input ->
        send(parent, {:historical_head, input})

        {:ok,
         %{
           "ETag" => "e",
           "ContentLength" => 5,
           "LastModified" => ~U[2026-10-14 00:00:00Z],
           "VersionId" => input["VersionId"]
         }}

      operation, input ->
        listing.(operation, input)
    end

    assert {:ok, data} =
             NativeInspection.stat(target(), %Context{}, %{rewind: "2026-10-15", recursive: true},
               tmp_dir: root,
               call: fetch
             )

    assert data["Returned"] == 3
    assert_receive {:historical_head, %{"Key" => "a/leaf", "VersionId" => "a-old"}}
    assert_receive {:historical_head, %{"Key" => "top.txt", "VersionId" => "older"}}
    refute_receive {:historical_head, _}
    assert File.ls!(root) == []
  end

  defp history_fetch do
    fn "ListObjectVersions", input ->
      data = [
        {"top.txt", "future", 5, ~U[2026-10-16 00:00:00Z]},
        {"top.txt", "equal", 5, ~U[2026-10-15 00:00:00Z]},
        {"top.txt", "older", 5, ~U[2026-10-14 00:00:00Z]},
        {"a/leaf", "a-old", 5, ~U[2026-10-14 00:00:00Z]},
        {"gone", "gone-old", 8, ~U[2026-10-13 00:00:00Z]}
      ]

      versions =
        for {key, version, size, time} <- data,
            String.starts_with?(key, input["Prefix"] || ""),
            do: %{
              "Key" => key,
              "VersionId" => version,
              "Size" => size,
              "LastModified" => time,
              "ETag" => "e"
            }

      markers =
        if String.starts_with?("gone", input["Prefix"] || ""),
          do: [
            %{
              "Key" => "gone",
              "VersionId" => "deleted",
              "LastModified" => ~U[2026-10-14 12:00:00Z]
            }
          ],
          else: []

      {:ok, %{"IsTruncated" => false, "Versions" => versions, "DeleteMarkers" => markers}}
    end
  end

  defp target(key \\ ""),
    do: %Target{
      kind: :remote,
      bucket: "bucket",
      key: key,
      trailing_slash: String.ends_with?(key, "/")
    }

  defp run(command, target, fields, options, opts),
    do:
      HighLevel.run(
        %Invocation{frontend: :native, command: [command], options: options},
        [target],
        [struct(Context, Map.merge(%{unsigned: true}, fields))],
        opts
      )

  defp record(key, size),
    do: %{"Key" => key, "Size" => size, "LastModified" => ~U[2026-10-14 00:00:00Z], "ETag" => "e"}

  defp listing,
    do: %{
      "IsTruncated" => false,
      "Contents" => Enum.map(@files, fn {key, size} -> record(key, size) end)
    }

  defp inventory_opts(root),
    do: [
      tmp_dir: root,
      call: fn
        "ListObjectsV2", _ ->
          {:ok, listing()}

        "ListObjectVersions", _ ->
          {:ok,
           %{
             "IsTruncated" => false,
             "Versions" =>
               for(
                 {key, size} <- @files,
                 version <- ["v1", "v2"],
                 do: Map.put(record(key, size), "VersionId", version)
               )
           }}
      end
    ]
end
