Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.MoveSafetyTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation}
  @moduletag :tmp_dir

  defp remote(key), do: %Target{kind: :remote, bucket: "bucket", key: key}
  defp local(path), do: %Target{kind: :local, path: path}

  defp invocation(command, options),
    do: %Invocation{
      frontend: :native,
      command: [command],
      options: Map.put(options, :progress, false)
    }

  defp opts(root), do: [max_attempts: 1, emit: fn _ -> :ok end, tmp_dir: root]

  defp head(version \\ "v1"),
    do: [{"ETag", "\"selected\""}, {"Content-Length", "3"}, {"x-amz-version-id", version}]

  defp listing do
    items =
      Enum.map_join(["a", "b"], "", fn name ->
        "<Contents><Key>src/#{name}</Key><Size>3</Size><ETag>&quot;selected&quot;</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>"
      end)

    "<ListBucketResult><IsTruncated>false</IsTruncated>#{items}</ListBucketResult>"
  end

  defp requests do
    receive do
      {:wire_request, request} -> [request | requests()]
    after
      0 -> []
    end
  end

  test "a GET that ignores selected version or ETag never replaces a destination or deletes source",
       %{tmp_dir: root} do
    for command <- ["get", "cp", "mv"],
        bad <- [
          [{"ETag", "\"selected\""}, {"x-amz-version-id", "wrong"}],
          [{"ETag", "\"wrong\""}, {"x-amz-version-id", "v1"}],
          [{"ETag", "\"selected\""}]
        ] do
      {endpoint, _} = WireServer.fixture([{200, head(), ""}, {200, bad, "bad"}])
      path = Path.join(root, command)
      File.write!(path, "keep")

      assert {:error, %Error{code: :source_version_mismatch}} =
               HighLevel.run(
                 invocation(command, %{version_id: "v1"}),
                 [remote("key"), local(path)],
                 [
                   %Context{
                     endpoint: endpoint,
                     unsigned: true,
                     capabilities: %{conditional_delete: true}
                   },
                   nil
                 ],
                 opts(root)
               )

      assert File.read!(path) == "keep"
      refute Enum.any?(requests(), &(&1.method == "DELETE"))
    end
  end

  test "selected stdout GET validates response before emitting any bytes", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, head(), ""}, {200, head("wrong"), "bad"}])
    owner = self()

    assert {:error, %Error{code: :source_version_mismatch}} =
             HighLevel.run(
               invocation("get", %{version_id: "v1"}),
               [remote("key"), %Target{kind: :stdio}],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               Keyword.put(opts(root), :write, fn bytes ->
                 send(owner, {:object_bytes, bytes})
                 :ok
               end)
             )

    refute_receive {:object_bytes, _}
  end

  test "recursive local moves preserve every source when a later upload fails", %{tmp_dir: root} do
    for frontend <- [:native, :s3] do
      source = Path.join(root, Atom.to_string(frontend))
      File.mkdir!(source)
      for name <- ["a", "b"], do: File.write!(Path.join(source, name), "raw")

      {endpoint, _} =
        WireServer.fixture([
          {200, [{"ETag", "\"uploaded\""}], ""},
          {403, [], "<Error><Code>AccessDenied</Code></Error>"}
        ])

      invocation = %{invocation("mv", %{recursive: true}) | frontend: frontend}

      assert {:error, %Error{}} =
               HighLevel.run(
                 invocation,
                 [local(source), remote("dest/")],
                 [nil, %Context{endpoint: endpoint, unsigned: true}],
                 opts(root)
               )

      for name <- ["a", "b"], do: assert(File.read!(Path.join(source, name)) == "raw")
      refute Enum.any?(requests(), &(&1.method == "DELETE"))
    end
  end

  test "recursive downloads preserve remote sources if a later GET fails", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing()},
        {200, head(), ""},
        {200, head(), "raw"},
        {200, head(), ""},
        {403, [], "<Error><Code>AccessDenied</Code></Error>"}
      ])

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [remote("src/"), local(Path.join(root, "dest"))],
               [
                 %Context{
                   endpoint: endpoint,
                   unsigned: true,
                   capabilities: %{conditional_delete: true}
                 },
                 nil
               ],
               opts(root)
             )

    assert File.read!(Path.join(root, "dest/a")) == "raw"
    refute Enum.any?(requests(), &(&1.method == "DELETE"))
  end

  test "recursive server copy preserves all sources if a later copy fails", %{tmp_dir: root} do
    copied = "<CopyObjectResult><ETag>&quot;copied&quot;</ETag></CopyObjectResult>"

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing()},
        {200, head(), ""},
        {200, head(), ""},
        {200, [], copied},
        {200, [{"ETag", "\"copied\""}, {"Content-Length", "3"}], ""},
        {200, head(), ""},
        {200, head(), ""},
        {200, head(), ""},
        {403, [], "<Error><Code>AccessDenied</Code></Error>"}
      ])

    context = %Context{endpoint: endpoint, unsigned: true}

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [remote("src/"), remote("dest/")],
               [context, context],
               opts(root)
             )

    refute Enum.any?(requests(), &(&1.method == "DELETE"))
  end

  test "successful recursive upload copies all objects before source deletion and rechecks destinations",
       %{tmp_dir: root} do
    source = Path.join(root, "src")
    File.mkdir!(source)
    for name <- ["a", "b"], do: File.write!(Path.join(source, name), "raw")

    {endpoint, _} =
      WireServer.start(fn listener, owner ->
        Enum.each(1..6, fn index ->
          {:ok, socket} = :gen_tcp.accept(listener, 5000)
          request = WireServer.request(socket)
          send(owner, {:wire_request, request})

          if index <= 2 do
            send(
              owner,
              {:sources_at_upload, Enum.map(["a", "b"], &File.exists?(Path.join(source, &1)))}
            )

            WireServer.response(socket, 200, [{"ETag", "\"uploaded\""}], "")
          else
            WireServer.response(
              socket,
              200,
              [{"ETag", "\"uploaded\""}, {"Content-Length", "3"}],
              ""
            )
          end

          :gen_tcp.close(socket)
        end)
      end)

    assert :ok =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [local(source), remote("dest/")],
               [nil, %Context{endpoint: endpoint, unsigned: true}],
               opts(root)
             )

    assert_receive {:sources_at_upload, [true, true]}
    assert_receive {:sources_at_upload, [true, true]}
    for name <- ["a", "b"], do: refute(File.exists?(Path.join(source, name)))
    assert [%{method: "PUT"}, %{method: "PUT"} | checks] = requests()
    assert Enum.all?(checks, &(&1.method == "HEAD"))
    assert Path.wildcard(Path.join(root, ".ess-inventory-*")) == []
  end

  test "changed copied destination preserves all local sources", %{tmp_dir: root} do
    source = Path.join(root, "src")
    File.mkdir!(source)
    for name <- ["a", "b"], do: File.write!(Path.join(source, name), "raw")

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"uploaded\""}], ""},
        {200, [{"ETag", "\"uploaded\""}], ""},
        {200, [{"ETag", "\"changed\""}, {"Content-Length", "3"}], ""}
      ])

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [local(source), remote("dest/")],
               [nil, %Context{endpoint: endpoint, unsigned: true}],
               opts(root)
             )

    for name <- ["a", "b"], do: assert(File.read!(Path.join(source, name)) == "raw")
  end

  test "changed local source after copying preserves all sources", %{tmp_dir: root} do
    source = Path.join(root, "src")
    File.mkdir!(source)
    for name <- ["a", "b"], do: File.write!(Path.join(source, name), "raw")

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"uploaded\""}], ""},
        {200, [{"ETag", "\"uploaded\""}], ""},
        {200, [{"ETag", "\"uploaded\""}, {"Content-Length", "3"}], ""}
      ])

    opts =
      Keyword.put(opts(root), :on_result, fn result ->
        if result.source.path == Path.join(source, "b"),
          do: File.write!(Path.join(source, "a"), "changed source")

        :ok
      end)

    assert {:error, %Error{}} =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [local(source), remote("dest/")],
               [nil, %Context{endpoint: endpoint, unsigned: true}],
               opts
             )

    assert File.read!(Path.join(source, "a")) == "changed source"
    assert File.read!(Path.join(source, "b")) == "raw"
  end

  test "nonversioned move rejects a GET that ignores the selected ETag before publication", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [{"ETag", "\"selected\""}, {"Content-Length", "3"}], ""},
        {200, [{"ETag", "\"different\""}], "bad"}
      ])

    path = Path.join(root, "existing")
    File.write!(path, "keep")

    assert {:error, %Error{code: :source_version_mismatch}} =
             HighLevel.run(
               invocation("mv", %{}),
               [remote("key"), local(path)],
               [
                 %Context{
                   endpoint: endpoint,
                   unsigned: true,
                   capabilities: %{conditional_delete: true}
                 },
                 nil
               ],
               opts(root)
             )

    assert File.read!(path) == "keep"
    refute Enum.any?(requests(), &(&1.method == "DELETE"))
  end

  test "successful recursive download deletes only captured versions after every copy completes",
       %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing()},
        {200, head("v1"), ""},
        {200, head("v1"), "raw"},
        {200, head("v2"), ""},
        {200, head("v2"), "raw"},
        {200, head("v1"), ""},
        {200, head("v2"), ""},
        {200, head("v1"), ""},
        {204, [], ""},
        {200, head("v2"), ""},
        {204, [], ""}
      ])

    assert :ok =
             HighLevel.run(
               invocation("mv", %{recursive: true}),
               [remote("src/"), local(Path.join(root, "dest"))],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               opts(root)
             )

    assert File.read!(Path.join(root, "dest/a")) == "raw"
    assert File.read!(Path.join(root, "dest/b")) == "raw"
    requests = requests()

    assert Enum.map(requests, & &1.method) == [
             "GET",
             "HEAD",
             "GET",
             "HEAD",
             "GET",
             "HEAD",
             "HEAD",
             "HEAD",
             "DELETE",
             "HEAD",
             "DELETE"
           ]

    assert Enum.filter(requests, &(&1.method == "DELETE")) |> Enum.map(& &1.target) ==
             ["/bucket/src/a?versionId=v1", "/bucket/src/b?versionId=v2"]

    assert Path.wildcard(Path.join(root, ".ess-inventory-*")) == []
  end

  test "recursive dryrun leaves local sources and performs no network mutation", %{tmp_dir: root} do
    source = Path.join(root, "src")
    File.mkdir!(source)
    File.write!(Path.join(source, "a"), "raw")

    assert :ok =
             HighLevel.run(
               invocation("mv", %{recursive: true, dryrun: true}),
               [local(source), remote("dest/")],
               [nil, %Context{endpoint: "http://unused.invalid", unsigned: true}],
               opts(root)
             )

    assert File.read!(Path.join(source, "a")) == "raw"
    assert requests() == []
    assert Path.wildcard(Path.join(root, ".ess-inventory-*")) == []
  end

  test "an ignored archived source prevents every recursive move deletion", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing()},
        {200, head(), ""},
        {200, head(), "raw"},
        {200, [{"x-amz-storage-class", "GLACIER"} | head()], ""}
      ])

    assert {:error, %Error{code: :move_source_skipped}} =
             HighLevel.run(
               invocation("mv", %{recursive: true, ignore_glacier_warnings: true}),
               [remote("src/"), local(Path.join(root, "dest"))],
               [%Context{endpoint: endpoint, unsigned: true}, nil],
               opts(root)
             )

    assert File.read!(Path.join(root, "dest/a")) == "raw"
    refute Enum.any?(requests(), &(&1.method == "DELETE"))
    assert Path.wildcard(Path.join(root, ".ess-inventory-*")) == []
  end
end
