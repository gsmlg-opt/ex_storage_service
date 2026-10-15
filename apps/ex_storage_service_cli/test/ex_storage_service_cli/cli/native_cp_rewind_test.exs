Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeCpRewindTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  @moduletag :tmp_dir
  @cutoff "2026-01-03T00:00:00Z"
  @modified "2026-01-01T00:00:00Z"
  @version "old+雪"

  test "native cp exposes rewind without adding an AWS option" do
    assert {:ok, %{options: %{rewind: @cutoff}}} =
             Parser.parse(["cp", "--rewind", @cutoff, "s3://bucket/key", "./out"])

    assert {:error, _} =
             Parser.parse(["s3", "cp", "--rewind", @cutoff, "s3://bucket/key", "./out"])
  end

  test "single download pins a historical version before publishing", %{tmp_dir: root} do
    history = version("key", "future", "2026-01-04T00:00:00Z") <> version("key", @version)
    {endpoint, _} = WireServer.fixture([{200, [], listing(history)}, head(), object()])
    destination = Path.join(root, "out")
    assert :ok = run(remote("key"), local(destination), endpoint, root)
    assert File.read!(destination) == "old"
    assert_receive {:wire_request, %{method: "GET", target: history_path}}
    assert history_path =~ "versions"
    assert_receive {:wire_request, %{method: "HEAD", target: selected}}
    assert selected =~ "versionId=old%2B%E9%9B%AA"
    assert_receive {:wire_request, %{method: "GET", target: ^selected, headers: fields}}
    assert fields["if-match"] == "\"old\""
  end

  test "recursive snapshot omits a selected tombstone and future recreation", %{tmp_dir: root} do
    history =
      version("dir/key", @version) <>
        version("dir/gone", "recreated", "2026-01-04T00:00:00Z") <>
        marker("dir/gone", "deleted", "2026-01-02T00:00:00Z") <>
        version("dir/gone", "before-delete")

    {endpoint, _} = WireServer.fixture([{200, [], listing(history)}, head(), object()])
    destination = Path.join(root, "out")
    assert :ok = run(remote("dir/"), local(destination), endpoint, root, %{recursive: true})
    assert File.read!(Path.join(destination, "key")) == "old"
    refute File.exists?(Path.join(destination, "gone"))
    assert_receive {:wire_request, %{target: path}}
    assert path =~ "prefix=dir%2F"
    assert_receive {:wire_request, %{method: "HEAD", target: selected}}
    assert selected =~ "/bucket/dir/key?"
    assert selected =~ "versionId=old%2B%E9%9B%AA"
  end

  test "strict cutoff equality and deletion leave the destination unchanged", %{tmp_dir: root} do
    for history <- [
          version("key", "equal", @cutoff),
          marker("key", "deleted", "2026-01-02T00:00:00Z") <> version("key", @version)
        ] do
      {endpoint, _} = WireServer.fixture([{200, [], listing(history)}])
      destination = Path.join(root, "out")
      File.write!(destination, "original")

      assert {:error, %{code: :rewind_not_found}} =
               run(remote("key"), local(destination), endpoint, root)

      assert File.read!(destination) == "original"
      assert_receive {:wire_request, %{method: "GET"}}
      refute_receive {:wire_request, _}
    end
  end

  test "a late version-list failure performs no transfers", %{tmp_dir: root} do
    page =
      "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>key</NextKeyMarker>" <>
        "<NextVersionIdMarker>next</NextVersionIdMarker>" <>
        version("key", @version) <>
        "</ListVersionsResult>"

    {endpoint, _} = WireServer.fixture([{200, [], page}, {403, [], ""}])
    destination = Path.join(root, "out")

    assert {:error, %{category: :service}} =
             run(remote(""), local(destination), endpoint, root, %{recursive: true})

    refute File.exists?(destination)
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "GET"}}
    refute_receive {:wire_request, _}
  end

  test "HEAD must confirm historical time as well as version size and etag", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(version("key", @version))},
        head("Wed, 07 Jan 2026 00:00:00 GMT")
      ])

    assert {:error, %{code: :inventory_changed}} =
             run(remote("key"), local(Path.join(root, "out")), endpoint, root)

    refute File.exists?(Path.join(root, "out"))
  end

  test "a provider ignoring GET version selection cannot replace an existing file", %{
    tmp_dir: root
  } do
    wrong =
      {200, headers() |> List.keyreplace("x-amz-version-id", 0, {"x-amz-version-id", "wrong"}),
       "old"}

    {endpoint, _} =
      WireServer.fixture([{200, [], listing(version("key", @version))}, head(), wrong])

    destination = Path.join(root, "out")
    File.write!(destination, "original")

    assert {:error, %{code: :source_version_mismatch}} =
             run(remote("key"), local(destination), endpoint, root)

    assert File.read!(destination) == "original"
    assert File.ls!(root) == ["out"]
  end

  test "server copy pins the selected version through recapture and copy source", %{tmp_dir: root} do
    destination_head =
      {200, List.keyreplace(headers(), "x-amz-version-id", 0, {"x-amz-version-id", "dest"}), ""}

    copied =
      {200, [{"x-amz-version-id", "dest"}],
       "<CopyObjectResult><ETag>\"old\"</ETag></CopyObjectResult>"}

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(version("key", @version))},
        head(),
        head(),
        copied,
        destination_head,
        head()
      ])

    assert :ok = run(remote("key"), remote("copied"), endpoint, root)
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD", target: selected}}
    assert_receive {:wire_request, %{method: "HEAD", target: ^selected, headers: pinned}}
    assert pinned["if-match"] == "\"old\""
    assert_receive {:wire_request, %{method: "PUT", headers: fields}}
    assert fields["x-amz-copy-source"] =~ "bucket/key?versionId=old%2B%E9%9B%AA"
    assert fields["x-amz-copy-source-if-match"] == "\"old\""
  end

  test "copy recapture cannot replace the historical identity", %{tmp_dir: root} do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(version("key", @version))},
        head(),
        head("Wed, 07 Jan 2026 00:00:00 GMT")
      ])

    assert {:error, %{code: :source_changed}} =
             run(remote("key"), remote("copied"), endpoint, root)

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
  end

  test "dry-run resolves and confirms history without reading the body", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([{200, [], listing(version("key", @version))}, head()])

    assert :ok =
             run(remote("key"), local(Path.join(root, "out")), endpoint, root, %{dryrun: true})

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
    assert File.ls!(root) == []
  end

  test "conflicting or local rewind inputs fail before network access", %{tmp_dir: root} do
    assert {:error, %{code: :version_selection}} =
             run(remote("key"), local(Path.join(root, "out")), nil, root, %{version_id: "v"})

    assert {:error, %{code: :version_selection}} =
             run(local(Path.join(root, "input")), remote("key"), nil, root)
  end

  test "an excluded single source performs no historical I/O", %{tmp_dir: root} do
    assert :ok =
             run(remote("key"), local(Path.join(root, "out")), nil, root, %{}, [{:exclude, "*"}])

    assert File.ls!(root) == []
  end

  defp run(source, destination, endpoint, root, extra \\ %{}, filters \\ []) do
    invocation = %Invocation{
      frontend: :native,
      command: ["cp"],
      options: Map.merge(%{rewind: @cutoff, progress: false}, extra),
      filters: filters
    }

    context = %Context{endpoint: endpoint || "http://127.0.0.1:1", unsigned: true}

    contexts =
      Enum.map([source, destination], fn target -> if target.kind == :remote, do: context end)

    HighLevel.run(invocation, [source, destination], contexts,
      max_attempts: 1,
      tmp_dir: root,
      emit: fn _ -> :ok end
    )
  end

  defp remote(key), do: %Target{kind: :remote, bucket: "bucket", key: key}
  defp local(path), do: %Target{kind: :local, path: path}

  defp listing(records),
    do:
      "<ListVersionsResult><IsTruncated>false</IsTruncated>" <> records <> "</ListVersionsResult>"

  defp version(key, id, time \\ @modified),
    do:
      "<Version><Key>#{key}</Key><VersionId>#{id}</VersionId><LastModified>#{time}</LastModified><ETag>\"old\"</ETag><Size>3</Size></Version>"

  defp marker(key, id, time),
    do:
      "<DeleteMarker><Key>#{key}</Key><VersionId>#{id}</VersionId><LastModified>#{time}</LastModified></DeleteMarker>"

  defp headers(time \\ "Thu, 01 Jan 2026 00:00:00 GMT"),
    do: [
      {"Content-Length", "3"},
      {"ETag", "\"old\""},
      {"Last-Modified", time},
      {"x-amz-version-id", @version}
    ]

  defp head(time \\ "Thu, 01 Jan 2026 00:00:00 GMT"), do: {200, headers(time), ""}
  defp object, do: {200, headers(), "old"}
end
