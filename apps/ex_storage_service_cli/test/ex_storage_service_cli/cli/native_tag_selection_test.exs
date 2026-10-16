Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeTagSelectionTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO

  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.CLI.{Invocation, Management, Parser, Runner}
  alias ExStorageServiceCli.Test.WireServer
  @moduletag :tmp_dir
  @time ~U[2026-10-14 00:00:00Z]
  @now ~U[2026-10-15 00:00:00Z]

  test "tag actions expose bounded recursive and historical selectors" do
    for action <- ~w(get list set remove) do
      args = if action == "set", do: ["team=ops"], else: []

      assert {:ok, %Invocation{options: %{recursive: true, versions: true, rewind: "1d"}}} =
               Parser.parse(
                 ["tag", action, "s3://bucket/p/" | args] ++
                   ["--recursive", "--versions", "--rewind", "1d", "--limit", "1"]
               )
    end
  end

  for {scope, key, operation} <- [
        {"bucket", "", "PutBucketTagging"},
        {"object", "key", "PutObjectTagging"}
      ] do
    test "disabled exclude-folders preserves direct #{scope} tagging", %{tmp_dir: root} do
      key = unquote(key)
      operation = unquote(operation)
      url = "s3://bucket/" <> key
      target = %Target{kind: :remote, bucket: "bucket", key: key}
      tagging = %{"TagSet" => [%{"Key" => "team", "Value" => "ops"}]}
      expected = %{"Bucket" => "bucket", "Tagging" => tagging}
      expected = if key == "", do: expected, else: Map.put(expected, "Key", key)

      for flag <- ["--no-exclude-folders", "--exclude-folders=false"],
          dry_run <- [false, true] do
        args = ["tag", "set", url, "team=ops", flag] ++ if(dry_run, do: ["--dry-run"], else: [])
        assert {:ok, invocation} = Parser.parse(args)
        assert invocation.options[:exclude_folders] == false

        call = fn actual_operation, input ->
          refute dry_run
          assert actual_operation == operation
          assert input == expected
          send(self(), {:direct_tag_write, actual_operation})
          {:ok, %{"Written" => true}}
        end

        result =
          Management.run(invocation, target, %Context{unsigned: true},
            call: call,
            tmp_dir: root
          )

        if dry_run do
          assert {:ok, %{"DryRun" => true, "Operation" => ^operation, "Input" => ^expected}} =
                   result

          refute_received {:direct_tag_write, _}
        else
          assert {:ok, %{"Written" => true}} = result
          assert_received {:direct_tag_write, ^operation}
        end

        assert File.ls!(root) == []
      end
    end
  end

  test "tag set exposes exclude-folders only with recursive selection", %{tmp_dir: root} do
    assert {:ok, %Invocation{options: %{recursive: true, exclude_folders: true}}} =
             Parser.parse([
               "tag",
               "set",
               "s3://bucket/",
               "team=ops",
               "--recursive",
               "--exclude-folders"
             ])

    assert {:error, %Error{category: :syntax}} =
             run(
               "set",
               %{exclude_folders: true},
               fn _, _ -> flunk("invalid selection called S3") end,
               root
             )

    for action <- ~w(get list remove) do
      assert {:error, %Error{category: :syntax}} =
               Parser.parse(["tag", action, "s3://bucket/", "--exclude-folders"])
    end
  end

  test "exclude-folders follows pinned full-key selection before current or historical tag effects",
       %{tmp_dir: root} do
    for versions <- [false, true], dry_run <- [false, true] do
      call = fn
        "ListObjectsV2", _ ->
          {:ok,
           %{
             "IsTruncated" => false,
             "Contents" => Enum.map(["top", "sub/child", "folder/", "/leading/key"], &object/1)
           }}

        "ListObjectVersions", _ ->
          {:ok,
           %{
             "IsTruncated" => false,
             "Versions" =>
               Enum.map(
                 ["top", "sub/child", "folder/", "/leading/key"],
                 &version(&1, "v1", @time)
               )
           }}

        "HeadObject", %{"Key" => key} ->
          assert key in ["top", "/leading/key"]
          send(self(), {:preflight, key})
          {:ok, head("v1")}

        "PutObjectTagging", %{"Key" => key, "VersionId" => "v1"} ->
          refute dry_run
          assert key in ["top", "/leading/key"]
          assert_received {:preflight, "top"}
          send(self(), {:preflight, "top"})
          assert_received {:preflight, "/leading/key"}
          send(self(), {:preflight, "/leading/key"})
          send(self(), {:tagged, key})
          {:ok, %{}}
      end

      options = %{recursive: true, exclude_folders: true, versions: versions, dry_run: dry_run}

      assert {:ok, %{"Processed" => 2, "MatchedObjects" => 2, "DryRun" => ^dry_run}} =
               Management.run(
                 %Invocation{
                   command: ["tag", "set"],
                   args: ["s3://bucket/", "team=ops"],
                   options: options
                 },
                 %Target{kind: :remote, bucket: "bucket", key: ""},
                 %Context{unsigned: true},
                 call: call,
                 tmp_dir: root,
                 now: @now
               )

      assert_received {:preflight, "top"}
      assert_received {:preflight, "/leading/key"}

      if dry_run do
        refute_received {:tagged, _}
      else
        assert_received {:tagged, "top"}
        assert_received {:tagged, "/leading/key"}
      end

      assert File.ls!(root) == []
    end
  end

  test "complete current inventory and every identity precede tag mutations", %{tmp_dir: root} do
    call = fn
      "ListObjectsV2", %{"Prefix" => "p/", "ContinuationToken" => "next"} ->
        {:ok, %{"Contents" => [object("p/b")], "IsTruncated" => false}}

      "ListObjectsV2", %{"Prefix" => "p/"} ->
        {:ok,
         %{
           "Contents" => [object("p/a")],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}

      "HeadObject", %{"Key" => key} ->
        send(self(), {:preflight, key})
        {:ok, head("v1")}

      "PutObjectTagging", input ->
        if input["Key"] == "p/a" do
          assert_received {:preflight, "p/a"}
          assert_received {:preflight, "p/b"}
        end

        assert input["VersionId"] == "v1"
        assert input["Tagging"] == %{"TagSet" => [%{"Key" => "team", "Value" => "ops"}]}
        send(self(), {:tagged, input["Key"]})
        {:ok, %{}}
    end

    assert {:ok, %{"Processed" => 2, "Returned" => 1, "Partial" => true}} =
             run("set", %{recursive: true, limit: 1}, call, root)

    assert_received {:tagged, "p/a"}
    assert_received {:tagged, "p/b"}
    assert File.ls!(root) == []
  end

  test "listing failure prevents all tag reads and writes", %{tmp_dir: root} do
    error = Error.new(:service, "denied", code: "AccessDenied")

    call = fn
      "ListObjectsV2", %{"ContinuationToken" => "next"} ->
        {:error, error}

      "ListObjectsV2", _ ->
        {:ok,
         %{
           "Contents" => [object("p/a")],
           "IsTruncated" => true,
           "NextContinuationToken" => "next"
         }}

      operation, _ ->
        flunk("Incomplete inventory reached #{operation}")
    end

    assert {:error, ^error} = run("set", %{recursive: true}, call, root)
    assert File.ls!(root) == []
  end

  test "rewind selects the old immutable data version and honors delete markers", %{tmp_dir: root} do
    call = fn
      "ListObjectVersions", _ ->
        {:ok,
         %{
           "Versions" => [
             version("p/a", "old", @time),
             version("p/a", "new", ~U[2026-10-16 00:00:00Z]),
             version("p/b", "hidden", ~U[2026-10-13 00:00:00Z])
           ],
           "DeleteMarkers" => [
             %{"Key" => "p/b", "VersionId" => "marker", "LastModified" => @time}
           ],
           "IsTruncated" => false
         }}

      "HeadObject", %{"Key" => "p/a", "VersionId" => "old"} ->
        {:ok, head("old")}

      "DeleteObjectTagging", %{"Key" => "p/a", "VersionId" => "old"} ->
        {:ok, %{}}
    end

    assert {:ok, %{"Processed" => 1, "MatchedObjects" => 1}} =
             run("remove", %{recursive: true, rewind: "2026-10-15"}, call, root)
  end

  test "version tag reads bound output after a complete inventory", %{tmp_dir: root} do
    call = fn
      "ListObjectVersions", _ ->
        {:ok,
         %{
           "Versions" => [version("p/a", "a", @time), version("p/b", "b", @time)],
           "IsTruncated" => false
         }}

      "HeadObject", %{"Key" => key, "VersionId" => version} ->
        send(self(), {:read_head, key})
        {:ok, head(version)}

      "GetObjectTagging", %{"Key" => key, "VersionId" => version} ->
        send(self(), {:read_tags, key, version})
        {:ok, %{"TagSet" => []}}
    end

    assert {:ok, %{"Processed" => 1, "MatchedObjects" => 2, "Partial" => true}} =
             run("list", %{recursive: true, versions: true, limit: 1}, call, root)

    assert_received {:read_head, "p/a"}
    assert_received {:read_tags, "p/a", "a"}
    refute_received {:read_head, "p/b"}
    refute_received {:read_tags, "p/b", _}
  end

  test "dry run validates identities but writes no tags even with zero output", %{tmp_dir: root} do
    call = fn
      "ListObjectsV2", _ -> {:ok, %{"Contents" => [object("p/a")], "IsTruncated" => false}}
      "HeadObject", _ -> {:ok, head("v1")}
      operation, _ -> flunk("Dry run reached #{operation}")
    end

    assert {:ok, %{"DryRun" => true, "Processed" => 1, "Entries" => []}} =
             run("set", %{recursive: true, dry_run: true, limit: 0}, call, root)
  end

  test "mutable versions and contradictory selectors fail before writes", %{tmp_dir: root} do
    call = fn
      "ListObjectsV2", _ -> {:ok, %{"Contents" => [object("p/a")], "IsTruncated" => false}}
      "HeadObject", _ -> {:ok, head("null")}
      operation, _ -> flunk("Unsafe selection reached #{operation}")
    end

    assert {:error, %Error{category: :syntax}} = run("remove", %{recursive: true}, call, root)

    for options <- [
          %{recursive: true, version_id: "v1"},
          %{versions: true, version_id: "v1"},
          %{rewind: "2026-10-15", version_id: "v1"},
          %{recursive: true, limit: 10_001}
        ] do
      assert {:error, %Error{category: :syntax}} =
               run("set", options, fn _, _ -> flunk("Invalid options reached I/O") end, root)
    end
  end

  test "a partial mutation failure reports completed and matched counts", %{tmp_dir: root} do
    call = fn
      "ListObjectsV2", _ ->
        {:ok, %{"Contents" => [object("p/a"), object("p/b")], "IsTruncated" => false}}

      "HeadObject", _ ->
        {:ok, head("v1")}

      "DeleteObjectTagging", %{"Key" => "p/a"} ->
        {:ok, %{}}

      "DeleteObjectTagging", %{"Key" => "p/b"} ->
        {:error, Error.new(:service, "denied", code: "AccessDenied")}
    end

    assert {:error, %Error{code: "AccessDenied", details: %{processed: 1, matched_objects: 2}}} =
             run("remove", %{recursive: true}, call, root)
  end

  test "Runner sends selected-version tag XML over actual HTTP", %{tmp_dir: root} do
    versions = """
    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <IsTruncated>false</IsTruncated>
      <Version><Key>p/a</Key><VersionId>v1</VersionId><ETag>"e"</ETag>
        <Size>3</Size><LastModified>2026-10-14T00:00:00Z</LastModified></Version>
    </ListVersionsResult>
    """

    {endpoint, _} =
      WireServer.fixture([
        {200, [{"Content-Length", to_string(byte_size(versions))}], versions},
        {200,
         [
           {"Content-Length", "3"},
           {"ETag", "\"e\""},
           {"x-amz-version-id", "v1"},
           {"Last-Modified", "Wed, 14 Oct 2026 00:00:00 GMT"}
         ], ""},
        {200, [{"Content-Length", "0"}], ""}
      ])

    output =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "tag",
                   "set",
                   "s3://bucket/p/",
                   "team=ops",
                   "--recursive",
                   "--versions",
                   "--no-sign-request",
                   "--service",
                   "s3",
                   "--config-dir",
                   root,
                   "--output",
                   "json",
                   "--endpoint",
                   endpoint
                 ])
      end)

    assert JSON.decode!(output)["Processed"] == 1
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "PUT", target: path, body: body}}
    assert URI.decode_query(URI.parse(path).query) == %{"tagging" => "", "versionId" => "v1"}
    assert body =~ "<TagSet><Tag><Key>team</Key><Value>ops</Value></Tag></TagSet>"
  end

  defp run(action, options, call, root) do
    args = if action == "set", do: ["s3://bucket/p/", "team=ops"], else: ["s3://bucket/p/"]

    Management.run(
      %Invocation{command: ["tag", action], args: args, options: options},
      %Target{kind: :remote, bucket: "bucket", key: "p/"},
      %Context{unsigned: true},
      call: call,
      state_root: root,
      now: @now
    )
  end

  defp object(key),
    do: %{"Key" => key, "ETag" => "\"e\"", "Size" => 3, "LastModified" => @time}

  defp version(key, id, time),
    do: object(key) |> Map.merge(%{"VersionId" => id, "LastModified" => time})

  defp head(version),
    do: %{
      "ETag" => "\"e\"",
      "ContentLength" => 3,
      "VersionId" => version,
      "LastModified" => @time
    }
end
