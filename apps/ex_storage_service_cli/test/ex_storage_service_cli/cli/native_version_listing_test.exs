Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeVersionListingTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}

  alias ExStorageServiceCli.CLI.{
    Invocation,
    NativeEvents,
    NativeInspection,
    NativeListing,
    Runner
  }

  @moduletag :tmp_dir

  test "literal target prefix and default folder delimiter reach ListObjectVersions" do
    fetch = fn "ListObjectVersions", input ->
      assert input == %{
               "Bucket" => "bucket",
               "Prefix" => "photos",
               "Delimiter" => "/",
               "MaxKeys" => 1000
             }

      {:ok, page([version("photos.txt", "one")], [], [%{"Prefix" => "photos/"}])}
    end

    assert {:ok, %{"Entries" => [object, folder], "Returned" => 2, "Partial" => false}} =
             NativeListing.versions(invocation(), target("photos"), %Context{}, fetch: fetch)

    assert object["key"] == "photos.txt"
    assert object["version_id"] == "one"
    assert folder == %{"prefix" => "photos/", "common_prefix" => true}
  end

  test "prefix override custom delimiter and MaxKeys survive paired-marker pagination" do
    fetch = fn "ListObjectVersions", input ->
      assert input["Prefix"] == "override:"
      assert input["Delimiter"] == ":"
      assert input["MaxKeys"] == 2

      case input do
        %{"KeyMarker" => "override:a", "VersionIdMarker" => "one"} ->
          {:ok, page([], [version("override:a", "two")], [%{"Prefix" => "override:dir:"}])}

        _ ->
          {:ok,
           page([version("override:a", "one")])
           |> Map.merge(%{
             "IsTruncated" => true,
             "NextKeyMarker" => "override:a",
             "NextVersionIdMarker" => "one"
           })}
      end
    end

    assert {:ok, %{"Entries" => [object, marker, folder], "Returned" => 3, "Partial" => false}} =
             NativeListing.versions(
               invocation(%{prefix: "override:", delimiter: ":", max_keys: 2}),
               target("ignored/"),
               %Context{},
               fetch: fetch
             )

    refute object["delete_marker"]
    assert marker["delete_marker"]
    assert marker["version_id"] == "two"
    assert folder["prefix"] == "override:dir:"
  end

  test "recursive overrides explicit delimiter just like legacy ls; empty prefix is literal" do
    fetch = fn "ListObjectVersions", input ->
      assert input == %{"Bucket" => "bucket", "Prefix" => "", "MaxKeys" => 1}
      {:ok, page([version("nested/key", "one")])}
    end

    assert {:ok, %{"Returned" => 1}} =
             NativeListing.versions(
               invocation(%{recursive: true, prefix: "", delimiter: ":", max_keys: 1}),
               target("ignored/"),
               %Context{},
               fetch: fetch
             )
  end

  test "explicit empty delimiter reaches the provider" do
    fetch = fn "ListObjectVersions", %{"Delimiter" => ""} -> {:ok, page()} end

    assert {:ok, %{"Returned" => 0, "Partial" => false}} =
             NativeListing.versions(invocation(%{delimiter: ""}), target(), %Context{},
               fetch: fetch
             )
  end

  test "limits include folders and stop after evidence of another row" do
    fetch = fn "ListObjectVersions", _ ->
      {:ok, page([version("a", "one")], [], [%{"Prefix" => "dir/"}])}
    end

    assert {:ok, %{"Returned" => 1, "Partial" => true, "Entries" => [_]}} =
             NativeListing.versions(invocation(%{limit: 1}), target(), %Context{}, fetch: fetch)

    assert {:ok, %{"Returned" => 0, "Partial" => true, "Entries" => []}} =
             NativeListing.versions(invocation(%{limit: 0}), target(), %Context{}, fetch: fetch)
  end

  test "later page errors and repeated tokens cannot become successful listings" do
    for result <- [:denied, :repeated] do
      fetch = fn "ListObjectVersions", input ->
        if input["KeyMarker"] && result == :denied do
          {:error, Error.new(:service, "Denied", code: "AccessDenied")}
        else
          {:ok,
           page([version("a", "one")])
           |> Map.merge(%{
             "IsTruncated" => true,
             "NextKeyMarker" => "a",
             "NextVersionIdMarker" => "one"
           })}
        end
      end

      assert {:error, %Error{}} =
               NativeListing.versions(invocation(), target(), %Context{}, fetch: fetch)
    end
  end

  test "invalid targets limits and historical combinations fail before requests" do
    no_call = fn _, _ -> flunk("invalid selection made a request") end

    for bad_target <- [nil, %Target{kind: :local, path: "."}, %Target{kind: :remote}] do
      assert {:error, %Error{category: :syntax}} =
               NativeListing.versions(invocation(), bad_target, %Context{}, fetch: no_call)
    end

    for options <- [
          %{limit: -1},
          %{limit: 10_001},
          %{max_keys: 0},
          %{max_keys: 1001},
          %{rewind: "2026-10-15"}
        ] do
      assert {:error, %Error{category: :syntax}} =
               NativeListing.versions(invocation(options), target(), %Context{}, fetch: no_call)
    end
  end

  test "invalid version and prefix rows return protocol errors" do
    for response <- [
          page([version("outside", "one")]),
          page([version("selected/a", "")]),
          page([], [], [%{"Prefix" => "outside/"}])
        ] do
      assert {:error, %Error{category: :protocol}} =
               NativeListing.versions(invocation(), target("selected/"), %Context{},
                 fetch: fn _, _ -> {:ok, response} end
               )
    end
  end

  test "plain version labels and partial warning retain their native contract" do
    parent = self()
    invocation = %{invocation(%{limit: 1}) | options: %{versions: true, limit: 1}}
    fetch = fn _, _ -> {:ok, page([], [version("a", "one"), version("b", "two")])} end

    assert :ok =
             NativeListing.versions(invocation, target(), %Context{},
               fetch: fetch,
               emit: fn row -> send(parent, {:row, row}) end,
               stderr: fn row -> send(parent, {:warning, row}) end
             )

    assert_received {:row, "s3://bucket/a (version one) [delete marker]"}
    assert_received {:warning, "Version listing is partial; increase --limit (maximum10000)"}
  end

  test "NDJSON includes version identity folder rows and bounded summary" do
    invocation = invocation(%{output: "ndjson"})
    fetch = fn _, _ -> {:ok, page([version("a", "one")], [], [%{"Prefix" => "dir/"}])} end

    output =
      capture_io(fn ->
        assert :ok =
                 NativeEvents.with_invocation(invocation, fn invocation ->
                   NativeListing.versions(invocation, target(), %Context{}, fetch: fetch)
                 end)
      end)

    [object, folder, bound, summary] =
      output |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)

    assert object["data"]["version_id"] == "one"
    assert folder["data"]["common_prefix"]
    assert bound["data"] == %{"Returned" => 2, "Partial" => false}
    assert summary["status"] == "ok"
  end

  test "historical listing applies prefix page size paired markers and version selection", %{
    tmp_dir: root
  } do
    fetch = fn "ListObjectVersions", input ->
      assert input["Prefix"] == "selected/"
      assert input["MaxKeys"] == 1
      refute Map.has_key?(input, "Delimiter")

      response =
        if input["KeyMarker"],
          do: page([version("selected/a", "two")]),
          else:
            page([version("selected/a", "one")])
            |> Map.merge(%{
              "IsTruncated" => true,
              "NextKeyMarker" => "selected/a",
              "NextVersionIdMarker" => "one"
            })

      if input["KeyMarker"], do: assert(input["VersionIdMarker"] == "one")
      {:ok, response}
    end

    assert {:ok, %{"Returned" => 2, "Partial" => false, "Entries" => entries}} =
             NativeInspection.ls(
               target("ignored/"),
               %Context{},
               %{
                 rewind: "2026-10-15",
                 prefix: "selected/",
                 max_keys: 1,
                 versions: true,
                 recursive: true
               },
               fetch: fetch,
               tmp_dir: root
             )

    assert Enum.sort(Enum.map(entries, & &1["version_id"])) == ["one", "two"]
    assert File.ls!(root) == []
  end

  test "historical delimiter selection fails before listing" do
    assert {:error, %Error{category: :syntax, code: :historical_listing_delimiter}} =
             NativeInspection.ls(target(), %Context{}, %{rewind: "2026-10-15", delimiter: "/"},
               fetch: fn _, _ -> flunk("unsupported historical selection made a request") end
             )
  end

  test "real Runner preserves selection and paired pagination markers on the wire", %{
    tmp_dir: root
  } do
    first =
      "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>selected:a</NextKeyMarker><NextVersionIdMarker>one</NextVersionIdMarker><Version><Key>selected:a</Key><VersionId>one</VersionId><Size>3</Size></Version></ListVersionsResult>"

    second =
      "<ListVersionsResult><IsTruncated>false</IsTruncated><DeleteMarker><Key>selected:a</Key><VersionId>two</VersionId></DeleteMarker><CommonPrefixes><Prefix>selected:dir:</Prefix></CommonPrefixes></ListVersionsResult>"

    {endpoint, _} = WireServer.fixture([{200, [], first}, {200, [], second}])

    output =
      capture_io(fn ->
        assert :ok =
                 Runner.run([
                   "ls",
                   "--versions",
                   "--prefix",
                   "selected:",
                   "--delimiter",
                   ":",
                   "--max-keys",
                   "2",
                   "--service",
                   "s3",
                   "--endpoint",
                   endpoint,
                   "--no-sign-request",
                   "--config-dir",
                   root,
                   "--output",
                   "json",
                   "s3://bucket/ignored/"
                 ])
      end)

    assert %{"Returned" => 3, "Partial" => false, "Entries" => [object, marker, folder]} =
             JSON.decode!(output)

    assert object["version_id"] == "one"
    assert marker["version_id"] == "two"
    assert marker["delete_marker"]
    assert folder == %{"prefix" => "selected:dir:", "common_prefix" => true}

    for page <- [1, 2] do
      assert_receive {:wire_request, %{target: path}}
      query = path |> URI.parse() |> Map.fetch!(:query) |> URI.decode_query()
      assert query["prefix"] == "selected:"
      assert query["delimiter"] == ":"
      assert query["max-keys"] == "2"
      assert Map.has_key?(query, "versions")

      if page == 2 do
        assert query["key-marker"] == "selected:a"
        assert query["version-id-marker"] == "one"
      end
    end
  end

  defp invocation(options \\ %{}),
    do: %Invocation{command: ["ls"], options: Map.merge(%{versions: true, json: true}, options)}

  defp target(key \\ ""), do: %Target{kind: :remote, bucket: "bucket", key: key}

  defp page(versions \\ [], markers \\ [], prefixes \\ []),
    do: %{
      "IsTruncated" => false,
      "Versions" => versions,
      "DeleteMarkers" => markers,
      "CommonPrefixes" => prefixes
    }

  defp version(key, id),
    do: %{
      "Key" => key,
      "VersionId" => id,
      "Size" => 3,
      "ETag" => "etag",
      "LastModified" => "2026-10-14T00:00:00Z"
    }
end
