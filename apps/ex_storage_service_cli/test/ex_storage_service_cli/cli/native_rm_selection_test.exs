Code.require_file("../../support/wire_server.exs", __DIR__)

defmodule ExStorageServiceCli.CLI.NativeRmSelectionTest do
  use ExUnit.Case, async: false
  alias ExStorageServiceCli.{Context, Error, Target, Test.WireServer}
  alias ExStorageServiceCli.CLI.{HighLevel, Invocation, Parser}
  @moduletag :tmp_dir
  @now ~U[2026-01-03 00:00:00Z]
  @old "2026-01-01T00:00:00Z"
  @edge "2026-01-02T00:00:00Z"
  @new "2026-01-02T12:00:00Z"

  defp target(key \\ "key"), do: %Target{kind: :remote, bucket: "bucket", key: key}
  defp invocation(options), do: %Invocation{frontend: :native, command: ["rm"], options: options}
  defp opts(root), do: [now: @now, tmp_dir: root, max_attempts: 1, emit: fn _ -> :ok end]

  defp context(endpoint, conditional \\ true),
    do: %Context{
      endpoint: endpoint,
      unsigned: true,
      capabilities: %{conditional_delete: conditional}
    }

  defp head(date \\ "Thu, 01 Jan 2026 00:00:00 GMT", extra \\ []),
    do:
      {200,
       [{"ETag", "\"e\""}, {"Content-Length", "1"}] ++
         if(date, do: [{"Last-Modified", date}], else: []) ++ extra, ""}

  defp listing(records, versions \\ false) do
    items =
      Enum.map_join(records, "", fn {key, date, version} ->
        type =
          if version == "marker",
            do: "DeleteMarker",
            else: if(versions, do: "Version", else: "Contents")

        stamp = if date, do: "<LastModified>#{date}</LastModified>", else: ""
        vid = if versions, do: "<VersionId>#{version}</VersionId>", else: ""

        "<#{type}><Key>#{key}</Key><Size>1</Size><ETag>&quot;e&quot;</ETag>#{stamp}#{vid}</#{type}>"
      end)

    root = if versions, do: "ListVersionsResult", else: "ListBucketResult"
    "<#{root}><IsTruncated>false</IsTruncated>#{items}</#{root}>"
  end

  defp deleted(records),
    do:
      "<DeleteResult>" <>
        Enum.map_join(records, "", fn {key, version} ->
          "<Deleted><Key>#{key}</Key>" <>
            if(version, do: "<VersionId>#{version}</VersionId>", else: "") <> "</Deleted>"
        end) <> "</DeleteResult>"

  test "native rm accepts versions and fake aliases without leaking them to AWS", %{tmp_dir: root} do
    assert {:ok, parsed} =
             Parser.parse([
               "rm",
               "s3://bucket/key",
               "--versions",
               "--fake",
               "--older-than",
               "1d",
               "--newer-than",
               "3d"
             ])

    assert parsed.options[:all_versions] == true
    assert parsed.options[:dry_run] == true
    {endpoint, _} = WireServer.fixture([{200, [], listing([{"key", @old, "v1"}], true)}])
    assert :ok = HighLevel.run(parsed, [target()], [context(endpoint)], opts(root))
    assert_receive {:wire_request, %{method: "GET", target: path}}
    assert path =~ "versions="
    refute_receive {:wire_request, _}

    for flag <- ["--versions", "--fake", "--older-than", "--newer-than"] do
      args = if flag in ["--older-than", "--newer-than"], do: [flag, "1d"], else: [flag]

      assert {:error, %Error{category: :syntax}} =
               Parser.parse(["s3", "rm", "s3://bucket/key"] ++ args)
    end
  end

  test "versions alias deletes explicitly confirmed version identifiers", %{tmp_dir: root} do
    assert {:ok, parsed} = Parser.parse(["rm", "s3://bucket/key", "--versions"])

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing([{"key", @old, "v1"}], true)},
        {200, [], deleted([{"key", "v1"}])}
      ])

    assert :ok = HighLevel.run(parsed, [target()], [context(endpoint, false)], opts(root))
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "POST", body: body}}
    assert body =~ "<VersionId>v1</VersionId>"
    refute_receive {:wire_request, _}
  end

  test "versions alias retains version-selection conflicts before requests", %{tmp_dir: root} do
    assert {:ok, parsed} =
             Parser.parse(["rm", "s3://bucket/key", "--versions", "--version-id", "v1"])

    assert {:error, %Error{code: :version_selection}} =
             HighLevel.run(parsed, [target()], [%Context{}], opts(root))
  end

  test "single older-than signs a condition against the selected current object", %{
    tmp_dir: root
  } do
    {endpoint, _} = WireServer.fixture([head(), {204, [], ""}])

    signed = %{
      context(endpoint)
      | unsigned: false,
        access_key_id: "fixture-access",
        secret_access_key: "fixture-secret"
    }

    assert :ok =
             HighLevel.run(
               invocation(%{older_than: "1d"}),
               [target()],
               [signed],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD"}}
    assert_receive {:wire_request, %{method: "DELETE", headers: headers, target: path}}
    assert headers["if-match"] == "\"e\""
    assert headers["authorization"] =~ "if-match"
    refute path =~ "versionId"
  end

  test "single newer-than skips old objects and deletes new objects", %{tmp_dir: root} do
    for {date, removed} <- [
          {"Thu, 01 Jan 2026 00:00:00 GMT", false},
          {"Fri, 02 Jan 2026 12:00:00 GMT", true}
        ] do
      responses = if removed, do: [head(date), {204, [], ""}], else: [head(date)]
      {endpoint, _} = WireServer.fixture(responses)

      assert :ok =
               HighLevel.run(
                 invocation(%{newer_than: "1d"}),
                 [target()],
                 [context(endpoint)],
                 opts(root)
               )

      assert_receive {:wire_request, %{method: "HEAD"}}

      if removed,
        do: assert_receive({:wire_request, %{method: "DELETE"}}),
        else: refute_receive({:wire_request, _})
    end
  end

  test "single selected version checks returned identity and deletes only that version", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        head("Thu, 01 Jan 2026 00:00:00 GMT", [{"x-amz-version-id", "v +%"}]),
        {204, [], ""}
      ])

    assert :ok =
             HighLevel.run(
               invocation(%{older_than: "1d", version_id: "v +%"}),
               [target()],
               [context(endpoint, false)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD", target: path}}
    assert path =~ "versionId=v%20%2B%25"
    assert_receive {:wire_request, %{method: "DELETE", target: ^path, headers: headers}}
    refute Map.has_key?(headers, "if-match")
  end

  test "unconfirmed or mutable single identities fail closed", %{tmp_dir: root} do
    for {options, response, conditional, code} <- [
          {%{older_than: "1d", version_id: "v1"}, head(), false, :source_version_mismatch},
          {%{older_than: "1d"}, head(nil), true, :native_transfer_timestamp},
          {%{older_than: "1d"}, head(), false, :conditional_delete_unavailable},
          {%{older_than: "1d", version_id: "null"},
           head("Thu, 01 Jan 2026 00:00:00 GMT", [{"x-amz-version-id", "null"}]), false,
           :conditional_delete_unavailable},
          {%{older_than: "1d"},
           {200, [{"Content-Length", "1"}, {"Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT"}],
            ""}, true, :invalid_remote_identity}
        ] do
      {endpoint, _} = WireServer.fixture([response])

      assert {:error, %Error{code: ^code}} =
               HighLevel.run(
                 invocation(options),
                 [target()],
                 [context(endpoint, conditional)],
                 opts(root)
               )

      assert_receive {:wire_request, %{method: "HEAD"}}
      refute_receive {:wire_request, _}
    end
  end

  test "age dry-run selects by HEAD without needing deletion capability", %{tmp_dir: root} do
    {endpoint, _} = WireServer.fixture([head()])

    assert :ok =
             HighLevel.run(
               invocation(%{older_than: "1d", dry_run: true}),
               [target()],
               [context(endpoint, false)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
  end

  test "recursive age filters preserve excluded keys and fence each selected current deletion", %{
    tmp_dir: root
  } do
    items = [
      {"prefix/old", @old, nil},
      {"prefix/edge", @edge, nil},
      {"prefix/new", @new, nil},
      {"prefix/excluded", nil, nil}
    ]

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(items)},
        head("Fri, 02 Jan 2026 00:00:00 GMT"),
        head(),
        {200, [], deleted([{"prefix/edge", nil}, {"prefix/old", nil}])}
      ])

    parsed = %{invocation(%{recursive: true, older_than: "1d"}) | filters: [exclude: "excluded"]}
    assert :ok = HighLevel.run(parsed, [target("prefix")], [context(endpoint)], opts(root))
    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/prefix/edge"}}
    assert_receive {:wire_request, %{method: "HEAD", target: "/bucket/prefix/old"}}
    assert_receive {:wire_request, %{method: "POST", body: body}}
    assert body =~ "<Key>prefix/old</Key>"
    assert body =~ "<Key>prefix/edge</Key>"
    refute body =~ "<Key>prefix/new</Key>"
    refute body =~ "<Key>prefix/excluded</Key>"
    refute body =~ "<VersionId>"
    assert length(Regex.scan(~r/<ETag>/, body)) == 2
    assert Path.wildcard(Path.join(root, "ess-*")) == []
  end

  test "all-version age selection includes delete markers with exact identities", %{tmp_dir: root} do
    items = [{"key", @old, "v1"}, {"key", @edge, "marker"}, {"key", @new, "v3"}]

    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing(items, true)},
        {200, [], deleted([{"key", "v1"}, {"key", "marker"}])}
      ])

    assert :ok =
             HighLevel.run(
               invocation(%{all_versions: true, older_than: "1d"}),
               [target()],
               [context(endpoint, false)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "POST", body: body}}
    assert body =~ "<VersionId>v1</VersionId>"
    assert body =~ "<VersionId>marker</VersionId>"
    refute body =~ "<VersionId>v3</VersionId>"
    refute_receive {:wire_request, _}
  end

  test "missing later timestamp prevents every batch and cleans complete manifests", %{
    tmp_dir: root
  } do
    for versions <- [false, true] do
      entries =
        Enum.map(1..101, fn n ->
          {"prefix/" <> String.pad_leading(Integer.to_string(n), 3, "0"),
           if(n == 101, do: nil, else: @old), if(versions, do: "v#{n}", else: nil)}
        end)

      {endpoint, _} = WireServer.fixture([{200, [], listing(entries, versions)}])

      assert {:error, %Error{code: :native_transfer_timestamp}} =
               HighLevel.run(
                 invocation(%{recursive: true, all_versions: versions, older_than: "1d"}),
                 [target("prefix")],
                 [context(endpoint)],
                 opts(root)
               )

      assert_receive {:wire_request, %{method: "GET"}}
      refute_receive {:wire_request, _}
      assert Path.wildcard(Path.join(root, "ess-*")) == []
    end
  end

  test "a current candidate whose timestamp changed is preserved even when ETag matches", %{
    tmp_dir: root
  } do
    {endpoint, _} =
      WireServer.fixture([
        {200, [], listing([{"prefix/key", @old, nil}])},
        head("Fri, 02 Jan 2026 12:00:00 GMT")
      ])

    assert {:error, %Error{code: :inventory_changed}} =
             HighLevel.run(
               invocation(%{recursive: true, older_than: "1d"}),
               [target("prefix")],
               [context(endpoint)],
               opts(root)
             )

    assert_receive {:wire_request, %{method: "GET"}}
    assert_receive {:wire_request, %{method: "HEAD"}}
    refute_receive {:wire_request, _}
  end

  test "invalid ages reject before network and exclusions require no HEAD", %{tmp_dir: root} do
    assert {:error, %Error{code: :native_transfer_time}} =
             HighLevel.run(
               invocation(%{older_than: "invalid"}),
               [target()],
               [%Context{}],
               opts(root)
             )

    excluded = %{invocation(%{older_than: "1d"}) | filters: [exclude: "*"]}
    assert :ok = HighLevel.run(excluded, [target()], [%Context{}], opts(root))
    refute_receive {:wire_request, _}
  end
end
