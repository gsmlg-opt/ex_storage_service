defmodule ExStorageServiceCli.CLI.FindTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.CLI.{Find, HighLevel, Invocation}
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.Transfer.Filter
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00Z]

  test "portable path and ignore predicates combine with literal-component name fallback", %{
    tmp_dir: root
  } do
    records = [
      item("p/docs/a.txt"),
      item("p/docs/private.txt"),
      item("p/other/docs/b.txt"),
      item("p/image.png")
    ]

    assert {:ok, %{"Entries" => ["docs/a.txt"]}} =
             find(root, records, %{name: "docs", path: "docs/*", ignore: "*private*"})

    assert {:ok, %{"Entries" => ["docs/a.txt", "other/docs/b.txt"]}} =
             find(root, records, %{name: "*.txt", ignore: "*private*"})

    assert File.ls!(root) == []
  end

  test "maxdepth projects paths before predicates and retains repeated matching object projections",
       %{tmp_dir: root} do
    records = [
      item("p/top.txt"),
      item("p/a/one.txt"),
      item("p/a/two.bin"),
      item("p/a/b/deep.txt")
    ]

    assert {:ok, %{"Entries" => ["a/", "a/", "a/", "top.txt"], "MatchedObjects" => 4}} =
             find(root, records, %{maxdepth: 1})

    assert {:ok, %{"Entries" => ["a/b/", "a/one.txt", "a/two.bin", "top.txt"]}} =
             find(root, records, %{maxdepth: 2})

    assert {:ok, %{"Entries" => ["top.txt"]}} = find(root, records, %{maxdepth: 1, name: "*.txt"})

    assert {:ok, %{"Entries" => ["a/b/deep.txt", "a/one.txt", "a/two.bin", "top.txt"]}} =
             find(root, records, %{maxdepth: 0})
  end

  test "size predicates use strict comparisons, metric/IEC units and decimal truncation", %{
    tmp_dir: root
  } do
    records = [
      item("p/below", 999),
      item("p/exact", 1000),
      item("p/within", 1023),
      item("p/upper", 1024)
    ]

    assert {:ok, %{"Entries" => ["within"]}} =
             find(root, records, %{larger: "1KB", smaller: "1kiB"})

    assert {:ok, %{"Entries" => ["below", "exact", "within"]}} =
             find(root, records, %{larger: "0", smaller: "1.024k"})

    assert {:ok, %{"MatchedObjects" => 4}} = find(root, records, %{smaller: "0"})
  end

  test "time predicates freeze now and use pinned older inclusive and newer exclusive boundaries",
       %{tmp_dir: root} do
    records = [
      item("p/old", 1, ~U[2026-10-13 00:00:00Z]),
      item("p/boundary", 1, ~U[2026-10-14 00:00:00Z]),
      item("p/recent", 1, ~U[2026-10-14 23:00:00Z])
    ]

    assert {:ok, %{"Entries" => ["boundary", "old"]}} = find(root, records, %{older_than: "1d"})
    assert {:ok, %{"Entries" => ["recent"]}} = find(root, records, %{newer_than: "24h"})

    assert {:ok, %{"Entries" => ["boundary"]}} =
             find(root, records, %{older_than: "1d", newer_than: "1d12h30m"})

    assert {:ok, %{"Entries" => ["recent"]}} =
             find(root, records, %{newer_than: "2026.10.14T12:00:00"})

    assert {:ok, %{"Entries" => ["boundary", "old"]}} =
             find(root, records, %{older_than: "2026-10-14T00:00:00Z"})

    assert {:ok, %{"Entries" => ["recent"]}} = find(root, records, %{newer_than: "0.5d"})
  end

  test "missing provider timestamps fail explicitly when a time predicate is selected", %{
    tmp_dir: root
  } do
    assert {:error, %Error{code: :find_timestamp}} =
             find(root, [Map.delete(item("p/a"), "LastModified")], %{older_than: "1d"})

    assert File.ls!(root) == []
  end

  test "bounded Unicode regular expressions match the supported common RE2 subset", %{
    tmp_dir: root
  } do
    records = [item("p/docs/雪.JPG"), item("p/docs/file.png"), item("p/docs/file.txt")]

    assert {:ok, %{"Entries" => ["docs/file.png", "docs/雪.JPG"]}} =
             find(root, records, %{regex: "(?i)\\.(jpg|png)$"})

    assert {:ok, %{"Entries" => ["docs/雪.JPG"]}} = find(root, records, %{regex: "雪"})
  end

  test "invalid filters and unsupported regex constructs fail before inventory requests", %{
    tmp_dir: root
  } do
    fetch = fn _, _ -> flunk("invalid predicates must fail before listing") end

    for options <- [
          %{larger: "-1"},
          %{smaller: "1xyz"},
          %{larger: "18446744073709551616"},
          %{older_than: "yesterday"},
          %{newer_than: "999999999999y"},
          %{maxdepth: -1},
          %{limit: 10001},
          %{regex: "["},
          %{regex: "(a)\\1"},
          %{regex: "(?<=x)y"},
          %{regex: String.duplicate("a", 4097)}
        ] do
      assert {:error, %Error{}} = Find.run(remote(), nil, settings(root, options, fetch))
    end

    assert File.ls!(root) == []
  end

  test "catastrophic regex backtracking returns a typed budget error and cleans manifests", %{
    tmp_dir: root
  } do
    assert {:error, %Error{code: :find_regex_budget}} =
             find(root, [item("p/" <> String.duplicate("a", 1000) <> "!")], %{regex: "(a+)+$"})

    assert File.ls!(root) == []
  end

  test "bounded results count all matches without retaining them and indicate partial output", %{
    tmp_dir: root
  } do
    records = for n <- 1..20, do: item("p/" <> String.pad_leading(to_string(n), 2, "0"))

    assert {:ok,
            %{
              "Entries" => ["01", "02"],
              "Returned" => 2,
              "MatchedObjects" => 20,
              "Partial" => true
            }} = find(root, records, %{limit: 2})

    assert {:ok, %{"Entries" => [], "Returned" => 0, "MatchedObjects" => 20, "Partial" => true}} =
             find(root, records, %{limit: 0})

    assert File.ls!(root) == []
  end

  test "versions retain immutable IDs, marker identity and modification times across pages", %{
    tmp_dir: root
  } do
    fetch = fn
      "ListObjectVersions", %{"KeyMarker" => "p/a"} ->
        {:ok, %{"Versions" => [version("old", ~U[2026-10-13 00:00:00Z])], "IsTruncated" => false}}

      "ListObjectVersions", input ->
        assert input["Prefix"] == "p/"

        {:ok,
         %{
           "Versions" => [version("new", ~U[2026-10-14 23:00:00Z])],
           "DeleteMarkers" => [%{"Key" => "p/a", "VersionId" => "marker", "LastModified" => @now}],
           "IsTruncated" => true,
           "NextKeyMarker" => "p/a",
           "NextVersionIdMarker" => "marker"
         }}
    end

    assert {:ok, %{"Entries" => entries, "MatchedObjects" => 3, "Partial" => false}} =
             Find.run(remote(), nil, settings(root, %{versions: true}, fetch))

    assert Enum.sort(
             Enum.map(entries, &{&1["version_id"], &1["delete_marker"], &1["key"], &1["name"]})
           ) == [
             {"marker", true, "p/a", "a"},
             {"new", false, "p/a", "a"},
             {"old", false, "p/a", "a"}
           ]

    assert Enum.all?(entries, &(is_integer(&1["modified"]) and &1["modified"] > 0))

    assert {:ok, %{"Entries" => [%{"version_id" => "old", "delete_marker" => false}]}} =
             Find.run(remote(), nil, settings(root, %{versions: true, older_than: "1d"}, fetch))

    assert File.ls!(root) == []
  end

  test "version listing errors on later pages cannot report successful partial completion", %{
    tmp_dir: root
  } do
    error = Error.new(:transport, "later page unavailable")

    fetch = fn
      _, %{"KeyMarker" => _} ->
        {:error, error}

      _, _ ->
        {:ok,
         %{
           "Versions" => [version("v", @now)],
           "IsTruncated" => true,
           "NextKeyMarker" => "p/a",
           "NextVersionIdMarker" => "v"
         }}
    end

    assert {:error, ^error} = Find.run(remote(), nil, settings(root, %{versions: true}, fetch))
    assert File.ls!(root) == []
  end

  test "local file timestamps, relative names and size predicates use the shared bounded inventory",
       %{tmp_dir: root} do
    data = Path.join(root, "files")
    File.mkdir_p!(Path.join(data, "nested"))
    file = Path.join(data, "nested/one.txt")
    File.write!(file, "hello")
    File.touch!(file, DateTime.to_unix(~U[2026-10-13 00:00:00Z]))
    File.write!(Path.join(data, "other.bin"), "x")

    assert {:ok, %{"Entries" => ["nested/one.txt"]}} =
             Find.run(
               %Target{kind: :local, path: data},
               nil,
               settings(root, %{path: "nested/*", larger: "4", older_than: "1d"}, nil)
             )

    assert {:error, %Error{code: :find_versions}} =
             Find.run(
               %Target{kind: :local, path: data},
               nil,
               settings(root, %{versions: true}, nil)
             )

    refute Enum.any?(File.ls!(root), &String.starts_with?(&1, ".ess-inventory-"))
  end

  test "HighLevel find delegates predicates while its du behavior stays unchanged", %{
    tmp_dir: root
  } do
    fetch = fn "ListObjectsV2", _ ->
      {:ok, %{"Contents" => [item("p/a.txt", 2), item("p/b.bin", 3)], "IsTruncated" => false}}
    end

    opts = [fetch: fetch, tmp_dir: root, now: @now]
    invocation = %Invocation{frontend: :native, command: ["find"], options: %{path: "*.txt"}}

    assert {:ok, %{"Entries" => ["a.txt"]}} =
             HighLevel.run(invocation, [remote()], [%Context{unsigned: true}], opts)

    invocation = %Invocation{frontend: :native, command: ["du"], options: %{limit: 1}}

    assert {:ok, %{"Objects" => 1, "Bytes" => 2, "Partial" => true, "MatchedObjects" => 2}} =
             HighLevel.run(invocation, [remote()], [%Context{unsigned: true}], opts)
  end

  defp find(root, records, options) do
    fetch = fn "ListObjectsV2", _ -> {:ok, %{"Contents" => records, "IsTruncated" => false}} end
    Find.run(remote(), nil, settings(root, options, fetch))
  end

  defp settings(root, options, fetch) do
    {:ok, filters} = Filter.compile([])

    %{
      options: options,
      filters: filters,
      input: %{input: %{}},
      opts: [tmp_dir: root, fetch: fetch, now: @now]
    }
  end

  defp remote, do: %Target{kind: :remote, bucket: "bucket", key: "p/"}

  defp item(key, size \\ 1, modified \\ @now),
    do: %{"Key" => key, "Size" => size, "ETag" => "e", "LastModified" => modified}

  defp version(id, modified), do: item("p/a", 1, modified) |> Map.put("VersionId", id)
end
