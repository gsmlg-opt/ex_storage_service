defmodule ExStorageService.Storage.VersionListingTest do
  use ExUnit.Case, async: true
  alias ExStorageService.Storage.VersionListing
  alias ExStorageService.Metadata.Keys
  @bucket "versions"

  defmodule FailedBackend do
    def get(_key, _opts), do: {:error, :timeout}
  end

  test "versions order by key and newest version with exactly the current head marked latest" do
    snapshot =
      base()
      |> version("b", "b-old", 1)
      |> version("a", "a-old", 1)
      |> version("a", "a-current", 2)
      |> version("a", "marker", 3, true)
      |> head("a", "marker")
      |> head("b", "b-old")

    assert {:ok, page} = VersionListing.page(snapshot, @bucket)

    assert Enum.map(page.entries, &{&1.key, &1.version_id, &1.is_latest, &1.delete_marker}) ==
             [
               {"a", "marker", true, true},
               {"a", "a-current", false, false},
               {"a", "a-old", false, false},
               {"b", "b-old", true, false}
             ]

    refute page.is_truncated
  end

  test "key and version markers visit every version and marker once across pages" do
    snapshot =
      base()
      |> version("a", "old", 1)
      |> version("a", "new", 2)
      |> version("a", "marker", 3, true)
      |> version("b", "only", 1)
      |> head("a", "marker")
      |> head("b", "only")

    assert {:ok, first} = VersionListing.page(snapshot, @bucket, max_keys: 1)
    assert first.next_key_marker == "a"
    assert first.next_version_id_marker == "marker"
    assert first.is_truncated

    assert {:ok, second} =
             VersionListing.page(snapshot, @bucket,
               max_keys: 2,
               key_marker: first.next_key_marker,
               version_id_marker: first.next_version_id_marker
             )

    assert Enum.map(second.entries, & &1.version_id) == ["new", "old"]
    assert second.next_version_id_marker == "old"

    assert {:ok, last} =
             VersionListing.page(snapshot, @bucket,
               max_keys: 2,
               key_marker: second.next_key_marker,
               version_id_marker: second.next_version_id_marker
             )

    assert Enum.map(last.entries, & &1.version_id) == ["only"]
    refute last.is_truncated
    assert {:ok, skipped_key} = VersionListing.page(snapshot, @bucket, key_marker: "a")
    assert Enum.map(skipped_key.entries, & &1.key) == ["b"]
  end

  test "common prefixes count once and can be the entire page with a resumable key marker" do
    snapshot =
      base()
      |> version("root/a/one", "one", 1)
      |> version("root/a/one", "two", 2)
      |> version("root/a/two", "three", 3)
      |> version("root/b/item", "four", 4)
      |> version("root/z", "five", 5)
      |> version("unrelated", "six", 6)

    assert {:ok, first} =
             VersionListing.page(snapshot, @bucket, prefix: "root/", delimiter: "/", max_keys: 1)

    assert first.entries == []
    assert first.common_prefixes == ["root/a/"]
    assert first.next_key_marker == "root/a/"
    assert first.next_version_id_marker == nil
    assert first.is_truncated

    assert {:ok, next} =
             VersionListing.page(snapshot, @bucket,
               prefix: "root/",
               delimiter: "/",
               max_keys: 2,
               key_marker: first.next_key_marker
             )

    assert next.common_prefixes == ["root/b/"]
    assert Enum.map(next.entries, & &1.key) == ["root/z"]
    refute next.is_truncated
  end

  test "legacy null and indexed plus unindexed versions are all visible without newer v2 state" do
    snapshot =
      base()
      |> Map.put("obj:#{@bucket}:plain", metadata("plain", 1))
      |> Map.put("obj_ver:#{@bucket}:legacy:colon:old", metadata("old", 1))
      |> Map.put("obj_ver:#{@bucket}:legacy:colon:new", metadata("new", 2))
      |> Map.put("obj_ver_list:#{@bucket}:legacy:colon", ["new"])

    assert {:ok, page} = VersionListing.page(snapshot, @bucket)

    assert Enum.map(page.entries, &{&1.key, &1.version_id, &1.is_latest}) ==
             [
               {"legacy:colon", "new", true},
               {"legacy:colon", "old", false},
               {"plain", "null", true}
             ]
  end

  test "retained v2 tombstones and newer v2 state suppress legacy resurrection" do
    snapshot =
      base()
      |> Map.put("obj:#{@bucket}:removed", metadata("legacy", 1))
      |> Map.put("obj_ver:#{@bucket}:removed:old", metadata("legacy-old", 1))
      |> Map.put("obj_ver_list:#{@bucket}:removed", ["old"])
      |> head("removed", nil)
      |> Map.put("obj:#{@bucket}:current", metadata("legacy", 1))
      |> version("current", "v2", 2)
      |> head("current", "v2")

    assert {:ok, page} = VersionListing.page(snapshot, @bucket)
    assert Enum.map(page.entries, &{&1.key, &1.version_id}) == [{"current", "v2"}]
    assert Map.has_key?(snapshot, "obj:#{@bucket}:removed")
  end

  test "null versions and null delete markers are ordinary cursor identities" do
    snapshot =
      base() |> version("a", "numbered", 1) |> version("a", "null", 2, true) |> head("a", "null")

    assert {:ok, page} = VersionListing.page(snapshot, @bucket, max_keys: 1)
    assert [%{version_id: "null", is_latest: true, delete_marker: true}] = page.entries

    assert {:ok, next} =
             VersionListing.page(snapshot, @bucket, key_marker: "a", version_id_marker: "null")

    assert [%{version_id: "numbered", is_latest: false}] = next.entries
  end

  test "zero pages, maximum cap, missing buckets, invalid options and failures are explicit" do
    snapshot = base() |> version("a", "id", 1) |> head("a", "id")

    assert {:ok, %{entries: [], common_prefixes: [], is_truncated: false, next_key_marker: nil}} =
             VersionListing.page(snapshot, @bucket, max_keys: 0)

    assert {:ok, %{max_keys: 1000}} = VersionListing.page(snapshot, @bucket, max_keys: 9999)

    for opts <- [
          [max_keys: -1],
          [max_keys: "bad"],
          [version_id_marker: "id"],
          [key_marker: "a", version_id_marker: "unknown"],
          [encoding_type: "base64"],
          [prefix: <<255>>]
        ] do
      assert {:error, :invalid_argument} = VersionListing.page(snapshot, @bucket, opts)
    end

    assert {:error, :bucket_not_found} = VersionListing.page(snapshot, "missing")
    assert {:error, :timeout} = VersionListing.list(@bucket, backend: FailedBackend)
  end

  test "a tied timestamp never lets a lexical version id displace the actual latest head" do
    snapshot =
      base() |> version("a", "zz-old", 1) |> version("a", "aa-new", 1) |> head("a", "aa-new")

    assert {:ok, %{entries: [latest, historical]}} = VersionListing.page(snapshot, @bucket)
    assert latest.version_id == "aa-new" and latest.is_latest
    assert historical.version_id == "zz-old" and not historical.is_latest
  end

  defp base, do: %{"bucket:#{@bucket}" => %{name: @bucket}}

  defp metadata(etag, second),
    do: %{
      etag: etag,
      size: byte_size(etag),
      created_at: "2026-10-18T00:00:#{String.pad_leading(to_string(second), 2, "0")}Z"
    }

  defp version(snapshot, key, id, second, marker \\ false),
    do:
      Map.put(
        snapshot,
        Keys.object_version(@bucket, key, id),
        Map.put(metadata(id, second), :delete_marker, marker)
      )

  defp head(snapshot, key, id),
    do: Map.put(snapshot, Keys.object_head(@bucket, key), %{version_id: id})
end
