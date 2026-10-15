defmodule ExStorageServiceCli.Transfer.VersionSelectionTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Context, Error, Target}
  alias ExStorageServiceCli.Transfer.{Manifest, VersionSelection}
  @moduletag :tmp_dir
  @now ~U[2026-10-15 00:00:00.000000Z]
  @cutoff DateTime.to_unix(@now, :nanosecond)

  defp entry(key, version, age, marker \\ false) do
    %{
      "name" => JSON.encode!([key, version]),
      "object_name" => key,
      "key" => key,
      "version_id" => version,
      "modified" => div(@cutoff, 1000) - age,
      "delete_marker" => marker,
      "etag" => "\"#{version}\"",
      "size" => if(marker, do: 0, else: 7)
    }
  end

  defp selected(entries, root, cutoff \\ @cutoff) do
    assert {:ok, versions} = Manifest.build(entries, tmp_dir: root)

    try do
      case VersionSelection.select(versions, cutoff, tmp_dir: root) do
        {:ok, snapshot} ->
          try do
            {:ok, Enum.to_list(Manifest.stream(snapshot))}
          after
            Manifest.cleanup(snapshot)
          end

        error ->
          error
      end
    after
      Manifest.cleanup(versions)
    end
  end

  test "bounded shared timestamp grammar retains exact duration precision" do
    assert {:ok, @cutoff} = VersionSelection.cutoff("2026-10-15T00:00:00Z")
    assert {:ok, @cutoff} = VersionSelection.cutoff("2026.10.15")
    assert {:ok, nanos} = VersionSelection.cutoff("1d2h3m4.000000001s", now: @now)
    assert nanos == @cutoff - 93_784_000_000_001
    assert {:ok, @cutoff} = VersionSelection.cutoff("0", now: @now)

    for value <- [nil, "", "-1d", "1D", "not-a-date", String.duplicate("1", 300)] do
      assert {:error, %Error{code: :rewind_time}} = VersionSelection.cutoff(value, now: @now)
    end
  end

  test "chooses newest prior versions independently of version identifier ordering", %{
    tmp_dir: root
  } do
    records = [
      entry("a", "z-new", -1),
      entry("a", "b-old", 20),
      entry("a", "a-selected", 10),
      entry("雪//../%+", "one", 1)
    ]

    assert {:ok, [a, b]} = selected(records, root)
    assert a["version_id"] == "a-selected"
    assert b["key"] == "雪//../%+"
    assert Enum.map([a, b], & &1["name"]) == ["a", "雪//../%+"]
    assert File.ls!(root) == []
  end

  test "equality is excluded while a one-nanosecond later cutoff includes it", %{tmp_dir: root} do
    records = [entry("a", "boundary", 0), entry("a", "older", 1)]
    assert {:ok, [%{"version_id" => "older"}]} = selected(records, root)
    assert {:ok, [%{"version_id" => "boundary"}]} = selected(records, root, @cutoff + 1)
  end

  test "historical deletion is a tombstone and later recreation is ignored", %{tmp_dir: root} do
    records = [
      entry("a", "old", 30),
      entry("a", "deleted", 20, true),
      entry("a", "new", -1),
      entry("b", "old", 30),
      entry("b", "future-delete", -1, true)
    ]

    assert {:ok, [%{"key" => "b", "version_id" => "old"}]} = selected(records, root)
  end

  test "inspection may retain selected tombstones without changing transfer defaults", %{
    tmp_dir: root
  } do
    assert {:ok, versions} =
             Manifest.build([entry("a", "old", 30), entry("a", "deleted", 20, true)],
               tmp_dir: root
             )

    try do
      assert {:ok, snapshot} =
               VersionSelection.select(versions, @cutoff,
                 tmp_dir: root,
                 include_delete_markers: true
               )

      try do
        assert [%{"key" => "a", "version_id" => "deleted", "delete_marker" => true}] =
                 Enum.to_list(Manifest.stream(snapshot))
      after
        Manifest.cleanup(snapshot)
      end
    after
      Manifest.cleanup(versions)
    end

    assert File.ls!(root) == []
  end

  test "selected timestamp ambiguity fails closed and cleans every manifest", %{tmp_dir: root} do
    assert {:error, %Error{code: :rewind_ambiguous}} =
             selected([entry("a", "one", 1), entry("a", "two", 1)], root)

    assert File.ls!(root) == []
    # Older ties do not make a unique newer selected version ambiguous.
    for version <- ["a-new", "z-new"] do
      assert {:ok, [%{"version_id" => ^version}]} =
               selected(
                 [entry("a", "b-old", 5), entry("a", "c-old", 5), entry("a", version, 1)],
                 root
               )
    end
  end

  test "missing timestamp or identity fails before publishing a snapshot", %{tmp_dir: root} do
    for invalid <- [
          Map.put(entry("a", "one", 1), "modified", 0),
          Map.put(entry("a", "one", 1), "version_id", "")
        ] do
      assert {:error, %Error{code: :rewind_inventory}} = selected([invalid], root)
      assert File.ls!(root) == []
    end
  end

  test "many versions and keys are reduced through bounded sorted disk runs", %{tmp_dir: root} do
    records =
      for key <- 1..500,
          version <- 1..10,
          do: entry(String.pad_leading(to_string(key), 4, "0"), to_string(version), version)

    assert {:ok, rows} = selected(Enum.reverse(records), root)
    assert length(rows) == 500
    assert Enum.all?(rows, &(&1["version_id"] == "1"))
    assert File.ls!(root) == []
  end

  test "complete version inventory precedes selected identity and exact keys stay exact", %{
    tmp_dir: root
  } do
    owner = self()

    fetch = fn "ListObjectVersions", input ->
      send(owner, {:input, input})

      {:ok,
       %{
         "IsTruncated" => false,
         "Versions" => [
           %{
             "Key" => "a",
             "VersionId" => "old",
             "LastModified" => "2026-10-14T00:00:00Z",
             "ETag" => "\"old\"",
             "Size" => 7
           },
           %{
             "Key" => "ab",
             "VersionId" => "other",
             "LastModified" => "2026-10-14T00:00:00Z",
             "Size" => 8
           }
         ]
       }}
    end

    target = %Target{kind: :remote, bucket: "bucket", key: "a"}

    assert {:ok, %{"key" => "a", "version_id" => "old", "etag" => "\"old\""}} =
             VersionSelection.single(%Context{}, target, "2026-10-15T00:00:00Z",
               fetch: fetch,
               tmp_dir: root
             )

    assert_receive {:input, %{"Bucket" => "bucket", "Prefix" => "a"}}
    assert File.ls!(root) == []
  end

  test "listing failure never produces a partial historical selection", %{tmp_dir: root} do
    fetch = fn "ListObjectVersions", _ ->
      {:error, Error.new(:service, "listing failed", code: "AccessDenied")}
    end

    target = %Target{kind: :remote, bucket: "bucket", key: "a"}

    assert {:error, %Error{code: "AccessDenied"}} =
             VersionSelection.single(%Context{}, target, "2026-10-15T00:00:00Z",
               fetch: fetch,
               tmp_dir: root
             )

    assert File.ls!(root) == []
  end

  test "tombstoned or not-yet-created exact source is absent", %{tmp_dir: root} do
    fetch = fn "ListObjectVersions", _ -> {:ok, %{"IsTruncated" => false}} end
    target = %Target{kind: :remote, bucket: "bucket", key: "a"}

    assert {:error, %Error{code: :rewind_not_found}} =
             VersionSelection.single(%Context{}, target, "2026-10-15T00:00:00Z",
               fetch: fetch,
               tmp_dir: root
             )

    assert File.ls!(root) == []
  end
end
