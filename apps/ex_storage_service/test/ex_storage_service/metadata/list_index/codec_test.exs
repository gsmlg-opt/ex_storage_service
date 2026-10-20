defmodule ExStorageService.Metadata.ListIndex.CodecTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.ListIndex.Codec

  @values ["", <<0>>, <<0, 255>>, "a", "a+", "a%", "a:", "aa", <<255>>, "資料/😀"]

  test "ascending and descending components preserve byte order including prefixes" do
    expected = Enum.sort(@values)
    assert Enum.sort_by(@values, &Codec.ascending/1) == expected
    assert Enum.sort_by(@values, &Codec.descending/1) == Enum.reverse(expected)
    assert Enum.all?(@values, &(Codec.current(&1) == &1))

    for prefix <- @values, suffix <- @values do
      assert starts_with?(Codec.ascending(prefix <> suffix), Codec.key_prefix(prefix))
    end
  end

  test "version tuples match current latest-first, timestamp-descending, ID-descending order" do
    rows =
      for key <- @values,
          latest <- [false, true],
          timestamp <- ["", "2026-01-01", "2026-01-02"],
          id <- ["", "null", "v1", "v10", <<0, 255>>] do
        %{key: key, is_latest: latest, last_modified: timestamp, version_id: id}
      end

    expected =
      Enum.sort_by(rows, fn row ->
        {row.key, if(row.is_latest, do: 0, else: 1), Codec.descending(row.last_modified),
         Codec.descending(row.version_id)}
      end)

    encoded =
      Enum.map(rows, fn row ->
        Codec.version(row.key, row.version_id, %{created_at: row.last_modified}, row.is_latest)
      end)

    assert Enum.map(Enum.sort(encoded), fn key ->
             assert {:ok, row} = Codec.decode_version(key)
             row
           end) == expected

    for row <- rows do
      encoded =
        Codec.version(row.key, row.version_id, %{created_at: row.last_modified}, row.is_latest)

      assert starts_with?(encoded, Codec.version_prefix(row.key))
    end
  end

  test "timestamp fallback matches the existing listing accessor without inventing dates" do
    assert Codec.last_modified(%{}) == ""
    assert Codec.last_modified(%{updated_at: "old"}) == "old"
    assert Codec.last_modified(%{created_at: "first", updated_at: "later"}) == "first"

    assert_raise ArgumentError, fn ->
      Codec.last_modified(%{created_at: nil, updated_at: "later"})
    end
  end

  test "upload tuples and fixed-width parts round trip in protocol order" do
    uploads = for key <- @values, id <- @values, do: {key, id}
    sorted = Enum.sort_by(uploads, fn {key, id} -> Codec.upload(key, id) end)
    assert sorted == Enum.sort(uploads)

    for {key, id} <- uploads do
      assert {:ok, %{key: ^key, upload_id: ^id}} = Codec.decode_upload(Codec.upload(key, id))
    end

    assert Enum.sort_by([10_000, 999, 1, 256, 255], &Codec.part/1) == [1, 255, 256, 999, 10_000]

    for number <- [1, 255, 256, 10_000],
        do: assert({:ok, ^number} = Codec.decode_part(Codec.part(number)))

    assert {:error, :invalid_index_key} = Codec.decode_part(<<0, 0>>)
    assert {:error, :invalid_index_key} = Codec.decode_part(<<255, 255>>)
  end

  test "raw prefix successor skips the entire descendant range" do
    assert Codec.prefix_successor("") == nil
    assert Codec.prefix_successor(<<255, 255>>) == nil
    assert Codec.prefix_successor("folder/") == "folder0"
    assert Codec.prefix_successor(<<0, 255, 255>>) == <<1>>
    assert Codec.prefix_successor(<<1, 254, 255>>) == <<1, 255>>

    for prefix <- ["a", "folder/", <<0, 255>>], suffix <- @values do
      successor = Codec.prefix_successor(prefix)
      assert prefix <> suffix < successor
      assert Codec.key_prefix(prefix <> suffix) < Codec.key_prefix(successor)
    end
  end

  test "malformed, noncanonical and trailing tuple bytes are rejected" do
    for encoded <- ["", <<1>>, <<2, 0>>, <<0, 2, 255, 255>>, Codec.upload("a", "b") <> <<0>>] do
      assert {:error, :invalid_index_key} = Codec.decode_version(encoded)
    end

    for encoded <- ["", <<1>>, <<2, 0>>, Codec.upload("a", "b") <> <<1, 0>>] do
      assert {:error, :invalid_index_key} = Codec.decode_upload(encoded)
    end

    encoded = Codec.version("a", "v", %{created_at: "now"}, true)
    assert {:error, :invalid_index_key} = Codec.decode_version(encoded <> <<0>>)
  end

  test "maximum public key and long legacy identities round trip without rewriting" do
    key = String.duplicate("k", 1024)
    id = String.duplicate("v", 1024)
    encoded = Codec.version(key, id, %{created_at: "2026-10-18T00:00:00.123456Z"}, false)
    assert {:ok, %{key: ^key, version_id: ^id}} = Codec.decode_version(encoded)
  end

  defp starts_with?(value, prefix),
    do:
      byte_size(value) >= byte_size(prefix) and binary_part(value, 0, byte_size(prefix)) == prefix
end
