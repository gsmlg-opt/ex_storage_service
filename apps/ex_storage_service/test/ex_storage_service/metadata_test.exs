defmodule ExStorageService.MetadataTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata

  setup do
    bucket = "list-test-#{:erlang.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)

    on_exit(fn ->
      case Metadata.list_objects(bucket, max_keys: 100_000) do
        {:ok, %{keys: keys}} ->
          Enum.each(keys, fn {key, _meta} -> Metadata.delete_object_meta(bucket, key) end)

        _ ->
          :ok
      end

      Metadata.delete_bucket(bucket)
    end)

    %{bucket: bucket}
  end

  defp put_keys(bucket, keys) do
    Enum.each(keys, fn key ->
      Metadata.put_object_meta(bucket, key, %{content_hash: "h", size: 0, etag: "e"})
    end)
  end

  describe "list_objects with a delimiter" do
    test "collapses keys into common prefixes and lists bare keys", %{bucket: bucket} do
      put_keys(bucket, ["a/1", "a/2", "b/1", "top"])

      {:ok, result} = Metadata.list_objects(bucket, delimiter: "/")

      assert result.common_prefixes == ["a/", "b/"]
      assert Enum.map(result.keys, fn {k, _} -> k end) == ["top"]
      refute result.is_truncated
    end

    test "paginates keys and common prefixes together with a round-tripping cursor",
         %{bucket: bucket} do
      # 2 common prefixes (a/, b/) + 2 bare keys (m, z) = 4 ordered items.
      put_keys(bucket, ["a/1", "a/2", "b/1", "m", "z"])

      {:ok, page1} = Metadata.list_objects(bucket, delimiter: "/", max_keys: 2)

      assert page1.is_truncated
      assert page1.common_prefixes == ["a/", "b/"]
      assert page1.keys == []
      assert page1.next_continuation_token == "b/"

      {:ok, page2} =
        Metadata.list_objects(bucket,
          delimiter: "/",
          max_keys: 2,
          continuation_token: page1.next_continuation_token
        )

      refute page2.is_truncated
      assert page2.common_prefixes == []
      assert Enum.map(page2.keys, fn {k, _} -> k end) == ["m", "z"]
    end
  end

  describe "list_objects without a delimiter" do
    test "truncates on max_keys and resumes from the continuation token", %{bucket: bucket} do
      put_keys(bucket, ["k1", "k2", "k3"])

      {:ok, page1} = Metadata.list_objects(bucket, max_keys: 2)
      assert page1.is_truncated
      assert Enum.map(page1.keys, fn {k, _} -> k end) == ["k1", "k2"]
      assert page1.next_continuation_token == "k2"

      {:ok, page2} =
        Metadata.list_objects(bucket,
          max_keys: 2,
          continuation_token: page1.next_continuation_token
        )

      refute page2.is_truncated
      assert Enum.map(page2.keys, fn {k, _} -> k end) == ["k3"]
    end
  end
end
