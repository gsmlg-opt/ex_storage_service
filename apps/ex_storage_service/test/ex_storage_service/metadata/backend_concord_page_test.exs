defmodule ExStorageService.Metadata.Backend.ConcordPageTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend

  test "lists a prefix in bounded non-overlapping pages" do
    suffix = System.unique_integer([:positive, :monotonic])
    prefix = "test:concord-page:#{suffix}:"
    keys = Enum.map(1..5, &"#{prefix}#{&1}")

    on_exit(fn -> Enum.each(keys, &Concord.delete/1) end)
    Enum.each(keys, &Concord.put(&1, %{key: &1}))

    assert {:ok, %{entries: first, next_cursor: cursor}} =
             ConcordBackend.list_page(prefix, nil, 2)

    assert Enum.map(first, & &1.key) == Enum.take(keys, 2)
    assert is_binary(cursor)

    assert {:ok, %{entries: second, next_cursor: next_cursor}} =
             ConcordBackend.list_page(prefix, cursor, 2)

    assert Enum.map(second, & &1.key) == Enum.slice(keys, 2, 2)

    assert {:ok, %{entries: last, next_cursor: nil}} =
             ConcordBackend.list_page(prefix, next_cursor, 2)

    assert Enum.map(last, & &1.key) == Enum.drop(keys, 4)
  end
end
