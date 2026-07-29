defmodule ExStorageService.Metadata.BlobCatalogTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{BlobCatalog, Keys}

  defmodule Backend do
    def start_link(entries), do: Agent.start_link(fn -> Map.new(entries) end)

    def get(key, opts) do
      case Agent.get(opts[:engine], &Map.get(&1, key)) do
        nil -> {:ok, nil}
        record -> {:ok, record}
      end
    end

    def list_page(prefix, cursor, limit, opts) do
      entries =
        opts[:engine]
        |> Agent.get(& &1)
        |> Enum.filter(fn {key, _record} -> String.starts_with?(key, prefix) end)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.drop_while(fn {key, _record} -> cursor && key <= cursor end)

      selected = Enum.take(entries, limit)
      has_more = length(entries) > length(selected)

      page_entries =
        Enum.map(selected, fn {key, record} ->
          %{key: key, value: record.value, mod_revision: record.mod_revision}
        end)

      next_cursor =
        if has_more, do: selected |> List.last() |> elem(0), else: nil

      {:ok, %{entries: page_entries, next_cursor: next_cursor}}
    end
  end

  test "descriptor pages stay inside one hash shard and expose a cursor" do
    entries =
      for {hash, revision} <- [{"00aa", 1}, {"00bb", 2}, {"01cc", 3}] do
        descriptor = %{
          schema: 2,
          hash: hash,
          algorithm: :sha256,
          size: revision,
          desired_replication_factor: 2,
          created_at: "2026-07-29T00:00:00Z"
        }

        {Keys.blob(hash), %{value: descriptor, mod_revision: revision}}
      end

    {:ok, engine} = Backend.start_link(entries)
    opts = [backend: Backend, engine: engine]

    assert {:ok, %{records: [%{descriptor: %{hash: "00aa"}}], next_cursor: cursor}} =
             BlobCatalog.list_page("00", nil, 1, opts)

    assert is_binary(cursor)

    assert {:ok, %{records: [%{descriptor: %{hash: "00bb"}}], next_cursor: nil}} =
             BlobCatalog.list_page("00", cursor, 1, opts)

    assert {:ok, %{records: [%{descriptor: %{hash: "01cc"}}]}} =
             BlobCatalog.list_page("01", nil, 10, opts)
  end

  test "get rejects a descriptor whose stored hash disagrees with its key" do
    key = Keys.blob("00aa")

    {:ok, engine} =
      Backend.start_link([
        {key,
         %{
           value: %{
             schema: 2,
             hash: "different",
             algorithm: :sha256,
             size: 1,
             desired_replication_factor: 1
           },
           mod_revision: 1
         }}
      ])

    assert {:error, :invalid_blob_descriptor} =
             BlobCatalog.get("00aa", backend: Backend, engine: engine)
  end
end
