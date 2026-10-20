defmodule ExStorageService.Metadata.ListIndexTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{Keys, ListIndex}
  alias ExStorageService.Metadata.ListIndex.Codec

  defmodule ExactBackend do
    def get(key, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        state = %{state | reads: state.reads + 1}

        state =
          if state.churn and String.contains?(key, ":node:") do
            update_in(state.records[state.root_key].mod_revision, &(&1 + 1))
          else
            state
          end

        {{:ok, state.records[key]}, state}
      end)
    end

    def commit(engine, spec) do
      Agent.get_and_update(engine, fn state ->
        if Enum.all?(spec.compare, &matches?(&1, state.records)) do
          records =
            Enum.reduce(spec.success, state.records, fn
              {:put, key, value, _}, records ->
                Map.put(records, key, %{value: value, mod_revision: state.revision + 1})

              {:delete, {:key, key}, _}, records ->
                Map.delete(records, key)
            end)

          {:ok, %{state | records: records, revision: state.revision + 1}}
        else
          {{:error, :compare_failed}, state}
        end
      end)
    end

    defp matches?({:exists, key, :==, expected}, records),
      do: Map.has_key?(records, key) == expected

    defp matches?({:mod_revision, key, :==, revision}, records),
      do: match?(%{mod_revision: ^revision}, records[key])
  end

  setup do
    generation = "index-test-generation"
    root_key = ListIndex.root_key("bucket", generation)

    records = %{
      "bucket:bucket" => %{
        value: %{name: "bucket", generation: generation},
        mod_revision: 1
      },
      root_key => %{value: ListIndex.empty(generation), mod_revision: 1}
    }

    {:ok, engine} =
      Agent.start_link(fn ->
        %{records: records, revision: 1, reads: 0, churn: false, root_key: root_key}
      end)

    %{engine: engine, opts: [backend: ExactBackend, engine: engine], root_key: root_key}
  end

  test "head, history and index become visible together and stale plans cannot publish",
       context do
    %{engine: engine, opts: opts, root_key: root_key} = context
    spec = publication("key", "first")
    assert {:ok, planned} = ListIndex.augment(ExactBackend, "bucket", spec, opts)
    assert {:ok, %{entries: []}} = ListIndex.page("bucket", :current, "", opts)
    assert :ok = ExactBackend.commit(engine, planned)

    assert {:ok, %{entries: [{"key", %{version_id: "first"}}]}} =
             ListIndex.page("bucket", :current, "", opts)

    assert {:error, :compare_failed} = ExactBackend.commit(engine, planned)
    root = Agent.get(engine, & &1.records[root_key].value)
    assert root.visible_count == 1
    assert root.version_count == 1
    assert root.commit_sequence == 1
  end

  test "overwrite, historical removal and null replacement retain exact index membership",
       context do
    %{engine: engine, opts: opts, root_key: root_key} = context
    publish(engine, opts, publication("key", "null"))
    publish(engine, opts, publication("key", "null", "replacement"))
    publish(engine, opts, publication("key", "next"))

    assert {:ok, %{entries: entries}} = ListIndex.page("bucket", :versions, "", opts)

    assert Enum.map(entries, fn {_, summary} -> {summary.version_id, summary.is_latest} end) ==
             [{"next", true}, {"null", false}]

    publish(engine, opts, %{
      compare: [],
      success: [{:delete, {:key, Keys.object_version("bucket", "key", "null")}, %{}}],
      failure: []
    })

    root = Agent.get(engine, & &1.records[root_key].value)
    assert root.version_count == 1
    assert root.visible_count == 1
  end

  test "a large common prefix is skipped structurally", %{engine: engine, opts: opts} do
    for number <- 1..200,
        do: publish(engine, opts, publication("directory/#{number}", "null"))

    publish(engine, opts, publication("outside", "null"))

    select = fn key, summary ->
      case :binary.match(key, "/") do
        {offset, 1} ->
          prefix = binary_part(key, 0, offset + 1)
          {:prefix, prefix, Codec.prefix_successor(prefix)}

        :nomatch ->
          {:emit, key, summary}
      end
    end

    assert {:ok, %{entries: [{"directory/", :common_prefix}, {"outside", _}], stats: stats}} =
             ListIndex.page("bucket", :current, "", opts ++ [select: select])

    assert stats.reads < 40
    assert stats.visited < 40
  end

  test "a changing root cannot yield a stale successful page", %{engine: engine, opts: opts} do
    publish(engine, opts, publication("key", "null"))
    Agent.update(engine, &%{&1 | churn: true, reads: 0})

    assert {:error, :listing_index_unavailable} = ListIndex.page("bucket", :current, "", opts)
    assert Agent.get(engine, & &1.reads) < 40
  end

  test "missing or rebuilding descriptors never fall back to scans", context do
    %{engine: engine, opts: opts, root_key: root_key} = context
    Agent.update(engine, &update_in(&1.records[root_key].value.state, fn _ -> :building end))
    assert {:error, :listing_index_unavailable} = ListIndex.page("bucket", :current, "", opts)

    Agent.update(engine, &%{&1 | records: Map.delete(&1.records, root_key)})

    assert {:error, :listing_index_migration_required} =
             ListIndex.augment(ExactBackend, "bucket", publication("key", "null"), opts)

    assert {:error, :listing_index_migration_required} =
             ListIndex.page("bucket", :current, "", opts)
  end

  defp publication(key, id, etag \\ "etag") do
    version = %{
      bucket: "bucket",
      key: key,
      version_id: id,
      created_at: "2026-10-18T00:00:00Z",
      etag: etag,
      size: 1,
      delete_marker: false
    }

    %{
      compare: [],
      success: [
        {:put, Keys.object_version("bucket", key, id), version, %{}},
        {:put, Keys.object_head("bucket", key), %{version_id: id, delete_marker: false}, %{}}
      ],
      failure: []
    }
  end

  defp publish(engine, opts, spec) do
    assert {:ok, spec} = ListIndex.augment(ExactBackend, "bucket", spec, opts)
    assert :ok = ExactBackend.commit(engine, spec)
  end
end
