defmodule ExStorageService.Metadata.ObjectCommitDeleteIndexTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{Keys, ListIndex, ObjectCommit}

  defmodule ExactBackend do
    def get(key, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        {{:ok, state.records[key]}, %{state | reads: state.reads + 1}}
      end)
    end

    def resolve_operation(key, opts), do: get(key, opts)

    def transaction(spec, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        state = %{state | transactions: [spec | state.transactions]}

        state =
          case state.touch_before_commit do
            nil ->
              state

            key ->
              state
              |> update_in([:records, key, :mod_revision], &(&1 + 1))
              |> Map.put(:touch_before_commit, nil)
          end

        if Enum.all?(spec.compare, &matches?(&1, state.records)) do
          revision = state.revision + 1

          records =
            Enum.reduce(spec.success, state.records, fn
              {:put, key, value, _}, records ->
                Map.put(records, key, %{value: value, mod_revision: revision})

              {:delete, {:key, key}, _}, records ->
                Map.delete(records, key)
            end)

          {{:ok, %{succeeded: true}}, %{state | records: records, revision: revision}}
        else
          {{:ok, %{succeeded: false}}, state}
        end
      end)
    end

    defp matches?({:exists, key, :==, expected}, records),
      do: Map.has_key?(records, key) == expected

    defp matches?({:mod_revision, key, :==, expected}, records),
      do: match?(%{mod_revision: ^expected}, records[key])
  end

  setup do
    generation = "delete-index-generation"
    root_key = ListIndex.root_key("bucket", generation)

    records = %{
      "bucket:bucket" => %{value: %{name: "bucket", generation: generation}, mod_revision: 1},
      root_key => %{value: ListIndex.empty(generation), mod_revision: 1}
    }

    {:ok, engine} =
      Agent.start_link(fn ->
        %{records: records, revision: 1, reads: 0, transactions: [], touch_before_commit: nil}
      end)

    %{engine: engine, opts: [backend: ExactBackend, engine: engine]}
  end

  test "a surviving parent wins even when another version has a newer timestamp", context do
    seed(
      context,
      [version("newer", "999"), version("parent", "001"), version("head", "500", "parent")],
      "head"
    )

    assert {:ok, %{kind: :deleted}} = delete(context, "head")
    assert {:ok, %{version_id: "parent"}} = ObjectCommit.get_head("bucket", "key", context.opts)
    spec = Agent.get(context.engine, &hd(&1.transactions))
    parent_key = Keys.object_version("bucket", "key", "parent")
    assert {:exists, parent_key, :==, true} in spec.compare
    assert Enum.any?(spec.compare, &match?({:mod_revision, ^parent_key, :==, _}, &1))
  end

  test "a missing parent falls back to timestamp and version ID without scanning history",
       context do
    history =
      for number <- 1..500 do
        id = Integer.to_string(number) |> String.pad_leading(4, "0")
        version(id, "100")
      end

    seed(context, [version("head", "001", "already-deleted") | history], "head")
    Agent.update(context.engine, &%{&1 | reads: 0, transactions: []})
    assert {:ok, %{kind: :deleted}} = delete(context, "head")
    assert {:ok, %{version_id: "0500"}} = ObjectCommit.get_head("bucket", "key", context.opts)
    assert Agent.get(context.engine, & &1.reads) < 100
    spec = Agent.get(context.engine, &hd(&1.transactions))

    assert {:exists, Keys.object_version("bucket", "key", "already-deleted"), :==, false} in spec.compare
  end

  test "selected replacement revision changes trigger normal transaction replanning", context do
    seed(context, [version("parent", "001"), version("head", "002", "parent")], "head")

    Agent.update(
      context.engine,
      &%{
        &1
        | transactions: [],
          touch_before_commit: Keys.object_version("bucket", "key", "parent")
      }
    )

    assert {:ok, %{kind: :deleted}} = delete(context, "head")
    assert {:ok, %{version_id: "parent"}} = ObjectCommit.get_head("bucket", "key", context.opts)
    assert Agent.get(context.engine, &length(&1.transactions)) == 2
  end

  test "historical and nonexistent deletion do not select unrelated history", context do
    seed(context, [version("older", "001"), version("head", "002", "older")], "head")
    assert {:ok, %{kind: :deleted}} = delete(context, "older")
    assert {:ok, %{version_id: "head"}} = ObjectCommit.get_head("bucket", "key", context.opts)
    assert {:ok, %{kind: :deleted}} = delete(context, "missing")
    assert {:ok, %{kind: :deleted}} = delete(context, "head")
    assert {:error, :not_found} = ObjectCommit.get_head("bucket", "key", context.opts)
    assert {:ok, true} = ObjectCommit.has_state("bucket", "key", context.opts)
    assert {:ok, %{entries: []}} = ListIndex.page("bucket", :versions, "", context.opts)
  end

  defp version(id, time, parent \\ nil),
    do: %{
      schema: 2,
      bucket: "bucket",
      key: "key",
      version_id: id,
      parent_version_id: parent,
      created_at: time,
      size: 1,
      content_hash: "hash-#{id}",
      etag: "etag-#{id}",
      delete_marker: false
    }

  defp seed(context, versions, latest) do
    for version <- versions do
      spec = %{
        compare: [],
        failure: [],
        success: [
          {:put, Keys.object_version("bucket", "key", version.version_id), version, %{}},
          {:put, Keys.object_head("bucket", "key"),
           %{
             schema: 2,
             bucket: "bucket",
             key: "key",
             version_id: version.version_id,
             delete_marker: false,
             etag: version.etag
           }, %{}}
        ]
      }

      assert {:ok, spec} = ListIndex.augment(ExactBackend, "bucket", spec, context.opts)
      assert {:ok, %{succeeded: true}} = ExactBackend.transaction(spec, context.opts)
    end

    head = %{
      schema: 2,
      bucket: "bucket",
      key: "key",
      version_id: latest,
      delete_marker: false,
      etag: "etag-#{latest}"
    }

    assert {:ok, spec} =
             ListIndex.augment(
               ExactBackend,
               "bucket",
               %{
                 compare: [],
                 failure: [],
                 success: [{:put, Keys.object_head("bucket", "key"), head, %{}}]
               },
               context.opts
             )

    assert {:ok, %{succeeded: true}} = ExactBackend.transaction(spec, context.opts)
  end

  defp delete(context, id),
    do:
      ObjectCommit.delete_version(
        "bucket",
        "key",
        id,
        context.opts ++ [operation_id: "delete-#{id}"]
      )
end
