defmodule ExStorageService.Metadata.ListIndex.RadixTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.ListIndex.{Codec, Radix}

  @namespace "test:index:node:"

  test "insert, overwrite, delete, split and merge match a sorted map oracle" do
    keys = ["", "a", "aa", "ab", "aba", "a/b", "a/b/c", "b", <<0>>, <<0, 255>>, <<255>>, "資料/😀"]

    mutations =
      Enum.map(keys, &{:put, &1, %{value: 1}}) ++
        Enum.map(Enum.reverse(keys), &{:put, &1, %{value: 2}}) ++
        Enum.map(Enum.take_every(keys, 2), &{:delete, &1}) ++
        Enum.map(keys, &{:delete, &1})

    {store, root, oracle, _revision} =
      Enum.reduce(mutations, {%{}, nil, %{}, 0}, fn mutation, {store, root, oracle, revision} ->
        state = planner(store, root)

        {state, expected} =
          case mutation do
            {:put, key, value} ->
              assert {:ok, state} = Radix.put(state, key, value)
              {state, Map.put(oracle, key, value)}

            {:delete, key} ->
              assert {:ok, state} = Radix.delete(state, key)
              {state, Map.delete(oracle, key)}
          end

        {store, root, delta, revision, _ops} = commit(state, store, revision)
        assert delta == map_size(expected) - map_size(oracle)
        assert list_all(store, root) == Enum.sort(expected)
        assert_tree(store, root)
        {store, root, expected, revision}
      end)

    assert oracle == %{}
    assert root == nil
    assert store == %{}
  end

  test "multiple mutations deduplicate reads and atomic transaction operations" do
    state = planner(%{}, nil)
    assert {:ok, state} = Radix.put(state, "alpha", %{value: 1})
    assert {:ok, state} = Radix.put(state, "alphabet", %{value: 2})
    assert {:ok, state} = Radix.put(state, "alpha", %{value: 3})
    assert {:ok, state} = Radix.delete(state, "alphabet")
    assert {:ok, {"alpha", %{value: 3}}, _} = Radix.seek(state, "")

    {store, root, 1, revision, ops} = commit(state, %{}, 0)
    assert length(ops.success) == 1
    assert length(ops.compare) == 1
    assert [{:exists, _, :==, false}] = ops.compare

    state = planner(store, root)
    assert {:ok, state} = Radix.put(state, "alpha", %{value: 3})
    assert %{success: [], count_delta: 0} = Radix.operations(state)
    assert {:ok, state} = Radix.delete(state, "alpha")
    assert {:ok, state} = Radix.put(state, "alpha", %{value: 4})
    {store, root, 0, _, ops} = commit(state, store, revision)
    assert Enum.uniq(ops.compare) == ops.compare
    assert Enum.uniq_by(ops.success, &operation_key/1) == ops.success
    assert list_all(store, root) == [{"alpha", %{value: 4}}]
  end

  test "lower bounds and exclusive successors agree with exhaustive byte-order oracle" do
    keys =
      for first <- [0, 1, ?a, ?b, 254, 255],
          tail <- ["", "a", "ab", <<0>>, <<255>>],
          do: <<first>> <> tail

    {store, root} = build(keys ++ ["", "資料", "資料/a", "資料/😀"])
    entries = list_all(store, root)
    probes = ["", "a/a", "aaa", "c", <<255, 255, 255>>, "資", "資料/💡"] ++ keys

    for bound <- probes, exclusive <- [false, true] do
      expected =
        Enum.find(entries, fn {key, _} -> if exclusive, do: key > bound, else: key >= bound end)

      assert {:ok, ^expected, stats} = Radix.seek(planner(store, root), bound, exclusive)
      assert stats.reads <= 2 * (Radix.max_key_bytes() + 1)
      assert stats.depth <= Radix.max_key_bytes()
    end
  end

  test "deep chains collapse after deletion rather than leaving empty or unary paths" do
    keys = for length <- 1..128, do: String.duplicate("a", length)
    {store, root} = build(keys)

    {store, root, _revision} =
      Enum.reduce(Enum.reverse(tl(keys)), {store, root, 1000}, fn key, {store, root, revision} ->
        assert {:ok, state} = Radix.delete(planner(store, root), key)
        {store, root, -1, revision, _} = commit(state, store, revision)
        assert_tree(store, root)
        {store, root, revision}
      end)

    assert map_size(store) == 1
    assert list_all(store, root) == [{"a", %{key: "a"}}]
  end

  test "a common prefix with ten thousand descendants skips structurally" do
    keys =
      for number <- 1..10_000,
          do: "group/" <> String.pad_leading(Integer.to_string(number), 5, "0")

    {store, root} = build(["after", "z-final" | keys])
    assert {:ok, {"group/00001", _}, before_stats} = Radix.seek(planner(store, root), "group/")

    assert {:ok, {"z-final", _}, after_stats} =
             Radix.seek(planner(store, root), Codec.prefix_successor("group/"))

    assert after_stats.reads <= 4
    assert before_stats.reads <= 16
    assert after_stats.visited < 10
    assert after_stats.bytes <= 4 * Radix.max_node_bytes()

    unrelated =
      for number <- 1..10_000,
          into: store,
          do: {"unrelated:#{number}", %{value: number, mod_revision: 1}}

    assert {:ok, {"z-final", _}, ^after_stats} =
             Radix.seek(planner(unrelated, root), Codec.prefix_successor("group/"))
  end

  test "maximum labels, summary and full byte fanout fit the persisted node ceiling" do
    prefix = String.duplicate("k", Radix.max_key_bytes() - 1)
    padding = Radix.max_terminal_bytes() - :erlang.external_size(%{payload: ""})
    summary = %{payload: String.duplicate("x", padding)}
    assert :erlang.external_size(summary) == Radix.max_terminal_bytes()
    assert {:ok, state} = Radix.put(planner(%{}, nil), prefix, summary)

    state =
      Enum.reduce(0..255, state, fn byte, state ->
        assert {:ok, state} = Radix.put(state, prefix <> <<byte>>, %{value: byte})
        state
      end)

    {store, root, 257, _, _} = commit(state, %{}, 0)
    assert_tree(store, root)
    assert {:ok, {key, %{value: 255}}, _} = Radix.seek(planner(store, root), prefix <> <<255>>)
    assert key == prefix <> <<255>>

    assert Enum.all?(store, fn {_, record} ->
             :erlang.external_size(record.value) <= Radix.max_node_bytes()
           end)

    assert {:error, :invalid_index_key} = Radix.put(planner(store, root), prefix <> "xx", %{})

    assert {:error, :invalid_index_summary} =
             Radix.put(planner(store, root), "x", %{payload: summary.payload <> "x"})

    assert {:error, :invalid_index_summary} = Radix.put(planner(store, root), "x", nil)
  end

  test "version and upload tuple trees preserve component boundaries for prefix skips" do
    versions =
      for key <- ["a", "a/1", "a/2", "a0", "ab"],
          latest <- [true, false],
          id <- ["v1", "v2"],
          do: Codec.version(key, id, %{created_at: "now"}, latest)

    {store, root} = build(versions)
    bound = Codec.key_prefix(Codec.prefix_successor("a/"))
    assert {:ok, {encoded, _}, stats} = Radix.seek(planner(store, root), bound)
    assert {:ok, %{key: "a0"}} = Codec.decode_version(encoded)
    assert stats.reads <= 8

    {store, root} =
      build(for key <- ["a", "a/1", "ab"], id <- ["1", "10", "2"], do: Codec.upload(key, id))

    assert {:ok, {encoded, _}, _} =
             Radix.seek(planner(store, root), Codec.upload("a", "10"), true)

    assert {:ok, %{key: "a", upload_id: "2"}} = Codec.decode_upload(encoded)
  end

  test "missing records, cycles, invalid edges and noncanonical nodes fail closed" do
    root = <<1::128>>
    child = <<2::128>>
    leaf = %{schema: 1, label: "a", terminal: %{value: 1}, children: []}

    malformed = [
      %{leaf | schema: 2},
      %{leaf | terminal: nil},
      %{leaf | children: [{?a, child}, {?a, root}]},
      %{leaf | children: [{?b, child}, {?a, root}]},
      %{leaf | children: [{?b, "short"}]},
      %{leaf | label: String.duplicate("x", Radix.max_key_bytes() + 1)}
    ]

    for node <- malformed do
      store = %{storage_key(root) => %{value: node, mod_revision: 1}}
      assert {:error, :corrupt_index} = Radix.seek(planner(store, root), "")
      assert {:error, :corrupt_index} = Radix.put(planner(store, root), "a", %{})
      assert {:error, :corrupt_index} = Radix.delete(planner(store, root), "a")
    end

    assert {:error, :corrupt_index} = Radix.seek(planner(%{}, root), "")
    assert {:error, :corrupt_index} = Radix.put(planner(%{}, root), "a", %{})

    for descendant <- [
          %{leaf | label: ""},
          %{leaf | label: "wrong"},
          %{leaf | children: [{?a, root}]}
        ] do
      parent = %{schema: 1, label: "", terminal: %{}, children: [{?a, child}]}

      store = %{
        storage_key(root) => %{value: parent, mod_revision: 1},
        storage_key(child) => %{value: descendant, mod_revision: 2}
      }

      assert {:error, :corrupt_index} = Radix.seek(planner(store, root), "a", true)
    end
  end

  test "read errors propagate and revision comparisons prevent stale overlay publication" do
    read = fn _ -> {:error, :metadata_unavailable} end
    root = <<1::128>>
    assert {:error, :metadata_unavailable} = Radix.seek(Radix.new(read, @namespace, root), "")

    assert {:error, :metadata_unavailable} =
             Radix.put(Radix.new(read, @namespace, root), "a", %{})

    {store, root} = build(["a", "b"])
    assert {:ok, state} = Radix.put(planner(store, root), "a", %{changed: true})
    ops = Radix.operations(state)
    assert comparisons_match?(ops.compare, store)
    stale = Map.update!(store, storage_key(root), &%{&1 | mod_revision: &1.mod_revision + 1})
    refute comparisons_match?(ops.compare, stale)
    assert list_all(store, root) == [{"a", %{key: "a"}}, {"b", %{key: "b"}}]
  end

  defp planner(store, root),
    do: Radix.new(fn key -> {:ok, Map.get(store, key)} end, @namespace, root)

  defp build(keys) do
    {store, root, _revision} =
      Enum.reduce(keys, {%{}, nil, 0}, fn key, {store, root, revision} ->
        assert {:ok, state} = Radix.put(planner(store, root), key, %{key: key})
        {store, root, _delta, revision, _ops} = commit(state, store, revision)
        {store, root, revision}
      end)

    {store, root}
  end

  defp commit(state, store, revision) do
    ops = Radix.operations(state)
    assert comparisons_match?(ops.compare, store)
    revision = revision + 1

    store =
      Enum.reduce(ops.success, store, fn
        {:put, key, value, %{}}, store ->
          Map.put(store, key, %{value: value, mod_revision: revision})

        {:delete, {:key, key}, %{}}, store ->
          Map.delete(store, key)
      end)

    {store, ops.root_id, ops.count_delta, revision, ops}
  end

  defp comparisons_match?(comparisons, store) do
    Enum.all?(comparisons, fn
      {:exists, key, :==, expected} ->
        Map.has_key?(store, key) == expected

      {:mod_revision, key, :==, expected} ->
        match?(%{mod_revision: ^expected}, Map.get(store, key))
    end)
  end

  defp list_all(store, root), do: collect(planner(store, root), "", false, [])

  defp collect(state, bound, exclusive, entries) do
    case Radix.seek(state, bound, exclusive) do
      {:ok, nil, _} -> Enum.reverse(entries)
      {:ok, {key, _} = entry, _} -> collect(state, key, true, [entry | entries])
    end
  end

  defp assert_tree(_store, nil), do: :ok
  defp assert_tree(store, root), do: inspect_node(store, root, true, MapSet.new())

  defp inspect_node(store, id, root?, ancestors) do
    refute MapSet.member?(ancestors, id)
    node = Map.fetch!(store, storage_key(id)).value
    assert root? or node.label != ""
    assert node.terminal != nil or length(node.children) >= 2
    assert node.children == Enum.sort(node.children)
    assert Enum.uniq_by(node.children, &elem(&1, 0)) == node.children
    assert length(node.children) <= 256
    assert :erlang.external_size(node) <= Radix.max_node_bytes()

    for {edge, child} <- node.children do
      assert :binary.first(Map.fetch!(store, storage_key(child)).value.label) == edge
      inspect_node(store, child, false, MapSet.put(ancestors, id))
    end

    :ok
  end

  defp operation_key({:put, key, _, _}), do: key
  defp operation_key({:delete, {:key, key}, _}), do: key
  defp storage_key(id), do: @namespace <> Base.url_encode64(id, padding: false)
end
