Code.require_file("index_offline_inventory.exs", __DIR__)
ExUnit.start()

defmodule ESSIndexAcceptance.InventoryTest.KV do
  import ExUnit.Assertions
  def revision(opts), do: call(:revision, opts)
  def list(opts), do: call(:list, opts)

  defp call(operation, opts) do
    [{expected_operation, expected_opts, result} | rest] = Process.get(:inventory_calls)
    assert(operation == expected_operation)
    assert(opts == expected_opts)
    Process.put(:inventory_calls, rest)
    if is_function(result, 0), do: result.(), else: result
  end
end

defmodule ESSIndexAcceptance.InventoryTest do
  use ExUnit.Case, async: true

  alias ESSIndexAcceptance.Inventory
  alias ESSIndexAcceptance.InventoryTest.KV

  test "full offline map retains unrelated records, raw values and both byte metrics" do
    raw = {:compressed, :fixture, <<0, 255, 17>>}

    snapshot =
      Map.new(1..1_005, fn number ->
        {"unrelated:" <> Integer.to_string(number), %{ordinal: number}}
      end)
      |> Map.merge(%{
        "bucket:fixture" => %{generation: "generation"},
        "bucket:unrelated" => %{generation: "other"},
        "ess:v2:list_index:fixture:root" => %{schema: 1, current_root: nil},
        "blob:raw" => raw,
        "nullable:value" => nil
      })

    calls(snapshot, 42)
    assert {:ok, actual} = Inventory.snapshot(KV)
    assert actual === snapshot
    assert actual["blob:raw"] === raw
    assert :erlang.external_size(actual) == :erlang.external_size(snapshot)

    nodes = fn map ->
      Enum.filter(map, fn {key, _} -> String.starts_with?(key, "ess:v2:list_index:") end)
    end

    assert :erlang.external_size(nodes.(actual)) == :erlang.external_size(nodes.(snapshot))
    assert Process.get(:inventory_calls) == []
  end

  test "descending exclusive cursor handles maximum-length and embedded zero keys" do
    maximum = String.duplicate("z", 4_096)

    snapshot = %{
      maximum => :maximum,
      ("a" <> <<0>>) => :zero_suffix,
      "a" => :short,
      <<0>> => :zero
    }

    calls(snapshot, 43, 1)
    assert Inventory.snapshot(KV) == {:ok, snapshot}
    assert Process.get(:inventory_calls) == []
  end

  test "empty inventory finishes with the captured revision and no extra read" do
    calls(%{}, 43)
    assert Inventory.snapshot(KV) == {:ok, %{}}
    assert Process.get(:inventory_calls) == []
  end

  test "every page uses the captured revision despite intervening mutations" do
    old = %{key: "a", value: :old}
    Process.put(:current_inventory, %{"a" => :old, "z" => :before})

    script([
      {:revision, [consistency: :linearizable], {:ok, 5}},
      {:list, options(5, nil),
       fn ->
         Process.put(:current_inventory, %{"a" => :new, "m" => :inserted})
         {:ok, [%{key: "z", value: :before}], %{has_more: true, last_key: "z"}}
       end},
      {:list, options(5, "z"), {:ok, [old], %{has_more: false, last_key: "a"}}}
    ])

    assert Inventory.snapshot(KV) == {:ok, %{"a" => :old, "z" => :before}}
    refute Process.get(:current_inventory) == %{"a" => :old, "z" => :before}
    assert Process.get(:inventory_calls) == []
  end

  test "revision errors propagate without requesting pages or retrying" do
    for reason <- [:quorum_unavailable, :timeout, :cluster_not_ready] do
      script([{:revision, [consistency: :linearizable], {:error, reason}}])
      assert Inventory.snapshot(KV) == {:error, reason}
      assert Process.get(:inventory_calls) == []
    end
  end

  test "first and later page failures propagate without retries or a fresh revision" do
    for reason <- [:quorum_unavailable, :timeout, {:compacted, 6}], later? <- [false, true] do
      first =
        {:list, options(5, nil), {:ok, [%{key: "z", value: 1}], %{has_more: true, last_key: "z"}}}

      failed = {:list, options(5, if(later?, do: "z")), {:error, reason}}

      script(
        [{:revision, [consistency: :linearizable], {:ok, 5}}] ++
          if(later?, do: [first, failed], else: [failed])
      )

      assert Inventory.snapshot(KV) == {:error, reason}
      assert Process.get(:inventory_calls) == []
    end
  end

  test "invalid or nonprogressing pages fail instead of returning a partial map or looping" do
    invalid = [
      {[], %{has_more: true, last_key: nil}},
      {[], %{has_more: false, last_key: "z"}},
      {[%{key: "z", value: 1}], %{has_more: true, last_key: "wrong"}},
      {[%{key: "z", value: 1}], %{has_more: :unknown, last_key: "z"}},
      {[%{key: "z", value: 1}, %{key: "z", value: 2}], %{has_more: false, last_key: "z"}},
      {[%{key: "a", value: 1}, %{key: "z", value: 2}], %{has_more: false, last_key: "z"}},
      {[%{key: "", value: 1}], %{has_more: false, last_key: ""}}
    ]

    for {records, page} <- invalid do
      script([
        {:revision, [consistency: :linearizable], {:ok, 5}},
        {:list, options(5, nil), {:ok, records, page}}
      ])

      assert Inventory.snapshot(KV) == {:error, :invalid_inventory_page}
      assert Process.get(:inventory_calls) == []
    end

    script([
      {:revision, [consistency: :linearizable], {:ok, 5}},
      {:list, options(5, nil), {:ok, [%{key: "z", value: 1}], %{has_more: true, last_key: "z"}}},
      {:list, options(5, "z"), {:ok, [%{key: "z", value: 1}], %{has_more: true, last_key: "z"}}}
    ])

    assert Inventory.snapshot(KV) == {:error, :invalid_inventory_page}
    assert Process.get(:inventory_calls) == []
  end

  defp calls(snapshot, revision, size \\ 1_000) do
    pages = snapshot |> Enum.sort(:desc) |> Enum.chunk_every(size)
    pages = if pages == [], do: [[]], else: pages

    {pages, _cursor} =
      pages
      |> Enum.with_index()
      |> Enum.map_reduce(nil, fn {entries, index}, cursor ->
        records = Enum.map(entries, fn {key, value} -> %{key: key, value: value} end)
        last_key = if entries == [], do: nil, else: entries |> List.last() |> elem(0)
        page = %{has_more: index < length(pages) - 1, last_key: last_key}
        {{:list, options(revision, cursor), {:ok, records, page}}, last_key}
      end)

    script([{:revision, [consistency: :linearizable], {:ok, revision}} | pages])
  end

  defp options(revision, cursor) do
    base = [consistency: :linearizable, revision: revision, order: :desc]
    if is_nil(cursor), do: base ++ [prefix: ""], else: base ++ [range: {"", cursor}]
  end

  defp script(calls), do: Process.put(:inventory_calls, calls)
end
