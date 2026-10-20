Code.require_file("index_setup_diagnostics.exs", __DIR__)
ExUnit.start()

defmodule ESSIndexSetupTest do
  use ExUnit.Case, async: false

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ess-index-progress-#{System.unique_integer([:positive])}.jsonl"
      )

    previous = System.get_env("ESS_INDEX_PROGRESS")
    System.put_env("ESS_INDEX_PROGRESS", path)

    on_exit(fn ->
      if previous,
        do: System.put_env("ESS_INDEX_PROGRESS", previous),
        else: System.delete_env("ESS_INDEX_PROGRESS")

      File.rm(path)
    end)

    %{path: path}
  end

  test "progress confirms only successful operations and retains a bounded latency window", %{
    path: path
  } do
    result =
      ESSIndexSetup.phase("test", 205, fn ordinal ->
        ESSIndexSetup.backend(:get, "bucket:test", fn -> {:ok, %{value: ordinal}} end)
        ESSIndexSetup.backend(:transaction, %{}, fn -> {:ok, %{succeeded: ordinal != 1}} end)
        {:ok, ordinal}
      end)

    assert result.completed == 205
    assert result.backend["get:bucket"].calls == 205
    assert result.backend["transaction:specification"].calls == 205
    assert result.backend["transaction:specification"].conflicts == 1
    assert result.backend["get:bucket"].returned_bytes > 0

    rows = path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
    progress = Enum.filter(rows, &(&1["event"] == "progress"))
    assert Enum.map(progress, & &1["completed"]) == [100, 200, 205]
    assert Enum.map(progress, & &1["window"]["count"]) == [100, 100, 5]
  end

  test "a failed commit never emits a successful final progress row", %{path: path} do
    assert_raise MatchError, fn ->
      ESSIndexSetup.phase("test-failure", 2, fn _ -> {:error, :injected} end)
    end

    [row] = path |> File.read!() |> String.split("\n", trim: true) |> Enum.map(&JSON.decode!/1)
    assert row["event"] == "start"
    assert row["completed"] == 0
  end

  test "latency summaries use only actual window samples" do
    assert %{count: 5, p50_us: 3, p95_us: 100, maximum_us: 100} =
             ESSIndexSetup.latency([4, 1, 100, 2, 3])
  end

  test "zero-sized phases are rejected rather than invoking a descending range" do
    assert_raise FunctionClauseError, fn ->
      ESSIndexSetup.phase("zero", 0, fn _ -> flunk() end)
    end
  end
end
