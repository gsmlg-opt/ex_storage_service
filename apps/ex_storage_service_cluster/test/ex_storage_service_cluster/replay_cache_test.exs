defmodule ExStorageServiceCluster.ReplayCacheTest do
  use ExUnit.Case, async: true

  alias ExStorageServiceCluster.ReplayCache

  test "direct claims do not serialize through the GenServer" do
    pid = start_supervised!({ReplayCache, name: nil, table: nil, sweep_interval: 60_000})
    table = ReplayCache.table(pid)

    :sys.suspend(pid)

    assert :ok = ReplayCache.claim(table, "request-id-00000001", 10_000)
    assert [{"request-id-00000001", 10_000}] = :ets.lookup(table, "request-id-00000001")

    :sys.resume(pid)
  end

  test "insert_new permits exactly one concurrent claimant" do
    table = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])
    parent = self()

    tasks =
      for _ <- 1..100 do
        Task.async(fn ->
          send(parent, {:ready, self()})
          receive do: (:claim -> ReplayCache.claim(table, "request-id-00000002", 10_000))
        end)
      end

    pids =
      for _ <- tasks do
        assert_receive {:ready, pid}
        pid
      end

    Enum.each(pids, &send(&1, :claim))
    results = Enum.map(tasks, &Task.await/1)

    assert 1 == Enum.count(results, &(&1 == :ok))
    assert 99 == Enum.count(results, &(&1 == {:error, :replayed_request}))
  end

  test "periodic sweep removes claims at their monotonic expiry" do
    clock = :atomics.new(1, [])
    :atomics.put(clock, 1, 100)

    pid =
      start_supervised!(
        {ReplayCache,
         name: nil, table: nil, sweep_interval: 60_000, now_ms: fn -> :atomics.get(clock, 1) end}
      )

    table = ReplayCache.table(pid)
    assert :ok = ReplayCache.claim(table, "request-id-00000003", 200)

    send(pid, :sweep)
    _state = :sys.get_state(pid)
    assert [{"request-id-00000003", 200}] = :ets.lookup(table, "request-id-00000003")

    :atomics.put(clock, 1, 200)
    send(pid, :sweep)
    _state = :sys.get_state(pid)
    assert [] == :ets.lookup(table, "request-id-00000003")
  end

  test "reports a missing table without crashing the caller" do
    table = :ets.new(__MODULE__, [:set, :public])
    :ets.delete(table)

    assert {:error, :cache_unavailable} =
             ReplayCache.claim(table, "request-id-00000004", 10_000)
  end
end
