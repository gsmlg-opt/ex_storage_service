defmodule ExStorageServiceCli.Transfer.CoordinatorTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Transfer.Coordinator}

  test "count and byte budgets both limit admission; process death releases reservations" do
    {:ok, pool} = start_supervised({Coordinator, max_requests: 2, memory_budget: 131_072})
    {:ok, first} = Coordinator.acquire(pool, 100_000)
    owner = self()

    worker =
      spawn(fn ->
        send(owner, :waiting)
        result = Coordinator.acquire(pool, 65_536)
        send(owner, {:admitted, self(), result})

        receive do
          :finish -> :ok
        end
      end)

    assert_receive :waiting
    refute_receive {:admitted, _, _}
    assert %{active: 1, queued: 1, bytes: 100_000} = Coordinator.stats(pool)
    Coordinator.release(first)
    assert_receive {:admitted, ^worker, {:ok, _lease}}
    Process.exit(worker, :kill)
    monitor = Process.monitor(worker)
    assert_receive {:DOWN, ^monitor, _, _, _}
    assert {:ok, last} = Coordinator.acquire(pool, 131_072)

    assert %{active: 1, bytes: 131_072, high_requests: 1, high_bytes: 131_072} =
             Coordinator.stats(pool)

    Coordinator.release(last)
  end

  test "cancellation stops queued/active work while admitting bounded owned cleanup" do
    {:ok, pool} = start_supervised({Coordinator, max_requests: 1})
    controller = HTTP.AbortController.new()
    {:ok, lease} = Coordinator.acquire(pool, 65_536)
    assert :ok = Coordinator.track(lease, controller)
    owner = self()
    worker = spawn(fn -> send(owner, {:waiting_result, Coordinator.acquire(pool, 65_536)}) end)
    assert :ok = Coordinator.cancel(pool, :sigterm)
    assert_receive {:ess_cancel, :sigterm}
    assert_receive {:waiting_result, {:error, %Error{category: :terminated}}}
    assert HTTP.AbortController.aborted?(controller)
    Coordinator.release(lease)
    assert {:error, %Error{category: :terminated}} = Coordinator.acquire(pool, 1)
    assert {:ok, cleanup} = Coordinator.acquire(pool, 65_536, cleanup: true)
    Coordinator.release(cleanup)
    Process.monitor(worker)
  end

  test "expired admission requests do not leak future reservations" do
    {:ok, pool} = start_supervised({Coordinator, max_requests: 1})
    {:ok, lease} = Coordinator.acquire(pool, 65_536)

    assert {:error, %Error{code: :admission_timeout}} =
             Coordinator.acquire(pool, 65_536, timeout: 1)

    Coordinator.release(lease)
    assert {:ok, next} = Coordinator.acquire(pool, 65_536, timeout: 1000)
    Coordinator.release(next)
    assert %{active: 0, queued: 0, bytes: 0} = Coordinator.stats(pool)
  end
end
