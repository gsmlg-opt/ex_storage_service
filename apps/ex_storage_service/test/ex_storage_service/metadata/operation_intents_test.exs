defmodule ExStorageService.Metadata.OperationIntentsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.{GCGuard, Keys, OperationIntents}
  alias ExStorageService.Metadata.Models.OperationIntent

  setup do
    suffix = System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
    operation_id = "intent-op-#{suffix}"
    second_operation_id = "intent-op-second-#{suffix}"
    hash = String.pad_leading(Integer.to_string(String.to_integer(suffix), 16), 64, "0")

    keys = [
      Keys.operation_intent(operation_id),
      Keys.operation_intent(second_operation_id),
      Keys.gc_guard(hash),
      Keys.gc_lock(hash)
    ]

    on_exit(fn -> Enum.each(keys, &Concord.delete/1) end)

    %{operation_id: operation_id, second_operation_id: second_operation_id, hash: hash}
  end

  test "open protects a hash and final publication closes the intent atomically", context do
    assert {:ok, %OperationIntent{state: :pending, protected_until_ms: 1_100}} =
             OperationIntents.open(
               context.operation_id,
               context.hash,
               12,
               "data-a",
               1,
               now_ms: 100,
               protection_ms: 1_000
             )

    assert {:error, :blob_protected} =
             GCGuard.claim(context.hash, "data-a", 1, 1_000, 100)

    assert {:ok, %{hashes: hashes}} =
             OperationIntents.protected_hashes_page(nil, 100, now_ms: 1_000)

    assert MapSet.member?(hashes, context.hash)

    assert {:ok, fragments} =
             OperationIntents.commit_operations(context.operation_id, context.hash, now_ms: 1_001)

    assert {:ok, %{succeeded: true}} =
             Backend.transaction(
               %{compare: fragments.compare, success: fragments.success, failure: []},
               idempotency_key: "intent-close-#{context.operation_id}"
             )

    assert {:ok, %{value: value}} = Backend.get(Keys.operation_intent(context.operation_id))
    assert {:ok, %OperationIntent{state: :committed}} = OperationIntent.cast(value)
  end

  test "GC lock excludes a new writer until deletion is released", context do
    assert {:ok, lock} = GCGuard.claim(context.hash, "data-a", 1, 2_000, 500)

    assert {:error, :gc_lock_active} =
             OperationIntents.open(
               context.second_operation_id,
               context.hash,
               4,
               "data-a",
               1,
               now_ms: 2_001,
               protection_ms: 1_000
             )

    assert :ok = GCGuard.release(context.hash, lock.token)

    assert {:ok, %OperationIntent{state: :pending}} =
             OperationIntents.open(
               context.second_operation_id,
               context.hash,
               4,
               "data-a",
               1,
               now_ms: 2_002,
               protection_ms: 1_000
             )
  end

  test "a writer atomically clears an expired collector lock", context do
    assert :ok =
             Concord.put(Keys.gc_lock(context.hash), %{
               schema: 2,
               hash: context.hash,
               node_id: "data-a",
               node_generation: 1,
               token: "expired",
               lease_until_ms: 99
             })

    assert {:ok, %OperationIntent{state: :pending}} =
             OperationIntents.open(
               context.operation_id,
               context.hash,
               4,
               "data-a",
               1,
               now_ms: 100,
               protection_ms: 1_000
             )

    assert {:ok, nil} = Backend.get(Keys.gc_lock(context.hash))
  end

  test "collector renewal extends only the lock still owned by its token", context do
    assert {:ok, lock} = GCGuard.claim(context.hash, "data-a", 1, 100, 10)

    assert {:ok, %{lease_until_ms: 205}} =
             GCGuard.renew(context.hash, lock.token, 105, 100)

    assert {:error, :gc_lock_active} =
             OperationIntents.open(
               context.operation_id,
               context.hash,
               4,
               "data-a",
               1,
               now_ms: 150,
               protection_ms: 1_000
             )

    assert {:error, :stale_gc_lock} =
             GCGuard.renew(context.hash, "not-the-owner", 151, 100)
  end

  test "expired collector cannot renew after a writer replaces its lock", context do
    assert {:ok, lock} = GCGuard.claim(context.hash, "data-a", 1, 100, 10)

    assert {:ok, %OperationIntent{state: :pending}} =
             OperationIntents.open(
               context.operation_id,
               context.hash,
               4,
               "data-a",
               1,
               now_ms: 111,
               protection_ms: 1_000
             )

    assert {:error, :stale_gc_lock} =
             GCGuard.renew(context.hash, lock.token, 112, 100)
  end

  test "a deterministic writer-GC race has exactly one hash-fence winner" do
    Enum.each(1..20, fn index ->
      hash =
        :sha256
        |> :crypto.hash("fence-race-#{index}")
        |> Base.encode16(case: :lower)

      operation_id = "fence-race-#{index}"
      parent = self()

      writer =
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> :ok
          end

          OperationIntents.open(operation_id, hash, 4, "data-a", 1,
            now_ms: 10_000,
            protection_ms: 1_000
          )
        end)

      collector =
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> :ok
          end

          GCGuard.claim(hash, "data-a", 1, 10_000, 1_000)
        end)

      assert_receive {:ready, writer_pid}
      assert_receive {:ready, collector_pid}
      send(writer_pid, :go)
      send(collector_pid, :go)

      results = [Task.await(writer), Task.await(collector)]
      assert Enum.count(results, &match?({:ok, _result}, &1)) == 1

      Enum.each(
        [
          Keys.operation_intent(operation_id),
          Keys.gc_guard(hash),
          Keys.gc_lock(hash)
        ],
        &Concord.delete/1
      )
    end)
  end
end
