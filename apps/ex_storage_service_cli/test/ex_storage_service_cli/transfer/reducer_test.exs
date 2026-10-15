defmodule ExStorageServiceCli.Transfer.ReducerTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Transfer.Planner, Transfer.Reducer}

  @multipart %Planner{
    mode: :multipart,
    size: 11,
    part_count: 2,
    parts: [%{number: 1, offset: 0, length: 6}, %{number: 2, offset: 6, length: 5}]
  }
  @single %Planner{mode: :single, size: 11, part_count: 1}
  @failure %Error{category: :transport, code: :timeout, message: "Timeout"}

  test "single upload verifies before publication and rejects premature success" do
    initial = Reducer.new(@single)
    assert {:error, %Error{code: :invalid_transfer_transition}} = Reducer.step(initial, :verified)
    assert {:ok, transferring, [{:transfer, @single}]} = Reducer.step(initial, :start)
    assert {:error, _} = Reducer.step(transferring, {:transferred, 10})
    assert {:ok, verifying, [:verify]} = Reducer.step(transferring, {:transferred, 11})
    assert {:ok, %{phase: :completed}, [:publish]} = Reducer.step(verifying, :verified)
  end

  test "multipart parts can arrive out of order, completion is sorted and verified" do
    transferring = started()
    assert {:ok, state, []} = Reducer.step(transferring, {:part_completed, 2, "etag2"})
    assert {:error, _} = Reducer.step(state, {:part_completed, 2, "etag2"})
    assert {:error, _} = Reducer.step(state, {:part_completed, 3, "etag3"})

    assert {:ok, completing, [{:complete, "owned", [{1, "etag1"}, {2, "etag2"}]}]} =
             Reducer.step(state, {:part_completed, 1, "etag1"})

    assert completing.phase == :completing
    assert {:ok, verifying, [:verify]} = Reducer.step(completing, :completion_confirmed)
    assert {:ok, %{phase: :completed}, [:publish]} = Reducer.step(verifying, :verified)
    assert {:ok, %{phase: :failed}, [:close]} = Reducer.step(verifying, {:failed, @failure})
  end

  test "cancellation and failure abort only owned uploads and keep failed abort inspectable" do
    assert {:ok, %{phase: :cancelled}, [:close]} = Reducer.step(Reducer.new(@multipart), :cancel)
    assert {:ok, initiating, _} = Reducer.step(Reducer.new(@multipart), :start)
    assert {:ok, %{phase: :failed}, [:close]} = Reducer.step(initiating, {:failed, @failure})

    for event <- [:cancel, {:failed, @failure}] do
      assert {:ok, aborting, [:close, {:abort, "owned"}]} = Reducer.step(started(), event)
      assert aborting.phase == :aborting
      assert {:ok, finished, []} = Reducer.step(aborting, :aborted)
      assert finished.phase in [:failed, :cancelled]

      assert {:ok, %{phase: :abort_failed, abort_result: @failure}, [{:retain, "owned"}]} =
               Reducer.step(aborting, {:abort_failed, @failure})
    end

    assert {:ok, %{phase: :cancelled}, [:close, {:retain, "owned"}]} =
             Reducer.step(started(keep_incomplete: true), :cancel)
  end

  test "uncertain completion requires inspection and never blindly aborts" do
    {:ok, state, []} = Reducer.step(started(), {:part_completed, 1, "one"})
    {:ok, completing, _} = Reducer.step(state, {:part_completed, 2, "two"})

    for event <- [:cancel, :completion_unknown, {:failed, @failure}] do
      assert {:ok, indeterminate, [{:inspect_completion, "owned"}]} =
               Reducer.step(completing, event)

      assert indeterminate.phase == :indeterminate

      assert {:ok, %{phase: :verifying}, [:verify]} =
               Reducer.step(indeterminate, :completion_confirmed)

      assert {:ok, %{phase: :aborting}, [:close, {:abort, "owned"}]} =
               Reducer.step(indeterminate, :completion_rejected)
    end
  end

  defp started(opts \\ []) do
    {:ok, state, [{:initiate, @multipart}]} = Reducer.step(Reducer.new(@multipart, opts), :start)
    {:ok, state, [{:transfer, @multipart}]} = Reducer.step(state, {:initiated, "owned"})
    state
  end
end
