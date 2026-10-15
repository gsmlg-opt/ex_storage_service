defmodule ExStorageServiceCli.Transfer.Reducer do
  @moduledoc "Pure transfer lifecycle transitions and data effects; performs no IO."
  alias ExStorageServiceCli.{Error, Transfer.Planner}

  defstruct [
    :plan,
    :upload_id,
    :error,
    :abort_result,
    phase: :planned,
    upload_active: false,
    completed_parts: %{},
    keep_incomplete: false,
    terminal_after_abort: nil
  ]

  def new(%Planner{} = plan, opts \\ []),
    do: %__MODULE__{plan: plan, keep_incomplete: opts[:keep_incomplete] == true}

  def step(%__MODULE__{phase: :planned, plan: %{mode: :multipart}} = state, :start),
    do: ok(state, :initiating, [{:initiate, state.plan}])

  def step(%__MODULE__{phase: :planned} = state, :start),
    do: ok(state, :transferring, [{:transfer, state.plan}])

  def step(%__MODULE__{phase: :initiating} = state, {:initiated, id})
      when is_binary(id) and id != "",
      do:
        ok(%{state | upload_id: id, upload_active: true}, :transferring, [{:transfer, state.plan}])

  def step(
        %__MODULE__{phase: :transferring, plan: %{mode: :single}} = state,
        {:transferred, bytes}
      )
      when bytes == state.plan.size,
      do: ok(state, :verifying, [:verify])

  def step(
        %__MODULE__{phase: :transferring, plan: %{mode: :multipart}} = state,
        {:part_completed, number, etag}
      )
      when is_integer(number) and is_binary(etag) and etag != "" do
    cond do
      number < 1 or number > state.plan.part_count ->
        invalid()

      Map.has_key?(state.completed_parts, number) ->
        invalid()

      true ->
        state = %{state | completed_parts: Map.put(state.completed_parts, number, etag)}

        if map_size(state.completed_parts) == state.plan.part_count do
          ok(state, :completing, [{:complete, state.upload_id, Enum.sort(state.completed_parts)}])
        else
          ok(state, :transferring, [])
        end
    end
  end

  def step(%__MODULE__{phase: :completing} = state, :completion_confirmed),
    do: ok(%{state | upload_active: false}, :verifying, [:verify])

  def step(%__MODULE__{phase: :verifying} = state, :verified),
    do: ok(state, :completed, [:publish])

  def step(%__MODULE__{phase: phase} = state, {:failed, %Error{} = error})
      when phase in [:planned, :initiating, :transferring, :verifying],
      do: terminate(%{state | error: error}, :failed)

  def step(%__MODULE__{phase: phase} = state, :cancel)
      when phase in [:planned, :initiating, :transferring, :verifying],
      do: terminate(state, :cancelled)

  def step(%__MODULE__{phase: :completing} = state, event)
      when event == :cancel or event == :completion_unknown,
      do: ok(state, :indeterminate, [{:inspect_completion, state.upload_id}])

  def step(%__MODULE__{phase: :completing} = state, {:failed, %Error{} = error}),
    do: ok(%{state | error: error}, :indeterminate, [{:inspect_completion, state.upload_id}])

  def step(%__MODULE__{phase: :indeterminate} = state, :completion_confirmed),
    do: ok(%{state | upload_active: false}, :verifying, [:verify])

  def step(%__MODULE__{phase: :indeterminate} = state, :completion_rejected),
    do: terminate(state, :failed)

  def step(%__MODULE__{phase: :aborting} = state, :aborted),
    do: ok(%{state | abort_result: :ok, upload_active: false}, state.terminal_after_abort, [])

  def step(%__MODULE__{phase: :aborting} = state, {:abort_failed, %Error{} = error}),
    do: ok(%{state | abort_result: error}, :abort_failed, [{:retain, state.upload_id}])

  def step(_state, _event), do: invalid()

  # Only an ID delivered by our own initiating transition can be aborted.
  defp terminate(%{upload_active: false} = state, phase), do: ok(state, phase, [:close])

  defp terminate(%{keep_incomplete: true} = state, phase),
    do: ok(state, phase, [:close, {:retain, state.upload_id}])

  defp terminate(state, phase),
    do: ok(%{state | terminal_after_abort: phase}, :aborting, [:close, {:abort, state.upload_id}])

  defp ok(state, phase, effects), do: {:ok, %{state | phase: phase}, effects}

  defp invalid,
    do:
      {:error,
       Error.new(:client, "Invalid transfer lifecycle transition",
         code: :invalid_transfer_transition
       )}
end
