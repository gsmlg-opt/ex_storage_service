defmodule ExStorageService.Metadata.Models.Job do
  @moduledoc """
  Typed boundary for one durable, fenced background job.

  Concord values are persisted as plain maps so native nested-field
  comparisons can inspect lease and fencing fields.
  """

  @kinds [:repair_blob, :cross_cluster_put, :cross_cluster_delete, :scrub, :cleanup]
  @states [:pending, :running, :completed, :failed]

  @enforce_keys [
    :job_id,
    :operation_id,
    :kind,
    :payload,
    :state,
    :lease_until_ms,
    :fencing_token,
    :attempts,
    :max_attempts,
    :next_attempt_at_ms,
    :created_at_ms,
    :updated_at_ms
  ]
  defstruct [
    :job_id,
    :operation_id,
    :kind,
    :payload,
    :state,
    :owner_node,
    :owner_generation,
    :lease_until_ms,
    :fencing_token,
    :attempts,
    :max_attempts,
    :next_attempt_at_ms,
    :created_at_ms,
    :updated_at_ms,
    :completed_at_ms,
    :last_error,
    schema: 2
  ]

  @type kind :: :repair_blob | :cross_cluster_put | :cross_cluster_delete | :scrub | :cleanup
  @type state :: :pending | :running | :completed | :failed

  @type t :: %__MODULE__{
          schema: 2,
          job_id: binary(),
          operation_id: binary(),
          kind: kind(),
          payload: map(),
          state: state(),
          owner_node: binary() | nil,
          owner_generation: pos_integer() | nil,
          lease_until_ms: non_neg_integer(),
          fencing_token: non_neg_integer(),
          attempts: non_neg_integer(),
          max_attempts: pos_integer(),
          next_attempt_at_ms: non_neg_integer(),
          created_at_ms: non_neg_integer(),
          updated_at_ms: non_neg_integer(),
          completed_at_ms: non_neg_integer() | nil,
          last_error: binary() | nil
        }

  @spec new(binary(), map(), non_neg_integer(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(operation_id, event, now_ms, opts \\ [])

  def new(operation_id, event, now_ms, opts)
      when is_binary(operation_id) and is_map(event) and is_integer(now_ms) and now_ms >= 0 do
    value = %{
      schema: 2,
      job_id: field(event, :id),
      operation_id: operation_id,
      kind: field(event, :kind),
      payload: field(event, :payload) || %{},
      state: :pending,
      owner_node: nil,
      owner_generation: nil,
      lease_until_ms: 0,
      fencing_token: 0,
      attempts: 0,
      max_attempts: field(event, :max_attempts) || Keyword.get(opts, :max_attempts, 3),
      next_attempt_at_ms: now_ms,
      created_at_ms: now_ms,
      updated_at_ms: now_ms,
      completed_at_ms: nil,
      last_error: nil
    }

    cast(value)
  end

  def new(_operation_id, _event, _now_ms, _opts), do: {:error, :invalid_job}

  @spec cast(map() | t()) :: {:ok, t()} | {:error, term()}
  def cast(%__MODULE__{} = job), do: validate(job)

  def cast(value) when is_map(value) do
    job = %__MODULE__{
      schema: field(value, :schema) || 2,
      job_id: field(value, :job_id),
      operation_id: field(value, :operation_id),
      kind: field(value, :kind),
      payload: field(value, :payload) || %{},
      state: field(value, :state),
      owner_node: field(value, :owner_node),
      owner_generation: field(value, :owner_generation),
      lease_until_ms: field(value, :lease_until_ms),
      fencing_token: field(value, :fencing_token),
      attempts: field(value, :attempts),
      max_attempts: field(value, :max_attempts),
      next_attempt_at_ms: field(value, :next_attempt_at_ms),
      created_at_ms: field(value, :created_at_ms),
      updated_at_ms: field(value, :updated_at_ms),
      completed_at_ms: field(value, :completed_at_ms),
      last_error: field(value, :last_error)
    }

    validate(job)
  end

  def cast(_value), do: {:error, :invalid_job}

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = job), do: Map.from_struct(job)

  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @spec states() :: [state()]
  def states, do: @states

  defp validate(%__MODULE__{} = job) do
    cond do
      job.schema != 2 ->
        {:error, :invalid_job_schema}

      not is_binary(job.job_id) or job.job_id == "" ->
        {:error, :invalid_job_id}

      not is_binary(job.operation_id) or job.operation_id == "" ->
        {:error, :invalid_operation_id}

      job.kind not in @kinds ->
        {:error, :invalid_job_kind}

      not is_map(job.payload) ->
        {:error, :invalid_job_payload}

      job.state not in @states ->
        {:error, :invalid_job_state}

      not valid_owner?(job.owner_node, job.owner_generation) ->
        {:error, :invalid_job_owner}

      not non_negative_integer?(job.lease_until_ms) ->
        {:error, :invalid_job_lease}

      not non_negative_integer?(job.fencing_token) ->
        {:error, :invalid_job_fencing_token}

      not non_negative_integer?(job.attempts) ->
        {:error, :invalid_job_attempts}

      not is_integer(job.max_attempts) or job.max_attempts < 1 ->
        {:error, :invalid_job_max_attempts}

      not non_negative_integer?(job.next_attempt_at_ms) ->
        {:error, :invalid_job_retry_time}

      not non_negative_integer?(job.created_at_ms) or
          not non_negative_integer?(job.updated_at_ms) ->
        {:error, :invalid_job_timestamp}

      not is_nil(job.completed_at_ms) and not non_negative_integer?(job.completed_at_ms) ->
        {:error, :invalid_job_completion_time}

      not is_nil(job.last_error) and not is_binary(job.last_error) ->
        {:error, :invalid_job_error}

      true ->
        {:ok, job}
    end
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp valid_owner?(nil, nil), do: true
  defp valid_owner?(node, nil), do: is_binary(node) and node != ""

  defp valid_owner?(node, generation),
    do: is_binary(node) and node != "" and is_integer(generation) and generation > 0

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0
end
