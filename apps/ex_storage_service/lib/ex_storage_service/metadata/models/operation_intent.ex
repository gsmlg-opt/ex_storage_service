defmodule ExStorageService.Metadata.Models.OperationIntent do
  @moduledoc """
  Durable protection for bytes that may be referenced by an unresolved write.

  Intents are created after a content hash is known and before replica
  publication. A final metadata transaction marks the matching intent
  committed. Pending and unknown intents remain garbage-collection roots until
  their safety deadline.
  """

  @states [:pending, :unknown, :committed, :aborted]

  @enforce_keys [
    :operation_id,
    :hash,
    :size,
    :node_id,
    :node_generation,
    :state,
    :protected_until_ms,
    :created_at_ms,
    :updated_at_ms
  ]
  defstruct [
    :operation_id,
    :hash,
    :size,
    :node_id,
    :node_generation,
    :state,
    :protected_until_ms,
    :created_at_ms,
    :updated_at_ms,
    schema: 2
  ]

  @type state :: :pending | :unknown | :committed | :aborted
  @type t :: %__MODULE__{
          schema: 2,
          operation_id: binary(),
          hash: binary(),
          size: non_neg_integer(),
          node_id: binary(),
          node_generation: pos_integer(),
          state: state(),
          protected_until_ms: non_neg_integer(),
          created_at_ms: non_neg_integer(),
          updated_at_ms: non_neg_integer()
        }

  @spec new(
          binary(),
          binary(),
          non_neg_integer(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, t()} | {:error, :invalid_operation_intent}
  def new(
        operation_id,
        hash,
        size,
        node_id,
        node_generation,
        protected_until_ms,
        now_ms \\ System.system_time(:millisecond)
      ) do
    cast(%{
      schema: 2,
      operation_id: operation_id,
      hash: hash,
      size: size,
      node_id: node_id,
      node_generation: node_generation,
      state: :pending,
      protected_until_ms: protected_until_ms,
      created_at_ms: now_ms,
      updated_at_ms: now_ms
    })
  end

  @spec cast(term()) :: {:ok, t()} | {:error, :invalid_operation_intent}
  def cast(%__MODULE__{} = intent), do: validate(intent)

  def cast(value) when is_map(value) do
    value
    |> Map.take([
      :schema,
      :operation_id,
      :hash,
      :size,
      :node_id,
      :node_generation,
      :state,
      :protected_until_ms,
      :created_at_ms,
      :updated_at_ms
    ])
    |> then(&struct(__MODULE__, &1))
    |> validate()
  rescue
    _error in [ArgumentError, KeyError] -> {:error, :invalid_operation_intent}
  end

  def cast(_value), do: {:error, :invalid_operation_intent}

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = intent), do: Map.from_struct(intent)

  @spec protected?(t(), non_neg_integer()) :: boolean()
  def protected?(%__MODULE__{state: state, protected_until_ms: deadline}, now_ms),
    do: state in [:pending, :unknown] and deadline >= now_ms

  defp validate(%__MODULE__{} = intent) do
    if intent.schema == 2 and non_empty_binary?(intent.operation_id) and
         non_empty_binary?(intent.hash) and is_integer(intent.size) and intent.size >= 0 and
         non_empty_binary?(intent.node_id) and is_integer(intent.node_generation) and
         intent.node_generation >= 1 and intent.state in @states and
         non_negative_integer?(intent.protected_until_ms) and
         non_negative_integer?(intent.created_at_ms) and
         non_negative_integer?(intent.updated_at_ms) do
      {:ok, intent}
    else
      {:error, :invalid_operation_intent}
    end
  end

  defp non_empty_binary?(value), do: is_binary(value) and value != ""
  defp non_negative_integer?(value), do: is_integer(value) and value >= 0
end
