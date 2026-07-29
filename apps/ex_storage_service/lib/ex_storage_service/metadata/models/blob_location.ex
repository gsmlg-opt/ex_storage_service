defmodule ExStorageService.Metadata.Models.BlobLocation do
  @moduledoc """
  Durable evidence that one node has a checksum-verified blob.
  """

  @enforce_keys [:hash, :node_id, :node_generation, :size, :verified_at]
  defstruct [
    :hash,
    :node_id,
    :node_generation,
    :size,
    :verified_at,
    :updated_at,
    :last_error,
    :cleanup_job_id,
    :cleanup_owner_node,
    :cleanup_owner_generation,
    :cleanup_fencing_token,
    :cleanup_retained,
    :cleanup_desired,
    :cleanup_descriptor_revision,
    :cleanup_replication_factor,
    schema: 2,
    state: :ready
  ]

  @type t :: %__MODULE__{
          schema: 2,
          hash: binary(),
          node_id: binary(),
          node_generation: pos_integer(),
          state: :ready | :suspect | :unavailable | :draining | :deleting,
          size: non_neg_integer(),
          verified_at: binary() | integer(),
          updated_at: binary() | integer() | nil,
          last_error: term(),
          cleanup_job_id: binary() | nil,
          cleanup_owner_node: binary() | nil,
          cleanup_owner_generation: pos_integer() | nil,
          cleanup_fencing_token: non_neg_integer() | nil,
          cleanup_retained: [map()] | nil,
          cleanup_desired: [map()] | nil,
          cleanup_descriptor_revision: non_neg_integer() | nil,
          cleanup_replication_factor: pos_integer() | nil
        }

  @spec cast(term()) :: {:ok, t()} | {:error, :invalid_blob_location}
  def cast(%__MODULE__{} = location), do: validate(location)

  def cast(%{} = value) do
    required = [:hash, :node_id, :node_generation, :size, :verified_at]

    if Enum.all?(required, &Map.has_key?(value, &1)) do
      value
      |> Map.take([
        :schema,
        :hash,
        :node_id,
        :node_generation,
        :state,
        :size,
        :verified_at,
        :updated_at,
        :last_error,
        :cleanup_job_id,
        :cleanup_owner_node,
        :cleanup_owner_generation,
        :cleanup_fencing_token,
        :cleanup_retained,
        :cleanup_desired,
        :cleanup_descriptor_revision,
        :cleanup_replication_factor
      ])
      |> then(&struct(__MODULE__, &1))
      |> validate()
    else
      {:error, :invalid_blob_location}
    end
  end

  def cast(_value), do: {:error, :invalid_blob_location}

  defp validate(%__MODULE__{} = location) do
    if location.schema == 2 and is_binary(location.hash) and location.hash != "" and
         is_binary(location.node_id) and location.node_id != "" and
         is_integer(location.node_generation) and location.node_generation >= 1 and
         location.state in [:ready, :suspect, :unavailable, :draining, :deleting] and
         is_integer(location.size) and location.size >= 0 do
      {:ok, location}
    else
      {:error, :invalid_blob_location}
    end
  end
end
