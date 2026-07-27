defmodule ExStorageService.Metadata.Models.ClusterNode do
  @moduledoc """
  Persistent cluster control-plane record.

  This record describes stable placement eligibility. It is not a heartbeat.
  """

  @enforce_keys [:node_id, :generation, :role, :enabled, :draining]
  defstruct [
    :node_id,
    :generation,
    :role,
    :erlang_endpoint,
    :internal_endpoint,
    :zone,
    :capacity,
    :updated_at,
    schema: 2,
    enabled: true,
    draining: false
  ]

  @type t :: %__MODULE__{
          schema: 2,
          node_id: binary(),
          generation: pos_integer(),
          role: :data | :metadata,
          erlang_endpoint: node() | nil,
          internal_endpoint: binary() | nil,
          enabled: boolean(),
          draining: boolean(),
          zone: binary() | nil,
          capacity: pos_integer() | nil,
          updated_at: binary() | nil
        }
end
