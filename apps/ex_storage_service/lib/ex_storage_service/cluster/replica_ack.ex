defmodule ExStorageService.Cluster.ReplicaAck do
  @moduledoc """
  Evidence that one node durably stored and verified a blob.
  """

  @enforce_keys [
    :node_id,
    :node_generation,
    :hash,
    :size,
    :verified_at,
    :fencing_or_request_id
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          node_id: String.t(),
          node_generation: String.t() | non_neg_integer(),
          hash: String.t(),
          size: non_neg_integer(),
          verified_at: DateTime.t() | String.t() | integer(),
          fencing_or_request_id: String.t()
        }
end
