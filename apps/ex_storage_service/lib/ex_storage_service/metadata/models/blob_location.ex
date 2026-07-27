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
    schema: 2,
    state: :ready
  ]

  @type t :: %__MODULE__{
          schema: 2,
          hash: binary(),
          node_id: binary(),
          node_generation: pos_integer(),
          state: :ready | :suspect | :draining,
          size: non_neg_integer(),
          verified_at: binary() | integer()
        }
end
