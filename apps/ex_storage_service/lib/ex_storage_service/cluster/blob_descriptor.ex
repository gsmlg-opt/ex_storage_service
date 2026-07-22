defmodule ExStorageService.Cluster.BlobDescriptor do
  @moduledoc """
  Immutable identity and desired durability for a content-addressed blob.
  """

  @enforce_keys [
    :schema,
    :hash,
    :algorithm,
    :size,
    :desired_replication_factor,
    :created_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          schema: 2,
          hash: String.t(),
          algorithm: :sha256,
          size: non_neg_integer(),
          desired_replication_factor: pos_integer(),
          created_at: DateTime.t() | String.t() | integer()
        }
end
