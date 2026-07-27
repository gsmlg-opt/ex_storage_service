defmodule ExStorageService.Metadata.Models.Blob do
  @moduledoc """
  Metadata descriptor for content-addressed object bytes.
  """

  @enforce_keys [:hash, :size]
  defstruct [
    :hash,
    :size,
    :created_at,
    schema: 2,
    algorithm: :sha256,
    desired_replication_factor: 1
  ]

  @type t :: %__MODULE__{
          schema: 2,
          hash: binary(),
          algorithm: :sha256,
          size: non_neg_integer(),
          created_at: binary() | nil,
          desired_replication_factor: pos_integer()
        }
end
