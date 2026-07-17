defmodule ExStorageService.Metadata.Models.Blob do
  @moduledoc """
  Metadata descriptor for content-addressed object bytes.
  """

  @enforce_keys [:sha256, :size]
  defstruct [
    :sha256,
    :size,
    :physical_path,
    :created_at,
    :last_seen_at,
    :locations,
    state: :active
  ]

  @type t :: %__MODULE__{
          sha256: binary(),
          size: non_neg_integer(),
          physical_path: binary() | nil,
          created_at: binary() | nil,
          last_seen_at: binary() | nil,
          locations: [term()] | nil,
          state: atom()
        }
end
