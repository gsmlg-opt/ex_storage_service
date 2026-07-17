defmodule ExStorageService.Metadata.Models.Operation do
  @moduledoc """
  Durable operation outcome used to resolve an ambiguous transaction timeout.
  """

  @enforce_keys [:operation_id, :bucket, :key, :version_id, :kind]
  defstruct [
    :operation_id,
    :bucket,
    :key,
    :version_id,
    :kind,
    :committed_at
  ]

  @type t :: %__MODULE__{
          operation_id: binary(),
          bucket: binary(),
          key: binary(),
          version_id: binary(),
          kind: :put | :delete_marker,
          committed_at: binary() | nil
        }
end
