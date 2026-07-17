defmodule ExStorageService.Metadata.Models.ObjectHead do
  @moduledoc """
  Mutable pointer to the latest immutable object version.
  """

  @enforce_keys [:bucket, :key, :version_id, :operation_id]
  defstruct [
    :bucket,
    :key,
    :version_id,
    :operation_id,
    :updated_at,
    is_delete_marker: false
  ]

  @type t :: %__MODULE__{
          bucket: binary(),
          key: binary(),
          version_id: binary(),
          operation_id: binary(),
          updated_at: binary() | nil,
          is_delete_marker: boolean()
        }
end
