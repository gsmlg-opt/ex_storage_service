defmodule ExStorageService.Metadata.Models.ObjectVersion do
  @moduledoc """
  Immutable metadata for one logical object version.
  """

  @enforce_keys [:bucket, :key, :version_id, :operation_id]
  defstruct [
    :bucket,
    :key,
    :version_id,
    :operation_id,
    :parent_version_id,
    :content_hash,
    :size,
    :etag,
    :content_type,
    :created_at,
    :metadata,
    object_type: :blob,
    is_delete_marker: false
  ]

  @type t :: %__MODULE__{
          bucket: binary(),
          key: binary(),
          version_id: binary(),
          operation_id: binary(),
          parent_version_id: binary() | nil,
          content_hash: binary() | nil,
          size: non_neg_integer() | nil,
          etag: binary() | nil,
          content_type: binary() | nil,
          created_at: binary() | nil,
          metadata: map() | nil,
          object_type: atom(),
          is_delete_marker: boolean()
        }
end
