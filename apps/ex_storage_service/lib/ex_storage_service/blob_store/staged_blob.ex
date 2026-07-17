defmodule ExStorageService.BlobStore.StagedBlob do
  @moduledoc """
  Bytes written to a temporary file but not yet published in the local CAS.
  """

  @enforce_keys [:path, :hash, :etag, :size]
  defstruct [:path, :hash, :etag, :size]

  @type t :: %__MODULE__{
          path: String.t(),
          hash: String.t(),
          etag: String.t() | nil,
          size: non_neg_integer()
        }
end
