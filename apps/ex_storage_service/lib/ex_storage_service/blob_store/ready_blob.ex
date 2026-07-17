defmodule ExStorageService.BlobStore.ReadyBlob do
  @moduledoc """
  A durable local blob ready to be referenced by object metadata.
  """

  @enforce_keys [:path, :hash, :etag, :size, :source]
  defstruct [:path, :hash, :etag, :size, :source]

  @type t :: %__MODULE__{
          path: String.t(),
          hash: String.t(),
          etag: String.t() | nil,
          size: non_neg_integer(),
          source: ExStorageService.BlobStore.Source.t()
        }
end
