defmodule ExStorageService.BlobStore.Source do
  @moduledoc """
  A servable blob source.

  File sources retain an explicit offset and length so callers can use
  `send_file` for loose, legacy, packed, and ranged reads.
  """

  @type t ::
          {:file, String.t(), non_neg_integer(), non_neg_integer()}
          | {:stream, Enumerable.t() | function(), non_neg_integer()}

  @spec file(String.t(), non_neg_integer(), non_neg_integer()) :: t()
  def file(path, offset, length), do: {:file, path, offset, length}

  @spec stream(Enumerable.t() | function(), non_neg_integer()) :: t()
  def stream(enumerable_or_callback, content_length),
    do: {:stream, enumerable_or_callback, content_length}
end
