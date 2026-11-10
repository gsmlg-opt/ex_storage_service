defprotocol ExStorageServiceSdk.ByteSource do
  @moduledoc """
  A replayable, bounded byte source that can be folded more than once.

  Request checksums pass `checksum_source:` values through this protocol so a body
  can be hashed before it is streamed, without materializing it in memory.
  """

  @doc """
  Folds the source's chunks in order. `opts` may carry `:coordinator`, the
  request's `ExStorageServiceSdk.Transfer.Coordinator`, for cancellation.
  """
  @spec reduce(t(), acc, (binary(), acc -> acc), keyword()) ::
          {:ok, acc} | {:error, ExStorageServiceSdk.Error.t()}
        when acc: term()
  def reduce(source, acc, reducer, opts)
end
