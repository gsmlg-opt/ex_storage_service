defmodule ExStorageService.BlobStore do
  @moduledoc """
  Behaviour for durable, content-addressed blob storage.

  Implementations stage bytes before publishing them so object metadata is
  never required to point at a partially written file.
  """

  alias ExStorageService.BlobStore.{ReadyBlob, StagedBlob}

  @type hash :: String.t()
  @type range :: nil | :all | {non_neg_integer(), non_neg_integer()}
  @type options :: keyword()

  @callback stage(Enumerable.t() | binary(), options()) ::
              {:ok, StagedBlob.t()} | {:error, term()}
  @callback commit(StagedBlob.t(), options()) :: {:ok, ReadyBlob.t()} | {:error, term()}
  @callback discard(StagedBlob.t(), options()) :: :ok | {:error, term()}
  @callback stat(hash(), options()) :: {:ok, map()} | {:error, term()}
  @callback open(hash(), range(), options()) ::
              {:ok, ExStorageService.BlobStore.Source.t()} | {:error, term()}
  @callback delete(hash(), options()) :: :ok | {:error, term()}
  @callback verify(hash(), options()) :: :ok | {:error, term()}
end
