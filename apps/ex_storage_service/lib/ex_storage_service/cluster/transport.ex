defmodule ExStorageService.Cluster.Transport do
  @moduledoc """
  Content-addressed transport contract for blob operations between cluster nodes.

  Core code depends only on this behaviour. Concrete transports live outside
  the core application so their protocol dependencies do not leak into the
  storage domain.
  """

  alias ExStorageService.BlobStore.{Source, StagedBlob}
  alias ExStorageService.Cluster.{BlobDescriptor, ReplicaAck}
  alias ExStorageService.Context

  @type node_ref :: term()
  @type options :: keyword()
  @type range :: nil | :all | {non_neg_integer(), non_neg_integer()}
  @type staged_source :: StagedBlob.t() | Source.t()
  @type blob_info :: %{required(:hash) => String.t(), required(:size) => non_neg_integer()}

  @callback put_blob(Context.t(), node_ref(), staged_source(), BlobDescriptor.t(), options()) ::
              {:ok, ReplicaAck.t()} | {:error, term()}
  @callback head_blob(Context.t(), node_ref(), String.t(), options()) ::
              {:ok, blob_info()} | {:error, term()}
  @callback open_blob(Context.t(), node_ref(), String.t(), range(), options()) ::
              {:ok, Source.t()} | {:error, term()}
  @callback delete_blob(Context.t(), node_ref(), String.t(), options()) ::
              :ok | {:error, term()}
  @callback health(Context.t(), node_ref(), options()) :: :ok | {:error, term()}
end
