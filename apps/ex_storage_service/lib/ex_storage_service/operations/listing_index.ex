defmodule ExStorageService.Operations.ListingIndex do
  @moduledoc """
  Bounded, per-bucket listing-index diagnostics for release RPCs.

  Reads the bucket and its active descriptor only. It never enumerates buckets,
  objects, index nodes, or historical generations. Node liveness and metadata/data
  readiness do not imply that a legacy bucket has completed index migration.
  """

  alias ExStorageService.Metadata.ListIndex
  alias ExStorageService.Metadata.ListIndex.Backfill

  @counts [:visible_count, :version_count, :active_upload_count, :commit_sequence]
  @states [:ready, :migration_required, :building, :validating]

  @doc "Returns a sanitized descriptor summary without exposing index tokens or roots."
  def status(bucket, opts \\ []) when is_binary(bucket) do
    with {:ok, descriptor} <- ListIndex.status(bucket, opts) do
      validation = ListIndex.ready(%{value: descriptor}, descriptor[:bucket_generation])
      state = if descriptor[:state] in @states, do: descriptor.state, else: :unavailable

      summary = %{
        state: state,
        ready: validation == :ok,
        reason: reason(validation),
        counts: Map.new(@counts, &{&1, count(descriptor[&1])})
      }

      :telemetry.execute(
        [:ex_storage_service, :listing, :index, :status],
        %{count: 1},
        %{state: if(summary.ready, do: :ready, else: state)}
      )

      {:ok, summary}
    end
  end

  @doc "Checks this bucket's active index; unrelated buckets cannot affect the result."
  def readiness(bucket, opts \\ []) do
    case status(bucket, opts) do
      {:ok, %{ready: true} = summary} -> {:ok, summary}
      {:ok, summary} -> {:error, summary}
      {:error, _} = error -> error
    end
  end

  @doc "Starts a gated rebuild after actual source quiescence and backup verification."
  def begin(bucket, opts \\ []), do: Backfill.begin(bucket, opts)

  @doc "Captures the explicitly offline coherent source manifest in private operator storage."
  def capture(bucket, owner_token, path, opts \\ []),
    do: Backfill.capture(bucket, owner_token, path, opts)

  @doc "Replays at most 100 source actions and persists their atomic checkpoints."
  def resume(bucket, owner_token, path, opts \\ []),
    do: Backfill.resume(bucket, owner_token, path, opts)

  @doc "Validates at most 100 manifest entries or tree steps before checkpointing."
  def validate(bucket, owner_token, path, opts \\ []),
    do: Backfill.validate(bucket, owner_token, path, opts)

  @doc "Publishes validated roots and releases the per-bucket mutation gate."
  def cutover(bucket, owner_token, opts \\ []), do: Backfill.cutover(bucket, owner_token, opts)

  @doc "Fences an interrupted build's former owner without discarding its checkpoint."
  def takeover(bucket, owner_token, opts \\ []), do: Backfill.takeover(bucket, owner_token, opts)

  defp count(value) when is_integer(value) and value >= 0, do: value
  defp count(_value), do: nil
  defp reason(:ok), do: nil
  defp reason({:error, reason}), do: reason
end
