defmodule ExStorageService.Cluster.Drain do
  @moduledoc """
  Node-drain control and bounded, derived progress reporting.

  Setting the durable draining flag immediately excludes the node from new
  rendezvous placement. Repair planners then copy missing desired replicas;
  cleanup jobs retire the old location only after metadata confirms RF.
  """

  alias ExStorageService.Cluster.{Membership, Repair.Planner}
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations}
  alias ExStorageService.Context

  @spec start(Context.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def start(%Context{} = context, node_id, opts \\ []) when is_binary(node_id) do
    membership(opts).set_draining(context.config, node_id, true, metadata_opts(opts))
  end

  @spec cancel(Context.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def cancel(%Context{} = context, node_id, opts \\ []) when is_binary(node_id) do
    membership(opts).set_draining(context.config, node_id, false, metadata_opts(opts))
  end

  @spec status_page(Context.t(), binary(), binary() | nil, pos_integer(), keyword()) ::
          {:ok,
           %{
             node_id: binary(),
             blobs_remaining: non_neg_integer(),
             awaiting_repair: non_neg_integer(),
             ready_to_retire: non_neg_integer(),
             blockers: [map()],
             next_cursor: binary() | nil
           }}
          | {:error, term()}
  def status_page(%Context{} = context, node_id, cursor \\ nil, limit \\ 100, opts \\ []) do
    with {:ok, member} <- membership(opts).member(context.config, node_id, metadata_opts(opts)),
         true <- member.node.draining,
         {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, page} <- catalog(opts).list_page(nil, cursor, limit, metadata_opts(opts)),
         {:ok, progress} <- progress(page.records, node_id, members, opts) do
      {:ok,
       Map.merge(progress, %{
         node_id: node_id,
         next_cursor: page.next_cursor
       })}
    else
      false -> {:error, :node_not_draining}
      error -> error
    end
  end

  defp progress(records, node_id, members, opts) do
    Enum.reduce_while(
      records,
      {:ok, %{blobs_remaining: 0, awaiting_repair: 0, ready_to_retire: 0, blockers: []}},
      fn record, {:ok, progress} ->
        with {:ok, location_records} <-
               locations(opts).list(record.descriptor.hash, metadata_opts(opts)) do
          if Enum.any?(location_records, &(&1.location.node_id == node_id)) do
            case Planner.plan_blob(record.descriptor, members, location_records) do
              {:ok, plan} ->
                field = if plan.missing == [], do: :ready_to_retire, else: :awaiting_repair

                {:cont,
                 {:ok,
                  progress
                  |> Map.update!(:blobs_remaining, &(&1 + 1))
                  |> Map.update!(field, &(&1 + 1))}}

              {:error, reason} ->
                blocker = %{hash: record.descriptor.hash, reason: reason}

                {:cont,
                 {:ok,
                  progress
                  |> Map.update!(:blobs_remaining, &(&1 + 1))
                  |> Map.update!(:blockers, &[blocker | &1])}}
            end
          else
            {:cont, {:ok, progress}}
          end
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
    |> case do
      {:ok, progress} -> {:ok, Map.update!(progress, :blockers, &Enum.reverse/1)}
      error -> error
    end
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp catalog(opts), do: Keyword.get(opts, :catalog, BlobCatalog)
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
end
