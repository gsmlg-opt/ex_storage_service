defmodule ExStorageService.Operations.Repair do
  @moduledoc """
  Bounded repair planning and durable repair-run operations.

  `plan_page/5` is strictly read-only. `run_page/5` delegates to the repair
  planner so work enters the durable outbox/job path used by background repair.
  """

  alias ExStorageService.Cluster.{Membership, Repair.Planner}
  alias ExStorageService.Context
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations}

  @spec plan_page(Context.t(), binary(), binary() | nil, pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_page(context, shard, cursor \\ nil, limit \\ 100, opts \\ [])

  def plan_page(
        %Context{config: %{mode: :cluster}} = context,
        shard,
        cursor,
        limit,
        opts
      )
      when is_binary(shard) and (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and
             limit > 0 do
    if shard in Planner.shards() do
      plan_valid_page(context, shard, cursor, limit, opts)
    else
      {:error, :invalid_shard}
    end
  end

  def plan_page(%Context{}, _shard, _cursor, _limit, _opts),
    do: {:error, :standalone_mode}

  @spec run_page(Context.t(), binary(), binary() | nil, pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run_page(context, shard, cursor \\ nil, limit \\ 100, opts \\ [])

  def run_page(
        %Context{config: %{mode: :cluster}} = context,
        shard,
        cursor,
        limit,
        opts
      ) do
    if shard in Planner.shards() do
      Planner.run_page(context, shard, cursor, limit, opts)
    else
      {:error, :invalid_shard}
    end
  end

  def run_page(%Context{}, _shard, _cursor, _limit, _opts),
    do: {:error, :standalone_mode}

  defp plan_valid_page(context, shard, cursor, limit, opts) do
    local_node_id = context.config.node_id

    with {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, ^local_node_id} <- Planner.owner(shard, members),
         {:ok, page} <- catalog(opts).list_page(shard, cursor, limit, metadata_opts(opts)),
         {:ok, plans} <- plans(page.records, members, opts) do
      {:ok,
       %{
         shard: shard,
         blobs: length(plans),
         actions: Enum.reduce(plans, 0, &(action_count(&1) + &2)),
         plans: plans,
         next_cursor: page.next_cursor
       }}
    else
      {:ok, _other_owner} -> {:error, :not_shard_owner}
      error -> error
    end
  end

  defp plans(records, members, opts) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, plans} ->
      with {:ok, locations} <-
             locations(opts).list(record.descriptor.hash, metadata_opts(opts)),
           {:ok, plan} <- Planner.plan_blob(record.descriptor, members, locations) do
        summary = %{
          hash: record.descriptor.hash,
          descriptor_revision: record.mod_revision,
          missing: Enum.map(plan.missing, & &1.node.node_id),
          excess: Enum.map(plan.excess, & &1.location.node_id),
          sources: Enum.map(plan.sources, & &1.location.node_id)
        }

        {:cont, {:ok, [summary | plans]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, plans} -> {:ok, Enum.reverse(plans)}
      error -> error
    end
  end

  defp action_count(plan), do: length(plan.missing) + length(plan.excess)

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp catalog(opts), do: Keyword.get(opts, :catalog, BlobCatalog)
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
end
