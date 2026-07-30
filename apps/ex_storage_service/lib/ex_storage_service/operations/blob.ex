defmodule ExStorageService.Operations.Blob do
  @moduledoc """
  Read-only blob location and bounded durability audit operations.
  """

  alias ExStorageService.Cluster.{Membership, Repair.Planner}
  alias ExStorageService.Context
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations}

  @spec locate(binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def locate(hash, opts \\ []) when is_binary(hash) and hash != "" do
    with {:ok, descriptor} <- catalog(opts).get(hash, metadata_opts(opts)),
         {:ok, locations} <- locations(opts).list(hash, metadata_opts(opts)) do
      {:ok,
       %{
         descriptor: descriptor.descriptor,
         descriptor_revision: descriptor.mod_revision,
         locations: Enum.map(locations, &location_summary/1)
       }}
    end
  end

  @spec audit_page(Context.t(), binary() | nil, pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def audit_page(context, cursor \\ nil, limit \\ 100, opts \\ [])

  def audit_page(%Context{config: %{mode: :cluster}} = context, cursor, limit, opts)
      when (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and limit > 0 do
    with {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, page} <- catalog(opts).list_page(nil, cursor, limit, metadata_opts(opts)),
         {:ok, audit} <- audit_records(page.records, members, opts) do
      {:ok, Map.put(audit, :next_cursor, page.next_cursor)}
    end
  end

  def audit_page(%Context{}, _cursor, _limit, _opts), do: {:error, :standalone_mode}

  defp audit_records(records, members, opts) do
    Enum.reduce_while(
      records,
      {:ok, %{blobs: 0, healthy: 0, under_replicated: 0, unavailable: 0, issues: []}},
      fn record, {:ok, audit} ->
        with {:ok, location_records} <-
               locations(opts).list(record.descriptor.hash, metadata_opts(opts)),
             {:ok, plan} <- Planner.plan_blob(record.descriptor, members, location_records) do
          issue = audit_issue(record, plan)

          audit =
            audit
            |> Map.update!(:blobs, &(&1 + 1))
            |> update_health(plan)
            |> add_issue(issue)

          {:cont, {:ok, audit}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
    |> case do
      {:ok, audit} -> {:ok, Map.update!(audit, :issues, &Enum.reverse/1)}
      error -> error
    end
  end

  defp update_health(audit, %{missing: [], sources: [_source | _sources]}),
    do: Map.update!(audit, :healthy, &(&1 + 1))

  defp update_health(audit, %{sources: []}),
    do:
      audit
      |> Map.update!(:under_replicated, &(&1 + 1))
      |> Map.update!(:unavailable, &(&1 + 1))

  defp update_health(audit, _plan),
    do: Map.update!(audit, :under_replicated, &(&1 + 1))

  defp add_issue(audit, nil), do: audit
  defp add_issue(audit, issue), do: Map.update!(audit, :issues, &[issue | &1])

  defp audit_issue(_record, %{missing: [], excess: []}), do: nil

  defp audit_issue(record, plan) do
    %{
      hash: record.descriptor.hash,
      descriptor_revision: record.mod_revision,
      missing: Enum.map(plan.missing, & &1.node.node_id),
      excess: Enum.map(plan.excess, & &1.location.node_id),
      sources: Enum.map(plan.sources, & &1.location.node_id)
    }
  end

  defp location_summary(%{location: location, mod_revision: revision}) do
    %{
      node_id: location.node_id,
      node_generation: location.node_generation,
      state: location.state,
      size: location.size,
      verified_at: location.verified_at,
      mod_revision: revision
    }
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp catalog(opts), do: Keyword.get(opts, :catalog, BlobCatalog)
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
end
