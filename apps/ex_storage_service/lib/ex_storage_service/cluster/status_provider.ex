defmodule ExStorageService.Cluster.StatusProvider do
  @moduledoc """
  Aggregates topology-fenced repair-owner snapshots and durable maintenance jobs.

  A planner snapshot contributes only when it matches the current membership
  topology, owner generation, deterministic shard assignment, and freshness
  window. Missing or stale owners make the result explicitly partial.
  """

  alias ExStorageService.Cluster.{Membership, Repair.Planner}
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.{JobStore, Keys}
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.{Context, Telemetry}

  @maintenance_kinds [:repair_blob, :scrub, :cleanup]
  @default_freshness_ms :timer.minutes(5)
  @default_page_size 100
  @default_max_job_pages 1_000

  @spec snapshot(keyword()) :: {:ok, map()} | {:error, term()}
  def snapshot(opts \\ []) do
    with {:ok, context} <- context(opts),
         {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, owners} <- expected_owners(members),
         topology <- Planner.topology_fingerprint(members),
         {:ok, owner_result} <- owner_snapshots(owners, topology, opts),
         {:ok, backlog, backlog_complete?} <- durable_backlog(opts) do
      complete? = owner_result.invalid == [] and backlog_complete?
      under_replicated = sum(owner_result.valid, :under_replicated_blobs)
      repair_backlog = Map.put(backlog, :under_replicated, under_replicated)

      if complete?, do: Telemetry.repair_backlog(repair_backlog)

      {:ok,
       %{
         complete: complete?,
         desired_replicas: context.config.replication_factor,
         required_write_quorum: context.config.write_quorum,
         eligible_nodes: map_size(owners),
         owners_expected: map_size(owners),
         owners_current: length(owner_result.valid),
         invalid_owners: owner_result.invalid,
         actual_replicas: sum(owner_result.valid, :actual_replicas),
         under_replicated_blobs: under_replicated,
         unavailable_blobs: sum(owner_result.valid, :unavailable_blobs),
         repair_backlog: repair_backlog,
         updated_at: oldest_update(owner_result.valid)
       }}
    end
  end

  defp expected_owners(members) do
    member_by_id = Map.new(members, &{&1.node.node_id, &1})

    Planner.shards()
    |> Enum.reduce_while({:ok, %{}}, fn shard, {:ok, owners} ->
      case Planner.owner(shard, members) do
        {:ok, owner_id} ->
          case Map.fetch(member_by_id, owner_id) do
            {:ok, member} ->
              {:cont,
               {:ok,
                Map.update(owners, owner_id, %{member: member, shards: [shard]}, fn owner ->
                  %{owner | shards: [shard | owner.shards]}
                end)}}

            :error ->
              {:halt, {:error, {:unknown_status_owner, owner_id}}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, owners} ->
        {:ok,
         Map.new(owners, fn {id, owner} -> {id, %{owner | shards: Enum.sort(owner.shards)}} end)}

      error ->
        error
    end
  end

  defp owner_snapshots(owners, topology, opts) do
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    freshness_ms = Keyword.get(opts, :freshness_ms, @default_freshness_ms)

    owners
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, %{valid: [], invalid: []}}, fn {owner_id, expected},
                                                              {:ok, result} ->
      case backend(opts).get(Keys.cluster_status_owner(owner_id), read_opts(opts)) do
        {:ok, %{value: snapshot}} ->
          case validate_owner_snapshot(
                 snapshot,
                 owner_id,
                 expected,
                 topology,
                 now_ms,
                 freshness_ms
               ) do
            :ok ->
              {:cont, {:ok, %{result | valid: [snapshot | result.valid]}}}

            {:error, reason} ->
              invalid = [%{node_id: owner_id, reason: reason} | result.invalid]
              {:cont, {:ok, %{result | invalid: invalid}}}
          end

        {:ok, nil} ->
          invalid = [%{node_id: owner_id, reason: :missing} | result.invalid]
          {:cont, {:ok, %{result | invalid: invalid}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, result} ->
        {:ok,
         %{
           valid: Enum.reverse(result.valid),
           invalid: Enum.reverse(result.invalid)
         }}

      error ->
        error
    end
  end

  defp validate_owner_snapshot(snapshot, owner_id, expected, topology, now_ms, freshness_ms)
       when is_map(snapshot) do
    updated_at_ms = Map.get(snapshot, :updated_at_ms)

    cond do
      Map.get(snapshot, :schema) != 2 ->
        {:error, :invalid_schema}

      Map.get(snapshot, :complete) != true ->
        {:error, :incomplete}

      Map.get(snapshot, :node_id) != owner_id ->
        {:error, :identity_mismatch}

      Map.get(snapshot, :node_generation) != expected.member.node.generation ->
        {:error, :generation_mismatch}

      Map.get(snapshot, :topology_fingerprint) != topology ->
        {:error, :topology_mismatch}

      Map.get(snapshot, :owned_shards) != expected.shards ->
        {:error, :shard_mismatch}

      not valid_counters?(snapshot) ->
        {:error, :invalid_counters}

      not is_integer(updated_at_ms) or updated_at_ms < 0 ->
        {:error, :invalid_timestamp}

      updated_at_ms > now_ms ->
        {:error, :future_timestamp}

      now_ms - updated_at_ms > freshness_ms ->
        {:error, :stale}

      true ->
        :ok
    end
  end

  defp validate_owner_snapshot(
         _snapshot,
         _owner_id,
         _expected,
         _topology,
         _now_ms,
         _freshness_ms
       ),
       do: {:error, :invalid_snapshot}

  defp valid_counters?(snapshot) do
    Enum.all?(
      [:actual_replicas, :under_replicated_blobs, :unavailable_blobs],
      fn field ->
        value = Map.get(snapshot, field)
        is_integer(value) and value >= 0
      end
    )
  end

  defp durable_backlog(opts) do
    do_durable_backlog(
      nil,
      Keyword.get(opts, :job_page_size, @default_page_size),
      Keyword.get(opts, :max_job_pages, @default_max_job_pages),
      %{pending: 0, running: 0, failed: 0},
      opts
    )
  end

  defp do_durable_backlog(_cursor, _page_size, 0, counts, _opts),
    do: {:ok, counts, false}

  defp do_durable_backlog(cursor, page_size, pages_left, counts, opts) do
    case job_store(opts).list_page(cursor, page_size, read_opts(opts)) do
      {:ok, %{jobs: jobs, next_cursor: next_cursor}} ->
        counts = Enum.reduce(jobs, counts, &count_job/2)

        if next_cursor,
          do: do_durable_backlog(next_cursor, page_size, pages_left - 1, counts, opts),
          else: {:ok, counts, true}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp count_job(%Job{kind: kind, state: state}, counts)
       when kind in @maintenance_kinds and state in [:pending, :running, :failed],
       do: Map.update!(counts, state, &(&1 + 1))

  defp count_job(_job, counts), do: counts

  defp sum(snapshots, field), do: Enum.reduce(snapshots, 0, &(Map.get(&1, field, 0) + &2))

  defp oldest_update([]), do: nil

  defp oldest_update(snapshots) do
    snapshots
    |> Enum.map(&Map.fetch!(&1, :updated_at_ms))
    |> Enum.min()
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_iso8601()
  end

  defp context(opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context -> {:ok, context}
      nil -> Context.default()
    end
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp read_opts(opts), do: Keyword.put_new(metadata_opts(opts), :consistency, :strong)
  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp job_store(opts), do: Keyword.get(opts, :job_store, JobStore)
end
