defmodule ExStorageService.Cluster.Repair.Planner do
  @moduledoc """
  Pure placement reconciliation plus bounded, deterministically owned scans.

  The 256 SHA-256 prefix shards are assigned with the same rendezvous function
  used for blobs. Only the owner scans a shard; every emitted action is still
  revalidated by its handler because membership and page contents are live.
  """

  use GenServer

  alias ExStorageService.Cluster.{Membership, Placement}
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations, Keys, Outbox}
  alias ExStorageService.Metadata.Models.{Blob, BlobLocation}
  alias ExStorageService.{Context, InstanceConfig}

  @shards for value <- 0..255,
              do:
                value
                |> Integer.to_string(16)
                |> String.downcase()
                |> String.pad_leading(2, "0")
  @default_scan_interval 250
  @default_page_size 100

  @type plan :: %{
          descriptor: Blob.t(),
          desired: [map()],
          ready: [map()],
          ready_desired: [map()],
          missing: [map()],
          excess: [map()],
          sources: [map()],
          topology_fingerprint: binary()
        }

  @spec shards() :: [binary()]
  def shards, do: @shards

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec scan_now(GenServer.server()) :: :ok
  def scan_now(server), do: GenServer.cast(server, :scan)

  @spec status(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def status(server) do
    GenServer.call(server, :status)
  catch
    :exit, _reason -> {:error, :repair_planner_unavailable}
  end

  @impl true
  def init(opts) do
    context = Keyword.fetch!(opts, :context)

    state = %{
      context: context,
      interval: Keyword.get(opts, :scan_interval, @default_scan_interval),
      page_size: Keyword.get(opts, :page_size, @default_page_size),
      planner_opts: Keyword.get(opts, :planner_opts, []),
      queue: [],
      cursor: nil,
      owned_shards: [],
      topology_fingerprint: nil,
      cycle: empty_cycle(),
      snapshot: initial_snapshot(context)
    }

    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(0)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:scan, state), do: {:noreply, scan(state)}

  @impl true
  def handle_call(:status, _from, state), do: {:reply, {:ok, state.snapshot}, state}

  @impl true
  def handle_info(:scan, state) do
    state = scan(state)
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @spec owner(binary(), [map()]) :: {:ok, binary()} | {:error, term()}
  def owner(shard, members) when shard in @shards do
    with {:ok, [node]} <- Placement.select("repair-shard:" <> shard, nodes(members), 1) do
      {:ok, node.node_id}
    end
  end

  @spec owned_shards(binary(), [map()]) :: [binary()]
  def owned_shards(node_id, members) when is_binary(node_id) do
    Enum.filter(@shards, &(owner(&1, members) == {:ok, node_id}))
  end

  @spec topology_fingerprint([map()]) :: binary()
  def topology_fingerprint(members) do
    members
    |> Enum.map(fn member ->
      node = member_node(member)

      {
        node.node_id,
        node.generation,
        node.role,
        node.enabled,
        node.draining,
        node.internal_endpoint,
        Map.get(member, :mod_revision)
      }
    end)
    |> Enum.sort()
    |> fingerprint()
  end

  @spec plan_blob(Blob.t() | map(), [map()], [map()]) :: {:ok, plan()} | {:error, term()}
  def plan_blob(descriptor, members, location_records) do
    descriptor = descriptor(descriptor)
    member_by_id = Map.new(members, &{member_node(&1).node_id, &1})

    with {:ok, desired_nodes} <-
           Placement.select(
             descriptor.hash,
             Enum.map(members, &member_node/1),
             descriptor.desired_replication_factor
           ) do
      desired = Enum.map(desired_nodes, &Map.fetch!(member_by_id, &1.node_id))
      desired_ids = MapSet.new(desired_nodes, & &1.node_id)

      current =
        Enum.filter(location_records, fn record ->
          location = location(record)

          case Map.get(member_by_id, location.node_id) do
            nil -> false
            member -> member_node(member).generation == location.node_generation
          end
        end)

      ready =
        Enum.filter(current, &(location(&1).state == :ready))

      ready_ids = MapSet.new(ready, &location(&1).node_id)
      ready_desired = Enum.filter(ready, &MapSet.member?(desired_ids, location(&1).node_id))

      missing =
        Enum.reject(desired, &MapSet.member?(ready_ids, member_node(&1).node_id))

      excess =
        location_records
        |> Enum.reject(&(location(&1).state == :absent))
        |> Enum.reject(fn record ->
          location = location(record)

          case Map.get(member_by_id, location.node_id) do
            nil ->
              false

            member ->
              MapSet.member?(desired_ids, location.node_id) and
                member_node(member).generation == location.node_generation
          end
        end)

      sources =
        current
        |> Enum.filter(fn record ->
          location = location(record)
          member = member_by_id[location.node_id]

          location.state in [:ready, :draining] and readable_source?(member_node(member))
        end)
        |> Enum.sort_by(&location(&1).node_id)

      {:ok,
       %{
         descriptor: descriptor,
         desired: desired,
         ready: ready,
         ready_desired: ready_desired,
         missing: missing,
         excess: excess,
         sources: sources,
         topology_fingerprint: topology_fingerprint(members)
       }}
    end
  end

  @doc """
  Plans and enqueues one bounded descriptor page for an owned shard.
  """
  @spec run_page(Context.t(), binary(), binary() | nil, pos_integer(), keyword()) ::
          {:ok,
           %{
             planned: non_neg_integer(),
             blobs: non_neg_integer(),
             actual_replicas: non_neg_integer(),
             under_replicated: non_neg_integer(),
             unavailable: non_neg_integer(),
             next_cursor: binary() | nil
           }}
          | {:error, term()}
  def run_page(%Context{} = context, shard, cursor \\ nil, limit \\ 100, opts \\ []) do
    local_node_id = context.config.node_id

    with {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, ^local_node_id} <- owner(shard, members),
         {:ok, page} <- catalog(opts).list_page(shard, cursor, limit, metadata_opts(opts)),
         {:ok, stats} <- plan_records(page.records, members, context, opts) do
      {:ok, Map.put(stats, :next_cursor, page.next_cursor)}
    else
      {:ok, _other_owner} -> {:error, :not_shard_owner}
      error -> error
    end
  end

  defp plan_records(records, members, context, opts) do
    Enum.reduce_while(records, {:ok, empty_cycle()}, fn record, {:ok, stats} ->
      with {:ok, locations} <-
             locations(opts).list(record.descriptor.hash, metadata_opts(opts)),
           {:ok, plan} <- plan_blob(record.descriptor, members, locations),
           events = events(record, plan, locations, context, opts),
           :ok <- enqueue(events, record, opts) do
        {:cont, {:ok, accumulate(stats, plan, events)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp events(record, plan, locations, context, opts) do
    repair_events =
      if InstanceConfig.worker_enabled?(context.config, :repair) do
        Enum.map(plan.missing, fn target ->
          node = member_node(target)

          id =
            event_id(
              :repair_blob,
              record,
              node.node_id,
              node.generation,
              locations,
              plan.topology_fingerprint
            )

          %{
            id: id,
            kind: :repair_blob,
            state: :pending,
            payload: %{
              hash: record.descriptor.hash,
              size: record.descriptor.size,
              target_node_id: node.node_id,
              target_node_generation: node.generation,
              source_node_ids: Enum.map(plan.sources, &location(&1).node_id)
            }
          }
        end)
      else
        []
      end

    cleanup_events =
      if InstanceConfig.worker_enabled?(context.config, :repair) and plan.missing == [] and
           length(plan.ready_desired) == record.descriptor.desired_replication_factor do
        Enum.map(plan.excess, fn excess ->
          target = location(excess)

          id =
            event_id(
              :cleanup,
              record,
              target.node_id,
              target.node_generation,
              locations,
              plan.topology_fingerprint
            )

          %{
            id: id,
            kind: :cleanup,
            state: :pending,
            payload: %{
              hash: record.descriptor.hash,
              size: record.descriptor.size,
              target_node_id: target.node_id,
              target_node_generation: target.node_generation
            }
          }
        end)
      else
        []
      end

    repair_events ++ cleanup_events ++ scrub_events(record, plan, context, opts)
  end

  defp scrub_events(record, plan, context, opts) do
    if InstanceConfig.worker_enabled?(context.config, :scrub) do
      plan.ready
      |> Enum.filter(fn item ->
        location = location(item)

        scrub_due?(location.verified_at, opts)
      end)
      |> Enum.map(fn item ->
        location = location(item)

        %{
          id:
            fingerprint({
              :scrub,
              record.descriptor.hash,
              location.node_id,
              location.node_generation,
              location.verified_at
            }),
          kind: :scrub,
          state: :pending,
          payload: %{
            hash: record.descriptor.hash,
            size: record.descriptor.size,
            target_node_id: location.node_id,
            target_node_generation: location.node_generation
          }
        }
      end)
    else
      []
    end
  end

  defp enqueue([], _record, _opts), do: :ok

  defp enqueue(events, record, opts) do
    operation_id =
      "repair-plan-" <>
        fingerprint({
          record.descriptor.hash,
          record.mod_revision,
          Enum.map(events, & &1.id)
        })

    outbox(opts).enqueue_legacy(
      events,
      Keyword.put(metadata_opts(opts), :operation_id, operation_id)
    )
  end

  defp event_id(kind, record, node_id, generation, locations, topology_fingerprint) do
    fingerprint({
      kind,
      record.descriptor.hash,
      record.mod_revision,
      node_id,
      generation,
      topology_fingerprint,
      Enum.map(locations, fn item ->
        location = location(item)
        {location.node_id, location.node_generation, location.state, Map.get(item, :mod_revision)}
      end)
    })
  end

  defp descriptor(%Blob{} = descriptor), do: descriptor
  defp descriptor(descriptor), do: struct!(Blob, descriptor)

  defp nodes(members), do: Enum.map(members, &member_node/1)
  defp member_node(%{node: node}), do: node
  defp member_node(node), do: node
  defp location(%{location: location}), do: location
  defp location(%BlobLocation{} = location), do: location

  defp readable_source?(%{
         role: :data,
         enabled: true,
         internal_endpoint: endpoint
       })
       when is_binary(endpoint) and endpoint != "",
       do: true

  defp readable_source?(_node), do: false

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier, :revision])

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp catalog(opts), do: Keyword.get(opts, :catalog, BlobCatalog)
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
  defp outbox(opts), do: Keyword.get(opts, :outbox, Outbox)
  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)

  defp scan(%{queue: []} = state) do
    case membership(state.planner_opts).members(
           state.context.config,
           metadata_opts(state.planner_opts)
         ) do
      {:ok, members} ->
        queue = owned_shards(state.context.config.node_id, members)

        state = %{
          state
          | queue: queue,
            cursor: nil,
            owned_shards: queue,
            topology_fingerprint: topology_fingerprint(members),
            cycle: empty_cycle()
        }

        if queue == [], do: state, else: scan(state)

      {:error, _reason} ->
        state
    end
  end

  defp scan(%{queue: [shard | rest]} = state) do
    case run_page(
           state.context,
           shard,
           state.cursor,
           state.page_size,
           state.planner_opts
         ) do
      {:ok, %{next_cursor: nil} = page} ->
        state = %{state | queue: rest, cursor: nil, cycle: merge_cycle(state.cycle, page)}
        if rest == [], do: finish_cycle(state), else: state

      {:ok, %{next_cursor: cursor} = page} ->
        %{state | cursor: cursor, cycle: merge_cycle(state.cycle, page)}

      {:error, _reason} ->
        state = %{
          state
          | queue: rest,
            cursor: nil,
            cycle: Map.update!(state.cycle, :scan_errors, &(&1 + 1))
        }

        if rest == [], do: finish_cycle(state), else: state
    end
  end

  defp accumulate(stats, plan, events) do
    desired_count = plan.descriptor.desired_replication_factor

    %{
      blobs: stats.blobs + 1,
      actual_replicas: stats.actual_replicas + length(plan.ready),
      planned: stats.planned + length(events),
      under_replicated:
        stats.under_replicated + if(length(plan.ready_desired) < desired_count, do: 1, else: 0),
      unavailable: stats.unavailable + if(plan.sources == [], do: 1, else: 0)
    }
  end

  defp merge_cycle(cycle, page) do
    %{
      blobs: cycle.blobs + page.blobs,
      actual_replicas: cycle.actual_replicas + page.actual_replicas,
      planned: cycle.planned + page.planned,
      under_replicated: cycle.under_replicated + page.under_replicated,
      unavailable: cycle.unavailable + page.unavailable,
      scan_errors: cycle.scan_errors
    }
  end

  defp finish_cycle(state) do
    now_ms =
      Keyword.get(
        state.planner_opts,
        :status_now_ms,
        System.system_time(:millisecond)
      )

    snapshot = %{
      schema: 2,
      complete: state.cycle.scan_errors == 0,
      node_id: state.context.config.node_id,
      node_generation: state.context.config.node_generation,
      topology_fingerprint: state.topology_fingerprint,
      owned_shards: state.owned_shards,
      desired_replicas: state.context.config.replication_factor,
      actual_replicas: state.cycle.actual_replicas,
      required_write_quorum: state.context.config.write_quorum,
      under_replicated_blobs: state.cycle.under_replicated,
      unavailable_blobs: state.cycle.unavailable,
      planned_actions: state.cycle.planned,
      scan_errors: state.cycle.scan_errors,
      updated_at_ms: now_ms
    }

    key = Keys.cluster_status_owner(state.context.config.node_id)

    case backend(state.planner_opts).put(key, snapshot, write_opts(state.planner_opts)) do
      :ok ->
        %{state | snapshot: snapshot}

      {:error, reason} ->
        %{
          state
          | snapshot: Map.merge(snapshot, %{complete: false, persistence_error: inspect(reason)})
        }
    end
  end

  defp scrub_due?(verified_at, opts) do
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    interval_ms = Keyword.get(opts, :scrub_interval_ms, :timer.hours(24))

    case timestamp_ms(verified_at) do
      {:ok, verified_ms} -> now_ms - verified_ms >= interval_ms
      :error -> true
    end
  end

  defp timestamp_ms(value) when is_integer(value) and value < 1_000_000_000_000,
    do: {:ok, value * 1_000}

  defp timestamp_ms(value) when is_integer(value), do: {:ok, value}

  defp timestamp_ms(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime, :millisecond)}
      _other -> :error
    end
  end

  defp timestamp_ms(_value), do: :error

  defp empty_cycle,
    do: %{
      blobs: 0,
      actual_replicas: 0,
      planned: 0,
      under_replicated: 0,
      unavailable: 0,
      scan_errors: 0
    }

  defp initial_snapshot(context) do
    %{
      schema: 2,
      complete: false,
      node_id: context.config.node_id,
      node_generation: context.config.node_generation,
      desired_replicas: context.config.replication_factor,
      required_write_quorum: context.config.write_quorum
    }
  end

  defp write_opts(opts),
    do: Keyword.take(opts, [:timeout, :engine, :barrier])

  defp schedule(delay), do: Process.send_after(self(), :scan, delay)
end
