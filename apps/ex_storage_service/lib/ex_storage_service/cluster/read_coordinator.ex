defmodule ExStorageService.Cluster.ReadCoordinator do
  @moduledoc """
  Selects a checksum-valid local or remote source for one committed blob.

  Metadata and membership reads are strong. Remote candidates are tried in a
  deterministic, bounded order before response bytes are emitted. A failure
  after streaming begins is propagated to the adapter so it can cancel the
  response; the same response is never restarted from another replica.
  """

  alias ExStorageService.BlobStore.{LocalCAS, ReadRepair, Source}
  alias ExStorageService.Cluster.{Membership, Node, Placement, RequestId}
  alias ExStorageService.Context
  alias ExStorageService.Metadata.BlobLocations

  @prefetch_bytes 65_536

  @type range :: nil | :all | {non_neg_integer(), non_neg_integer()}

  @spec open(Context.t(), binary(), non_neg_integer(), range(), keyword()) ::
          {:ok, Source.t()} | {:error, term()}
  def open(%Context{config: %{mode: :standalone}} = context, hash, _size, range, opts) do
    blob_store(opts).open(hash, range, blob_opts(context, opts))
  end

  def open(%Context{} = context, hash, size, range, opts)
      when is_binary(hash) and is_integer(size) and size >= 0 do
    with {:ok, locations} <- locations(opts).list(hash, metadata_opts(opts)) do
      local_location = find_location(locations, context.config.node_id)

      case open_local(context, hash, size, range, local_location, opts) do
        {:ok, source} ->
          {:ok, source}

        {:error, local_reason} ->
          with {:ok, members} <- member_records(context, opts) do
            local_member = find_member(members, context.config.node_id)

            maybe_mark_local_unhealthy(
              context,
              hash,
              local_location,
              local_member,
              local_reason,
              opts
            )

            open_remote(
              context,
              hash,
              size,
              range,
              locations,
              members,
              local_location,
              opts
            )
          end
      end
    end
  end

  defp open_local(context, hash, size, range, location, opts) do
    store = blob_store(opts)
    store_opts = blob_opts(context, opts)

    with {:ok, %{size: ^size}} <- store.stat(hash, store_opts),
         :ok <- verify_local(context, store, hash, store_opts, location, opts),
         {:ok, source} <- store.open(hash, range, store_opts),
         :ok <- validate_source_length(source, range_length(range, size)) do
      {:ok, source}
    else
      {:ok, %{size: _other}} -> {:error, :size_mismatch}
      {:error, reason} -> {:error, reason}
    end
  end

  defp verify_local(context, store, hash, store_opts, location, opts) do
    verify? =
      case Keyword.fetch(opts, :verify_local) do
        {:ok, value} -> value
        :error -> local_verification_required?(context, location)
      end

    if verify?, do: store.verify(hash, store_opts), else: :ok
  end

  defp local_verification_required?(_context, nil), do: true

  defp local_verification_required?(context, %{location: location}) do
    location.node_generation != context.config.node_generation or
      location.state not in [:ready, :draining]
  end

  defp open_remote(
         context,
         hash,
         size,
         range,
         locations,
         members,
         local_location,
         opts
       ) do
    candidates =
      remote_candidates(context, hash, locations, members, opts)
      |> Enum.take(Keyword.get(opts, :max_remote_attempts, length(locations)))

    case find_remote_source(context, hash, size, range, candidates, opts) do
      {:ok, prefetched, candidate, remaining, next_offset, remaining_length} ->
        source =
          prefetched_remote_source(
            prefetched,
            candidate,
            remaining,
            context,
            hash,
            size,
            next_offset,
            remaining_length,
            opts
          )

        {:ok,
         maybe_wrap_read_repair(
           source,
           context,
           hash,
           size,
           range,
           members,
           local_location,
           opts
         )}

      {:error, _reason} ->
        {:error, :all_blob_replicas_unavailable}
    end
  end

  defp find_remote_source(_context, _hash, _size, _range, [], _opts),
    do: {:error, :no_remote_replica}

  defp find_remote_source(context, hash, size, range, [candidate | rest], opts) do
    {offset, total_length} = normalize_read_range(range, size)
    prefetch_length = min(total_length, bounded_prefetch_bytes(opts))
    prefetch_range = {offset, prefetch_length}

    case open_transport_source(
           context,
           candidate,
           hash,
           size,
           prefetch_range,
           "prefetch:#{offset}:#{prefetch_length}",
           opts
         ) do
      {:ok, source} ->
        case collect_prefetch(source, prefetch_length) do
          {:ok, prefetched} ->
            {:ok, prefetched, candidate, rest, offset + prefetch_length,
             total_length - prefetch_length}

          {:error, reason} ->
            maybe_mark_remote_unhealthy(hash, candidate, reason, opts)
            find_remote_source(context, hash, size, range, rest, opts)
        end

      {:error, reason} ->
        maybe_mark_remote_unhealthy(hash, candidate, reason, opts)
        find_remote_source(context, hash, size, range, rest, opts)
    end
  end

  defp open_transport_source(context, candidate, hash, size, range, scope, opts) do
    transport_opts =
      opts
      |> Keyword.get(:transport_opts, [])
      |> Keyword.put(:expected_size, size)
      |> Keyword.put(:expected_node_id, candidate.node.node_id)
      |> Keyword.put(:expected_node_generation, candidate.node.generation)
      |> Keyword.put(:verified_head, Keyword.get(opts, :skip_transport_head, false))
      |> Keyword.put(
        :request_id,
        remote_request_id(opts, candidate.node.node_id, scope)
      )

    case transport(opts).open_blob(context, candidate.node, hash, range, transport_opts) do
      {:ok, source} ->
        case validate_source_length(source, range_length(range, size)) do
          :ok -> {:ok, source}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp collect_prefetch(source, expected_length) do
    case Source.reduce(source, [], fn chunk, acc -> {:cont, [chunk | acc]} end) do
      {:ok, chunks} ->
        prefetched = chunks |> Enum.reverse() |> IO.iodata_to_binary()

        if byte_size(prefetched) == expected_length,
          do: {:ok, prefetched},
          else: {:error, :invalid_source_length}

      {:error, reason, _partial} ->
        {:error, reason}
    end
  end

  defp bounded_prefetch_bytes(opts) do
    case Keyword.get(opts, :prefetch_bytes, @prefetch_bytes) do
      requested when is_integer(requested) and requested > 0 ->
        min(requested, @prefetch_bytes)

      _invalid ->
        @prefetch_bytes
    end
  end

  defp prefetched_remote_source(
         prefetched,
         candidate,
         remaining,
         context,
         hash,
         size,
         next_offset,
         remaining_length,
         opts
       ) do
    total_length = byte_size(prefetched) + remaining_length

    Source.stateful_stream(
      fn initial, reducer ->
        case emit_prefetched(prefetched, initial, reducer) do
          {:cont, current} ->
            consume_remote_remainder(
              candidate,
              remaining,
              context,
              hash,
              size,
              next_offset,
              remaining_length,
              current,
              reducer,
              opts
            )

          {:halt, reason, current} ->
            {:error, {:sink, reason}, current}
        end
      end,
      total_length
    )
  end

  defp emit_prefetched("", current, _reducer), do: {:cont, current}
  defp emit_prefetched(prefetched, current, reducer), do: reducer.(prefetched, current)

  defp consume_remote_remainder(
         candidate,
         remaining,
         context,
         hash,
         size,
         offset,
         length,
         initial,
         reducer,
         opts
       ) do
    if length == 0 do
      {:ok, initial}
    else
      range = {offset, length}

      case open_transport_source(
             context,
             candidate,
             hash,
             size,
             range,
             "stream:#{offset}:#{length}",
             Keyword.put(opts, :skip_transport_head, true)
           ) do
        {:ok, source} ->
          consume_remote_source(
            source,
            candidate,
            remaining,
            context,
            hash,
            size,
            range,
            initial,
            reducer,
            opts
          )

        {:error, reason} ->
          maybe_mark_remote_unhealthy(hash, candidate, reason, opts)
          consume_remote_fallback(context, hash, size, range, remaining, initial, reducer, opts)
      end
    end
  end

  defp consume_remote_source(
         source,
         candidate,
         remaining,
         context,
         hash,
         size,
         range,
         initial,
         reducer,
         opts
       ) do
    delivered = :atomics.new(1, [])

    result =
      Source.reduce(source, initial, fn chunk, current ->
        case reducer.(chunk, current) do
          {:cont, _next} = continue ->
            :atomics.add(delivered, 1, byte_size(chunk))
            continue

          {:halt, _reason, _next} = halt ->
            halt
        end
      end)

    case result do
      {:error, reason, current} ->
        if retryable_before_body?(reason, delivered) do
          maybe_mark_remote_unhealthy(hash, candidate, reason, opts)
          consume_remote_fallback(context, hash, size, range, remaining, current, reducer, opts)
        else
          result
        end

      {:ok, _current} ->
        result
    end
  end

  defp consume_remote_fallback(
         context,
         hash,
         size,
         range,
         remaining,
         current,
         reducer,
         opts
       ) do
    case find_remote_source(context, hash, size, range, remaining, opts) do
      {:ok, prefetched, candidate, rest, next_offset, remaining_length} ->
        case emit_prefetched(prefetched, current, reducer) do
          {:cont, next} ->
            consume_remote_remainder(
              candidate,
              rest,
              context,
              hash,
              size,
              next_offset,
              remaining_length,
              next,
              reducer,
              opts
            )

          {:halt, reason, next} ->
            {:error, {:sink, reason}, next}
        end

      {:error, reason} ->
        {:error, reason, current}
    end
  end

  defp retryable_before_body?({:sink, _reason}, _delivered), do: false
  defp retryable_before_body?(:closed, _delivered), do: false
  defp retryable_before_body?(_reason, delivered), do: :atomics.get(delivered, 1) == 0

  defp remote_candidates(context, hash, locations, members, opts) do
    nodes = Map.new(members, fn %{node: node} = member -> {node.node_id, member} end)

    locations
    |> Enum.flat_map(fn %{location: location} ->
      case Map.get(nodes, location.node_id) do
        %{node: %Node{} = node} = member ->
          if readable_remote?(context, node, location),
            do: [%{node: node, node_record: member, location: location}],
            else: []

        nil ->
          []
      end
    end)
    |> Enum.sort_by(
      fn %{node: node} -> {Placement.score(hash, node.node_id), node.node_id} end,
      :desc
    )
    |> order_candidates(opts)
  end

  defp order_candidates(candidates, opts) do
    case Keyword.get(opts, :source_order) do
      node_ids when is_list(node_ids) ->
        rank = node_ids |> Enum.with_index() |> Map.new()
        Enum.sort_by(candidates, &Map.get(rank, &1.node.node_id, length(node_ids)))

      _ ->
        candidates
    end
  end

  defp readable_remote?(context, node, location) do
    node.node_id != context.config.node_id and node.role == :data and node.enabled and
      is_binary(node.internal_endpoint) and node.internal_endpoint != "" and
      node.generation == location.node_generation and location.state in [:ready, :draining]
  end

  defp maybe_wrap_read_repair(
         source,
         context,
         hash,
         size,
         range,
         members,
         local_location,
         opts
       ) do
    if repair_allowed?(context, hash, size, range, members, local_location, opts) do
      local_member = find_member(members, context.config.node_id)

      repair_opts = [
        blob_store: blob_store(opts),
        blob_store_opts: blob_opts(context, opts),
        task_supervisor:
          Keyword.get(opts, :repair_task_supervisor, context.replica_task_supervisor),
        finalizer: Keyword.get(opts, :repair_finalizer, :async),
        on_ready: fn _ready ->
          locations(opts).mark_ready(
            hash,
            context.config.node_id,
            context.config.node_generation,
            size,
            mark_opts(opts, local_member, context.config.node_generation)
          )
        end
      ]

      read_repair(opts).wrap(source, hash, size, repair_opts)
    else
      source
    end
  end

  defp repair_allowed?(context, hash, size, range, members, local_location, opts) do
    Keyword.get(opts, :read_repair, true) and full_range?(range, size) and
      repairable_local_location?(context, local_location) and
      desired_local?(context, hash, members, opts) and
      capacity_permits?(context, size, opts)
  end

  defp repairable_local_location?(_context, nil), do: true

  defp repairable_local_location?(context, %{location: location}),
    do: location.node_generation == context.config.node_generation

  defp desired_local?(context, hash, members, opts) do
    nodes = Enum.map(members, & &1.node)
    rf = Keyword.get(opts, :replication_factor, context.config.replication_factor)

    case placement(opts).select(hash, nodes, rf) do
      {:ok, desired} -> Enum.any?(desired, &(&1.node_id == context.config.node_id))
      {:error, _reason} -> false
    end
  end

  defp capacity_permits?(context, size, opts) do
    case Keyword.get(opts, :capacity_policy) do
      callback when is_function(callback, 2) -> callback.(context, size)
      nil -> true
    end
  end

  defp maybe_mark_local_unhealthy(_context, _hash, nil, _member, _reason, _opts), do: :ok

  defp maybe_mark_local_unhealthy(context, hash, %{location: location}, member, reason, opts) do
    current_generation? =
      location.node_generation == context.config.node_generation and
        match?(
          %{node: %{generation: generation}} when generation == context.config.node_generation,
          member
        )

    if current_generation? do
      mark_local_unhealthy(hash, location, member, reason, opts)
    else
      :ok
    end
  end

  defp mark_local_unhealthy(hash, location, member, reason, opts) do
    state =
      if reason in [:not_found, :enoent, :missing],
        do: :unavailable,
        else: :suspect

    _ =
      locations(opts).mark_unhealthy(
        hash,
        location.node_id,
        state,
        reason,
        mark_opts(opts, member, location.node_generation)
      )

    :ok
  end

  defp maybe_mark_remote_unhealthy(hash, candidate, reason, opts)
       when reason in [
              :not_found,
              :checksum_mismatch,
              :size_mismatch,
              :replica_identity_mismatch,
              :invalid_source_length
            ] do
    state = if reason == :not_found, do: :unavailable, else: :suspect

    _ =
      locations(opts).mark_unhealthy(
        hash,
        candidate.location.node_id,
        state,
        reason,
        mark_opts(opts, candidate.node_record, candidate.node.generation)
      )

    :ok
  end

  defp maybe_mark_remote_unhealthy(_hash, _candidate, _reason, _opts), do: :ok

  defp member_records(context, opts) do
    case Keyword.get(opts, :placement_records) do
      records when is_list(records) -> {:ok, records}
      nil -> membership(opts).members(context.config, metadata_opts(opts))
    end
  end

  defp find_location(locations, node_id),
    do: Enum.find(locations, &(&1.location.node_id == node_id))

  defp find_member(members, node_id),
    do: Enum.find(members, &(&1.node.node_id == node_id))

  defp validate_source_length({:file, _path, _offset, length}, length), do: :ok
  defp validate_source_length({:stream, _producer, length}, length), do: :ok
  defp validate_source_length(_source, _expected), do: {:error, :invalid_source_length}

  defp range_length(nil, size), do: size
  defp range_length(:all, size), do: size
  defp range_length({_offset, length}, _size), do: length

  defp normalize_read_range(nil, size), do: {0, size}
  defp normalize_read_range(:all, size), do: {0, size}
  defp normalize_read_range({offset, length}, _size), do: {offset, length}

  defp full_range?(nil, _size), do: true
  defp full_range?(:all, _size), do: true
  defp full_range?({0, size}, size), do: true
  defp full_range?(_range, _size), do: false

  defp remote_request_id(_opts, _node_id, _scope), do: RequestId.generate()

  defp metadata_opts(opts),
    do:
      opts
      |> Keyword.take([:backend, :consistency, :timeout, :engine, :barrier, :timestamp])
      |> Keyword.put_new(:consistency, :strong)

  defp mark_opts(opts, nil, generation),
    do: Keyword.put(metadata_opts(opts), :expected_generation, generation)

  defp mark_opts(opts, member, generation) do
    opts
    |> metadata_opts()
    |> Keyword.put(:node_record, member)
    |> Keyword.put(:expected_generation, generation)
  end

  defp blob_opts(context, opts),
    do:
      context
      |> Context.blob_store_options()
      |> Keyword.merge(Keyword.get(opts, :blob_store_opts, []))
      |> Keyword.merge(Keyword.take(opts, [:bucket]))

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp placement(opts), do: Keyword.get(opts, :placement, Placement)
  defp transport(opts), do: Keyword.get(opts, :transport, configured_transport())
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
  defp blob_store(opts), do: Keyword.get(opts, :blob_store, LocalCAS)
  defp read_repair(opts), do: Keyword.get(opts, :read_repair_module, ReadRepair)

  defp configured_transport do
    Application.fetch_env!(:ex_storage_service, :cluster_transport)
  end
end
