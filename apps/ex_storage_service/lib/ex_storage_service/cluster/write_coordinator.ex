defmodule ExStorageService.Cluster.WriteCoordinator do
  @moduledoc """
  Establishes durable blob quorum before object metadata becomes visible.

  Placement and acknowledgement validation are pure request-time decisions.
  Transfers run under a bounded `Task.Supervisor`; this module deliberately
  owns no process or mutable coordination state.
  """

  alias ExStorageService.BlobStore.{ReadyBlob, Source, StagedBlob}

  alias ExStorageService.Cluster.{
    BlobDescriptor,
    Membership,
    Placement,
    ReplicaAck
  }

  alias ExStorageService.{Context, Telemetry}

  @default_timeout 60_000

  @type evidence :: %{
          descriptor: BlobDescriptor.t(),
          placement: [map()],
          acknowledgements: [ReplicaAck.t()],
          missing_node_ids: [String.t()],
          configured_write_quorum: pos_integer(),
          required_write_quorum: pos_integer(),
          achieved_replica_count: pos_integer(),
          durability: :strict | :degraded,
          ready_blob: ReadyBlob.t() | map() | nil
        }

  @spec ensure_blob(Context.t(), StagedBlob.t() | ReadyBlob.t() | Source.t() | map(), keyword()) ::
          {:ok, evidence()} | {:error, term()}
  def ensure_blob(%Context{} = context, source, opts \\ []) do
    started_at = System.monotonic_time()
    result = do_ensure_blob(context, source, opts)
    emit_quorum_telemetry(started_at, context, result, opts)
    result
  end

  defp do_ensure_blob(context, source, opts) do
    with {:ok, descriptor} <- descriptor(source, context, opts),
         {:ok, records} <- member_records(context, opts),
         {:ok, selected_nodes} <-
           placement(opts).select(
             descriptor.hash,
             Enum.map(records, & &1.node),
             descriptor.desired_replication_factor
           ),
         selected_records <- select_records(records, selected_nodes),
         {:ok, remote_acks} <-
           transfer_remote(context, source, descriptor, selected_records, opts),
         {:ok, local_ack, ready_blob} <-
           commit_local(context, source, descriptor, selected_records, opts),
         acks <- Enum.reject([local_ack | remote_acks], &is_nil/1),
         {:ok, valid_acks} <-
           validate_acknowledgements(
             acks,
             selected_records,
             descriptor,
             expected_ack_opts(selected_records, opts)
           ),
         {:ok, policy} <- quorum_policy(valid_acks, selected_records, context, opts) do
      maybe_discard_unselected(context, source, selected_records, opts)

      {:ok,
       Map.merge(policy, %{
         descriptor: descriptor,
         placement: selected_records,
         acknowledgements: valid_acks,
         ready_blob: ready_blob
       })}
    else
      {:error, _reason} = error ->
        error
    end
  end

  defp emit_quorum_telemetry(started_at, context, result, opts) do
    {result_name, achieved} =
      case result do
        {:ok, evidence} -> {:ok, evidence.achieved_replica_count}
        {:error, reason} -> {normalize_result(reason), 0}
      end

    Telemetry.quorum_stop(
      System.monotonic_time() - started_at,
      %{
        configured_write_quorum: Keyword.get(opts, :write_quorum, context.config.write_quorum),
        achieved_replica_count: achieved
      },
      %{result: result_name}
    )
  end

  defp normalize_result(reason) when is_atom(reason), do: reason
  defp normalize_result(_reason), do: :error

  @doc false
  @spec validate_acknowledgements([ReplicaAck.t()], [map()], BlobDescriptor.t(), keyword()) ::
          {:ok, [ReplicaAck.t()]} | {:error, term()}
  def validate_acknowledgements(acks, selected_records, descriptor, opts \\ []) do
    selected = Map.new(selected_records, &{&1.node.node_id, &1.node})
    expected_request_ids = Keyword.get(opts, :expected_request_ids, %{})

    acks
    |> Enum.reduce_while({:ok, %{}}, fn ack, {:ok, unique} ->
      case validate_ack(ack, selected, descriptor, expected_request_ids) do
        :ok -> {:cont, {:ok, Map.put(unique, ack.node_id, ack)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, unique} -> {:ok, unique |> Map.values() |> Enum.sort_by(& &1.node_id)}
      error -> error
    end
  end

  defp descriptor(source, context, opts) do
    with {:ok, %{hash: hash, size: size} = identity} <- source_identity(source) do
      {:ok,
       %BlobDescriptor{
         schema: 2,
         hash: hash,
         algorithm: :sha256,
         size: size,
         desired_replication_factor:
           Keyword.get(opts, :replication_factor, context.config.replication_factor),
         created_at:
           Keyword.get_lazy(opts, :timestamp, fn ->
             DateTime.utc_now() |> DateTime.to_iso8601()
           end)
       }
       |> maybe_descriptor_etag(identity)}
    end
  end

  defp maybe_descriptor_etag(descriptor, _identity), do: descriptor

  defp source_identity(%_{} = source), do: source |> Map.from_struct() |> source_identity()

  defp source_identity(source) when is_map(source) do
    hash = Map.get(source, :hash, Map.get(source, :content_hash))
    size = Map.get(source, :size)

    if is_binary(hash) and is_integer(size) and size >= 0,
      do: {:ok, %{hash: hash, size: size, etag: Map.get(source, :etag)}},
      else: {:error, :invalid_ready_blob}
  end

  defp source_identity(_source), do: {:error, :invalid_ready_blob}

  defp member_records(context, opts) do
    case Keyword.get(opts, :placement_records) do
      records when is_list(records) ->
        {:ok, records}

      nil ->
        membership(opts).members(context.config, membership_opts(opts))
    end
  end

  defp select_records(records, nodes) do
    revisions = Map.new(records, &{&1.node.node_id, &1})
    Enum.map(nodes, &Map.fetch!(revisions, &1.node_id))
  end

  defp transfer_remote(context, source, descriptor, records, opts) do
    remote = Enum.reject(records, &local_node?(&1.node, context))

    results =
      run_bounded(
        context,
        remote,
        fn record ->
          ensure_remote(context, record.node, source, descriptor, opts)
        end,
        opts
      )

    {:ok,
     Enum.flat_map(results, fn
       {:ok, {:ok, %ReplicaAck{} = ack}} -> [ack]
       _failed -> []
     end)}
  end

  defp ensure_remote(context, node, source, descriptor, opts) do
    request_id = replica_request_id(opts, node.node_id)
    transport_opts = transport_opts(opts, request_id)

    case transport(opts).head_blob(context, node, descriptor.hash, transport_opts) do
      {:ok, info} ->
        head_ack(info, node, descriptor, request_id)

      {:error, reason} when reason in [:not_found, :blob_not_found] ->
        transport(opts).put_blob(
          context,
          node,
          source_for_transport(source),
          descriptor,
          transport_opts
        )

      {:error, _reason} ->
        transport(opts).put_blob(
          context,
          node,
          source_for_transport(source),
          descriptor,
          transport_opts
        )
    end
  end

  defp head_ack(info, node, descriptor, request_id) do
    ack = %ReplicaAck{
      node_id: Map.get(info, :node_id, node.node_id),
      node_generation: Map.get(info, :node_generation, node.generation),
      hash: Map.get(info, :hash),
      size: Map.get(info, :size),
      verified_at: Map.get(info, :verified_at, System.system_time(:second)),
      fencing_or_request_id: Map.get(info, :fencing_or_request_id, request_id)
    }

    case validate_ack(ack, %{node.node_id => node}, descriptor, %{node.node_id => request_id}) do
      :ok -> {:ok, ack}
      {:error, _reason} -> {:error, :invalid_replica_ack}
    end
  end

  defp commit_local(context, source, descriptor, records, opts) do
    case Enum.find(records, &local_node?(&1.node, context)) do
      nil ->
        {:ok, nil, nil}

      %{node: node} ->
        with {:ok, ready} <- ensure_local_ready(context, source, descriptor, opts),
             :ok <- blob_store(opts).verify(descriptor.hash, blob_opts(context, opts)) do
          request_id = replica_request_id(opts, node.node_id)

          ack = %ReplicaAck{
            node_id: node.node_id,
            node_generation: node.generation,
            hash: descriptor.hash,
            size: descriptor.size,
            verified_at: System.system_time(:second),
            fencing_or_request_id: request_id
          }

          {:ok, ack, ready}
        end
    end
  end

  defp ensure_local_ready(context, %StagedBlob{} = staged, _descriptor, opts),
    do: blob_store(opts).commit(staged, blob_opts(context, opts))

  defp ensure_local_ready(_context, %ReadyBlob{} = ready, _descriptor, _opts),
    do: {:ok, ready}

  defp ensure_local_ready(context, ready, descriptor, opts) when is_map(ready) do
    with {:ok, stat} <- blob_store(opts).stat(descriptor.hash, blob_opts(context, opts)) do
      {:ok, Map.merge(Map.new(ready), stat)}
    end
  end

  defp ensure_local_ready(context, {:file, _path, _offset, _length}, descriptor, opts) do
    blob_store(opts).stat(descriptor.hash, blob_opts(context, opts))
  end

  defp ensure_local_ready(_context, _source, _descriptor, _opts),
    do: {:error, :unsupported_source}

  defp source_for_transport(%ReadyBlob{source: source}), do: source
  defp source_for_transport(%{source: source}), do: source
  defp source_for_transport(source), do: source

  defp validate_ack(%ReplicaAck{} = ack, selected, descriptor, request_ids) do
    with %{generation: generation} <- Map.get(selected, ack.node_id),
         true <- ack.node_generation == generation,
         true <- ack.hash == descriptor.hash,
         true <- ack.size == descriptor.size,
         true <- valid_request_id?(ack, request_ids) do
      :ok
    else
      _invalid -> {:error, :invalid_replica_ack}
    end
  end

  defp validate_ack(_ack, _selected, _descriptor, _request_ids),
    do: {:error, :invalid_replica_ack}

  defp valid_request_id?(ack, request_ids) when map_size(request_ids) == 0,
    do: is_binary(ack.fencing_or_request_id) and ack.fencing_or_request_id != ""

  defp valid_request_id?(ack, request_ids),
    do: Map.get(request_ids, ack.node_id) == ack.fencing_or_request_id

  defp quorum_policy(acks, records, context, opts) do
    configured = Keyword.get(opts, :write_quorum, context.config.write_quorum)

    allow_degraded =
      Keyword.get(opts, :allow_degraded_writes, context.config.allow_degraded_writes)

    acknowledged_ids = MapSet.new(acks, & &1.node_id)

    missing_ids =
      records
      |> Enum.map(& &1.node.node_id)
      |> Enum.reject(&MapSet.member?(acknowledged_ids, &1))
      |> Enum.sort()

    count = length(acks)

    cond do
      count >= configured ->
        {:ok,
         %{
           missing_node_ids: missing_ids,
           configured_write_quorum: configured,
           required_write_quorum: configured,
           achieved_replica_count: count,
           durability: :strict
         }}

      allow_degraded and count >= 1 ->
        {:ok,
         %{
           missing_node_ids: missing_ids,
           configured_write_quorum: configured,
           required_write_quorum: count,
           achieved_replica_count: count,
           durability: :degraded
         }}

      true ->
        {:error, :blob_write_quorum_unavailable}
    end
  end

  defp run_bounded(_context, [], _fun, _opts), do: []

  defp run_bounded(context, records, fun, opts) do
    concurrency =
      min(
        length(records),
        Keyword.get(opts, :replica_concurrency, context.config.replica_concurrency)
      )

    stream_opts = [
      ordered: false,
      max_concurrency: concurrency,
      timeout: Keyword.get(opts, :transfer_timeout, @default_timeout),
      on_timeout: :kill_task
    ]

    supervisor =
      Keyword.get(opts, :task_supervisor, context.replica_task_supervisor)

    if process_alive?(supervisor) do
      Task.Supervisor.async_stream_nolink(supervisor, records, fun, stream_opts)
      |> Enum.to_list()
    else
      Task.async_stream(records, fun, stream_opts)
      |> Enum.to_list()
    end
  end

  defp maybe_discard_unselected(context, %StagedBlob{} = staged, records, opts) do
    unless Enum.any?(records, &local_node?(&1.node, context)) do
      _ = blob_store(opts).discard(staged, blob_opts(context, opts))
    end

    :ok
  end

  defp maybe_discard_unselected(_context, _source, _records, _opts), do: :ok

  defp local_node?(node, context), do: node.node_id == context.config.node_id

  defp replica_request_id(opts, node_id) do
    "#{Keyword.get(opts, :operation_id, "blob")}:replica:#{node_id}"
  end

  defp expected_ack_opts(records, opts) do
    expected =
      Map.new(records, fn %{node: node} ->
        {node.node_id, replica_request_id(opts, node.node_id)}
      end)

    Keyword.put(opts, :expected_request_ids, expected)
  end

  defp transport_opts(opts, request_id) do
    opts
    |> Keyword.get(:transport_opts, [])
    |> Keyword.put(:request_id, request_id)
  end

  defp membership_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier])

  defp blob_opts(context, opts),
    do:
      Keyword.merge(Context.blob_store_options(context), Keyword.get(opts, :blob_store_opts, []))

  defp process_alive?(name) when is_atom(name), do: Process.whereis(name) != nil

  defp process_alive?({:via, module, term}) do
    case module.whereis_name(term) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end

  defp process_alive?(_name), do: false

  defp placement(opts), do: Keyword.get(opts, :placement, Placement)
  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp transport(opts), do: Keyword.get(opts, :transport, configured_transport())
  defp blob_store(opts), do: Keyword.get(opts, :blob_store, ExStorageService.BlobStore.LocalCAS)

  defp configured_transport do
    Application.fetch_env!(:ex_storage_service, :cluster_transport)
  end
end
