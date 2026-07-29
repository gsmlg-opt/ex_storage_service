defmodule ExStorageService.Cluster.Repair.Handler do
  @moduledoc """
  Idempotent execution for repair and excess-replica cleanup jobs.

  Job payloads are hints only. Every attempt strongly reloads the descriptor,
  membership, and locations and recomputes placement before touching bytes.
  """

  alias ExStorageService.BlobStore.{LocalCAS, Source}

  alias ExStorageService.Cluster.{
    BlobDescriptor,
    Membership,
    ReplicaAck,
    Scrubber
  }

  alias ExStorageService.Cluster.Repair.Planner
  alias ExStorageService.Metadata.{BlobCatalog, BlobLocations}
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.Context

  @spec perform(Job.t(), Context.t()) :: :ok | {:error, term()}
  def perform(%Job{} = job, %Context{} = context), do: perform(job, context, [])

  @spec perform(Job.t(), Context.t(), keyword()) :: :ok | {:error, term()}
  def perform(%Job{kind: :repair_blob} = job, %Context{} = context, opts),
    do: repair(job, context, opts)

  def perform(%Job{kind: :cleanup} = job, %Context{} = context, opts),
    do: cleanup(job, context, opts)

  def perform(%Job{kind: :scrub} = job, %Context{} = context, opts),
    do: scrub(job, context, opts)

  def perform(%Job{kind: kind}, %Context{}, _opts),
    do: {:error, {:unsupported_maintenance_job, kind}}

  defp repair(job, context, opts) do
    hash = payload(job, :hash)
    target_id = payload(job, :target_node_id)

    with {:ok, record, members, location_records, plan} <- current_plan(context, hash, opts),
         {:ok, target} <- desired_target(plan, target_id),
         :continue <- repair_needed(plan, target),
         {:ok, source, source_member} <-
           open_source(context, job, plan, members, target_id, opts),
         {:ok, ack} <-
           copy_to_target(context, job, source, source_member, target, record.descriptor, opts),
         :ok <- validate_ack(ack, target, record.descriptor),
         :ok <-
           locations(opts).mark_ready(
             hash,
             target.node.node_id,
             target.node.generation,
             record.descriptor.size,
             repair_mark_opts(job, target, location_records, opts)
           ) do
      :ok
    else
      :already_ready -> :ok
      :not_desired -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp cleanup(job, context, opts) do
    hash = payload(job, :hash)
    target_id = payload(job, :target_node_id)

    with {:ok, record, members, location_records, plan} <- current_plan(context, hash, opts),
         true <-
           plan.missing == [] and
             length(plan.ready_desired) == record.descriptor.desired_replication_factor,
         {:ok, target_location} <- excess_location(plan, target_id),
         {:ok, target_member} <- member(members, target_id),
         :ok <-
           locations(opts).mark_draining(
             hash,
             target_id,
             cleanup_prepare_opts(job, target_member, record, plan, opts)
           ),
         :ok <- delete_target(context, job, target_member, target_location, hash, opts),
         :ok <-
           locations(opts).retire(
             hash,
             target_id,
             plan.ready_desired,
             retire_opts(job, plan.desired, opts)
           ) do
      _ = target_location
      _ = location_records
      :ok
    else
      false -> {:error, :replication_factor_not_confirmed}
      :not_excess -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp scrub(job, context, opts) do
    hash = payload(job, :hash)
    target_id = payload(job, :target_node_id)
    target_generation = payload(job, :target_node_generation)

    with true <- target_id == context.config.node_id,
         true <- target_generation == context.config.node_generation,
         {:ok, _record, members, location_records, _plan} <- current_plan(context, hash, opts),
         {:ok, target_member} <- member(members, target_id),
         {:ok, _location_record} <-
           current_location(location_records, target_id, target_generation) do
      case scrubber(opts).scrub(hash, scrubber_opts(context, opts)) do
        {:ok, %{size: size}} ->
          locations(opts).mark_ready(
            hash,
            target_id,
            target_generation,
            size,
            mark_opts(job, target_member, opts, false)
          )

        {:error, reason} ->
          case locations(opts).mark_unhealthy(
                 hash,
                 target_id,
                 scrub_failure_state(reason),
                 reason,
                 mark_opts(job, target_member, opts, false)
               ) do
            :ok -> :ok
            {:error, _reason} = error -> error
          end
      end
    else
      false -> {:error, :wrong_scrub_target}
      {:error, :not_found} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp current_plan(context, hash, opts) when is_binary(hash) do
    with {:ok, record} <- catalog(opts).get(hash, metadata_opts(opts)),
         {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)),
         {:ok, location_records} <- locations(opts).list(hash, metadata_opts(opts)),
         {:ok, plan} <- Planner.plan_blob(record.descriptor, members, location_records) do
      {:ok, record, members, location_records, plan}
    end
  end

  defp current_plan(_context, _hash, _opts), do: {:error, :invalid_repair_payload}

  defp desired_target(plan, target_id) do
    case Enum.find(plan.desired, &(&1.node.node_id == target_id)) do
      nil -> :not_desired
      target -> {:ok, target}
    end
  end

  defp repair_needed(plan, target) do
    if Enum.any?(plan.ready_desired, &(&1.location.node_id == target.node.node_id)),
      do: :already_ready,
      else: :continue
  end

  defp open_source(context, job, plan, members, target_id, opts) do
    hinted = payload(job, :source_node_ids) || []

    candidates =
      Enum.sort_by(plan.sources, fn record ->
        id = record.location.node_id
        {if(id in hinted, do: 0, else: 1), id}
      end)

    Enum.reduce_while(candidates, {:error, :no_ready_repair_source}, fn location_record, _acc ->
      source_id = location_record.location.node_id

      if source_id == target_id do
        {:cont, {:error, :no_ready_repair_source}}
      else
        case Enum.find(members, &(&1.node.node_id == source_id)) do
          nil ->
            {:cont, {:error, :no_ready_repair_source}}

          source_member ->
            case open_from(context, source_member, location_record.location, opts) do
              {:ok, source} -> {:halt, {:ok, source, source_member}}
              {:error, _reason} -> {:cont, {:error, :no_ready_repair_source}}
            end
        end
      end
    end)
  end

  defp open_from(context, %{node: node}, location, opts) do
    if node.node_id == context.config.node_id do
      blob_store(opts).open(location.hash, nil, blob_opts(context, opts))
    else
      transport(opts).open_blob(
        context,
        node,
        location.hash,
        nil,
        transport_opts(opts,
          expected_size: location.size,
          expected_node_id: node.node_id,
          expected_node_generation: node.generation
        )
      )
    end
  end

  defp copy_to_target(context, job, source, _source_member, target, descriptor, opts) do
    if target.node.node_id == context.config.node_id do
      case blob_store(opts).stage(Source.request_body(source), blob_opts(context, opts)) do
        {:ok, staged} ->
          if staged.hash == descriptor.hash and staged.size == descriptor.size do
            case commit_repair(blob_store(opts), staged, blob_opts(context, opts)) do
              {:ok, _ready} -> {:ok, replica_ack(job, target.node, descriptor)}
              {:error, reason} -> {:error, reason}
            end
          else
            _ = blob_store(opts).discard(staged, blob_opts(context, opts))
            {:error, :repair_identity_mismatch}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      transport(opts).put_blob(
        context,
        target.node,
        source,
        blob_descriptor(descriptor),
        transport_opts(opts, request_id: request_id(job, target.node.node_id))
      )
    end
  end

  defp validate_ack(
         %ReplicaAck{
           node_id: node_id,
           node_generation: generation,
           hash: hash,
           size: size
         },
         %{node: %{node_id: node_id, generation: generation}},
         %{hash: hash, size: size}
       ),
       do: :ok

  defp validate_ack(_ack, _target, _descriptor), do: {:error, :invalid_replica_ack}

  defp excess_location(plan, target_id) do
    case Enum.find(plan.excess, &(&1.location.node_id == target_id)) do
      nil -> :not_excess
      location -> {:ok, location}
    end
  end

  defp member(members, target_id) do
    case Enum.find(members, &(&1.node.node_id == target_id)) do
      nil -> {:error, :target_node_not_found}
      member -> {:ok, member}
    end
  end

  defp current_location(location_records, node_id, generation) do
    case Enum.find(location_records, fn record ->
           record.location.node_id == node_id and
             record.location.node_generation == generation and
             record.location.state in [:ready, :draining]
         end) do
      nil -> {:error, :not_found}
      record -> {:ok, record}
    end
  end

  defp delete_target(
         _context,
         _job,
         %{node: node},
         %{location: %{node_generation: stored_generation}},
         _hash,
         _opts
       )
       when stored_generation != node.generation,
       do: :ok

  defp delete_target(context, job, %{node: node}, _target_location, hash, opts) do
    if node.node_id == context.config.node_id do
      with true <- context.config.node_generation == node.generation,
           :ok <-
             locations(opts).authorize_cleanup(
               hash,
               node.node_id,
               node.generation,
               job.job_id,
               job.owner_node,
               job.owner_generation,
               job.fencing_token,
               Keyword.put(metadata_opts(opts), :now_ms, System.system_time(:millisecond))
             ) do
        blob_store(opts).delete(hash, blob_opts(context, opts))
      else
        false -> {:error, :stale_target_generation}
        {:error, _reason} = error -> error
      end
    else
      transport(opts).delete_blob(
        context,
        node,
        hash,
        transport_opts(opts,
          request_id: request_id(job, node.node_id),
          cleanup_job_id: job.job_id,
          cleanup_target_generation: node.generation,
          cleanup_owner_node: job.owner_node,
          cleanup_owner_generation: job.owner_generation,
          cleanup_fencing_token: job.fencing_token
        )
      )
    end
  end

  defp mark_opts(job, member, opts, require_eligible_node) do
    opts
    |> metadata_opts()
    |> Keyword.put(:node_record, member)
    |> Keyword.put(:expected_generation, member.node.generation)
    |> Keyword.put(:require_eligible_node, require_eligible_node)
    |> Keyword.put(:job_fence, job)
    |> Keyword.put(:now_ms, System.system_time(:millisecond))
  end

  defp repair_mark_opts(job, target, location_records, opts) do
    base = mark_opts(job, target, opts, true)

    case Enum.find(location_records, fn record ->
           record.location.node_id == target.node.node_id
         end) do
      %{location: %{state: :absent}, mod_revision: revision} ->
        base
        |> Keyword.put(:resurrect_absent, true)
        |> Keyword.put(:expected_location_revision, revision)

      _other ->
        base
    end
  end

  defp retire_opts(job, desired, opts) do
    opts
    |> metadata_opts()
    |> Keyword.put(:desired_members, desired)
    |> Keyword.put(:job_fence, job)
    |> Keyword.put(:now_ms, System.system_time(:millisecond))
  end

  defp cleanup_prepare_opts(job, member, record, plan, opts) do
    job
    |> mark_opts(member, opts, false)
    |> Keyword.put(:retained_locations, plan.ready_desired)
    |> Keyword.put(:desired_members, plan.desired)
    |> Keyword.put(:descriptor_record, record)
  end

  defp commit_repair(store, staged, opts) do
    if function_exported?(store, :commit_repair, 2),
      do: store.commit_repair(staged, opts),
      else: store.commit(staged, opts)
  end

  defp replica_ack(job, node, descriptor) do
    %ReplicaAck{
      node_id: node.node_id,
      node_generation: node.generation,
      hash: descriptor.hash,
      size: descriptor.size,
      verified_at: System.system_time(:second),
      fencing_or_request_id: request_id(job, node.node_id)
    }
  end

  defp blob_descriptor(descriptor) do
    %BlobDescriptor{
      schema: 2,
      hash: descriptor.hash,
      algorithm: :sha256,
      size: descriptor.size,
      desired_replication_factor: descriptor.desired_replication_factor,
      created_at: descriptor.created_at
    }
  end

  defp request_id(job, node_id) do
    {job.job_id, job.fencing_token, node_id}
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> binary_part(0, 18)
    |> Base.url_encode64(padding: false)
  end

  defp payload(%Job{payload: payload}, key),
    do: Map.get(payload, key, Map.get(payload, Atom.to_string(key)))

  defp metadata_opts(opts),
    do: Keyword.take(opts, [:backend, :consistency, :timeout, :engine, :barrier])

  defp transport_opts(opts, extra) do
    opts
    |> Keyword.get(:transport_opts, [])
    |> Keyword.merge(extra)
  end

  defp scrubber_opts(context, opts) do
    [
      blob_store: blob_store(opts),
      blob_store_opts: blob_opts(context, opts),
      bytes_per_second: Keyword.get(opts, :scrub_bytes_per_second, 16 * 1_024 * 1_024)
    ]
    |> Keyword.merge(Keyword.get(opts, :scrubber_opts, []))
  end

  defp scrub_failure_state(:checksum_mismatch), do: :suspect
  defp scrub_failure_state({:size_mismatch, _expected, _actual}), do: :suspect
  defp scrub_failure_state(_reason), do: :unavailable

  defp blob_opts(context, opts),
    do:
      context
      |> Context.blob_store_options()
      |> Keyword.merge(Keyword.get(opts, :blob_store_opts, []))

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)
  defp catalog(opts), do: Keyword.get(opts, :catalog, BlobCatalog)
  defp locations(opts), do: Keyword.get(opts, :locations, BlobLocations)
  defp scrubber(opts), do: Keyword.get(opts, :scrubber, Scrubber)
  defp blob_store(opts), do: Keyword.get(opts, :blob_store, LocalCAS)
  defp transport(opts), do: Keyword.get(opts, :transport, configured_transport())

  defp configured_transport,
    do: Application.fetch_env!(:ex_storage_service, :cluster_transport)
end
