defmodule ExStorageService.Metadata.MultipartCommit do
  @moduledoc """
  Atomic multipart-part publication for the cluster data plane.

  Durable blob acknowledgements, their ready locations, the part record, and
  the operation outcome share one Concord transaction. Retrying an upload-part
  request therefore cannot expose metadata without its recorded durability.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.OperationIntents

  @max_attempts 8

  @spec put_part(String.t(), String.t(), pos_integer(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def put_part(bucket, upload_id, part_number, part, durability, opts \\ []) do
    with :ok <- validate_evidence(part, durability) do
      operation_id =
        Keyword.get(
          opts,
          :operation_id,
          "multipart-part:#{upload_id}:#{part_number}:#{part.hash}"
        )

      fingerprint =
        fingerprint({
          bucket,
          upload_id,
          part_number,
          part.hash,
          part.size,
          part.etag,
          durability.descriptor.desired_replication_factor,
          durability.configured_write_quorum
        })

      do_put_part(
        backend(opts),
        bucket,
        upload_id,
        part_number,
        part,
        durability,
        operation_id,
        fingerprint,
        opts,
        @max_attempts
      )
    end
  end

  defp do_put_part(
         _backend,
         _bucket,
         _upload_id,
         _part_number,
         _part,
         _durability,
         _operation_id,
         _fingerprint,
         _opts,
         0
       ),
       do: {:error, :metadata_quorum_unavailable}

  defp do_put_part(
         backend,
         bucket,
         upload_id,
         part_number,
         part,
         durability,
         operation_id,
         fingerprint,
         opts,
         attempts_left
       ) do
    operation_key = Keys.outbox(operation_id)

    with {:ok, prior} <- backend.get(operation_key, read_opts(opts)),
         :continue <- prior_result(prior, fingerprint),
         upload_key = Keys.multipart_upload(upload_id),
         {:ok, upload} <- backend.get(upload_key, read_opts(opts)),
         :ok <- validate_upload(upload, bucket, upload_id),
         part_key = Keys.multipart_part(upload_id, part_number),
         {:ok, observed_part} <- backend.get(part_key, read_opts(opts)),
         descriptor_key = Keys.blob(part.hash),
         {:ok, observed_descriptor} <- backend.get(descriptor_key, read_opts(opts)),
         :ok <- validate_descriptor(observed_descriptor, durability.descriptor),
         {:ok, observed_locations} <- read_locations(backend, durability, opts),
         :ok <- validate_publishable_locations(observed_locations),
         {:ok, intent} <- intent_operations(operation_id, part.hash, backend, opts) do
      now = Keyword.get_lazy(opts, :timestamp, &timestamp/0)

      part_record =
        part
        |> Map.new()
        |> Map.merge(%{
          schema: 2,
          bucket: bucket,
          upload_id: upload_id,
          part_number: part_number,
          uploaded_at: now,
          durability: durability_record(durability)
        })

      result = %{etag: part.etag, part_number: part_number, hash: part.hash, size: part.size}

      operation = %{
        schema: 2,
        operation_id: operation_id,
        request_fingerprint: fingerprint,
        result: result,
        events: repair_events(durability),
        committed_at: now
      }

      spec = %{
        compare:
          [
            revision_compare(upload_key, upload),
            revision_compare(part_key, observed_part),
            revision_compare(descriptor_key, observed_descriptor),
            {:exists, operation_key, :==, false}
          ] ++
            location_compares(observed_locations) ++ node_compares(durability) ++ intent.compare,
        success:
          [
            {:put, part_key, part_record, %{}},
            {:put, descriptor_key, merged_descriptor(observed_descriptor, durability.descriptor),
             %{}}
          ] ++
            location_operations(durability) ++
            intent.success ++ [{:put, operation_key, operation, %{}}],
        failure: []
      }

      attempt_key = attempt_key(operation_id, spec)

      case backend.transaction(spec, transaction_opts(opts, attempt_key)) do
        {:ok, %{succeeded: true}} ->
          {:ok, result}

        {:ok, %{succeeded: false}} ->
          retry(
            backend,
            operation_key,
            fingerprint,
            fn ->
              do_put_part(
                backend,
                bucket,
                upload_id,
                part_number,
                part,
                durability,
                operation_id,
                fingerprint,
                opts,
                attempts_left - 1
              )
            end,
            opts
          )

        {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
          resolve_ambiguous(
            backend,
            attempt_key,
            operation_key,
            fingerprint,
            spec,
            opts,
            fn ->
              do_put_part(
                backend,
                bucket,
                upload_id,
                part_number,
                part,
                durability,
                operation_id,
                fingerprint,
                opts,
                attempts_left - 1
              )
            end
          )

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:return, result} -> {:ok, result}
      {:error, _reason} = error -> error
    end
  end

  defp prior_result(nil, _fingerprint), do: :continue

  defp prior_result(%{value: %{request_fingerprint: fingerprint, result: result}}, fingerprint),
    do: {:return, result}

  defp prior_result(%{value: _other}, _fingerprint), do: {:error, :operation_id_conflict}

  defp validate_upload(nil, _bucket, _upload_id), do: {:error, :not_found}

  defp validate_upload(%{value: upload}, bucket, upload_id) do
    if Map.get(upload, :bucket) == bucket and Map.get(upload, :upload_id) == upload_id and
         Map.get(upload, :status) == :initiated,
       do: :ok,
       else: {:error, :not_found}
  end

  defp validate_descriptor(nil, _descriptor), do: :ok

  defp validate_descriptor(%{value: current}, descriptor) do
    if Map.get(current, :hash) == descriptor.hash and
         Map.get(current, :algorithm) == descriptor.algorithm and
         Map.get(current, :size) == descriptor.size,
       do: :ok,
       else: {:error, :blob_metadata_conflict}
  end

  defp merged_descriptor(nil, descriptor), do: Map.from_struct(descriptor)

  defp merged_descriptor(%{value: current}, descriptor) do
    Map.put(
      current,
      :desired_replication_factor,
      max(
        Map.get(current, :desired_replication_factor, 1),
        descriptor.desired_replication_factor
      )
    )
  end

  defp validate_evidence(part, durability) when is_map(part) and is_map(durability) do
    descriptor = Map.get(durability, :descriptor)
    acknowledgements = Map.get(durability, :acknowledgements, [])
    placement = Map.get(durability, :placement, [])
    acknowledged_ids = Enum.map(acknowledgements, &Map.get(&1, :node_id))
    selected = Map.new(placement, fn %{node: node} -> {node.node_id, node} end)

    missing_ids =
      selected
      |> Map.keys()
      |> Enum.reject(&(&1 in acknowledged_ids))
      |> Enum.sort()

    valid_acks? =
      Enum.all?(acknowledgements, fn ack ->
        case Map.get(selected, Map.get(ack, :node_id)) do
          nil ->
            false

          node ->
            Map.get(ack, :hash) == descriptor.hash and
              Map.get(ack, :size) == descriptor.size and
              Map.get(ack, :node_generation) == node.generation
        end
      end)

    quorum_valid? =
      case Map.get(durability, :durability) do
        :strict ->
          durability.required_write_quorum == durability.configured_write_quorum

        :degraded ->
          durability.required_write_quorum < durability.configured_write_quorum

        _other ->
          false
      end

    if match?(%{hash: _, size: _, algorithm: :sha256}, descriptor) and
         descriptor.hash == Map.get(part, :hash) and descriptor.size == Map.get(part, :size) and
         acknowledgements != [] and
         length(acknowledgements) == durability.achieved_replica_count and
         length(acknowledgements) >= durability.required_write_quorum and
         length(acknowledged_ids) == length(Enum.uniq(acknowledged_ids)) and
         missing_ids == Enum.sort(Enum.uniq(durability.missing_node_ids)) and valid_acks? and
         quorum_valid? do
      :ok
    else
      {:error, :invalid_durability_evidence}
    end
  rescue
    KeyError -> {:error, :invalid_durability_evidence}
  end

  defp validate_evidence(_part, _durability), do: {:error, :invalid_durability_evidence}

  defp read_locations(backend, durability, opts) do
    durability.acknowledgements
    |> Enum.reduce_while({:ok, []}, fn ack, {:ok, records} ->
      key = Keys.blob_location(ack.hash, ack.node_id)

      case backend.get(key, read_opts(opts)) do
        {:ok, record} -> {:cont, {:ok, [{key, record} | records]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp location_compares(records),
    do: Enum.map(records, fn {key, record} -> revision_compare(key, record) end)

  defp validate_publishable_locations(records) do
    if Enum.any?(records, fn
         {_key, %{value: value}} ->
           Map.get(value, :state, Map.get(value, "state")) in [:deleting, "deleting"]

         {_key, nil} ->
           false
       end),
       do: {:error, :blob_cleanup_in_progress},
       else: :ok
  end

  defp location_operations(durability) do
    Enum.map(durability.acknowledgements, fn ack ->
      location = %{
        schema: 2,
        hash: ack.hash,
        node_id: ack.node_id,
        node_generation: ack.node_generation,
        state: :ready,
        size: ack.size,
        verified_at: ack.verified_at
      }

      {:put, Keys.blob_location(ack.hash, ack.node_id), location, %{}}
    end)
  end

  defp node_compares(durability) do
    Enum.flat_map(durability.placement, fn %{node: node, mod_revision: revision} ->
      key = Keys.cluster_node(node.node_id)

      [
        {:mod_revision, key, :==, revision},
        {:field, key, [:generation], :==, node.generation},
        {:field, key, [:role], :==, :data},
        {:field, key, [:enabled], :==, true},
        {:field, key, [:draining], :==, false}
      ]
    end)
  end

  defp repair_events(durability) do
    source_ids = Enum.map(durability.acknowledgements, & &1.node_id) |> Enum.sort()

    repair_epoch =
      durability.acknowledgements
      |> Enum.map(&{&1.node_id, &1.node_generation, &1.verified_at, &1.fencing_or_request_id})
      |> Enum.sort()

    Enum.map(durability.missing_node_ids, fn node_id ->
      %{
        id: fingerprint({durability.descriptor.hash, node_id, repair_epoch}),
        kind: :repair_blob,
        state: :pending,
        payload: %{
          hash: durability.descriptor.hash,
          target_node_id: node_id,
          source_node_ids: source_ids
        }
      }
    end)
  end

  defp durability_record(durability) do
    %{
      desired_replication_factor: durability.descriptor.desired_replication_factor,
      configured_write_quorum: durability.configured_write_quorum,
      effective_write_quorum: durability.required_write_quorum,
      acknowledged_replica_count: durability.achieved_replica_count,
      acknowledged_node_ids: durability.acknowledgements |> Enum.map(& &1.node_id) |> Enum.sort(),
      degraded: durability.durability == :degraded
    }
  end

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}
  defp revision_compare(key, %{mod_revision: revision}), do: {:mod_revision, key, :==, revision}

  defp retry(backend, operation_key, fingerprint, retry, opts) do
    case backend.get(operation_key, read_opts(opts)) do
      {:ok, prior} ->
        case prior_result(prior, fingerprint) do
          {:return, result} -> {:ok, result}
          :continue -> retry.()
          {:error, _reason} = error -> error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_ambiguous(
         backend,
         attempt_key,
         operation_key,
         fingerprint,
         spec,
         opts,
         retry
       ) do
    case backend.get(operation_key, read_opts(opts)) do
      {:ok, prior} when not is_nil(prior) ->
        case prior_result(prior, fingerprint) do
          {:return, result} -> {:ok, result}
          {:error, _reason} = error -> error
        end

      {:ok, nil} ->
        case backend.resolve_transaction(attempt_key, read_opts(opts)) do
          {:ok, %{succeeded: true}} ->
            retry(backend, operation_key, fingerprint, retry, opts)

          {:ok, %{succeeded: false}} ->
            retry.()

          {:error, :not_found} ->
            case backend.transaction(spec, transaction_opts(opts, attempt_key)) do
              {:ok, %{succeeded: true}} -> retry(backend, operation_key, fingerprint, retry, opts)
              {:ok, %{succeeded: false}} -> retry.()
              {:error, _reason} -> {:error, :metadata_quorum_unavailable}
            end

          {:error, _reason} ->
            {:error, :metadata_quorum_unavailable}
        end

      {:error, _reason} ->
        {:error, :metadata_quorum_unavailable}
    end
  end

  defp attempt_key(operation_id, spec),
    do: operation_id <> ":" <> fingerprint(spec)

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp transaction_opts(opts, attempt_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, attempt_key)
  end

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp intent_operations(operation_id, hash, backend, opts) do
    if Keyword.get(opts, :operation_intent, false) do
      operation_intents(opts).commit_operations(
        operation_id,
        hash,
        Keyword.put(opts, :backend, backend)
      )
    else
      {:ok, %{compare: [], success: []}}
    end
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp operation_intents(opts), do: Keyword.get(opts, :operation_intents, OperationIntents)
  defp timestamp, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
