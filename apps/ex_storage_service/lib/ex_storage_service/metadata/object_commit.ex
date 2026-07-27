defmodule ExStorageService.Metadata.ObjectCommit do
  @moduledoc """
  Atomic v2 object metadata commits.

  Each logical operation writes an operation record in the same Concord
  transaction as the immutable version and mutable head. That record is the
  source of truth when the transaction result is ambiguous.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys

  @default_max_attempts 16

  @type commit_result :: %{
          operation_id: String.t(),
          version_id: String.t(),
          kind: :put | :delete_marker | :deleted
        }

  @spec put(String.t(), String.t(), map(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def put(bucket, key, metadata, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      version_id = Keyword.get_lazy(opts, :version_id, &generate_version_id/0)

      commit_new_version(
        bucket,
        key,
        metadata,
        operation_id,
        version_id,
        :put,
        opts
      )
    end
  end

  @spec delete_marker(String.t(), String.t(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def delete_marker(bucket, key, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      version_id = Keyword.get_lazy(opts, :version_id, &generate_version_id/0)
      now = Keyword.get_lazy(opts, :timestamp, &timestamp/0)

      metadata = %{
        is_delete_marker: true,
        delete_marker: true,
        object_type: :blob,
        created_at: now,
        updated_at: now
      }

      commit_new_version(
        bucket,
        key,
        metadata,
        operation_id,
        version_id,
        :delete_marker,
        opts
      )
    end
  end

  @doc """
  Permanently removes one metadata version and atomically repairs the head.

  Blob bytes are intentionally untouched.
  """
  @spec delete_version(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, commit_result()} | {:error, term()}
  def delete_version(bucket, key, version_id, opts \\ []) do
    with :ok <- ensure_v2_writes(opts) do
      operation_id = Keyword.get_lazy(opts, :operation_id, &generate_operation_id/0)
      backend = backend(opts)
      operation_key = Keys.outbox(operation_id)

      request_fingerprint =
        request_fingerprint(bucket, key, :deleted, %{version_id: version_id}, opts)

      case resolve(backend, operation_key, request_fingerprint, opts) do
        {:ok, result} ->
          {:ok, result}

        :not_found ->
          do_delete_version(
            backend,
            bucket,
            key,
            version_id,
            operation_id,
            operation_key,
            request_fingerprint,
            opts,
            max_attempts(opts)
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec get_head(String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def get_head(bucket, key, opts \\ []) do
    backend = backend(opts)

    with {:ok, %{value: head}} <- backend.get(Keys.object_head(bucket, key), read_opts(opts)),
         {:ok, %{value: version}} <-
           backend.get(Keys.object_version(bucket, key, head.version_id), read_opts(opts)) do
      {:ok, version_to_public(version)}
    else
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  @spec get_version(String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()}
  def get_version(bucket, key, version_id, opts \\ []) do
    case backend(opts).get(Keys.object_version(bucket, key, version_id), read_opts(opts)) do
      {:ok, %{value: version}} -> {:ok, version_to_public(version)}
      {:ok, nil} -> {:error, :not_found}
      error -> error
    end
  end

  @spec list_versions(String.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def list_versions(bucket, key, opts \\ []) do
    prefix = Keys.object_version_prefix(bucket, key)

    with {:ok, records} <- backend(opts).prefix_scan(prefix, read_opts(opts)) do
      versions =
        records
        |> Enum.map(fn {_record_key, version} -> version_to_public(version) end)
        |> Enum.sort_by(
          fn version -> {Map.get(version, :created_at, ""), version.version_id} end,
          :desc
        )

      {:ok, versions}
    end
  end

  defp commit_new_version(
         bucket,
         key,
         metadata,
         operation_id,
         version_id,
         kind,
         opts
       ) do
    backend = backend(opts)
    operation_key = Keys.outbox(operation_id)
    request_fingerprint = request_fingerprint(bucket, key, kind, metadata, opts)

    case resolve(backend, operation_key, request_fingerprint, opts) do
      {:ok, result} ->
        {:ok, result}

      :not_found ->
        do_commit_new_version(
          backend,
          bucket,
          key,
          metadata,
          operation_id,
          operation_key,
          version_id,
          kind,
          request_fingerprint,
          opts,
          max_attempts(opts)
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_commit_new_version(
         _backend,
         _bucket,
         _key,
         _meta,
         _op,
         _op_key,
         _vid,
         _kind,
         _fingerprint,
         _opts,
         0
       ),
       do: {:error, :compare_retry_exhausted}

  defp do_commit_new_version(
         backend,
         bucket,
         key,
         metadata,
         operation_id,
         operation_key,
         version_id,
         kind,
         request_fingerprint,
         opts,
         attempts_left
       ) do
    head_key = Keys.object_head(bucket, key)

    with {:ok, observed_head} <- backend.get(head_key, read_opts(opts)),
         {:ok, parent_version_id} <-
           observed_parent_version_id(backend, bucket, key, observed_head, opts),
         {:ok, cluster_metadata} <-
           cluster_metadata_operations(backend, metadata, opts) do
      now = Map.get(metadata, :created_at, timestamp())
      delete_marker? = kind == :delete_marker

      version =
        metadata
        |> Map.put(:schema, 2)
        |> Map.put(:bucket, bucket)
        |> Map.put(:key, key)
        |> Map.put(:version_id, version_id)
        |> Map.put(:parent_version_id, parent_version_id)
        |> Map.put(:delete_marker, delete_marker?)
        |> Map.put(:is_delete_marker, delete_marker?)
        |> Map.put_new(:object_type, :blob)
        |> Map.put_new(:created_at, now)
        |> put_if_present(:durability, cluster_metadata.durability)

      head = %{
        schema: 2,
        bucket: bucket,
        key: key,
        version_id: version_id,
        delete_marker: delete_marker?,
        etag: Map.get(version, :etag),
        updated_at: Map.get(metadata, :updated_at, now)
      }

      result = %{
        operation_id: operation_id,
        version_id: version_id,
        kind: kind
      }

      operation = %{
        schema: 2,
        operation_id: operation_id,
        request_fingerprint: request_fingerprint,
        result: result,
        events: cluster_metadata.repair_events,
        committed_at: now
      }

      spec = %{
        compare:
          [
            head_compare(head_key, observed_head),
            {:exists, operation_key, :==, false},
            {:exists, Keys.object_version(bucket, key, version_id), :==, false}
          ] ++ cluster_metadata.compare,
        success:
          [
            {:put, Keys.object_version(bucket, key, version_id), version, %{}},
            {:put, head_key, head, %{}}
          ] ++
            blob_operations(version, now, cluster_metadata) ++
            cluster_metadata.success ++ [{:put, operation_key, operation, %{}}],
        failure: []
      }

      attempt_key = attempt_key(operation_id, spec)

      case backend.transaction(spec, transaction_opts(opts, attempt_key)) do
        {:ok, %{succeeded: true}} ->
          {:ok, result}

        {:ok, %{succeeded: false}} ->
          retry_or_resolve(
            backend,
            operation_key,
            request_fingerprint,
            opts,
            fn ->
              do_commit_new_version(
                backend,
                bucket,
                key,
                metadata,
                operation_id,
                operation_key,
                version_id,
                kind,
                request_fingerprint,
                opts,
                attempts_left - 1
              )
            end
          )

        {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
          resolve_ambiguous(
            backend,
            attempt_key,
            operation_key,
            request_fingerprint,
            result,
            spec,
            opts,
            fn ->
              do_commit_new_version(
                backend,
                bucket,
                key,
                metadata,
                operation_id,
                operation_key,
                version_id,
                kind,
                request_fingerprint,
                opts,
                attempts_left - 1
              )
            end
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp do_delete_version(
         _backend,
         _bucket,
         _key,
         _version_id,
         _operation_id,
         _operation_key,
         _request_fingerprint,
         _opts,
         0
       ),
       do: {:error, :compare_retry_exhausted}

  defp do_delete_version(
         backend,
         bucket,
         key,
         version_id,
         operation_id,
         operation_key,
         request_fingerprint,
         opts,
         attempts_left
       ) do
    head_key = Keys.object_head(bucket, key)
    version_key = Keys.object_version(bucket, key, version_id)

    with {:ok, observed_head} <- backend.get(head_key, read_opts(opts)),
         {:ok, observed_version} <- backend.get(version_key, read_opts(opts)),
         {:ok, versions} <- list_versions(bucket, key, Keyword.put(opts, :backend, backend)) do
      if observed_version == nil do
        {:ok, %{operation_id: operation_id, version_id: version_id, kind: :deleted}}
      else
        remaining = Enum.reject(versions, &(&1.version_id == version_id))
        deleting_head? = value_field(observed_head, :version_id) == version_id

        replacement =
          if deleting_head?,
            do: replacement_version(observed_version, remaining),
            else: nil

        result = %{operation_id: operation_id, version_id: version_id, kind: :deleted}

        operation = %{
          schema: 2,
          operation_id: operation_id,
          request_fingerprint: request_fingerprint,
          result: result,
          events: [],
          committed_at: timestamp()
        }

        head_ops =
          if deleting_head? do
            replacement_head_operation(head_key, bucket, key, replacement)
          else
            []
          end

        spec = %{
          compare:
            [
              head_compare(head_key, observed_head),
              {:mod_revision, version_key, :==, observed_version.mod_revision},
              {:exists, operation_key, :==, false}
            ] ++ replacement_compare(bucket, key, replacement),
          success:
            [{:delete, {:key, version_key}, %{}}] ++
              head_ops ++ [{:put, operation_key, operation, %{}}],
          failure: []
        }

        attempt_key = attempt_key(operation_id, spec)

        case backend.transaction(spec, transaction_opts(opts, attempt_key)) do
          {:ok, %{succeeded: true}} ->
            {:ok, result}

          {:ok, %{succeeded: false}} ->
            retry_or_resolve(backend, operation_key, request_fingerprint, opts, fn ->
              do_delete_version(
                backend,
                bucket,
                key,
                version_id,
                operation_id,
                operation_key,
                request_fingerprint,
                opts,
                attempts_left - 1
              )
            end)

          {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
            resolve_ambiguous(
              backend,
              attempt_key,
              operation_key,
              request_fingerprint,
              result,
              spec,
              opts,
              fn ->
                do_delete_version(
                  backend,
                  bucket,
                  key,
                  version_id,
                  operation_id,
                  operation_key,
                  request_fingerprint,
                  opts,
                  attempts_left - 1
                )
              end
            )

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  defp replacement_head_operation(head_key, _bucket, _key, nil),
    do: [{:delete, {:key, head_key}, %{}}]

  defp replacement_head_operation(head_key, bucket, key, version) do
    head = %{
      schema: 2,
      bucket: bucket,
      key: key,
      version_id: version.version_id,
      delete_marker: Map.get(version, :delete_marker, false),
      etag: Map.get(version, :etag),
      updated_at: Map.get(version, :created_at)
    }

    [{:put, head_key, head, %{}}]
  end

  defp replacement_compare(_bucket, _key, nil), do: []

  defp replacement_compare(bucket, key, version),
    do: [{:exists, Keys.object_version(bucket, key, version.version_id), :==, true}]

  defp replacement_version(observed_version, remaining) do
    parent_version_id = value_field(observed_version, :parent_version_id)
    Enum.find(remaining, &(&1.version_id == parent_version_id)) || List.first(remaining)
  end

  defp blob_operations(_version, _now, %{descriptor: descriptor})
       when not is_nil(descriptor),
       do: []

  defp blob_operations(%{content_hash: hash, size: size}, now, _cluster_metadata)
       when is_binary(hash) and is_integer(size) do
    blob = %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: size,
      desired_replication_factor: 1,
      created_at: now
    }

    [{:put, Keys.blob(hash), blob, %{}}]
  end

  defp blob_operations(_version, _now, _cluster_metadata), do: []

  defp cluster_metadata_operations(backend, metadata, opts) do
    case Keyword.fetch(opts, :durability) do
      {:ok, durability} ->
        do_cluster_metadata_operations(backend, metadata, durability, opts)

      :error ->
        empty_cluster_metadata()
    end
  end

  defp empty_cluster_metadata do
    {:ok,
     %{
       compare: [],
       success: [],
       descriptor: nil,
       durability: nil,
       repair_events: []
     }}
  end

  defp do_cluster_metadata_operations(backend, metadata, durability, opts) do
    with {:ok, evidence} <- validate_durability(metadata, durability),
         descriptor_key = Keys.blob(evidence.descriptor.hash),
         {:ok, observed_descriptor} <- backend.get(descriptor_key, read_opts(opts)),
         :ok <- validate_blob_descriptor(observed_descriptor, evidence.descriptor),
         {:ok, observed_locations} <-
           read_locations(backend, evidence.acknowledgements, opts) do
      descriptor =
        merge_blob_descriptor(observed_descriptor, evidence.descriptor)

      {:ok,
       %{
         compare:
           [revision_compare(descriptor_key, observed_descriptor)] ++
             location_compares(observed_locations) ++ node_compares(evidence.placement),
         success:
           [{:put, descriptor_key, descriptor, %{}}] ++
             location_operations(evidence.acknowledgements),
         descriptor: descriptor,
         durability: durability_record(evidence),
         repair_events: repair_events(evidence)
       }}
    end
  end

  defp validate_durability(metadata, durability) when is_map(durability) do
    descriptor = plain_map(Map.get(durability, :descriptor))
    acknowledgements = Map.get(durability, :acknowledgements, [])
    placement = Map.get(durability, :placement, [])
    missing_node_ids = Map.get(durability, :missing_node_ids, [])
    configured_write_quorum = Map.get(durability, :configured_write_quorum)
    required_write_quorum = Map.get(durability, :required_write_quorum)
    achieved_replica_count = Map.get(durability, :achieved_replica_count)
    durability_mode = Map.get(durability, :durability)

    evidence = %{
      descriptor: descriptor,
      acknowledgements: acknowledgements,
      placement: placement,
      missing_node_ids: missing_node_ids,
      configured_write_quorum: configured_write_quorum,
      required_write_quorum: required_write_quorum,
      achieved_replica_count: achieved_replica_count,
      durability: durability_mode
    }

    with :ok <- validate_descriptor_identity(metadata, descriptor),
         :ok <- validate_quorum_evidence(evidence),
         :ok <- validate_ack_evidence(evidence) do
      {:ok, evidence}
    end
  end

  defp validate_durability(_metadata, _durability), do: {:error, :invalid_durability_evidence}

  defp validate_descriptor_identity(metadata, descriptor)
       when is_map(descriptor) do
    if Map.get(descriptor, :hash) == Map.get(metadata, :content_hash) and
         Map.get(descriptor, :size) == Map.get(metadata, :size) and
         Map.get(descriptor, :algorithm) == :sha256 and
         is_integer(Map.get(descriptor, :desired_replication_factor)) and
         Map.get(descriptor, :desired_replication_factor) > 0 do
      :ok
    else
      {:error, :invalid_durability_evidence}
    end
  end

  defp validate_descriptor_identity(_metadata, _descriptor),
    do: {:error, :invalid_durability_evidence}

  defp validate_quorum_evidence(evidence) do
    acknowledged = length(evidence.acknowledgements)

    if is_integer(evidence.configured_write_quorum) and
         is_integer(evidence.required_write_quorum) and
         is_integer(evidence.achieved_replica_count) and
         evidence.configured_write_quorum > 0 and evidence.required_write_quorum > 0 and
         evidence.achieved_replica_count == acknowledged and
         acknowledged >= evidence.required_write_quorum and valid_durability_mode?(evidence) do
      :ok
    else
      {:error, :invalid_durability_evidence}
    end
  end

  defp valid_durability_mode?(%{
         durability: :strict,
         required_write_quorum: write_quorum,
         configured_write_quorum: write_quorum
       }),
       do: true

  defp valid_durability_mode?(%{
         durability: :degraded,
         required_write_quorum: effective,
         configured_write_quorum: configured
       })
       when is_integer(effective) and is_integer(configured),
       do: effective < configured

  defp valid_durability_mode?(_evidence), do: false

  defp validate_ack_evidence(evidence) do
    selected_nodes =
      Map.new(evidence.placement, fn %{node: node} ->
        node = plain_map(node)
        {node.node_id, node}
      end)

    acknowledged_ids = Enum.map(evidence.acknowledgements, &Map.get(&1, :node_id))

    valid_acks? =
      Enum.all?(evidence.acknowledgements, fn ack ->
        ack = plain_map(ack)

        case Map.get(selected_nodes, ack.node_id) do
          nil ->
            false

          node ->
            ack.hash == evidence.descriptor.hash and
              ack.size == evidence.descriptor.size and
              ack.node_generation == node.generation
        end
      end)

    missing_ids = evidence.missing_node_ids |> Enum.sort() |> Enum.uniq()

    expected_missing_ids =
      selected_nodes
      |> Map.keys()
      |> Enum.reject(&(&1 in acknowledged_ids))
      |> Enum.sort()

    if valid_acks? and length(acknowledged_ids) == length(Enum.uniq(acknowledged_ids)) and
         missing_ids == expected_missing_ids do
      :ok
    else
      {:error, :invalid_durability_evidence}
    end
  end

  defp validate_blob_descriptor(nil, _descriptor), do: :ok

  defp validate_blob_descriptor(%{value: current}, descriptor) do
    if Map.get(current, :hash) == descriptor.hash and
         Map.get(current, :algorithm) == descriptor.algorithm and
         Map.get(current, :size) == descriptor.size do
      :ok
    else
      {:error, :blob_metadata_conflict}
    end
  end

  defp merge_blob_descriptor(nil, descriptor), do: descriptor

  defp merge_blob_descriptor(%{value: current}, descriptor) do
    Map.put(
      current,
      :desired_replication_factor,
      max(
        Map.get(current, :desired_replication_factor, 1),
        descriptor.desired_replication_factor
      )
    )
  end

  defp read_locations(backend, acknowledgements, opts) do
    Enum.reduce_while(acknowledgements, {:ok, []}, fn ack, {:ok, records} ->
      key = Keys.blob_location(Map.get(ack, :hash), Map.get(ack, :node_id))

      case backend.get(key, read_opts(opts)) do
        {:ok, record} -> {:cont, {:ok, [{key, record} | records]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp location_compares(records),
    do: Enum.map(records, fn {key, record} -> revision_compare(key, record) end)

  defp location_operations(acknowledgements) do
    Enum.map(acknowledgements, fn ack ->
      ack = plain_map(ack)

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

  defp node_compares(placement) do
    Enum.flat_map(placement, fn %{node: node, mod_revision: revision} ->
      node = plain_map(node)
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

  defp durability_record(evidence) do
    %{
      desired_replication_factor: evidence.descriptor.desired_replication_factor,
      configured_write_quorum: evidence.configured_write_quorum,
      effective_write_quorum: evidence.required_write_quorum,
      acknowledged_replica_count: evidence.achieved_replica_count,
      acknowledged_node_ids:
        evidence.acknowledgements |> Enum.map(&Map.get(&1, :node_id)) |> Enum.sort(),
      degraded: evidence.durability == :degraded
    }
  end

  defp repair_events(evidence) do
    source_ids = evidence.acknowledgements |> Enum.map(&Map.get(&1, :node_id)) |> Enum.sort()

    Enum.map(evidence.missing_node_ids, fn node_id ->
      %{
        id: fingerprint({evidence.descriptor.hash, node_id}),
        kind: :repair_blob,
        state: :pending,
        payload: %{
          hash: evidence.descriptor.hash,
          target_node_id: node_id,
          source_node_ids: source_ids
        }
      }
    end)
  end

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}
  defp revision_compare(key, %{mod_revision: revision}), do: {:mod_revision, key, :==, revision}

  defp head_compare(head_key, nil), do: {:mod_revision, head_key, :==, 0}

  defp head_compare(head_key, %{mod_revision: revision}),
    do: {:mod_revision, head_key, :==, revision}

  defp value_field(nil, _field), do: nil
  defp value_field(%{value: value}, field), do: Map.get(value, field)

  defp observed_parent_version_id(_backend, _bucket, _key, observed_head, _opts)
       when not is_nil(observed_head),
       do: {:ok, value_field(observed_head, :version_id)}

  defp observed_parent_version_id(backend, bucket, key, nil, opts) do
    case backend.get("obj_ver_list:#{bucket}:#{key}", read_opts(opts)) do
      {:ok, %{value: [version_id | _]}} when is_binary(version_id) ->
        {:ok, version_id}

      {:ok, _missing_or_invalid_list} ->
        legacy_object_parent_version_id(backend, bucket, key, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp legacy_object_parent_version_id(backend, bucket, key, opts) do
    case backend.get("obj:#{bucket}:#{key}", read_opts(opts)) do
      {:ok, %{value: metadata}} when is_map(metadata) -> {:ok, Map.get(metadata, :version_id)}
      {:ok, _missing_or_invalid_metadata} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp retry_or_resolve(backend, operation_key, request_fingerprint, opts, retry) do
    case resolve(backend, operation_key, request_fingerprint, opts) do
      {:ok, result} -> {:ok, result}
      :not_found -> retry.()
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_ambiguous(
         backend,
         attempt_key,
         operation_key,
         request_fingerprint,
         result,
         spec,
         opts,
         retry
       ) do
    case resolve(backend, operation_key, request_fingerprint, opts) do
      {:ok, prior_result} ->
        {:ok, prior_result}

      :not_found ->
        resolve_transaction_or_replay(
          backend,
          attempt_key,
          operation_key,
          request_fingerprint,
          result,
          spec,
          opts,
          retry
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_transaction_or_replay(
         backend,
         attempt_key,
         operation_key,
         request_fingerprint,
         result,
         spec,
         opts,
         retry
       ) do
    case backend.resolve_transaction(attempt_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        {:ok, result}

      {:ok, %{succeeded: false}} ->
        retry.()

      {:error, :not_found} ->
        replay_transaction(
          backend,
          attempt_key,
          operation_key,
          request_fingerprint,
          result,
          spec,
          opts,
          retry
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replay_transaction(
         backend,
         attempt_key,
         operation_key,
         request_fingerprint,
         result,
         spec,
         opts,
         retry
       ) do
    case backend.transaction(spec, transaction_opts(opts, attempt_key)) do
      {:ok, %{succeeded: true}} ->
        {:ok, result}

      {:ok, %{succeeded: false}} ->
        retry_or_resolve(backend, operation_key, request_fingerprint, opts, retry)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve(backend, operation_key, request_fingerprint, opts) do
    case backend.resolve_operation(operation_key, read_opts(opts)) do
      {:ok, %{value: %{request_fingerprint: ^request_fingerprint, result: result}}} ->
        {:ok, result}

      {:ok, %{value: %{request_fingerprint: _other}}} ->
        {:error, :operation_id_conflict}

      {:ok, %{value: %{operation_id: _operation_id, version_id: _version_id} = result}} ->
        {:ok, result}

      {:ok, nil} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp request_fingerprint(bucket, key, kind, metadata, opts) do
    stable_metadata =
      Map.drop(metadata, [:created_at, :updated_at, :version_id, :parent_version_id])

    fingerprint({bucket, key, kind, stable_metadata, durability_fingerprint(opts[:durability])})
  end

  defp durability_fingerprint(nil), do: nil

  defp durability_fingerprint(durability) when is_map(durability) do
    descriptor = plain_map(Map.get(durability, :descriptor)) || %{}

    %{
      hash: Map.get(descriptor, :hash),
      size: Map.get(descriptor, :size),
      desired_replication_factor: Map.get(descriptor, :desired_replication_factor),
      configured_write_quorum: Map.get(durability, :configured_write_quorum)
    }
  end

  defp durability_fingerprint(_other), do: :invalid

  defp attempt_key(operation_id, spec), do: operation_id <> ":" <> fingerprint(spec)

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp plain_map(%_{} = struct), do: Map.from_struct(struct)
  defp plain_map(map) when is_map(map), do: map
  defp plain_map(_other), do: nil

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp version_to_public(version) do
    version
    |> Map.put(:is_delete_marker, Map.get(version, :delete_marker, false))
    |> Map.put(:metadata, Map.get(version, :user_metadata, Map.get(version, :metadata, %{})))
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp max_attempts(opts), do: Keyword.get(opts, :max_attempts, @default_max_attempts)

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, attempt_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, attempt_key)
  end

  defp ensure_v2_writes(opts) do
    schema =
      Keyword.get_lazy(opts, :metadata_schema, fn ->
        :ex_storage_service
        |> Application.get_env(:instance_config, [])
        |> Keyword.get(:metadata_schema, :v2)
      end)

    if schema == :v2, do: :ok, else: {:error, :v2_metadata_writes_disabled}
  end

  defp generate_operation_id, do: "object-commit-" <> random_id()

  defp generate_version_id do
    "#{System.system_time(:microsecond)}-#{random_id()}"
  end

  defp random_id, do: :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
  defp timestamp, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
