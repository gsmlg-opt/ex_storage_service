defmodule ExStorageService.Metadata.BlobLocations do
  @moduledoc """
  Strongly reads blob-location evidence and records read-path failures.

  A failed or corrupt location transition and its durable repair intent are one
  Concord transaction. Phase 8 will dispatch these outbox events; reads never
  wait for that background work.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.BlobLocation

  @max_attempts 4

  @type record :: %{
          key: binary(),
          location: BlobLocation.t(),
          mod_revision: non_neg_integer()
        }

  @spec list(binary(), keyword()) :: {:ok, [record()]} | {:error, term()}
  def list(hash, opts \\ []) when is_binary(hash) do
    with {:ok, entries} <-
           backend(opts).prefix_scan(Keys.blob_location_prefix(hash), read_opts(opts)) do
      entries
      |> Enum.reduce_while({:ok, []}, fn {key, _scanned_value}, {:ok, records} ->
        with {:ok, %{value: value, mod_revision: revision}} <-
               backend(opts).get(key, read_opts(opts)),
             {:ok, %BlobLocation{hash: ^hash} = location} <- BlobLocation.cast(value) do
          {:cont, {:ok, [%{key: key, location: location, mod_revision: revision} | records]}}
        else
          {:ok, nil} -> {:cont, {:ok, records}}
          {:ok, %BlobLocation{}} -> {:halt, {:error, :invalid_blob_location}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, records} ->
          {:ok, Enum.sort_by(records, & &1.location.node_id)}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @spec mark_unhealthy(binary(), binary(), :suspect | :unavailable, term(), keyword()) ::
          :ok | {:error, term()}
  def mark_unhealthy(hash, node_id, state, reason, opts \\ [])
      when state in [:suspect, :unavailable] do
    update(hash, node_id, {:unhealthy, state, reason}, opts, @max_attempts)
  end

  @spec mark_ready(binary(), binary(), pos_integer(), non_neg_integer(), keyword()) ::
          :ok | {:error, term()}
  def mark_ready(hash, node_id, generation, size, opts \\ []) do
    opts =
      opts
      |> Keyword.put(:repair_hash, hash)
      |> Keyword.put(:repair_node_id, node_id)

    update(hash, node_id, {:ready, generation, size}, opts, @max_attempts)
  end

  @spec mark_draining(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def mark_draining(hash, node_id, opts \\ []) when is_binary(hash) and is_binary(node_id) do
    update(hash, node_id, :draining, opts, @max_attempts)
  end

  @doc """
  Revalidates a prepared cleanup fence immediately before physical deletion.

  The draining location stores the job token and the retained replica/topology
  snapshot established by `mark_draining/3`. This check fails closed if any of
  those records changed or the durable job lease is no longer live.
  """
  @spec authorize_cleanup(
          binary(),
          binary(),
          pos_integer(),
          binary(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          keyword()
        ) :: :ok | {:error, term()}
  def authorize_cleanup(
        hash,
        node_id,
        target_generation,
        job_id,
        owner_node,
        owner_generation,
        fencing_token,
        opts \\ []
      ) do
    backend = backend(opts)
    key = Keys.blob_location(hash, node_id)
    job_key = Keys.job(job_id)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    lease_ms = Keyword.get(opts, :cleanup_lease_ms, 30_000)

    with {:ok, %{value: value, mod_revision: revision}} <-
           required_location(backend, key, opts),
         {:ok, location} <- BlobLocation.cast(value),
         :ok <-
           validate_cleanup_location(
             location,
             target_generation,
             job_id,
             owner_node,
             owner_generation,
             fencing_token
           ),
         {:ok, %{value: job, mod_revision: job_revision}} <-
           required_location(backend, job_key, opts),
         :ok <-
           validate_cleanup_job(
             job,
             owner_node,
             owner_generation,
             fencing_token,
             now_ms
           ) do
      renewed_job =
        Map.put(
          job,
          :lease_until_ms,
          max(field(job, :lease_until_ms, 0), now_ms + lease_ms)
        )

      spec = %{
        compare:
          [
            {:mod_revision, key, :==, revision},
            {:field, key, [:state], :==, location.state},
            {:field, key, [:node_generation], :==, target_generation},
            {:field, key, [:cleanup_job_id], :==, job_id},
            {:field, key, [:cleanup_owner_node], :==, owner_node},
            {:field, key, [:cleanup_owner_generation], :==, owner_generation},
            {:field, key, [:cleanup_fencing_token], :==, fencing_token},
            {:mod_revision, job_key, :==, job_revision}
          ] ++
            cleanup_retained_compares(hash, location.cleanup_retained) ++
            cleanup_desired_compares(location.cleanup_desired) ++
            cleanup_descriptor_compares(
              hash,
              location.cleanup_descriptor_revision,
              location.cleanup_replication_factor
            ) ++
            cleanup_job_compares(
              job_id,
              owner_node,
              owner_generation,
              fencing_token,
              now_ms
            ),
        success: [
          {:put, job_key, renewed_job, %{}},
          {:put, key, Map.from_struct(%{location | state: :deleting}), %{}}
        ],
        failure: []
      }

      case backend.transaction(spec, write_opts(opts)) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> {:error, :stale_cleanup_fence}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :blob_location_not_found} -> {:error, :stale_cleanup_fence}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Removes a draining location only while all supplied retained locations remain
  checksum-ready and the optional durable-job fence is still live.
  """
  @spec retire(binary(), binary(), [record()], keyword()) :: :ok | {:error, term()}
  def retire(hash, node_id, retained, opts \\ [])
      when is_binary(hash) and is_binary(node_id) and is_list(retained) do
    retire(hash, node_id, retained, opts, @max_attempts)
  end

  @doc """
  Removes the local location and releases its GC lock in one transaction.

  Callers must invoke this only after the physical representation was removed
  successfully. The generation and lock token comparisons prevent an old
  collector from deleting metadata for newly published bytes.
  """
  @spec remove_after_gc(binary(), binary(), pos_integer(), binary(), keyword()) ::
          :ok | {:error, term()}
  def remove_after_gc(hash, node_id, generation, lock_token, opts \\ []) do
    backend = backend(opts)
    location_key = Keys.blob_location(hash, node_id)
    lock_key = Keys.gc_lock(hash)

    with {:ok, location} <- backend.get(location_key, read_opts(opts)),
         {:ok, lock} <- backend.get(lock_key, read_opts(opts)),
         :ok <- validate_gc_lock(lock, hash, node_id, generation, lock_token) do
      compare =
        [
          revision_compare(lock_key, lock),
          {:field, lock_key, [:token], :==, lock_token},
          {:field, lock_key, [:node_generation], :==, generation}
        ] ++ gc_location_compares(location_key, location, generation)

      success =
        gc_location_delete(location_key, location) ++
          [{:delete, {:key, lock_key}, %{}}]

      spec = %{compare: compare, success: success, failure: []}
      attempt_key = "blob-gc-retire:" <> fingerprint({hash, node_id, generation, lock_token})

      case backend.transaction(spec, Keyword.put(write_opts(opts), :idempotency_key, attempt_key)) do
        {:ok, %{succeeded: true}} ->
          :ok

        {:ok, %{succeeded: false}} ->
          {:error, :stale_gc_lock}

        {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
          case backend.resolve_transaction(attempt_key, write_opts(opts)) do
            {:ok, %{succeeded: true}} -> :ok
            _other -> {:error, :unknown_gc_retire_outcome}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp update(_hash, _node_id, _change, _opts, 0), do: {:error, :location_compare_failed}

  defp update(hash, node_id, change, opts, attempts_left) do
    key = Keys.blob_location(hash, node_id)

    with {:ok, observed} <- backend(opts).get(key, read_opts(opts)),
         {:ok, current} <- current_location(observed, hash, node_id),
         {:ok, updated, operation} <- updated_location(current, change, observed, opts) do
      if current == updated do
        :ok
      else
        success =
          [{:put, key, Map.from_struct(updated), %{}}] ++
            operation_write(operation)

        compare =
          [revision_compare(key, observed)] ++
            node_compares(node_id, opts) ++
            retained_compares(hash, Keyword.get(opts, :retained_locations, [])) ++
            desired_member_compares(opts) ++
            descriptor_compares(hash, Keyword.get(opts, :descriptor_record)) ++
            job_compares(opts) ++ operation_compare(operation)

        spec = %{compare: compare, success: success, failure: []}
        attempt_key = attempt_key(key, observed, change)
        transaction_opts = Keyword.put(write_opts(opts), :idempotency_key, attempt_key)

        case backend(opts).transaction(spec, transaction_opts) do
          {:ok, %{succeeded: true}} ->
            :ok

          {:ok, %{succeeded: false}} ->
            update(hash, node_id, change, opts, attempts_left - 1)

          {:error, reason} when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
            resolve_ambiguous(
              hash,
              node_id,
              change,
              attempt_key,
              opts,
              attempts_left
            )

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  defp resolve_ambiguous(hash, node_id, change, attempt_key, opts, attempts_left) do
    case backend(opts).resolve_transaction(attempt_key, write_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        :ok

      {:ok, _not_committed} ->
        update(hash, node_id, change, opts, attempts_left - 1)

      {:error, _reason} ->
        update(hash, node_id, change, opts, attempts_left - 1)
    end
  end

  defp current_location(nil, _hash, _node_id), do: {:ok, nil}

  defp current_location(%{value: value}, hash, node_id) do
    case BlobLocation.cast(value) do
      {:ok, %BlobLocation{hash: ^hash, node_id: ^node_id} = location} -> {:ok, location}
      {:ok, _other} -> {:error, :invalid_blob_location}
      {:error, reason} -> {:error, reason}
    end
  end

  defp updated_location(nil, {:unhealthy, _state, _reason}, _observed, _opts),
    do: {:error, :blob_location_not_found}

  defp updated_location(nil, :draining, _observed, _opts),
    do: {:error, :blob_location_not_found}

  defp updated_location(
         %BlobLocation{state: :deleting},
         {:unhealthy, _state, _reason},
         _observed,
         _opts
       ),
       do: {:error, :cleanup_in_progress}

  defp updated_location(
         %BlobLocation{state: :deleting},
         {:ready, _generation, _size},
         _observed,
         _opts
       ),
       do: {:error, :cleanup_in_progress}

  defp updated_location(current, {:unhealthy, state, reason}, observed, opts) do
    if current.state == state and current.last_error == reason do
      {:ok, current, nil}
    else
      now = timestamp(opts)

      updated = %{
        current
        | state: state,
          last_error: reason,
          updated_at: now,
          cleanup_job_id: nil,
          cleanup_owner_node: nil,
          cleanup_owner_generation: nil,
          cleanup_fencing_token: nil,
          cleanup_retained: nil,
          cleanup_desired: nil,
          cleanup_descriptor_revision: nil,
          cleanup_replication_factor: nil
      }

      operation_id =
        "read-repair-" <>
          fingerprint({
            current.hash,
            current.node_id,
            observed.mod_revision,
            state
          })

      event = %{
        id: fingerprint({operation_id, :repair_blob}),
        kind: :repair_blob,
        state: :pending,
        payload: %{
          hash: current.hash,
          target_node_id: current.node_id,
          source_node_ids: []
        }
      }

      operation = %{
        schema: 2,
        operation_id: operation_id,
        request_fingerprint: fingerprint({current.hash, current.node_id, state}),
        result: %{kind: :repair_enqueued, hash: current.hash, node_id: current.node_id},
        events: [event],
        committed_at: now
      }

      {:ok, updated, {operation_id, operation}}
    end
  end

  defp updated_location(nil, {:ready, generation, size}, _observed, opts) do
    now = timestamp(opts)
    hash = Keyword.fetch!(opts, :repair_hash)
    node_id = Keyword.fetch!(opts, :repair_node_id)

    {:ok,
     %BlobLocation{
       hash: hash,
       node_id: node_id,
       node_generation: generation,
       state: :ready,
       size: size,
       verified_at: now,
       updated_at: now
     }, nil}
  end

  defp updated_location(current, {:ready, generation, size}, _observed, opts) do
    updated = %{
      current
      | state: :ready,
        node_generation: generation,
        size: size,
        verified_at: timestamp(opts),
        updated_at: timestamp(opts),
        last_error: nil,
        cleanup_job_id: nil,
        cleanup_owner_node: nil,
        cleanup_owner_generation: nil,
        cleanup_fencing_token: nil,
        cleanup_retained: nil,
        cleanup_desired: nil,
        cleanup_descriptor_revision: nil,
        cleanup_replication_factor: nil
    }

    {:ok, updated, nil}
  end

  defp updated_location(current, :draining, _observed, opts) do
    cleanup = cleanup_fields(opts)
    state = if current.state == :deleting, do: :deleting, else: :draining

    {:ok,
     %{
       current
       | state: state,
         updated_at: timestamp(opts),
         last_error: nil,
         cleanup_job_id: cleanup.job_id,
         cleanup_owner_node: cleanup.owner_node,
         cleanup_owner_generation: cleanup.owner_generation,
         cleanup_fencing_token: cleanup.fencing_token,
         cleanup_retained: cleanup.retained,
         cleanup_desired: cleanup.desired,
         cleanup_descriptor_revision: cleanup.descriptor_revision,
         cleanup_replication_factor: cleanup.replication_factor
     }, nil}
  end

  defp retire(_hash, _node_id, _retained, _opts, 0),
    do: {:error, :location_compare_failed}

  defp retire(hash, node_id, retained, opts, attempts_left) do
    backend = backend(opts)
    key = Keys.blob_location(hash, node_id)

    case backend.get(key, read_opts(opts)) do
      {:ok, nil} ->
        :ok

      {:ok, %{value: value} = observed} ->
        with {:ok, %BlobLocation{state: :deleting} = location} <- BlobLocation.cast(value) do
          spec = %{
            compare:
              [revision_compare(key, observed)] ++
                cleanup_record_compares(key, location, opts) ++
                retained_compares(hash, retained) ++
                desired_member_compares(opts) ++
                cleanup_descriptor_compares(
                  hash,
                  location.cleanup_descriptor_revision,
                  location.cleanup_replication_factor
                ) ++ job_compares(opts),
            success: [{:delete, {:key, key}, %{}}],
            failure: []
          }

          attempt_key = "blob-retire:" <> fingerprint({key, observed.mod_revision, spec.compare})
          transaction_opts = Keyword.put(write_opts(opts), :idempotency_key, attempt_key)

          case backend.transaction(spec, transaction_opts) do
            {:ok, %{succeeded: true}} ->
              :ok

            {:ok, %{succeeded: false}} ->
              retire(hash, node_id, retained, opts, attempts_left - 1)

            {:error, reason}
            when reason in [:timeout, :unknown, :cluster_not_ready, :no_leader] ->
              case backend.resolve_transaction(attempt_key, write_opts(opts)) do
                {:ok, %{succeeded: true}} ->
                  :ok

                _other ->
                  retire(hash, node_id, retained, opts, attempts_left - 1)
              end

            {:error, reason} ->
              {:error, reason}
          end
        else
          {:ok, _location} -> {:error, :location_not_deleting}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp operation_compare(nil), do: []

  defp operation_compare({operation_id, _operation}),
    do: [{:exists, Keys.outbox(operation_id), :==, false}]

  defp operation_write(nil), do: []

  defp operation_write({operation_id, operation}),
    do: [{:put, Keys.outbox(operation_id), operation, %{}}]

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}

  defp revision_compare(key, %{mod_revision: revision}),
    do: {:mod_revision, key, :==, revision}

  defp validate_gc_lock(
         %{value: lock},
         hash,
         node_id,
         generation,
         token
       ) do
    if Map.get(lock, :hash) == hash and Map.get(lock, :node_id) == node_id and
         Map.get(lock, :node_generation) == generation and Map.get(lock, :token) == token,
       do: :ok,
       else: {:error, :stale_gc_lock}
  end

  defp validate_gc_lock(_lock, _hash, _node_id, _generation, _token),
    do: {:error, :gc_lock_not_found}

  defp gc_location_compares(_key, nil, _generation), do: []

  defp gc_location_compares(key, %{mod_revision: revision}, generation) do
    [
      {:mod_revision, key, :==, revision},
      {:field, key, [:node_generation], :==, generation}
    ]
  end

  defp gc_location_delete(_key, nil), do: []
  defp gc_location_delete(key, _location), do: [{:delete, {:key, key}, %{}}]

  defp required_location(backend, key, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, :blob_location_not_found}
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:error, reason}
    end
  end

  defp node_compares(node_id, opts) do
    case Keyword.get(opts, :node_record) do
      %{node: %{node_id: ^node_id, generation: node_generation}, mod_revision: revision} ->
        key = Keys.cluster_node(node_id)
        expected_generation = Keyword.get(opts, :expected_generation, node_generation)

        [
          {:mod_revision, key, :==, revision},
          {:field, key, [:generation], :==, expected_generation}
        ] ++ eligible_node_compares(key, opts)

      _ ->
        []
    end
  end

  defp eligible_node_compares(key, opts) do
    if Keyword.get(opts, :require_eligible_node, false) do
      [
        {:field, key, [:role], :==, :data},
        {:field, key, [:enabled], :==, true},
        {:field, key, [:draining], :==, false}
      ]
    else
      []
    end
  end

  defp retained_compares(hash, retained) do
    Enum.flat_map(retained, fn %{location: location} ->
      key = Keys.blob_location(hash, location.node_id)

      [
        {:field, key, [:state], :==, :ready},
        {:field, key, [:node_generation], :==, location.node_generation},
        {:field, key, [:size], :==, location.size}
      ]
    end)
  end

  defp cleanup_retained_compares(_hash, nil), do: []

  defp cleanup_retained_compares(hash, retained) when is_list(retained) do
    Enum.flat_map(retained, fn retained ->
      key = Keys.blob_location(hash, retained.node_id)

      [
        {:field, key, [:state], :==, :ready},
        {:field, key, [:node_generation], :==, retained.node_generation},
        {:field, key, [:size], :==, retained.size}
      ]
    end)
  end

  defp desired_member_compares(opts) do
    opts
    |> Keyword.get(:desired_members, [])
    |> Enum.flat_map(fn %{node: node, mod_revision: revision} ->
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

  defp descriptor_compares(_hash, nil), do: []

  defp descriptor_compares(hash, %{descriptor: descriptor, mod_revision: revision}) do
    key = Keys.blob(hash)

    [
      {:mod_revision, key, :==, revision},
      {:field, key, [:desired_replication_factor], :==, descriptor.desired_replication_factor}
    ]
  end

  defp cleanup_desired_compares(nil), do: []

  defp cleanup_desired_compares(desired) when is_list(desired) do
    Enum.flat_map(desired, fn member ->
      key = Keys.cluster_node(member.node_id)

      [
        {:mod_revision, key, :==, member.mod_revision},
        {:field, key, [:generation], :==, member.generation},
        {:field, key, [:role], :==, :data},
        {:field, key, [:enabled], :==, true},
        {:field, key, [:draining], :==, false}
      ]
    end)
  end

  defp cleanup_descriptor_compares(_hash, nil, nil), do: []

  defp cleanup_descriptor_compares(hash, revision, replication_factor)
       when is_integer(revision) and is_integer(replication_factor) do
    key = Keys.blob(hash)

    [
      {:mod_revision, key, :==, revision},
      {:field, key, [:desired_replication_factor], :==, replication_factor}
    ]
  end

  defp job_compares(opts) do
    case Keyword.get(opts, :job_fence) do
      %{
        job_id: job_id,
        owner_node: owner_node,
        owner_generation: owner_generation,
        fencing_token: fencing_token
      } ->
        key = Keys.job(job_id)
        now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

        [
          {:field, key, [:state], :==, :running},
          {:field, key, [:owner_node], :==, owner_node},
          {:field, key, [:owner_generation], :==, owner_generation},
          {:field, key, [:fencing_token], :==, fencing_token},
          {:field, key, [:lease_until_ms], :>, now_ms}
        ]

      _other ->
        []
    end
  end

  defp cleanup_job_compares(
         job_id,
         owner_node,
         owner_generation,
         fencing_token,
         now_ms
       ) do
    key = Keys.job(job_id)

    [
      {:field, key, [:state], :==, :running},
      {:field, key, [:owner_node], :==, owner_node},
      {:field, key, [:owner_generation], :==, owner_generation},
      {:field, key, [:fencing_token], :==, fencing_token},
      {:field, key, [:lease_until_ms], :>, now_ms}
    ]
  end

  defp validate_cleanup_job(job, owner_node, owner_generation, fencing_token, now_ms) do
    if field(job, :state, nil) == :running and
         field(job, :owner_node, nil) == owner_node and
         field(job, :owner_generation, nil) == owner_generation and
         field(job, :fencing_token, nil) == fencing_token and
         field(job, :lease_until_ms, 0) > now_ms,
       do: :ok,
       else: {:error, :stale_cleanup_fence}
  end

  defp validate_cleanup_location(
         %BlobLocation{
           state: state,
           node_generation: target_generation,
           cleanup_job_id: job_id,
           cleanup_owner_node: owner_node,
           cleanup_owner_generation: owner_generation,
           cleanup_fencing_token: fencing_token
         },
         target_generation,
         job_id,
         owner_node,
         owner_generation,
         fencing_token
       )
       when state in [:draining, :deleting],
       do: :ok

  defp validate_cleanup_location(
         %BlobLocation{},
         _target_generation,
         _job_id,
         _owner_node,
         _owner_generation,
         _fencing_token
       ),
       do: {:error, :stale_cleanup_fence}

  defp cleanup_record_compares(key, location, opts) do
    case Keyword.get(opts, :job_fence) do
      %{
        job_id: job_id,
        owner_node: owner_node,
        owner_generation: owner_generation,
        fencing_token: fencing_token
      } ->
        [
          {:field, key, [:cleanup_job_id], :==, job_id},
          {:field, key, [:cleanup_owner_node], :==, owner_node},
          {:field, key, [:cleanup_owner_generation], :==, owner_generation},
          {:field, key, [:cleanup_fencing_token], :==, fencing_token},
          {:field, key, [:cleanup_retained], :==, location.cleanup_retained},
          {:field, key, [:cleanup_desired], :==, location.cleanup_desired},
          {:field, key, [:cleanup_descriptor_revision], :==,
           location.cleanup_descriptor_revision},
          {:field, key, [:cleanup_replication_factor], :==, location.cleanup_replication_factor}
        ]

      _other ->
        []
    end
  end

  defp cleanup_fields(opts) do
    job = Keyword.get(opts, :job_fence)

    %{
      job_id: job && job.job_id,
      owner_node: job && job.owner_node,
      owner_generation: job && job.owner_generation,
      fencing_token: job && job.fencing_token,
      retained:
        Enum.map(Keyword.get(opts, :retained_locations, []), fn %{location: location} ->
          %{
            node_id: location.node_id,
            node_generation: location.node_generation,
            size: location.size
          }
        end),
      desired:
        Enum.map(Keyword.get(opts, :desired_members, []), fn %{node: node} = member ->
          %{
            node_id: node.node_id,
            generation: node.generation,
            mod_revision: member.mod_revision
          }
        end),
      descriptor_revision:
        opts
        |> Keyword.get(:descriptor_record, %{})
        |> Map.get(:mod_revision),
      replication_factor:
        opts
        |> Keyword.get(:descriptor_record, %{})
        |> Map.get(:descriptor)
        |> case do
          %{desired_replication_factor: replication_factor} -> replication_factor
          _other -> nil
        end
    }
  end

  defp attempt_key(key, observed, change),
    do: "blob-location:" <> fingerprint({key, observed_revision(observed), change})

  defp observed_revision(nil), do: 0
  defp observed_revision(%{mod_revision: revision}), do: revision

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp timestamp(opts) do
    Keyword.get_lazy(opts, :timestamp, fn ->
      DateTime.utc_now() |> DateTime.to_iso8601()
    end)
  end

  defp field(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp read_opts(opts),
    do:
      opts
      |> Keyword.take([:consistency, :timeout, :engine, :barrier])
      |> Keyword.put_new(:consistency, :strong)

  defp write_opts(opts), do: Keyword.take(opts, [:timeout, :engine, :barrier])
  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
end
