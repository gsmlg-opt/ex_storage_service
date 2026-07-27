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

  @type record :: %{key: binary(), location: BlobLocation.t()}

  @spec list(binary(), keyword()) :: {:ok, [record()]} | {:error, term()}
  def list(hash, opts \\ []) when is_binary(hash) do
    with {:ok, entries} <-
           backend(opts).prefix_scan(Keys.blob_location_prefix(hash), read_opts(opts)) do
      entries
      |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, records} ->
        case BlobLocation.cast(value) do
          {:ok, %BlobLocation{hash: ^hash} = location} ->
            {:cont, {:ok, [%{key: key, location: location} | records]}}

          {:ok, _other_hash} ->
            {:halt, {:error, :invalid_blob_location}}

          {:error, reason} ->
            {:halt, {:error, reason}}
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
            node_compares(node_id, opts) ++ operation_compare(operation)

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

  defp updated_location(current, {:unhealthy, state, reason}, observed, opts) do
    if current.state == state and current.last_error == reason do
      {:ok, current, nil}
    else
      now = timestamp(opts)

      updated = %{
        current
        | state: state,
          last_error: reason,
          updated_at: now
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
        last_error: nil
    }

    {:ok, updated, nil}
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

  defp node_compares(node_id, opts) do
    case Keyword.get(opts, :node_record) do
      %{node: %{node_id: ^node_id, generation: node_generation}, mod_revision: revision} ->
        key = Keys.cluster_node(node_id)
        expected_generation = Keyword.get(opts, :expected_generation, node_generation)

        [
          {:mod_revision, key, :==, revision},
          {:field, key, [:generation], :==, expected_generation}
        ]

      _ ->
        []
    end
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

  defp read_opts(opts),
    do:
      opts
      |> Keyword.take([:consistency, :timeout, :engine, :barrier])
      |> Keyword.put_new(:consistency, :strong)

  defp write_opts(opts), do: Keyword.take(opts, [:timeout, :engine, :barrier])
  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
end
