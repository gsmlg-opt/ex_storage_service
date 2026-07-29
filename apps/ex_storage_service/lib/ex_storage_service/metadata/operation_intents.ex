defmodule ExStorageService.Metadata.OperationIntents do
  @moduledoc """
  Atomic pre-commit intent and per-hash GC protection.

  Opening an intent and extending the hash guard share one Concord
  transaction. The transaction also requires the local GC lock to be absent,
  so a new writer cannot race a collector that has already fenced deletion.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.OperationIntent

  @max_attempts 4
  @ambiguous_errors [:timeout, :unknown, :cluster_not_ready, :no_leader]

  @spec open(binary(), binary(), non_neg_integer(), binary(), pos_integer(), keyword()) ::
          {:ok, OperationIntent.t()} | {:error, term()}
  def open(operation_id, hash, size, node_id, node_generation, opts \\ []) do
    now_ms = now_ms(opts)
    protection_ms = Keyword.get(opts, :protection_ms, 86_400_000)

    with {:ok, intent} <-
           OperationIntent.new(
             operation_id,
             hash,
             size,
             node_id,
             node_generation,
             now_ms + protection_ms,
             now_ms
           ) do
      persist_open(intent, opts, @max_attempts)
    end
  end

  @spec transition(binary(), :unknown | :committed | :aborted, keyword()) ::
          {:ok, OperationIntent.t()} | {:error, term()}
  def transition(operation_id, state, opts \\ [])
      when state in [:unknown, :committed, :aborted] do
    key = Keys.operation_intent(operation_id)
    backend = backend(opts)

    with {:ok, %{value: value, mod_revision: revision}} <-
           required_record(backend, key, opts),
         {:ok, intent} <- OperationIntent.cast(value) do
      updated = %{intent | state: state, updated_at_ms: now_ms(opts)}

      spec = %{
        compare: [{:mod_revision, key, :==, revision}],
        success: [{:put, key, OperationIntent.to_map(updated), %{}}],
        failure: []
      }

      commit_transition(backend, spec, updated, opts)
    end
  end

  @doc """
  Returns transaction fragments that close an existing intent atomically with
  the object or multipart metadata publication.
  """
  @spec commit_operations(binary(), binary(), keyword()) ::
          {:ok, %{compare: [term()], success: [term()]}} | {:error, term()}
  def commit_operations(operation_id, hash, opts \\ []) do
    key = Keys.operation_intent(operation_id)

    with {:ok, %{value: value, mod_revision: revision}} <-
           required_record(backend(opts), key, opts),
         {:ok, %OperationIntent{hash: ^hash} = intent} <- OperationIntent.cast(value) do
      updated = %{intent | state: :committed, updated_at_ms: now_ms(opts)}

      {:ok,
       %{
         compare: [{:mod_revision, key, :==, revision}],
         success: [{:put, key, OperationIntent.to_map(updated), %{}}]
       }}
    else
      {:ok, %OperationIntent{}} -> {:error, :operation_intent_conflict}
      {:error, _reason} = error -> error
    end
  end

  @spec protected_hashes_page(binary() | nil, pos_integer(), keyword()) ::
          {:ok, %{hashes: MapSet.t(binary()), next_cursor: binary() | nil}} | {:error, term()}
  def protected_hashes_page(cursor \\ nil, limit \\ 100, opts \\ []) do
    with {:ok, page} <-
           backend(opts).list_page(
             Keys.operation_intent_prefix(),
             cursor,
             limit,
             read_opts(opts)
           ),
         {:ok, intents} <- cast_entries(page.entries) do
      now_ms = now_ms(opts)

      hashes =
        intents
        |> Enum.filter(&OperationIntent.protected?(&1, now_ms))
        |> MapSet.new(& &1.hash)

      {:ok, %{hashes: hashes, next_cursor: page.next_cursor}}
    end
  end

  defp persist_open(_intent, _opts, 0), do: {:error, :operation_intent_compare_failed}

  defp persist_open(intent, opts, attempts_left) do
    backend = backend(opts)
    intent_key = Keys.operation_intent(intent.operation_id)
    guard_key = Keys.gc_guard(intent.hash)
    lock_key = Keys.gc_lock(intent.hash)

    with {:ok, observed_intent} <- backend.get(intent_key, read_opts(opts)),
         {:ok, observed_guard} <- backend.get(guard_key, read_opts(opts)),
         {:ok, observed_lock} <- backend.get(lock_key, read_opts(opts)),
         :ok <- ensure_lock_expired(observed_lock, now_ms(opts)),
         :continue <- existing_intent(observed_intent, intent) do
      guard = %{
        schema: 2,
        hash: intent.hash,
        protected_until_ms: max(guard_deadline(observed_guard), intent.protected_until_ms),
        operation_id: intent.operation_id,
        updated_at_ms: now_ms(opts)
      }

      spec = %{
        compare: [
          revision_compare(intent_key, observed_intent),
          revision_compare(guard_key, observed_guard),
          revision_compare(lock_key, observed_lock)
        ],
        success:
          expired_lock_delete(lock_key, observed_lock) ++
            [
              {:put, intent_key, OperationIntent.to_map(intent), %{}},
              {:put, guard_key, guard, %{}}
            ],
        failure: []
      }

      idempotency_key = fingerprint({:operation_intent, intent.operation_id, spec})

      case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
        {:ok, %{succeeded: true}} ->
          {:ok, intent}

        {:ok, %{succeeded: false}} ->
          persist_open(intent, opts, attempts_left - 1)

        {:error, reason} when reason in @ambiguous_errors ->
          resolve_open(backend, idempotency_key, intent, opts, attempts_left - 1)

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:return, existing} -> {:ok, existing}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_open(backend, idempotency_key, intent, opts, attempts_left) do
    case backend.resolve_transaction(idempotency_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} -> {:ok, intent}
      {:ok, %{succeeded: false}} -> persist_open(intent, opts, attempts_left)
      {:error, :not_found} -> persist_open(intent, opts, attempts_left)
      {:error, reason} -> {:error, reason}
    end
  end

  defp commit_transition(backend, spec, updated, opts) do
    idempotency_key = fingerprint({:operation_intent_transition, spec})

    case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
      {:ok, %{succeeded: true}} -> {:ok, updated}
      {:ok, %{succeeded: false}} -> {:error, :operation_intent_compare_failed}
      {:error, reason} -> {:error, reason}
    end
  end

  defp existing_intent(nil, _intent), do: :continue

  defp existing_intent(%{value: value}, intent) do
    case OperationIntent.cast(value) do
      {:ok, %OperationIntent{operation_id: id, hash: hash, size: size} = existing}
      when id == intent.operation_id and hash == intent.hash and size == intent.size ->
        if existing.protected_until_ms >= intent.protected_until_ms,
          do: {:return, existing},
          else: :continue

      {:ok, _other} ->
        {:error, :operation_intent_conflict}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp required_record(backend, key, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, :operation_intent_not_found}
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:error, reason}
    end
  end

  defp cast_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, intents} ->
      case OperationIntent.cast(entry.value) do
        {:ok, intent} -> {:cont, {:ok, [intent | intents]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, intents} -> {:ok, Enum.reverse(intents)}
      error -> error
    end)
  end

  defp guard_deadline(nil), do: 0

  defp guard_deadline(%{value: guard}),
    do: Map.get(guard, :protected_until_ms, Map.get(guard, "protected_until_ms", 0))

  defp ensure_lock_expired(nil, _now_ms), do: :ok

  defp ensure_lock_expired(%{value: lock}, now_ms) do
    deadline = Map.get(lock, :lease_until_ms, Map.get(lock, "lease_until_ms", 0))
    if is_integer(deadline) and deadline < now_ms, do: :ok, else: {:error, :gc_lock_active}
  end

  defp expired_lock_delete(_lock_key, nil), do: []
  defp expired_lock_delete(lock_key, _lock), do: [{:delete, {:key, lock_key}, %{}}]

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}

  defp revision_compare(key, %{mod_revision: revision}),
    do: {:mod_revision, key, :==, revision}

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp now_ms(opts), do: Keyword.get(opts, :now_ms, System.system_time(:millisecond))

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, idempotency_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, idempotency_key)
  end
end
