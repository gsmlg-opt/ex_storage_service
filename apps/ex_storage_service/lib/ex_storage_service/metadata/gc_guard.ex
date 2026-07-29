defmodule ExStorageService.Metadata.GCGuard do
  @moduledoc """
  Fences local physical deletion against concurrent object publication.

  Writers atomically extend a per-hash guard while requiring the deletion lock
  to be absent. Collectors atomically claim the inverse condition. This closes
  the metadata-scan-to-file-delete race that a grace period alone cannot close.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys

  @spec claim(binary(), binary(), pos_integer(), non_neg_integer(), pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def claim(hash, node_id, node_generation, now_ms, lease_ms, opts \\ []) do
    backend = backend(opts)
    guard_key = Keys.gc_guard(hash)
    lock_key = Keys.gc_lock(hash)

    with {:ok, guard} <- backend.get(guard_key, read_opts(opts)),
         :ok <- ensure_unprotected(guard, now_ms),
         {:ok, lock} <- backend.get(lock_key, read_opts(opts)),
         :ok <- ensure_lock_available(lock, now_ms) do
      token = fingerprint({hash, node_id, node_generation, now_ms, observed_revision(lock)})

      value = %{
        schema: 2,
        hash: hash,
        node_id: node_id,
        node_generation: node_generation,
        token: token,
        lease_until_ms: now_ms + lease_ms
      }

      spec = %{
        compare: [
          revision_compare(guard_key, guard),
          revision_compare(lock_key, lock)
        ],
        success: [{:put, lock_key, value, %{}}],
        failure: []
      }

      case backend.transaction(
             spec,
             transaction_opts(opts, fingerprint({:gc_lock, lock_key, spec}))
           ) do
        {:ok, %{succeeded: true}} -> {:ok, value}
        {:ok, %{succeeded: false}} -> {:error, :gc_guard_changed}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Extends a collector lease while proving that the caller still owns the lock.

  Collectors renew immediately before a physical rename or delete boundary so
  an expired owner cannot mutate bytes after a writer has replaced its lock.
  """
  @spec renew(binary(), binary(), non_neg_integer(), pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def renew(hash, token, now_ms, lease_ms, opts \\ []) do
    backend = backend(opts)
    key = Keys.gc_lock(hash)

    with {:ok, %{value: lock, mod_revision: revision}} <-
           required_lock(backend, key, opts),
         true <- field(lock, :hash, nil) == hash and field(lock, :token, nil) == token do
      updated = Map.put(lock, :lease_until_ms, now_ms + lease_ms)

      spec = %{
        compare: [
          {:mod_revision, key, :==, revision},
          {:field, key, [:hash], :==, hash},
          {:field, key, [:token], :==, token}
        ],
        success: [{:put, key, updated, %{}}],
        failure: []
      }

      case backend.transaction(
             spec,
             transaction_opts(opts, fingerprint({:gc_lock_renew, key, token, spec}))
           ) do
        {:ok, %{succeeded: true}} -> {:ok, updated}
        {:ok, %{succeeded: false}} -> {:error, :stale_gc_lock}
        {:error, reason} -> {:error, reason}
      end
    else
      false -> {:error, :stale_gc_lock}
      {:error, :gc_lock_not_found} -> {:error, :stale_gc_lock}
      {:error, _reason} = error -> error
    end
  end

  @spec release(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def release(hash, token, opts \\ []) do
    backend = backend(opts)
    key = Keys.gc_lock(hash)

    with {:ok, %{value: %{token: ^token}, mod_revision: revision}} <-
           required_lock(backend, key, opts) do
      spec = %{
        compare: [
          {:mod_revision, key, :==, revision},
          {:field, key, [:token], :==, token}
        ],
        success: [{:delete, {:key, key}, %{}}],
        failure: []
      }

      case backend.transaction(
             spec,
             transaction_opts(opts, fingerprint({:gc_unlock, key, token}))
           ) do
        {:ok, %{succeeded: true}} -> :ok
        {:ok, %{succeeded: false}} -> {:error, :stale_gc_lock}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, _other} -> {:error, :stale_gc_lock}
      {:error, _reason} = error -> error
    end
  end

  @spec lock_compare(binary(), binary()) :: term()
  def lock_compare(hash, token),
    do: {:field, Keys.gc_lock(hash), [:token], :==, token}

  defp ensure_unprotected(nil, _now_ms), do: :ok

  defp ensure_unprotected(%{value: guard}, now_ms) do
    if field(guard, :protected_until_ms, 0) < now_ms,
      do: :ok,
      else: {:error, :blob_protected}
  end

  defp ensure_lock_available(nil, _now_ms), do: :ok

  defp ensure_lock_available(%{value: lock}, now_ms) do
    if field(lock, :lease_until_ms, 0) < now_ms,
      do: :ok,
      else: {:error, :gc_already_running}
  end

  defp required_lock(backend, key, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, :gc_lock_not_found}
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:error, reason}
    end
  end

  defp revision_compare(key, nil), do: {:mod_revision, key, :==, 0}

  defp revision_compare(key, %{mod_revision: revision}),
    do: {:mod_revision, key, :==, revision}

  defp observed_revision(nil), do: 0
  defp observed_revision(%{mod_revision: revision}), do: revision

  defp field(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, idempotency_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, idempotency_key)
  end
end
