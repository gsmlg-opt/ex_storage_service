defmodule ExStorageService.Metadata.JobStore do
  @moduledoc """
  Durable job persistence with atomic lease and fencing transitions.

  A worker may mutate a running job only while it owns the current fencing
  token and an unexpired lease. Expired work can be reclaimed, which increments
  the fencing token and makes every stale worker transition fail.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.Job

  @default_max_attempts 8
  @ambiguous_errors [:timeout, :unknown, :cluster_not_ready, :no_leader]

  @spec get(binary(), keyword()) :: {:ok, Job.t()} | {:error, :not_found | term()}
  def get(job_id, opts \\ []) when is_binary(job_id) do
    case backend(opts).get(Keys.job(job_id), read_opts(opts)) do
      {:ok, %{value: value}} -> Job.cast(value)
      {:ok, nil} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec list_page(binary() | nil, pos_integer(), keyword()) ::
          {:ok, %{jobs: [Job.t()], next_cursor: binary() | nil}} | {:error, term()}
  def list_page(cursor \\ nil, limit \\ 100, opts \\ [])
      when (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and limit > 0 do
    with {:ok, page} <-
           backend(opts).list_page(Keys.job_prefix(), cursor, limit, read_opts(opts)),
         {:ok, jobs} <- cast_jobs(page.entries) do
      {:ok, %{jobs: jobs, next_cursor: page.next_cursor}}
    end
  end

  @spec claim(binary(), binary(), non_neg_integer(), pos_integer(), keyword()) ::
          {:ok, Job.t()} | {:error, term()}
  def claim(job_id, owner_node, now_ms, lease_ms, opts \\ [])
      when is_binary(job_id) and is_binary(owner_node) and owner_node != "" and
             is_integer(now_ms) and now_ms >= 0 and is_integer(lease_ms) and lease_ms > 0 do
    transition(job_id, opts, fn job, revision ->
      claim_transition(job, revision, owner_node, now_ms, lease_ms, opts)
    end)
  end

  @spec renew(binary(), binary(), non_neg_integer(), non_neg_integer(), pos_integer(), keyword()) ::
          {:ok, Job.t()} | {:error, term()}
  def renew(job_id, owner_node, fencing_token, now_ms, lease_ms, opts \\ [])
      when is_binary(job_id) and is_binary(owner_node) and is_integer(fencing_token) and
             fencing_token >= 0 and is_integer(now_ms) and now_ms >= 0 and
             is_integer(lease_ms) and lease_ms > 0 do
    transition(job_id, opts, fn job, revision ->
      with :ok <- ensure_live_owner(job, owner_node, fencing_token, now_ms, opts) do
        updated = %{job | lease_until_ms: now_ms + lease_ms, updated_at_ms: now_ms}

        {:ok, revision, owner_compares(job, owner_node, fencing_token, now_ms, opts), updated}
      end
    end)
  end

  @spec complete(binary(), binary(), non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, Job.t()} | {:error, term()}
  def complete(job_id, owner_node, fencing_token, now_ms, opts \\ [])
      when is_binary(job_id) and is_binary(owner_node) and is_integer(fencing_token) and
             fencing_token >= 0 and is_integer(now_ms) and now_ms >= 0 do
    transition(job_id, opts, fn job, revision ->
      with :ok <- ensure_live_owner(job, owner_node, fencing_token, now_ms, opts) do
        updated = %{
          job
          | state: :completed,
            lease_until_ms: 0,
            completed_at_ms: now_ms,
            updated_at_ms: now_ms,
            last_error: nil
        }

        {:ok, revision, owner_compares(job, owner_node, fencing_token, now_ms, opts), updated}
      end
    end)
  end

  @spec fail(binary(), binary(), non_neg_integer(), term(), non_neg_integer(), keyword()) ::
          {:ok, Job.t()} | {:error, term()}
  def fail(job_id, owner_node, fencing_token, reason, now_ms, opts \\ [])
      when is_binary(job_id) and is_binary(owner_node) and is_integer(fencing_token) and
             fencing_token >= 0 and is_integer(now_ms) and now_ms >= 0 do
    transition(job_id, opts, fn job, revision ->
      with :ok <- ensure_live_owner(job, owner_node, fencing_token, now_ms, opts) do
        updated = failure_job(job, reason, now_ms, opts)

        {:ok, revision, owner_compares(job, owner_node, fencing_token, now_ms, opts), updated}
      end
    end)
  end

  defp transition(job_id, opts, transition_builder, attempts_left \\ nil)

  defp transition(job_id, opts, transition_builder, nil) do
    transition(job_id, opts, transition_builder, max_attempts(opts))
  end

  defp transition(_job_id, _opts, _transition_builder, 0),
    do: {:error, :compare_retry_exhausted}

  defp transition(job_id, opts, transition_builder, attempts_left) do
    backend = backend(opts)
    key = Keys.job(job_id)

    with {:ok, record} <- read_record(backend, key, opts),
         {:ok, job} <- Job.cast(record.value),
         {:ok, revision, compares, updated} <- transition_builder.(job, record.mod_revision) do
      spec = %{
        compare: [{:mod_revision, key, :==, revision} | compares],
        success: [{:put, key, Job.to_map(updated), %{}}],
        failure: [{:get, {:key, key}, %{}}]
      }

      idempotency_key = idempotency_key(key, revision, updated)

      case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
        {:ok, %{succeeded: true}} ->
          {:ok, updated}

        {:ok, %{succeeded: false}} ->
          transition(job_id, opts, transition_builder, attempts_left - 1)

        {:error, reason} when reason in @ambiguous_errors ->
          resolve_ambiguous(
            backend,
            key,
            updated,
            spec,
            idempotency_key,
            opts,
            attempts_left - 1
          )

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :not_found} -> {:error, :not_found}
      {:error, :not_claimable} -> current_error(backend, key, :not_claimable, opts)
      {:error, :stale_lease} -> current_error(backend, key, :stale_lease, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp claim_transition(job, revision, owner_node, now_ms, lease_ms, opts) do
    generation = Keyword.get(opts, :owner_generation)

    cond do
      job.state == :pending and job.next_attempt_at_ms <= now_ms ->
        updated = claim_job(job, owner_node, generation, now_ms, lease_ms)

        compares = [
          {:field, Keys.job(job.job_id), [:state], :==, :pending},
          {:field, Keys.job(job.job_id), [:next_attempt_at_ms], :<=, now_ms}
        ]

        {:ok, revision, compares, updated}

      job.state == :running and job.lease_until_ms < now_ms ->
        updated = claim_job(job, owner_node, generation, now_ms, lease_ms)

        compares = [
          {:field, Keys.job(job.job_id), [:state], :==, :running},
          {:field, Keys.job(job.job_id), [:lease_until_ms], :<, now_ms}
        ]

        {:ok, revision, compares, updated}

      true ->
        {:error, :not_claimable}
    end
  end

  defp claim_job(job, owner_node, generation, now_ms, lease_ms) do
    %{
      job
      | state: :running,
        owner_node: owner_node,
        owner_generation: generation,
        lease_until_ms: now_ms + lease_ms,
        fencing_token: job.fencing_token + 1,
        attempts: job.attempts + 1,
        updated_at_ms: now_ms,
        completed_at_ms: nil
    }
  end

  defp owner_compares(job, owner_node, fencing_token, now_ms, opts) do
    key = Keys.job(job.job_id)

    [
      {:field, key, [:state], :==, :running},
      {:field, key, [:owner_node], :==, owner_node},
      {:field, key, [:owner_generation], :==, Keyword.get(opts, :owner_generation)},
      {:field, key, [:fencing_token], :==, fencing_token},
      {:field, key, [:lease_until_ms], :>, now_ms}
    ]
  end

  defp ensure_live_owner(job, owner_node, fencing_token, now_ms, opts) do
    if job.state == :running and job.owner_node == owner_node and
         job.owner_generation == Keyword.get(opts, :owner_generation) and
         job.fencing_token == fencing_token and job.lease_until_ms > now_ms do
      :ok
    else
      {:error, :stale_lease}
    end
  end

  defp failure_job(job, reason, now_ms, opts) do
    base = %{
      job
      | owner_node: nil,
        owner_generation: nil,
        lease_until_ms: 0,
        completed_at_ms: nil,
        updated_at_ms: now_ms,
        last_error: inspect(reason)
    }

    if job.attempts >= job.max_attempts do
      %{base | state: :failed, next_attempt_at_ms: now_ms}
    else
      %{base | state: :pending, next_attempt_at_ms: now_ms + retry_delay_ms(job, opts)}
    end
  end

  defp retry_delay_ms(job, opts) do
    base = Keyword.get(opts, :retry_base_ms, 1_000)
    cap = Keyword.get(opts, :retry_max_ms, 60_000)
    min(base * Integer.pow(2, max(job.attempts - 1, 0)), cap)
  end

  defp resolve_ambiguous(
         backend,
         key,
         updated,
         spec,
         idempotency_key,
         opts,
         attempts_left
       ) do
    case backend.resolve_transaction(idempotency_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        {:ok, updated}

      {:ok, %{succeeded: false}} ->
        {:error, :stale_lease}

      {:error, :not_found} ->
        replay_ambiguous(
          backend,
          key,
          updated,
          spec,
          idempotency_key,
          opts,
          attempts_left
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replay_ambiguous(backend, key, updated, spec, idempotency_key, opts, attempts_left) do
    expected = Job.to_map(updated)

    case backend.get(key, read_opts(opts)) do
      {:ok, %{value: value}} ->
        if value == expected do
          {:ok, updated}
        else
          replay_missing_result(
            backend,
            key,
            updated,
            spec,
            idempotency_key,
            opts,
            attempts_left
          )
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replay_missing_result(
         backend,
         key,
         updated,
         spec,
         idempotency_key,
         opts,
         attempts_left
       )
       when attempts_left > 0 do
    case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
      {:ok, %{succeeded: true}} -> {:ok, updated}
      {:ok, %{succeeded: false}} -> current_error(backend, key, :stale_lease, opts)
      {:error, reason} -> {:error, reason}
    end
  end

  defp replay_missing_result(
         _backend,
         _key,
         _updated,
         _spec,
         _idempotency_key,
         _opts,
         _attempts_left
       ),
       do: {:error, :compare_retry_exhausted}

  defp read_record(backend, key, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:error, reason}
    end
  end

  defp current_error(backend, key, reason, opts) do
    case backend.get(key, read_opts(opts)) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, %{value: value}} -> {:error, {reason, job_or_value(value)}}
      {:error, read_reason} -> {:error, read_reason}
    end
  end

  defp cast_jobs(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, jobs} ->
      case Job.cast(entry.value) do
        {:ok, job} -> {:cont, {:ok, [job | jobs]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      error -> error
    end)
  end

  defp job_or_value(value) do
    case Job.cast(value) do
      {:ok, job} -> job
      {:error, _reason} -> value
    end
  end

  defp idempotency_key(key, revision, updated) do
    {key, revision, Job.to_map(updated)}
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp max_attempts(opts), do: Keyword.get(opts, :max_attempts, @default_max_attempts)

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, idempotency_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, idempotency_key)
  end
end
