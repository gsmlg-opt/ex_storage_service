defmodule ExStorageService.Metadata.Outbox do
  @moduledoc """
  Materializes committed outbox events into durable, independently leased jobs.

  The job and the event's dispatched state are written in one Concord
  transaction. Replaying an operation is therefore safe and returns the
  existing job instead of creating duplicate work.
  """

  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.Models.Job

  @default_page_size 100
  @default_max_attempts 8
  @ambiguous_errors [:timeout, :unknown, :cluster_not_ready, :no_leader]

  @spec enqueue_legacy([map()], keyword()) :: :ok | {:error, term()}
  def enqueue_legacy(events, opts \\ []) when is_list(events) do
    operation_id =
      Keyword.get_lazy(opts, :operation_id, fn ->
        "legacy-outbox-" <>
          Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      end)

    operation_key = Keys.outbox(operation_id)
    now_ms = now_ms(opts)
    request_fingerprint = fingerprint(events)

    operation = %{
      schema: 2,
      operation_id: operation_id,
      request_fingerprint: request_fingerprint,
      result: %{operation_id: operation_id, kind: :legacy_outbox},
      events: events,
      committed_at_ms: now_ms
    }

    spec = %{
      compare: [{:exists, operation_key, :==, false}],
      success: [{:put, operation_key, operation, %{}}],
      failure: []
    }

    backend = backend(opts)

    transaction_id = idempotency_key(:enqueue, operation_key, 0, request_fingerprint)

    case backend.transaction(spec, transaction_opts(opts, transaction_id)) do
      {:ok, _result} ->
        finish_legacy_enqueue(backend, operation_id, request_fingerprint, opts)

      {:error, reason} when reason in @ambiguous_errors ->
        case backend.resolve_transaction(transaction_id, read_opts(opts)) do
          {:ok, _result} ->
            finish_legacy_enqueue(backend, operation_id, request_fingerprint, opts)

          {:error, resolve_reason} ->
            {:error, resolve_reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec materialize(binary(), keyword()) :: {:ok, [Job.t()]} | {:error, term()}
  def materialize(operation_id, opts \\ []) when is_binary(operation_id) do
    do_materialize(operation_id, opts, max_attempts(opts))
  end

  @spec materialize_page(binary() | nil, pos_integer(), keyword()) ::
          {:ok, %{jobs: [Job.t()], next_cursor: binary() | nil}} | {:error, term()}
  def materialize_page(
        cursor \\ nil,
        limit \\ @default_page_size,
        opts \\ []
      )
      when (is_binary(cursor) or is_nil(cursor)) and is_integer(limit) and limit > 0 do
    backend = backend(opts)

    with {:ok, page} <-
           backend.list_page(Keys.outbox_prefix(), cursor, limit, read_opts(opts)),
         {:ok, jobs} <- materialize_entries(page.entries, opts) do
      {:ok, %{jobs: jobs, next_cursor: page.next_cursor}}
    end
  end

  defp do_materialize(_operation_id, _opts, 0), do: {:error, :compare_retry_exhausted}

  defp do_materialize(operation_id, opts, attempts_left) do
    backend = backend(opts)
    operation_key = Keys.outbox(operation_id)

    case backend.get(operation_key, read_opts(opts)) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, %{value: operation, mod_revision: revision}} when is_map(operation) ->
        materialize_operation(
          backend,
          operation_key,
          operation,
          revision,
          opts,
          attempts_left
        )

      {:ok, _invalid} ->
        {:error, :invalid_outbox_record}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp materialize_operation(backend, operation_key, operation, revision, opts, attempts_left) do
    pending_events =
      operation
      |> field(:events, [])
      |> Enum.filter(&(normalize_state(field(&1, :state)) == :pending))

    case pending_events do
      [] ->
        load_materialized_jobs(backend, operation, opts)

      [event | _rest] ->
        materialize_event(
          backend,
          operation_key,
          operation,
          revision,
          event,
          opts,
          attempts_left
        )
    end
  end

  defp materialize_event(
         backend,
         operation_key,
         operation,
         revision,
         event,
         opts,
         attempts_left
       ) do
    operation_id = field(operation, :operation_id)
    now_ms = now_ms(opts)

    with {:ok, job} <-
           Job.new(operation_id, event, now_ms, max_attempts: job_max_attempts(opts)) do
      job_key = Keys.job(job.job_id)
      updated_operation = mark_dispatched(operation, job.job_id, now_ms)

      spec = %{
        compare: [
          {:mod_revision, operation_key, :==, revision},
          {:exists, job_key, :==, false}
        ],
        success: [
          {:put, job_key, Job.to_map(job), %{}},
          {:put, operation_key, updated_operation, %{}}
        ],
        failure: []
      }

      idempotency_key = idempotency_key(:materialize, operation_key, revision, job.job_id)

      case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
        {:ok, %{succeeded: true}} ->
          do_materialize(operation_id, opts, max_attempts(opts))

        {:ok, %{succeeded: false}} ->
          do_materialize(operation_id, opts, attempts_left - 1)

        {:error, reason} when reason in @ambiguous_errors ->
          resolve_ambiguous(
            backend,
            idempotency_key,
            operation_id,
            operation_key,
            updated_operation,
            spec,
            opts,
            attempts_left - 1
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp resolve_ambiguous(
         backend,
         idempotency_key,
         operation_id,
         operation_key,
         updated_operation,
         spec,
         opts,
         attempts_left
       ) do
    case backend.resolve_transaction(idempotency_key, read_opts(opts)) do
      {:ok, %{succeeded: true}} ->
        do_materialize(operation_id, opts, max_attempts(opts))

      {:ok, %{succeeded: false}} ->
        do_materialize(operation_id, opts, attempts_left)

      {:error, :not_found} ->
        replay_ambiguous(
          backend,
          idempotency_key,
          operation_id,
          operation_key,
          updated_operation,
          spec,
          opts,
          attempts_left
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replay_ambiguous(
         backend,
         idempotency_key,
         operation_id,
         operation_key,
         updated_operation,
         spec,
         opts,
         attempts_left
       ) do
    case backend.get(operation_key, read_opts(opts)) do
      {:ok, %{value: ^updated_operation}} ->
        do_materialize(operation_id, opts, max_attempts(opts))

      {:ok, _other} ->
        case backend.transaction(spec, transaction_opts(opts, idempotency_key)) do
          {:ok, %{succeeded: true}} ->
            do_materialize(operation_id, opts, max_attempts(opts))

          {:ok, %{succeeded: false}} ->
            do_materialize(operation_id, opts, attempts_left)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp materialize_entries(entries, opts) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, jobs} ->
      operation_id = field(entry.value, :operation_id)

      case materialize(operation_id, opts) do
        {:ok, materialized} -> {:cont, {:ok, materialized ++ jobs}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      error -> error
    end)
  end

  defp finish_legacy_enqueue(backend, operation_id, request_fingerprint, opts) do
    case backend.get(Keys.outbox(operation_id), read_opts(opts)) do
      {:ok, %{value: operation}} ->
        if field(operation, :request_fingerprint) == request_fingerprint do
          case materialize(operation_id, opts) do
            {:ok, _jobs} -> :ok
            {:error, reason} -> {:error, reason}
          end
        else
          {:error, :operation_id_conflict}
        end

      {:ok, nil} ->
        {:error, :unknown_transaction_outcome}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_materialized_jobs(backend, operation, opts) do
    operation
    |> field(:events, [])
    |> Enum.filter(&(normalize_state(field(&1, :state)) == :dispatched))
    |> Enum.reduce_while({:ok, []}, fn event, {:ok, jobs} ->
      job_id = field(event, :job_id) || field(event, :id)

      case backend.get(Keys.job(job_id), read_opts(opts)) do
        {:ok, %{value: value}} ->
          case Job.cast(value) do
            {:ok, job} -> {:cont, {:ok, [job | jobs]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:ok, nil} ->
          {:halt, {:error, {:missing_materialized_job, job_id}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      error -> error
    end)
  end

  defp mark_dispatched(operation, job_id, now_ms) do
    events =
      operation
      |> field(:events, [])
      |> Enum.map(fn event ->
        if field(event, :id) == job_id do
          event
          |> plain_map()
          |> Map.put(:state, :dispatched)
          |> Map.put(:job_id, job_id)
          |> Map.put(:dispatched_at_ms, now_ms)
        else
          event
        end
      end)

    operation
    |> plain_map()
    |> Map.put(:events, events)
  end

  defp normalize_state("pending"), do: :pending
  defp normalize_state("dispatched"), do: :dispatched
  defp normalize_state(state), do: state

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp plain_map(%_{} = struct), do: Map.from_struct(struct)
  defp plain_map(map), do: map

  defp idempotency_key(action, operation_key, revision, job_id) do
    fingerprint({action, operation_key, revision, job_id})
  end

  defp fingerprint(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp backend(opts), do: Keyword.get(opts, :backend, ConcordBackend)
  defp max_attempts(opts), do: Keyword.get(opts, :max_attempts, @default_max_attempts)
  defp job_max_attempts(opts), do: Keyword.get(opts, :job_max_attempts, 3)

  defp now_ms(opts),
    do: Keyword.get_lazy(opts, :now_ms, fn -> System.system_time(:millisecond) end)

  defp read_opts(opts),
    do: Keyword.take(opts, [:consistency, :timeout, :engine, :barrier])

  defp transaction_opts(opts, idempotency_key) do
    opts
    |> Keyword.take([:timeout, :engine, :barrier])
    |> Keyword.put(:idempotency_key, idempotency_key)
  end
end
