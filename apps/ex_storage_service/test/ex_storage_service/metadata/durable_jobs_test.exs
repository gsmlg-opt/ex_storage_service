defmodule ExStorageService.Metadata.DurableJobsTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{JobStore, Keys, Outbox}
  alias ExStorageService.Metadata.Models.Job

  defmodule Barrier do
    use GenServer

    def start_link(participants), do: GenServer.start_link(__MODULE__, participants)
    def wait(server), do: GenServer.call(server, :wait, :infinity)

    @impl true
    def init(participants), do: {:ok, %{remaining: participants, waiting: []}}

    @impl true
    def handle_call(:wait, _from, %{remaining: 0} = state), do: {:reply, :ok, state}

    def handle_call(:wait, _from, %{remaining: 1, waiting: waiting} = state) do
      Enum.each(waiting, &GenServer.reply(&1, :ok))
      {:reply, :ok, %{state | remaining: 0, waiting: []}}
    end

    def handle_call(:wait, from, state) do
      {:noreply, %{state | remaining: state.remaining - 1, waiting: [from | state.waiting]}}
    end
  end

  defmodule Backend do
    @behaviour ExStorageService.Metadata.Backend

    def start_link(opts \\ []) do
      Agent.start_link(fn ->
        %{
          records: %{},
          revision: 0,
          results: %{},
          transactions: [],
          timeout_after_commit: Keyword.get(opts, :timeout_after_commit, 0)
        }
      end)
    end

    def seed(engine, key, value) do
      Agent.update(engine, &put_record(&1, key, value))
    end

    def timeout_next(engine) do
      Agent.update(engine, &%{&1 | timeout_after_commit: &1.timeout_after_commit + 1})
    end

    def records(engine), do: Agent.get(engine, & &1.records)
    def transactions(engine), do: Agent.get(engine, &Enum.reverse(&1.transactions))

    @impl true
    def get(key, opts) do
      result = Agent.get(engine(opts), &read(&1, key))

      if barrier = Keyword.get(opts, :barrier) do
        Barrier.wait(barrier)
      end

      result
    end

    @impl true
    def put(key, value, opts) do
      Agent.update(engine(opts), &put_record(&1, key, value))
    end

    @impl true
    def delete(key, opts) do
      Agent.update(engine(opts), fn state ->
        %{state | records: Map.delete(state.records, key), revision: state.revision + 1}
      end)
    end

    @impl true
    def get_all(opts) do
      entries =
        Agent.get(engine(opts), fn state ->
          Enum.map(state.records, fn {key, %{value: value}} -> {key, value} end)
        end)

      {:ok, entries}
    end

    @impl true
    def prefix_scan(prefix, opts) do
      with {:ok, entries} <- get_all(opts) do
        {:ok,
         entries
         |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
         |> Enum.sort()}
      end
    end

    @impl true
    def scan(prefix, opts), do: prefix_scan(prefix, opts)

    @impl true
    def list_page(prefix, cursor, limit, opts) do
      records =
        Agent.get(engine(opts), fn state ->
          state.records
          |> Enum.filter(fn {key, _record} ->
            String.starts_with?(key, prefix) and (is_nil(cursor) or key > cursor)
          end)
          |> Enum.sort_by(&elem(&1, 0))
          |> Enum.take(limit + 1)
        end)

      page_records = Enum.take(records, limit)
      has_more = length(records) > limit

      entries =
        Enum.map(page_records, fn {key, record} ->
          %{key: key, value: record.value, mod_revision: record.mod_revision}
        end)

      next_cursor =
        if has_more do
          page_records |> List.last() |> elem(0)
        end

      {:ok, %{entries: entries, next_cursor: next_cursor}}
    end

    @impl true
    def transaction(spec, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        idempotency_key = Keyword.get(opts, :idempotency_key)
        state = %{state | transactions: [spec | state.transactions]}

        case Map.get(state.results, idempotency_key) do
          %{spec: ^spec, result: result} ->
            {{:ok, result}, state}

          %{spec: _other} ->
            {{:error, :idempotency_conflict}, state}

          nil ->
            execute_transaction(state, idempotency_key, spec)
        end
      end)
    end

    @impl true
    def resolve_transaction(idempotency_key, opts) do
      Agent.get(engine(opts), fn state ->
        case Map.get(state.results, idempotency_key) do
          %{result: result} -> {:ok, result}
          nil -> {:error, :not_found}
        end
      end)
    end

    @impl true
    def resolve_operation(key, opts), do: get(key, opts)

    defp execute_transaction(state, idempotency_key, spec) do
      succeeded = Enum.all?(spec.compare, &compare?(&1, state.records))
      result = %{succeeded: succeeded}

      state =
        if succeeded do
          Enum.reduce(spec.success, state, &apply_operation/2)
        else
          Enum.reduce(spec.failure, state, &apply_operation/2)
        end

      state = %{
        state
        | results: Map.put(state.results, idempotency_key, %{spec: spec, result: result})
      }

      if succeeded and state.timeout_after_commit > 0 do
        {{:error, :timeout}, %{state | timeout_after_commit: state.timeout_after_commit - 1}}
      else
        {{:ok, result}, state}
      end
    end

    defp compare?({:exists, key, op, expected}, records),
      do: compare(Map.has_key?(records, key), op, expected)

    defp compare?({:mod_revision, key, op, expected}, records) do
      actual = records |> Map.get(key, %{mod_revision: 0}) |> Map.fetch!(:mod_revision)
      compare(actual, op, expected)
    end

    defp compare?({:field, key, path, op, expected}, records) do
      case Map.get(records, key) do
        %{value: value} -> compare(get_in(value, path), op, expected)
        nil -> false
      end
    end

    defp compare(actual, :==, expected), do: actual == expected
    defp compare(actual, :!=, expected), do: actual != expected
    defp compare(actual, :>, expected), do: actual > expected
    defp compare(actual, :>=, expected), do: actual >= expected
    defp compare(actual, :<, expected), do: actual < expected
    defp compare(actual, :<=, expected), do: actual <= expected

    defp apply_operation({:put, key, value, _opts}, state), do: put_record(state, key, value)
    defp apply_operation({:get, _selector, _opts}, state), do: state

    defp put_record(state, key, value) do
      revision = state.revision + 1

      %{
        state
        | revision: revision,
          records: Map.put(state.records, key, %{value: value, mod_revision: revision})
      }
    end

    defp read(state, key) do
      case Map.get(state.records, key) do
        nil -> {:ok, nil}
        record -> {:ok, record}
      end
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  test "materializes an outbox event exactly once" do
    {:ok, backend} = Backend.start_link()
    operation = operation("op-1", [event("event-1", :repair_blob)])
    Backend.seed(backend, Keys.outbox("op-1"), operation)

    assert {:ok, [%Job{job_id: "event-1", state: :pending}]} =
             Outbox.materialize("op-1", backend: Backend, engine: backend, now_ms: 100)

    assert {:ok, [%Job{job_id: "event-1"}]} =
             Outbox.materialize("op-1", backend: Backend, engine: backend, now_ms: 200)

    records = Backend.records(backend)
    assert Map.has_key?(records, Keys.job("event-1"))

    assert %{value: %{events: [%{state: :dispatched, job_id: "event-1"}]}} =
             records[Keys.outbox("op-1")]

    assert 1 ==
             Backend.transactions(backend)
             |> Enum.count(fn transaction ->
               Enum.any?(transaction.success, fn
                 {:put, key, _value, _opts} -> key == Keys.job("event-1")
                 _other -> false
               end)
             end)
  end

  test "ambiguous materialization timeout resolves by the transaction id" do
    {:ok, backend} = Backend.start_link(timeout_after_commit: 1)
    Backend.seed(backend, Keys.outbox("op-timeout"), operation("op-timeout", [event("job-t")]))

    assert {:ok, [%Job{job_id: "job-t"}]} =
             Outbox.materialize("op-timeout",
               backend: Backend,
               engine: backend,
               now_ms: 10
             )
  end

  test "legacy enqueue creates one operation and materializes its event immediately" do
    {:ok, backend} = Backend.start_link()

    assert :ok =
             Outbox.enqueue_legacy([event("legacy-event")],
               backend: Backend,
               engine: backend,
               operation_id: "legacy-operation",
               now_ms: 10
             )

    assert {:ok, %Job{job_id: "legacy-event", operation_id: "legacy-operation"}} =
             JobStore.get("legacy-event", backend: Backend, engine: backend)

    assert :ok =
             Outbox.enqueue_legacy([event("legacy-event")],
               backend: Backend,
               engine: backend,
               operation_id: "legacy-operation",
               now_ms: 10
             )

    assert 1 ==
             Backend.records(backend)
             |> Map.keys()
             |> Enum.count(&String.starts_with?(&1, Keys.job_prefix()))
  end

  test "two workers racing at a deterministic read barrier claim one live lease" do
    {:ok, backend} = Backend.start_link()
    {:ok, barrier} = Barrier.start_link(2)
    seed_job(backend, "race-job", 0)

    tasks =
      for owner <- ["worker-a", "worker-b"] do
        Task.async(fn ->
          JobStore.claim("race-job", owner, 1_000, 100,
            backend: Backend,
            engine: backend,
            barrier: barrier
          )
        end)
      end

    results = Task.await_many(tasks)

    assert 1 == Enum.count(results, &match?({:ok, %Job{state: :running}}, &1))
    assert 1 == Enum.count(results, &match?({:error, {:not_claimable, _job}}, &1))

    assert {:ok, %Job{state: :running, fencing_token: 1, lease_until_ms: 1_100}} =
             JobStore.get("race-job", backend: Backend, engine: backend)
  end

  test "expired lease takeover fences the stale owner" do
    {:ok, backend} = Backend.start_link()
    seed_job(backend, "takeover", 0)

    assert {:ok, first} =
             JobStore.claim("takeover", "worker-a", 100, 20,
               backend: Backend,
               engine: backend
             )

    assert first.fencing_token == 1

    assert {:ok, second} =
             JobStore.claim("takeover", "worker-b", 121, 20,
               backend: Backend,
               engine: backend
             )

    assert second.fencing_token == 2

    assert {:error, {:stale_lease, %Job{owner_node: "worker-b", fencing_token: 2}}} =
             JobStore.complete("takeover", "worker-a", 1, 122,
               backend: Backend,
               engine: backend
             )

    assert {:ok, %Job{state: :completed, fencing_token: 2}} =
             JobStore.complete("takeover", "worker-b", 2, 122,
               backend: Backend,
               engine: backend
             )
  end

  test "renewal and completion require the live fence and resolve ambiguous outcomes" do
    {:ok, backend} = Backend.start_link()
    seed_job(backend, "renew-job", 0)

    assert {:ok, claimed} =
             JobStore.claim("renew-job", "worker", 10, 20,
               backend: Backend,
               engine: backend
             )

    assert claimed.fencing_token == 1

    Backend.timeout_next(backend)

    assert {:ok, renewed} =
             JobStore.renew("renew-job", "worker", 1, 20, 30,
               backend: Backend,
               engine: backend
             )

    assert renewed.lease_until_ms == 50

    assert {:error, {:stale_lease, _current}} =
             JobStore.complete("renew-job", "worker", 0, 21,
               backend: Backend,
               engine: backend
             )
  end

  test "failure persists retry backoff and eventually reaches terminal failure" do
    {:ok, backend} = Backend.start_link()
    seed_job(backend, "retry-job", 0, max_attempts: 2)

    assert {:ok, first} =
             JobStore.claim("retry-job", "worker", 0, 100,
               backend: Backend,
               engine: backend
             )

    assert {:ok, pending} =
             JobStore.fail("retry-job", "worker", first.fencing_token, :unavailable, 10,
               backend: Backend,
               engine: backend,
               retry_base_ms: 50
             )

    assert pending.state == :pending
    assert pending.next_attempt_at_ms == 60

    assert {:error, {:not_claimable, _job}} =
             JobStore.claim("retry-job", "worker", 59, 100,
               backend: Backend,
               engine: backend
             )

    assert {:ok, second} =
             JobStore.claim("retry-job", "worker", 60, 100,
               backend: Backend,
               engine: backend
             )

    assert {:ok, failed} =
             JobStore.fail("retry-job", "worker", second.fencing_token, :still_unavailable, 70,
               backend: Backend,
               engine: backend
             )

    assert failed.state == :failed
    assert failed.last_error == ":still_unavailable"
  end

  test "job pagination is bounded and cursor based" do
    {:ok, backend} = Backend.start_link()
    Enum.each(["a", "b", "c"], &seed_job(backend, &1, 0))

    assert {:ok, %{jobs: [%Job{job_id: "a"}, %Job{job_id: "b"}], next_cursor: cursor}} =
             JobStore.list_page(nil, 2, backend: Backend, engine: backend)

    assert is_binary(cursor)

    assert {:ok, %{jobs: [%Job{job_id: "c"}], next_cursor: nil}} =
             JobStore.list_page(cursor, 2, backend: Backend, engine: backend)
  end

  test "job keys encode arbitrary identifiers" do
    job_id = "repair:/雪:"
    encoded = String.replace_prefix(Keys.job(job_id), Keys.job_prefix(), "")
    assert {:ok, ^job_id} = Keys.decode_component(encoded)
  end

  defp seed_job(backend, job_id, now_ms, opts \\ []) do
    {:ok, job} =
      Job.new(
        "operation-#{job_id}",
        event(job_id),
        now_ms,
        max_attempts: Keyword.get(opts, :max_attempts, 3)
      )

    Backend.seed(backend, Keys.job(job_id), Job.to_map(job))
  end

  defp operation(operation_id, events) do
    %{
      schema: 2,
      operation_id: operation_id,
      request_fingerprint: "request",
      result: %{operation_id: operation_id},
      events: events
    }
  end

  defp event(id, kind \\ :repair_blob) do
    %{
      id: id,
      kind: kind,
      state: :pending,
      payload: %{hash: "hash", source_node_ids: ["data-a"]}
    }
  end
end
