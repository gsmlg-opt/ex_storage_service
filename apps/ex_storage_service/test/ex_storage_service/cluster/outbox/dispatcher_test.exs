defmodule ExStorageService.Cluster.Outbox.DispatcherTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.Outbox.Dispatcher
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.{Context, InstanceConfig}

  defmodule OutboxStub do
    def materialize_page(_cursor, _limit, _opts), do: {:ok, %{jobs: [], next_cursor: nil}}
  end

  defmodule JobStoreStub do
    def list_page(_cursor, _limit, opts) do
      jobs = Agent.get(opts[:engine], &Map.values(&1))
      {:ok, %{jobs: jobs, next_cursor: nil}}
    end

    def claim(job_id, owner, now_ms, lease_ms, opts) do
      Agent.get_and_update(opts[:engine], fn jobs ->
        job = Map.fetch!(jobs, job_id)

        if job.state == :pending do
          claimed = %{
            job
            | state: :running,
              owner_node: owner,
              owner_generation: opts[:owner_generation],
              lease_until_ms: now_ms + lease_ms,
              fencing_token: job.fencing_token + 1,
              attempts: job.attempts + 1
          }

          {{:ok, claimed}, Map.put(jobs, job_id, claimed)}
        else
          {{:error, {:not_claimable, job}}, jobs}
        end
      end)
    end

    def renew(job_id, _owner, _fence, now_ms, lease_ms, opts) do
      Agent.get_and_update(opts[:engine], fn jobs ->
        job = Map.fetch!(jobs, job_id)
        renewed = %{job | lease_until_ms: now_ms + lease_ms}
        {{:ok, renewed}, Map.put(jobs, job_id, renewed)}
      end)
    end

    def complete(job_id, _owner, _fence, now_ms, opts) do
      update(job_id, opts, fn job ->
        %{job | state: :completed, completed_at_ms: now_ms, lease_until_ms: 0}
      end)
    end

    def fail(job_id, _owner, _fence, reason, now_ms, opts) do
      update(job_id, opts, fn job ->
        %{job | state: :pending, last_error: inspect(reason), next_attempt_at_ms: now_ms}
      end)
    end

    defp update(job_id, opts, callback) do
      Agent.get_and_update(opts[:engine], fn jobs ->
        updated = callback.(Map.fetch!(jobs, job_id))
        {{:ok, updated}, Map.put(jobs, job_id, updated)}
      end)
    end
  end

  test "keeps handler execution within configured concurrency" do
    parent = self()
    {:ok, engine} = Agent.start_link(fn -> jobs(3) end)
    {:ok, tasks} = Task.Supervisor.start_link()
    {:ok, config} = InstanceConfig.new(auto_start: false, replica_concurrency: 2)
    context = Context.new(config)

    processor = fn job, _context ->
      send(parent, {:started, job.job_id, self()})

      receive do
        :release -> :ok
      end
    end

    {:ok, dispatcher} =
      start_supervised(
        {Dispatcher,
         context: context,
         task_supervisor: tasks,
         outbox: OutboxStub,
         job_store: JobStoreStub,
         processor: processor,
         metadata_opts: [engine: engine],
         poll_interval: 60_000,
         renew_interval: 60_000}
      )

    assert_receive {:started, first_id, first_pid}
    assert_receive {:started, second_id, second_pid}
    refute_receive {:started, _, _}, 50
    assert first_id != second_id

    send(first_pid, :release)
    send(second_pid, :release)
    assert eventually(fn -> completed_count(engine) == 2 end)

    Dispatcher.process_jobs(dispatcher)
    assert_receive {:started, third_id, third_pid}
    assert third_id not in [first_id, second_id]
    send(third_pid, :release)
    assert eventually(fn -> completed_count(engine) == 3 end)
  end

  defp jobs(count) do
    Map.new(1..count, fn index ->
      id = "job-#{index}"

      {:ok, job} =
        Job.new(
          "operation-#{index}",
          %{
            id: id,
            kind: :cross_cluster_put,
            state: :pending,
            payload: %{bucket: "bucket", key: "key-#{index}"}
          },
          0
        )

      {id, job}
    end)
  end

  defp completed_count(engine) do
    Agent.get(engine, fn jobs ->
      Enum.count(jobs, fn {_id, job} -> job.state == :completed end)
    end)
  end

  defp eventually(callback, attempts \\ 50)

  defp eventually(callback, attempts) when attempts > 0 do
    if callback.() do
      true
    else
      Process.sleep(10)
      eventually(callback, attempts - 1)
    end
  end

  defp eventually(callback, 0), do: callback.()
end
