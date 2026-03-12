defmodule ExStorageService.Replication.JobQueueTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Replication.JobQueue

  defp concord_ready? do
    case Concord.get("__test_health__") do
      {:ok, _} -> true
      _ -> false
    end
  end

  setup context do
    if context[:integration] do
      unless concord_ready?() do
        {:ok, skip: true}
      else
        # Clean up any leftover job keys
        case Concord.get_all() do
          {:ok, all} ->
            all
            |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "job:") end)
            |> Enum.each(fn {k, _v} -> Concord.delete(k) end)

          _ ->
            :ok
        end

        :ok
      end
    else
      :ok
    end
  end

  describe "enqueue/1" do
    @tag :integration
    test "enqueues a job and returns job_id", context do
      unless context[:skip] do
        assert {:ok, job_id} =
                 JobQueue.enqueue(
                   queue: :replication,
                   payload: %{action: :put, bucket: "test-bucket", key: "test-key"}
                 )

        assert is_binary(job_id)

        {:ok, job} = JobQueue.get_job(:replication, job_id)
        assert job.id == job_id
        assert job.queue == :replication
        assert job.status == :pending
        assert job.payload.action == :put
        assert job.attempts == 0
        assert job.max_attempts == 3
      end
    end

    @tag :integration
    test "enqueues with custom max_attempts", context do
      unless context[:skip] do
        assert {:ok, job_id} =
                 JobQueue.enqueue(
                   queue: :gc,
                   payload: %{action: :cleanup},
                   max_attempts: 5
                 )

        {:ok, job} = JobQueue.get_job(:gc, job_id)
        assert job.max_attempts == 5
        assert job.queue == :gc
      end
    end
  end

  describe "process_jobs/0" do
    @tag :integration
    test "processes pending jobs via poll", context do
      unless context[:skip] do
        {:ok, job_id} =
          JobQueue.enqueue(
            queue: :replication,
            payload: %{action: :put, bucket: "b", key: "k"}
          )

        JobQueue.process_jobs()

        assert eventually(fn ->
                 case JobQueue.get_job(:replication, job_id) do
                   {:ok, %{status: status}} when status in [:completed, :failed] -> true
                   _ -> false
                 end
               end)
      end
    end
  end

  describe "retry with backoff" do
    test "backoff_ms returns exponential values" do
      assert JobQueue.backoff_ms(1) == 1_000
      assert JobQueue.backoff_ms(2) == 2_000
      assert JobQueue.backoff_ms(3) == 4_000
    end
  end

  describe "dead letter queue" do
    @tag :integration
    test "list_dead_letter_jobs returns a list", context do
      unless context[:skip] do
        {:ok, dead_jobs} = JobQueue.list_dead_letter_jobs()
        assert is_list(dead_jobs)
      end
    end
  end

  describe "job persistence" do
    @tag :integration
    test "jobs are persisted in Concord", context do
      unless context[:skip] do
        {:ok, job_id} =
          JobQueue.enqueue(
            queue: :sync,
            payload: %{action: :put, bucket: "sync-test", key: "obj1"}
          )

        {:ok, data} = Concord.get("job:sync:#{job_id}")
        assert data != nil
        assert data[:id] == job_id
        assert data[:status] == :pending
        assert data[:queue] == :sync
      end
    end

    @tag :integration
    test "get_job returns not_found for missing jobs", context do
      unless context[:skip] do
        assert {:error, :not_found} = JobQueue.get_job(:replication, "nonexistent-id")
      end
    end
  end

  describe "Job struct" do
    test "has correct defaults" do
      job = %JobQueue.Job{}
      assert job.attempts == 0
      assert job.max_attempts == 3
      assert job.status == nil
      assert job.error == nil
    end
  end

  defp eventually(func, attempts \\ 20) do
    if func.() do
      true
    else
      if attempts > 0 do
        Process.sleep(100)
        eventually(func, attempts - 1)
      else
        false
      end
    end
  end
end
