defmodule ExStorageService.CrossClusterReplication.StatusTest do
  use ExUnit.Case, async: false

  alias ExStorageService.CrossClusterReplication.Status
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.Metrics

  defmodule JobStoreDouble do
    def list_page(cursor, limit, opts) do
      Agent.get_and_update(opts[:engine], fn state ->
        {Map.fetch!(state.pages, cursor), %{state | calls: [{cursor, limit} | state.calls]}}
      end)
    end
  end

  setup do
    :ok = Metrics.setup()
    :ok = Metrics.reset()

    {:ok, engine} = Agent.start_link(fn -> %{pages: %{}, calls: []} end)
    %{engine: engine, opts: [job_store: JobStoreDouble, engine: engine, now_ms: 10_000]}
  end

  test "counts only unfinished external jobs and reports age without sensitive labels", fixture do
    pages = %{
      nil =>
        {:ok,
         %{
           jobs: [
             job("private-key", :cross_cluster_put, :pending, 1_000),
             job("delete", :cross_cluster_delete, :running, 4_000),
             job("maintenance", :repair_blob, :pending, 0)
           ],
           next_cursor: "next"
         }},
      "next" =>
        {:ok,
         %{
           jobs: [
             job("failed", :cross_cluster_put, :failed, 500),
             job("done", :cross_cluster_put, :completed, 0)
           ],
           next_cursor: nil
         }}
    }

    Agent.update(fixture.engine, &%{&1 | pages: pages})

    assert {:ok, status} = Status.snapshot(fixture.opts)
    assert status.complete
    assert status.pending == 1 and status.running == 1 and status.failed == 1
    assert status.oldest_age_ms == 9_500
    assert status.scanned == 5
    output = Metrics.format_metrics()
    assert output =~ ~s(replication_backlog{state="pending"} 1)
    assert output =~ "replication_oldest_job_age_seconds 9.5"
    assert output =~ "replication_backlog_scan_complete 1"
    refute output =~ "private-key"
    refute output =~ "private-token"

    partial =
      {:ok,
       %{
         jobs:
           for(
             number <- 1..3,
             do: job("partial-#{number}", :cross_cluster_put, :pending, 0)
           ),
         next_cursor: "unread"
       }}

    Agent.update(fixture.engine, &%{&1 | pages: %{nil => partial}})

    assert {:ok, %{complete: false, pending: 3}} =
             Status.snapshot(fixture.opts ++ [max_pages: 1])

    retained = Metrics.format_metrics()
    assert retained =~ ~s(replication_backlog{state="pending"} 1)
    assert retained =~ "replication_oldest_job_age_seconds 9.5"
    assert retained =~ "replication_backlog_scan_complete 0"
  end

  test "bounded partial scans never publish an incomplete count as a total", fixture do
    pages = %{
      nil =>
        {:ok,
         %{jobs: [job("pending", :cross_cluster_put, :pending, 1_000)], next_cursor: "unread"}}
    }

    Agent.update(fixture.engine, &%{&1 | pages: pages})

    assert {:ok, %{complete: false, pending: 1, next_cursor: "unread"}} =
             Status.snapshot(fixture.opts ++ [page_size: 1, max_pages: 1])

    assert Agent.get(fixture.engine, & &1.calls) == [{nil, 1}]
    output = Metrics.format_metrics()
    assert output =~ "replication_backlog_scan_complete 0"
    refute output =~ ~s(replication_backlog{state="pending"})
  end

  test "scan failures stay errors and invalid bounds do not access metadata", fixture do
    Agent.update(fixture.engine, &%{&1 | pages: %{nil => {:error, :timeout}}})
    assert {:error, :timeout} = Status.snapshot(fixture.opts)
    assert Metrics.format_metrics() =~ "replication_backlog_scan_complete 0"
    Agent.update(fixture.engine, &%{&1 | calls: []})
    assert {:error, :invalid_scan_options} = Status.snapshot(fixture.opts ++ [max_pages: 0])
    assert Agent.get(fixture.engine, & &1.calls) == []
  end

  defp job(id, kind, state, created) do
    {:ok, job} =
      Job.new(
        "operation",
        %{id: id, kind: kind, payload: %{key: "private-key", token: "private-token"}},
        created
      )

    %{job | state: state}
  end
end
