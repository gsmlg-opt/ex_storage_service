defmodule ExStorageService.Cluster.Outbox.DispatcherMetricsTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Cluster.Outbox.Dispatcher
  alias ExStorageService.Metadata.{JobStore, Keys}
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.Metrics

  setup do
    :ok = Metrics.setup()
    :ok = Metrics.reset()
  end

  test "real durable completion, retry and rejected stale completion have distinct metrics" do
    # Future-dated private fixtures cannot be claimed by ordinary wall-clock
    # dispatchers. The dispatcher completion callback uses the real JobStore
    # and Concord transactions; only the clock and task-result delivery are controlled.
    now = System.system_time(:millisecond) + 86_400_000

    for outcome <- [:ok, :error, :stale] do
      id = "metrics-#{outcome}-#{System.unique_integer([:positive])}"

      {:ok, pending} =
        Job.new("operation-#{id}", %{id: id, kind: :cross_cluster_put, payload: %{}}, now)

      assert :ok = Concord.put(Keys.job(id), Job.to_map(pending))
      on_exit(fn -> Concord.delete(Keys.job(id)) end)
      assert {:ok, claimed} = JobStore.claim(id, "metrics-owner", now, 1000, owner_generation: 1)

      if outcome == :stale do
        assert {:ok, replacement} =
                 JobStore.claim(id, "new-owner", now + 1001, 1000, owner_generation: 2)

        assert replacement.fencing_token > claimed.fencing_token
      end

      ref = Process.monitor(self())

      state = %{
        running: %{ref => %{job: claimed}},
        job_store: JobStore,
        owner_node: "metrics-owner",
        owner_generation: 1,
        clock: fn -> now end,
        metadata_opts: []
      }

      result = if outcome == :error, do: {:error, :upstream_unavailable}, else: :ok
      assert {:noreply, %{running: running}} = Dispatcher.handle_info({ref, result}, state)
      assert running == %{}
      assert {:ok, persisted} = JobStore.get(id)

      case outcome do
        :ok ->
          assert persisted.state == :completed

        :error ->
          assert persisted.state == :pending
          assert persisted.last_error =~ "upstream_unavailable"

        :stale ->
          assert persisted.state == :running
          assert persisted.owner_node == "new-owner"
          assert persisted.fencing_token > claimed.fencing_token
      end
    end

    output = Metrics.format_metrics()

    assert output =~
             ~s(durable_job_transitions_total{execution="ok",kind="cross_cluster_put",transition="ok"} 1)

    assert output =~
             ~s(durable_job_transitions_total{execution="error",kind="cross_cluster_put",transition="ok"} 1)

    assert output =~
             ~s(durable_job_transitions_total{execution="ok",kind="cross_cluster_put",transition="stale"} 1)

    refute output =~ "metrics-owner"
    refute output =~ "new-owner"
    refute output =~ "operation-"
  end
end
