defmodule ExStorageService.MetricsTest do
  use ExUnit.Case, async: false

  alias ExStorageService.{Metrics, Telemetry}

  setup do
    :ok = Metrics.setup()
    :ok = Metrics.reset()
  end

  test "exports the required low-cardinality cluster metrics" do
    :telemetry.execute(
      [:ex_storage_service, :cluster, :blob_transport, :stop],
      %{duration: 10, bytes: 128},
      %{direction: :client, operation: :put_blob, hash: "not-a-label", peer: "node-a"}
    )

    :telemetry.execute(
      [:ex_storage_service, :cluster, :blob_transport, :stop],
      %{duration: 10, bytes: 64},
      %{direction: :client, operation: :open_blob, hash: "not-a-label", peer: "node-b"}
    )

    :telemetry.execute(
      [:ex_storage_service, :cluster, :blob_transport, :checksum_failure],
      %{count: 1, bytes: 64},
      %{hash: "not-a-label", request_id: "not-a-label"}
    )

    Telemetry.quorum_stop(1_000, %{configured_write_quorum: 2, achieved_replica_count: 2}, %{
      result: :ok
    })

    Telemetry.repair_backlog(%{pending: 3, running: 1, failed: 2, under_replicated: 4})
    Telemetry.lease_contention(:repair_blob, :owned)
    Telemetry.orphan_counts(%{candidates: 5, quarantined: 2, orphans: 7})

    output = Metrics.format_metrics()

    assert output =~ ~s(cluster_replica_bytes_total{direction="client"} 128)
    assert output =~ "cluster_remote_read_bytes_total 64"
    assert output =~ "cluster_checksum_failures_total 1"
    assert output =~ ~s(cluster_repair_backlog{state="pending"} 3)
    assert output =~ ~s(cluster_repair_backlog{state="under_replicated"} 4)

    assert output =~
             ~s(cluster_lease_contention_total{kind="repair_blob",reason="owned"} 1)

    assert output =~ ~s(storage_orphan_blobs{state="orphans"} 7)
    assert output =~ ~s(cluster_quorum_duration_milliseconds_count{result="ok"} 1)
    refute output =~ "not-a-label"
  end

  test "setup is idempotent" do
    assert :ok = Metrics.setup()
    assert :ok = Metrics.setup()
  end

  test "listing metrics have finite labels and omit object, bucket and arbitrary values" do
    for value <- [:current, "secret-object-key", {:private, "credential"}] do
      :telemetry.execute(
        [:ex_storage_service, :listing, :page, :stop],
        %{duration: 100},
        %{kind: value, result: :ok, bucket: "secret-bucket", key: "secret-object-key"}
      )

      :telemetry.execute(
        [:ex_storage_service, :listing, :index, :status],
        %{count: 1},
        %{state: value, bucket: "secret-bucket"}
      )
    end

    output = Metrics.format_metrics()
    assert output =~ ~s(listing_pages_total{kind="current",result="ok"} 1)
    assert output =~ ~s(listing_pages_total{kind="other",result="ok"} 2)
    assert output =~ ~s(listing_index_checks_total{state="other"} 3)
    assert output =~ "listing_duration_milliseconds_count"
    refute output =~ "secret-"
    refute output =~ "credential"
  end

  test "ordinary returned HTTP failures are errors and do not change span results" do
    for status <- [200, 204, 304, 400, 404, 412, 503] do
      result = %{status: status}
      assert ^result = Telemetry.span(:get_object, %{key: "private-key"}, fn -> result end)
    end

    assert_raise RuntimeError, "private failure", fn ->
      Telemetry.span(:put_object, %{}, fn -> raise "private failure" end)
    end

    output = Metrics.format_metrics()
    assert output =~ ~s(s3_requests_total{operation="get_object",status="ok"} 3)
    assert output =~ ~s(s3_requests_total{operation="get_object",status="error"} 4)
    assert output =~ ~s(s3_requests_total{operation="put_object",status="error"} 1)
    refute output =~ "private"
  end

  test "operational labels collapse arbitrary values without leaking metadata" do
    for value <- ["secret-key", {:credential, "secret-value"}, 9_999] do
      :telemetry.execute([:ex_storage_service, :s3, :request, :stop], %{duration: 1}, %{
        operation: value,
        result: :error
      })

      :telemetry.execute([:ex_storage_service, :gateway, :upstream, :stop], %{duration: 1}, %{
        operation: value,
        result: value,
        endpoint: "secret-endpoint"
      })

      :telemetry.execute([:ex_storage_service, :gateway, :cache, :stop], %{}, %{
        operation: value,
        result: value
      })

      :telemetry.execute([:ex_storage_service, :job, :transition], %{}, %{
        kind: value,
        execution: value,
        transition: value,
        job_id: "secret-job"
      })

      :telemetry.execute([:ex_storage_service, :cluster, :quorum, :stop], %{duration: 1}, %{
        result: value
      })

      :telemetry.execute([:ex_storage_service, :cluster, :lease, :contention], %{}, %{
        kind: value,
        reason: value
      })
    end

    output = Metrics.format_metrics()
    assert output =~ ~s(gateway_upstream_requests_total{operation="other",result="other"} 3)

    assert output =~
             ~s(durable_job_transitions_total{execution="other",kind="other",transition="other"} 3)

    assert output =~ ~s(s3_requests_total{operation="other",status="error"} 3)
    refute output =~ "secret"
    refute output =~ "9999"
    refute output =~ "credential"
  end

  test "staging observation is bounded and never pretends overflow is complete" do
    limit = Metrics.staging_snapshot().limit

    for index <- 1..(limit + 1) do
      :telemetry.execute([:ex_storage_service, :storage, :stage, :start], %{}, %{
        path: "secret-path-#{index}"
      })
    end

    assert %{tracked_files: ^limit, complete_since_setup: false} = Metrics.staging_snapshot()
    assert Metrics.format_metrics() =~ "storage_staging_tracking_complete_since_setup 0"
    refute Metrics.format_metrics() =~ "secret-path"
  end
end
