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
end
