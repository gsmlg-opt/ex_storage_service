defmodule ExStorageService.Cluster.HealthTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.Health

  test "readiness requires both metadata and data checks" do
    ready = fn _opts -> {:ok, %{}} end

    assert {:ok,
            %{
              status: :ready,
              checks: %{metadata: %{ready: true}, data: %{ready: true}}
            }} =
             Health.readiness(metadata_checker: ready, data_checker: ready)
  end

  test "readiness reports a stable reason without leaking collaborator details" do
    metadata = fn _opts -> {:ok, %{leader: "node-a"}} end
    data = fn _opts -> {:error, {:insufficient_healthy_nodes, %{healthy: 1, required: 2}}} end

    assert {:error,
            %{
              status: :not_ready,
              checks: %{
                metadata: %{ready: true},
                data: %{ready: false, reason: "insufficient_healthy_nodes"}
              }
            }} =
             Health.readiness(metadata_checker: metadata, data_checker: data)
  end

  test "failed collaborators make readiness false instead of crashing the caller" do
    crashing = fn _opts -> raise "health backend failed" end

    assert {:error, %{checks: %{metadata: %{ready: false, reason: "check_failed"}}}} =
             Health.readiness(metadata_checker: crashing, data_checker: crashing)
  end

  test "liveness supports an injected process check" do
    assert {:ok, %{status: :ok}} =
             Health.liveness(liveness_checker: fn _opts -> {:ok, %{process: :alive}} end)

    assert {:error, %{status: :failed, reason: "stopped"}} =
             Health.liveness(liveness_checker: fn _opts -> {:error, :stopped} end)
  end
end
