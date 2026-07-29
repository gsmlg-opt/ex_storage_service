defmodule ExStorageService.Cluster.StatusTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.Status

  test "returns a sanitized complete planner snapshot" do
    provider = fn _opts ->
      {:ok,
       %{
         complete: true,
         desired_replicas: 2,
         actual_replicas: 3,
         required_write_quorum: 2,
         under_replicated_blobs: 1,
         unavailable_blobs: 0,
         repair_backlog: %{pending: 1},
         secret: "must-not-leak"
       }}
    end

    snapshot = Status.snapshot(provider: provider)

    assert snapshot.status == :ok
    assert snapshot.complete
    assert snapshot.required_write_quorum == 2
    assert snapshot.repair_backlog == %{pending: 1}
    refute Map.has_key?(snapshot, :secret)
  end

  test "marks incomplete snapshots as partial" do
    assert %{status: :partial, complete: false} =
             Status.snapshot(provider: fn -> {:ok, %{complete: false}} end)
  end

  test "provider failure returns a bounded unavailable response" do
    assert %{status: :unavailable, complete: false, reason: "status_provider_failed"} =
             Status.snapshot(provider: fn -> raise "planner unavailable" end)
  end
end
