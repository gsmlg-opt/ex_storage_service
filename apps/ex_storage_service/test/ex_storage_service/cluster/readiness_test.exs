defmodule ExStorageService.Cluster.ReadinessTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Cluster.Readiness

  defmodule ReadyBackend do
    def status(_opts),
      do: {:ok, %{cluster: %{status: :normal, primary_id: "node-a"}, storage: %{}}}
  end

  defmodule RecoveringBackend do
    def status(_opts),
      do: {:ok, %{cluster: %{status: :recovery, primary_id: nil}, storage: %{}}}
  end

  defmodule TimeoutBackend do
    def status(_opts), do: {:error, :timeout}
  end

  test "requires a quorum-backed normal status with a primary" do
    assert {:ok, %{cluster: %{primary_id: "node-a"}}} =
             Readiness.check(backend: ReadyBackend)

    assert Readiness.ready?(backend: ReadyBackend)
  end

  test "recovering and unavailable clusters are not ready" do
    assert {:error, {:not_ready, %{cluster: %{status: :recovery}}}} =
             Readiness.check(backend: RecoveringBackend)

    assert {:error, :timeout} = Readiness.check(backend: TimeoutBackend)
    refute Readiness.ready?(backend: TimeoutBackend)
  end
end
