defmodule ExStorageService.Operations.CapacityTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metrics
  alias ExStorageService.Operations.Capacity
  @moduletag :tmp_dir

  setup do
    :ok = Metrics.setup()
    :ok = Metrics.reset()
  end

  test "real disposable filesystems report plausible capacity without inventory", %{tmp_dir: root} do
    result = Capacity.snapshot(roots: %{blob: root, tmp: root, metadata: root})
    assert result.complete

    for kind <- [:blob, :tmp, :metadata] do
      assert %{complete: true, total_bytes: total, available_bytes: available} =
               result.roots[kind]

      assert total > 0
      assert available >= 0 and available <= total
    end

    assert Metrics.format_metrics() =~ ~s(storage_capacity_observation_complete{root="blob"} 1)
    refute Metrics.format_metrics() =~ root
  end

  test "failed acquisition is explicit and retains the previous sample instead of healthy zero",
       %{tmp_dir: root} do
    roots = %{blob: root, tmp: root, metadata: root}

    Capacity.snapshot(
      roots: roots,
      probe: fn _ -> {:ok, %{total_bytes: 4096, available_bytes: 1024}} end
    )

    failed = Capacity.snapshot(roots: roots, probe: fn _ -> {:error, :deadline} end)
    refute failed.complete
    assert failed.roots.blob == %{complete: false, error: :deadline}
    output = Metrics.format_metrics()
    assert output =~ ~s(storage_capacity_observation_complete{root="blob"} 0)
    assert output =~ ~s(storage_filesystem_available_bytes{root="blob"} 1024)
    refute output =~ ~s(storage_filesystem_available_bytes{root="blob"} 0)
  end

  test "invalid and missing roots never imply healthy capacity" do
    missing = Capacity.snapshot(roots: %{})
    refute missing.complete
    assert missing.roots.tmp == %{complete: false, error: :missing_root}

    failed =
      Capacity.snapshot(roots: %{blob: "/private"}, probe: fn _ -> raise "private secret" end)

    assert failed.roots.blob == %{complete: false, error: :probe_failed}
    refute Metrics.format_metrics() =~ "private"
  end
end
