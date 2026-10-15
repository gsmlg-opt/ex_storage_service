defmodule ExStorageServiceCli.Transfer.PlannerTest do
  use ExUnit.Case, async: true
  alias ExStorageServiceCli.{Error, Transfer.Planner}
  @limits_file Path.expand("../../../../../docs/cli/compatibility/provider-limits.json", __DIR__)
  @providers @limits_file |> File.read!() |> JSON.decode!() |> Map.fetch!("providers")
  @aws Enum.find(@providers, &(&1["id"] == "aws-s3-general-purpose"))

  test "zero and exact single PUT boundary remain single, boundary plus one is multipart" do
    for size <- [0, 1, @aws["max_single_put_bytes"]] do
      assert {:ok, %{mode: :single, size: ^size, part_count: 1}} = Planner.plan(size, @aws)
    end

    assert {:ok, plan} = Planner.plan(@aws["max_single_put_bytes"] + 1, @aws)
    assert plan.mode == :multipart
    assert_ranges(plan)
  end

  test "AWS maximum is exact 10000 times 5 GiB, not an obsolete 5 TB ceiling" do
    maximum = 53_687_091_200_000
    assert @aws["max_object_bytes"] == maximum
    assert {:ok, plan} = Planner.plan(maximum, @aws)
    assert plan.part_count == 10_000
    assert plan.part_size == 5_368_709_120
    assert_ranges(plan)
    assert {:error, %Error{code: :object_size_limit}} = Planner.plan(maximum + 1, @aws)
  end

  test "part size is derived from minimum, count, user choice, and short final part" do
    limits = %{
      max_single_put_bytes: 10,
      min_nonfinal_part_bytes: 5,
      max_part_bytes: 10,
      max_parts: 3,
      max_object_bytes: 30
    }

    for {size, options, part_size, count} <- [
          {11, [], 5, 3},
          {21, [], 7, 3},
          {11, [part_size: 2], 5, 3},
          {11, [part_size: 8], 8, 2},
          {1, [force_multipart: true], 5, 1}
        ] do
      assert {:ok, plan} = Planner.plan(size, limits, options)
      assert plan.part_size == part_size
      assert plan.part_count == count
      assert_ranges(plan)
    end

    assert {:error, %Error{code: :multipart_limit}} = Planner.plan(11, limits, part_size: 11)
    assert {:ok, %{mode: :single}} = Planner.plan(0, limits, force_multipart: true)
  end

  test "provider-specific limits and unknown generic capabilities fail safely" do
    ess = Enum.find(@providers, &(&1["id"] == "ess-0.6.7-default-runtime"))
    generic = Enum.find(@providers, &(&1["id"] == "generic-s3-compatible"))
    rustfs = Enum.find(@providers, &(&1["id"] == "rustfs-1.0.1"))

    assert {:error, %Error{code: :object_size_limit}} =
             Planner.plan(ess["max_object_bytes"] + 1, ess)

    assert {:error, %Error{code: :unknown_provider_limits}} = Planner.plan(1, generic)
    assert {:ok, _} = Planner.plan(6_000_000_000, rustfs)

    assert {:error, %Error{code: :multipart_limit}} =
             Planner.plan(rustfs["max_parts"] * rustfs["max_part_bytes"] + 1, rustfs)

    for size <- [-1, nil, "10"] do
      assert {:error, %Error{code: :invalid_transfer_size}} = Planner.plan(size, @aws)
    end

    for opts <- [[part_size: 0], [multipart_threshold: -1], [part_size: "5"]] do
      assert {:error, %Error{code: :invalid_transfer_option}} = Planner.plan(1, @aws, opts)
    end
  end

  defp assert_ranges(plan) do
    assert length(plan.parts) == plan.part_count
    assert Enum.sum(Enum.map(plan.parts, & &1.length)) == plan.size

    Enum.reduce(plan.parts, 0, fn part, offset ->
      assert part.offset == offset
      assert part.number in 1..10_000
      assert part.length > 0
      offset + part.length
    end)
  end
end
