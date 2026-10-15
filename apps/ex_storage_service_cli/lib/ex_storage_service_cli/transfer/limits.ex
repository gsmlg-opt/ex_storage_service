defmodule ExStorageServiceCli.Transfer.Limits do
  @moduledoc "Explicit provider byte limits. Native legacy defaults retain the ESS contract."
  @aws %{
    max_single_put_bytes: 5_368_709_120,
    min_nonfinal_part_bytes: 5_242_880,
    max_part_bytes: 5_368_709_120,
    max_parts: 10_000,
    max_object_bytes: 53_687_091_200_000
  }
  @ess Map.put(@aws, :max_object_bytes, 5_368_709_120)
  def for_context(%{capabilities: %{transfer_limits: limits}}), do: limits
  def for_context(%{frontend: :aws}), do: @aws
  def for_context(_context), do: @ess
end
