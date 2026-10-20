defmodule ExStorageService.Operations.ListingIndexTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.ListIndex
  alias ExStorageService.Operations.ListingIndex, as: Operations

  defmodule ExactBackend do
    def get(key, opts) do
      {owner, descriptor} = Keyword.fetch!(opts, :engine)
      send(owner, {:read, key})

      case key do
        "bucket:fixture" -> {:ok, %{value: %{generation: "generation"}, mod_revision: 1}}
        _ -> {:ok, descriptor}
      end
    end
  end

  test "readiness performs only two exact reads and never exposes private descriptor fields" do
    descriptor = ListIndex.empty("generation") |> Map.put(:private_field, "private-token")
    opts = [backend: ExactBackend, engine: {self(), %{value: descriptor}}]

    assert {:ok, %{ready: true, state: :ready, counts: %{visible_count: 0}} = summary} =
             Operations.readiness("fixture", opts)

    assert_receive {:read, "bucket:fixture"}
    assert_receive {:read, key}
    assert key == ListIndex.root_key("fixture", "generation")
    refute_receive {:read, _}
    refute inspect(summary) =~ "private-token"
    refute Map.has_key?(summary, :epoch)
    refute Map.has_key?(summary, :current_root)
  end

  test "missing, interrupted and corrupt indexes fail bucket readiness" do
    for descriptor <- [
          nil,
          %{value: %{ListIndex.empty("generation") | state: :building}},
          %{value: %{ListIndex.empty("generation") | state: :validating}},
          %{value: %{ListIndex.empty("generation") | visible_count: -1}}
        ] do
      assert {:error, %{ready: false}} =
               Operations.readiness("fixture",
                 backend: ExactBackend,
                 engine: {self(), descriptor}
               )
    end
  end
end
