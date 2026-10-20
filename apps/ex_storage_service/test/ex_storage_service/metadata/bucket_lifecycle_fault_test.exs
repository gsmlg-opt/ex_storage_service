defmodule ExStorageService.Metadata.BucketLifecycleFaultTest do
  use ExUnit.Case, async: true
  alias ExStorageService.Metadata.BucketLifecycle

  defmodule Backend do
    def get("bucket:failed", consistency: :linearizable),
      do: {:ok, %{mod_revision: 7, value: %{generation: "original"}}}

    def get("ess:v2:list_index:" <> _, consistency: :linearizable),
      do: {:ok, %{mod_revision: 8, value: ExStorageService.Metadata.ListIndex.empty("original")}}

    def get("bucket:missing", consistency: :linearizable), do: {:ok, nil}
    def get_all(consistency: :linearizable), do: {:ok, %{}}

    def transaction(spec) do
      send(self(), {:attempted_delete, spec})
      {:error, :timeout}
    end
  end

  test "uncertain delete outcome is returned without success or blind transaction replay" do
    assert {:error, :timeout} = BucketLifecycle.delete("failed", 32, backend: Backend)

    assert_received {:attempted_delete,
                     %{
                       compare: [
                         {:mod_revision, "bucket:failed", :==, 7},
                         {:mod_revision, _, :==, 8}
                       ],
                       success: [{:delete, {:key, "bucket:failed"}, %{}} | _]
                     }}

    refute_received {:attempted_delete, _}
  end

  test "missing bucket never initiates a delete transaction" do
    assert {:error, :not_found} = BucketLifecycle.delete("missing", 32, backend: Backend)
    refute_received {:attempted_delete, _}
  end
end
