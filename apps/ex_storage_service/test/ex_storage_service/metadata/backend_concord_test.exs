defmodule ExStorageService.Metadata.Backend.ConcordTest do
  use ExUnit.Case, async: false

  alias Concord.Txn.Result
  alias ExStorageService.Metadata.Backend.Concord, as: ConcordBackend
  alias ExStorageService.Metadata.Keys

  test "Concord 3 commits and reads a compare-guarded multi-key transaction" do
    suffix = System.unique_integer([:positive, :monotonic])
    prefix = "test:concord-v3:#{suffix}:"
    head_key = prefix <> "head"
    version_key = prefix <> "version"
    operation_key = prefix <> "operation"
    keys = [head_key, version_key, operation_key]

    on_exit(fn ->
      Enum.each(keys, &Concord.delete/1)
    end)

    spec = %{
      compare: Enum.map(keys, &{:exists, &1, :==, false}),
      success: [
        {:put, version_key, %{version_id: "v1"}, %{}},
        {:put, head_key, %{version_id: "v1"}, %{}},
        {:put, operation_key, %{operation_id: "op1"}, %{}}
      ],
      failure: []
    }

    assert {:ok, %Result{succeeded: true}} =
             ConcordBackend.transaction(spec, idempotency_key: "op1")

    assert {:ok, %{value: %{version_id: "v1"}}} = ConcordBackend.get(head_key)

    assert {:ok,
            [
              {^head_key, %{version_id: "v1"}},
              {^operation_key, %{operation_id: "op1"}},
              {^version_key, %{version_id: "v1"}}
            ]} = ConcordBackend.scan(prefix)

    assert {:ok, %Result{succeeded: false}} =
             ConcordBackend.transaction(spec, idempotency_key: "op1")
  end

  test "commits encoded v2 metadata for a 1024-byte S3 object key" do
    bucket = String.duplicate("b", 63)
    object_key = String.duplicate("k", 1_024)
    head_key = Keys.object_head(bucket, object_key)
    version_key = Keys.object_version(bucket, object_key, "version-1")
    keys = [head_key, version_key]

    assert byte_size(head_key) > 1_024
    assert byte_size(version_key) > 1_024

    on_exit(fn ->
      Enum.each(keys, &Concord.delete/1)
    end)

    spec = %{
      compare: Enum.map(keys, &{:exists, &1, :==, false}),
      success: [
        {:put, version_key, %{version_id: "version-1"}, %{}},
        {:put, head_key, %{version_id: "version-1"}, %{}}
      ],
      failure: []
    }

    assert {:ok, %Result{succeeded: true}} =
             ConcordBackend.transaction(spec, idempotency_key: "long-key-operation")

    assert {:ok, %{value: %{version_id: "version-1"}}} = ConcordBackend.get(head_key)
    assert {:ok, %{value: %{version_id: "version-1"}}} = ConcordBackend.get(version_key)
  end
end
