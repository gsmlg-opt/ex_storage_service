defmodule ExStorageService.Operations.RecoveryPolicyTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Operations.RecoveryPolicy

  test "ordinary persistent publication guards do not block enabled-worker activation" do
    refute RecoveryPolicy.unresolved?(Keys.gc_guard("hash"), %{protected_until_ms: 999_999})
    assert RecoveryPolicy.unresolved?(Keys.gc_lock("hash"), %{lease_until_ms: 0})
    assert RecoveryPolicy.unresolved?("ess:gateway-write:operation", %{})
    assert RecoveryPolicy.unresolved?(Keys.operation_intent("operation"), %{state: :unknown})
    refute RecoveryPolicy.unresolved?(Keys.operation_intent("operation"), %{state: :committed})
  end

  test "reviewed terminal intent resolution preserves all content and fencing fields" do
    key = Keys.operation_intent("operation")

    source = %{
      operation_id: "operation",
      state: :unknown,
      hash: "hash",
      size: 10,
      node_id: "source",
      node_generation: 1,
      protected_until_ms: 20,
      updated_at_ms: 10
    }

    resolved = %{source | state: :committed, updated_at_ms: 11}
    policy = %{"operation" => :committed}
    assert RecoveryPolicy.metadata_matches?(key, source, source, %{})
    refute RecoveryPolicy.metadata_matches?(key, source, resolved, %{})
    assert RecoveryPolicy.metadata_matches?(key, source, resolved, policy)

    for field <- [:hash, :size, :node_id, :node_generation, :protected_until_ms] do
      refute RecoveryPolicy.metadata_matches?(
               key,
               source,
               Map.put(resolved, field, :changed),
               policy
             )
    end

    refute RecoveryPolicy.metadata_matches?(key, source, %{resolved | updated_at_ms: 9}, policy)
    refute RecoveryPolicy.metadata_matches?("unrelated", source, resolved, policy)
    refute RecoveryPolicy.metadata_matches?(key, source, %{resolved | state: :aborted}, policy)
  end
end
