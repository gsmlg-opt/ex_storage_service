defmodule ExStorageService.Operations.OperationStatusTest do
  use ExUnit.Case, async: false
  alias ExStorageService.Metadata.{Keys, OperationIntents}
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Operations.OperationStatus
  alias ExStorageService.Metrics

  defmodule ExactBackend do
    def get(key, opts) do
      send(self(), {:exact_read, key, opts})
      Process.get(:operation_status_result, {:ok, nil})
    end

    def list_page(_, _, _, _), do: raise("diagnostics must not list metadata")
    def get_all(_), do: raise("diagnostics must not scan metadata")
    def transaction(_, _), do: raise("diagnostics must not mutate metadata")
  end

  setup do
    :ok = Metrics.setup()
    :ok = Metrics.reset()
  end

  test "one exact read per fixed namespace redacts journal contents and bounds labels" do
    id = "private-operation-id"

    for {source, state, expected} <- [
          {:gateway_write, :unknown,
           "ess:gateway-write:" <> Base.url_encode64(id, padding: false)},
          {:gateway_delete, :prepared,
           "ess:gateway-delete:" <> Base.url_encode64(id, padding: false)}
        ] do
      Process.put(
        :operation_status_result,
        {:ok,
         %{
           value: %{
             operation_id: id,
             state: state,
             key: "private-key",
             path: "/private-stage",
             endpoint: "https://private-endpoint",
             secret: "private-secret"
           }
         }}
      )

      assert {:ok, %{operation_id: ^id, state: ^state} = summary} =
               OperationStatus.get(source, id, backend: ExactBackend, timeout: 100)

      assert map_size(summary) == 2
      assert_received {:exact_read, ^expected, opts}
      assert opts[:consistency] == :linearizable
      assert opts[:timeout] == 100
      refute_received {:exact_read, _, _}
    end

    :telemetry.execute([:ex_storage_service, :operation_status, :read], %{count: 1}, %{
      source: "private-source",
      state: "private-state",
      result: "private-result"
    })

    :telemetry.execute([:ex_storage_service, :operation, :uncertain], %{count: 1}, %{
      source: "private-source",
      outcome: "private-outcome",
      operation_id: id
    })

    output = Metrics.format_metrics()

    assert output =~
             ~s(operation_status_reads_total{result="ok",source="gateway_write",state="unknown"} 1)

    assert output =~
             ~s(operation_status_reads_total{result="other",source="other",state="other"} 1)

    assert output =~ ~s(uncertain_operation_outcomes_total{outcome="other",source="other"} 1)
    refute output =~ "private-"
  end

  test "real durable unknown and atomic committed states can be read by known ID" do
    id = "operation-status-#{System.unique_integer([:positive, :monotonic])}"
    hash = Base.encode16(:crypto.hash(:sha256, id), case: :lower)

    on_exit(fn ->
      Concord.delete(Keys.operation_intent(id))
      Concord.delete(Keys.gc_guard(hash))
    end)

    assert {:ok, _} = OperationIntents.open(id, hash, 3, "status-fixture", 1, now_ms: 100)
    assert {:ok, %{operation_id: ^id, state: :pending}} = OperationStatus.get(:hosted, id)
    assert {:ok, _} = OperationIntents.transition(id, :unknown, now_ms: 101)
    assert {:ok, %{operation_id: ^id, state: :unknown}} = OperationStatus.get(:hosted, id)
    assert {:ok, fragments} = OperationIntents.commit_operations(id, hash, now_ms: 102)

    assert {:ok, %{succeeded: true}} =
             Backend.transaction(
               %{compare: fragments.compare, success: fragments.success, failure: []},
               idempotency_key: "status-close-#{id}"
             )

    assert {:ok, %{operation_id: ^id, state: :committed}} = OperationStatus.get(:hosted, id)
    assert {:ok, %{value: _}} = Backend.get(Keys.gc_guard(hash))

    assert Metrics.format_metrics() =~
             ~s(operation_status_reads_total{result="ok",source="hosted",state="unknown"} 1)

    refute Metrics.format_metrics() =~ id
  end

  test "absence, failed reads and malformed states are distinct and never silently omitted" do
    assert {:ok, nil} = OperationStatus.get(:gateway_delete, "id", backend: ExactBackend)
    Process.put(:operation_status_result, {:error, :metadata_unavailable})

    assert {:error, :metadata_unavailable} =
             OperationStatus.get(:gateway_delete, "id", backend: ExactBackend)

    for value <- [
          %{operation_id: "different", state: :prepared},
          %{operation_id: "id", state: "secret"}
        ] do
      Process.put(:operation_status_result, {:ok, %{value: value}})

      assert {:error, :invalid_operation_record} =
               OperationStatus.get(:gateway_delete, "id", backend: ExactBackend)
    end

    output = Metrics.format_metrics()

    assert output =~
             ~s(operation_status_reads_total{result="ok",source="gateway_delete",state="absent"} 1)

    assert output =~
             ~s(operation_status_reads_total{result="error",source="gateway_delete",state="other"} 3)

    refute output =~ "secret"
  end

  test "invalid selectors and oversized IDs cannot trigger a read or arbitrary prefix lookup" do
    for {source, id} <- [{:unknown, "id"}, {:hosted, ""}, {:hosted, String.duplicate("a", 1025)}] do
      assert {:error, :invalid_operation_lookup} =
               OperationStatus.get(source, id, backend: ExactBackend)
    end

    refute_received {:exact_read, _, _}
  end
end
