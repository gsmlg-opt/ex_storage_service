defmodule ExStorageService.CloudCache.DeleteThroughTest do
  use ExUnit.Case, async: true
  alias ExStorageService.CloudCache.{Config, DeleteThrough}
  alias ExStorageService.Metadata.Keys

  defmodule Backend do
    def put(key, value, _opts) do
      revision = Process.get(:revision, 0) + 1
      Process.put(:revision, revision)
      Process.put({__MODULE__, key}, %{value: value, mod_revision: revision})
      :ok
    end

    def get(key, _opts), do: {:ok, Process.get({__MODULE__, key})}

    def delete(key, _opts),
      do:
        (
          Process.delete({__MODULE__, key})
          :ok
        )

    def list_page(prefix, _cursor, limit, _opts) do
      entries =
        for {{__MODULE__, key}, value} <- Process.get(),
            String.starts_with?(key, prefix),
            do: Map.put(value, :key, key)

      {:ok, %{entries: Enum.take(entries, limit), next_cursor: nil}}
    end

    def transaction(spec, _opts) do
      passes =
        Enum.all?(spec.compare, fn
          {:exists, key, :==, value} ->
            Process.get({__MODULE__, key}) != nil == value

          {:mod_revision, key, :==, revision} ->
            match?(%{mod_revision: ^revision}, Process.get({__MODULE__, key}))
        end)

      if passes do
        Enum.each(spec.success, fn
          {:put, key, value, _} -> put(key, value, [])
          {:delete, {:key, key}, _} -> delete(key, [])
        end)
      end

      {:ok, %{succeeded: passes}}
    end
  end

  defmodule Client do
    def head_object(_config, _key), do: Process.get(:head, {:ok, %{etag: "original"}})

    def delete_object(_config, _key, opts) do
      Process.put(:deletes, Process.get(:deletes, 0) + 1)
      Process.put(:delete_condition, opts[:if_match])
      assert_prepared!()
      if Process.get(:crash), do: throw(:simulated_process_exit)
      if Process.get(:remote_removed, true), do: Process.put(:head, {:error, :not_found})
      Process.get(:delete_result, :ok)
    end

    defp assert_prepared! do
      true =
        Enum.any?(Process.get(), fn
          {{Backend, "ess:gateway-delete:" <> _}, %{value: %{state: :prepared}}} -> true
          _ -> false
        end)
    end
  end

  setup do
    config = %Config{
      enabled: true,
      provider: :s3_compat,
      endpoint: "http://isolated.invalid",
      bucket: "remote",
      cache_enabled: true
    }

    :ok = Backend.put("bucket:local", %{generation: "original"}, [])

    :ok =
      Backend.put(
        ExStorageService.Metadata.ListIndex.root_key("local", "original"),
        ExStorageService.Metadata.ListIndex.empty("original"),
        []
      )

    :ok = Backend.put("obj:local:key", %{etag: "original"}, [])

    :ok =
      Backend.put(
        "cloud_cache_index:local",
        %{"key" => %{content_hash: "old"}, "other" => %{content_hash: "shared"}},
        []
      )

    opts = [
      backend: Backend,
      client: Client,
      get_config: fn "local" -> {:ok, config} end,
      before_local_commit: fn -> Process.get(:local_result, :ok) end
    ]

    {:ok, config: config, opts: opts}
  end

  test "journals before a conditional delete and atomically removes local metadata and cache entry",
       context do
    assert :ok = delete(context)
    assert Process.get(:delete_condition) == "\"original\""
    assert {:ok, nil} = Backend.get("obj:local:key", [])

    assert {:ok, %{value: %{"other" => %{content_hash: "shared"}}}} =
             Backend.get("cloud_cache_index:local", [])

    assert {:ok, %{results: []}} = reconcile(context)
  end

  test "known upstream rejection preserves both metadata and cache", context do
    Process.put(:delete_result, {:error, {:http_error, 403}})
    Process.put(:remote_removed, false)
    assert {:error, {:http_error, 403}} = delete(context)
    assert {:ok, %{value: %{etag: "original"}}} = Backend.get("obj:local:key", [])
    assert {:ok, %{value: %{"key" => _}}} = Backend.get("cloud_cache_index:local", [])
    assert {:ok, %{results: []}} = reconcile(context)
  end

  test "local failure is recovered from durable record without repeating the mutation", context do
    capture_uncertainty()
    Process.put(:local_result, {:error, :injected_failure})
    assert {:error, {:local_reconciliation_required, :injected_failure}} = delete(context)

    assert_received {:operation_uncertainty,
                     %{source: :gateway_delete, outcome: :reconciliation_required}}

    Process.put(:local_result, :ok)
    assert {:ok, %{results: [{"operation", :ok}]}} = reconcile(context)
    assert Process.get(:deletes) == 1
    assert {:ok, nil} = Backend.get("obj:local:key", [])
  end

  test "unknown success reconciles only after HEAD confirms absence", context do
    capture_uncertainty()
    Process.put(:delete_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = delete(context)
    assert_received {:operation_uncertainty, %{source: :gateway_delete, outcome: :unknown}}

    assert {:ok, %{operation_id: "operation", state: :prepared}} =
             ExStorageService.Operations.OperationStatus.get(:gateway_delete, "operation",
               backend: Backend
             )

    assert {:ok, %{results: [{"operation", :ok}]}} = reconcile(context)
    assert Process.get(:deletes) == 1

    assert {:ok, nil} =
             ExStorageService.Operations.OperationStatus.get(:gateway_delete, "operation",
               backend: Backend
             )
  end

  test "unknown outcome with a live remote object never triggers a second DELETE", context do
    Process.put(:delete_result, {:error, :timeout})
    Process.put(:remote_removed, false)
    assert {:error, {:upstream_unknown, :timeout}} = delete(context)
    Process.put(:head, {:ok, %{etag: "new-upstream-version"}})

    assert {:ok, %{results: [{"operation", {:error, :upstream_outcome_unresolved}}]}} =
             reconcile(context)

    assert Process.get(:deletes) == 1
    assert {:ok, %{value: %{etag: "original"}}} = Backend.get("obj:local:key", [])
  end

  test "crash during outbound mutation leaves prepared intent which does not repeat DELETE",
       context do
    Process.put(:crash, true)
    assert catch_throw(delete(context)) == :simulated_process_exit
    Process.put(:head, {:error, :not_found})
    assert {:ok, %{results: [{"operation", :ok}]}} = reconcile(context)
    assert Process.get(:deletes) == 1
  end

  test "reconciliation never removes a newer local write", context do
    Process.put(:delete_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = delete(context)
    :ok = Backend.put("obj:local:key", %{etag: "newer"}, [])

    assert {:ok,
            %{
              results: [
                {"operation",
                 {:error, {:local_reconciliation_required, :local_metadata_conflict}}}
              ]
            }} = reconcile(context)

    assert {:ok, %{value: %{etag: "newer"}}} = Backend.get("obj:local:key", [])
  end

  test "reconciliation fences replaced buckets and newer cache entries", context do
    Process.put(:delete_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = delete(context)
    :ok = Backend.put("cloud_cache_index:local", %{"key" => %{content_hash: "new"}}, [])

    assert {:ok,
            %{
              results: [
                {"operation",
                 {:error, {:local_reconciliation_required, :local_metadata_conflict}}}
              ]
            }} = reconcile(context)

    :ok = Backend.put("bucket:local", %{generation: "recreated"}, [])

    assert {:ok,
            %{
              results: [
                {"operation",
                 {:error, {:local_reconciliation_required, :bucket_generation_changed}}}
              ]
            }} = reconcile(context)
  end

  test "preflight failures and hosted history cannot initiate an external mutation", context do
    Process.put(:head, {:error, {:http_error, 503}})
    assert {:error, {:http_error, 503}} = delete(context)
    :ok = Backend.put(Keys.object_head("local", "key"), %{version_id: "hosted"}, [])
    assert {:error, :gateway_hosted_history_unsupported} = delete(context)
    refute Process.get(:deletes)
    assert {:ok, %{results: []}} = reconcile(context)
  end

  test "already absent remote key avoids creating another upstream delete marker", context do
    Process.put(:head, {:error, :not_found})
    assert :ok = delete(context)
    refute Process.get(:deletes)
    assert {:ok, nil} = Backend.get("obj:local:key", [])
  end

  test "changed gateway configuration cannot reconcile against another endpoint", context do
    Process.put(:delete_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = delete(context)

    opts =
      Keyword.put(context.opts, :get_config, fn _ ->
        {:ok, %{context.config | bucket: "different"}}
      end)

    assert {:ok, %{results: [{"operation", {:error, :upstream_identity_changed}}]}} =
             DeleteThrough.reconcile_page(nil, 10, opts)

    assert Process.get(:deletes) == 1
  end

  defp delete(context),
    do: DeleteThrough.delete("local", "key", context.config, "operation", context.opts)

  defp reconcile(context), do: DeleteThrough.reconcile_page(nil, 10, context.opts)

  defp capture_uncertainty do
    handler = {__MODULE__, :uncertainty, make_ref()}
    owner = self()

    :ok =
      :telemetry.attach(
        handler,
        [:ex_storage_service, :operation, :uncertain],
        fn _, _, metadata, owner ->
          if self() == owner, do: send(owner, {:operation_uncertainty, metadata})
        end,
        owner
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end
end
