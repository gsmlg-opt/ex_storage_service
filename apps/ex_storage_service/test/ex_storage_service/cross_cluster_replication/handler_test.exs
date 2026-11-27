defmodule ExStorageService.CrossClusterReplication.HandlerTest do
  use ExUnit.Case, async: true

  alias ExStorageService.CrossClusterReplication.Handler
  alias ExStorageService.{Context, InstanceConfig}

  defmodule ConfigStub do
    def get_bucket_replicas("bucket"), do: {:ok, [%{endpoint: "https://dr.example"}]}
  end

  defmodule RemovedConfig do
    def get_bucket_replicas("bucket"), do: {:ok, []}
  end

  defmodule FailedConfig do
    def get_bucket_replicas("bucket"), do: {:error, :timeout}
  end

  defmodule RotatedConfig do
    def get_bucket_replicas("bucket") do
      {:ok,
       [%{"endpoint" => "https://dr.example/", "bucket" => "bucket", "access_key" => "current"}]}
    end
  end

  setup do
    {:ok, config} = InstanceConfig.new(auto_start: false)
    %{context: Context.new(config)}
  end

  test "applies current objects and reconciles superseded PUT jobs", %{context: context} do
    parent = self()
    job = put_job()

    current = fn _bucket, _key, _context ->
      {:ok,
       %{
         delete_marker: false,
         metadata: %{content_hash: "hash-v1", version_id: "v1"}
       }}
    end

    put = fn bucket, key, replica, object, _opts ->
      send(parent, {:put, bucket, key, replica, object})
      :ok
    end

    assert :ok = perform(job, context, head: current, put: put)

    assert_received {:put, "bucket", "key", %{endpoint: "https://dr.example"},
                     %{version_id: "v1"}}

    newer = fn _bucket, _key, _context ->
      {:ok,
       %{
         delete_marker: false,
         metadata: %{content_hash: "new-hash", version_id: "v2"}
       }}
    end

    assert :ok = perform(job, context, head: newer, put: put)

    assert_received {:put, "bucket", "key", %{endpoint: "https://dr.example"},
                     %{content_hash: "new-hash", version_id: "v2"}}
  end

  test "a late old PUT schedules durable reconciliation after a newer PUT", %{context: context} do
    assert_inverted_completion(context, object("v1"), object("v2"))
  end

  test "a late DELETE schedules durable reconciliation after a newer PUT", %{context: context} do
    assert_inverted_completion(context, :absent, object("v2"))
  end

  test "a late PUT schedules durable reconciliation after a newer DELETE", %{context: context} do
    assert_inverted_completion(context, object("v1"), :absent)
  end

  test "ordered PUT and DELETE jobs finish without convergence retries", %{context: context} do
    Enum.each(
      [{object("v1"), object("v2")}, {object("v1"), :absent}, {:absent, object("v2")}],
      fn {initial, latest} ->
        {:ok, state} = Agent.start_link(fn -> %{head: initial, remote: nil} end)

        opts = [
          head: fn _, _, _ -> head_result(Agent.get(state, & &1.head)) end,
          put: fn _, _, _, current, _ -> Agent.update(state, &Map.put(&1, :remote, current)) end,
          delete: fn _, _, _ -> Agent.update(state, &Map.put(&1, :remote, :absent)) end
        ]

        assert :ok = perform(put_job(), context, opts)
        assert Agent.get(state, & &1.remote) == initial
        Agent.update(state, &Map.put(&1, :head, latest))
        assert :ok = perform(delete_job(), context, opts)
        assert Agent.get(state, & &1.remote) == latest
      end
    )
  end

  test "replacing null-version bytes requires reconciliation", %{context: context} do
    initial = object("null")
    latest = %{initial | content_hash: "new-null-hash"}
    assert_inverted_completion(context, initial, latest)
  end

  test "an unavailable post-transfer head keeps reconciliation pending", %{context: context} do
    parent = self()
    {:ok, reads} = Agent.start_link(fn -> 0 end)

    head = fn _bucket, _key, _context ->
      case Agent.get_and_update(reads, &{&1, &1 + 1}) do
        1 -> {:error, :timeout}
        _ -> head_result(object("v1"))
      end
    end

    put = fn _bucket, _key, _replica, _object, _opts ->
      send(parent, :transferred)
      :ok
    end

    assert {:error, {:replication_reconciliation_required, {:head_unavailable, :timeout}}} =
             perform(put_job(), context, head: head, put: put)

    assert_received :transferred
    assert :ok = perform(put_job(), context, head: head, put: put)
    assert_received :transferred
  end

  test "a failed transfer still detects a concurrently replaced head", %{context: context} do
    {:ok, current} = Agent.start_link(fn -> object("v1") end)

    head = fn _bucket, _key, _context -> head_result(Agent.get(current, & &1)) end

    put = fn _bucket, _key, _replica, _object, _opts ->
      Agent.update(current, fn _ -> object("v2") end)
      {:error, :timeout}
    end

    assert {:error, {:replication_reconciliation_required, :head_changed}} =
             perform(put_job(), context, head: head, put: put)
  end

  test "a stable head preserves a definitive endpoint failure", %{context: context} do
    head = fn _bucket, _key, _context -> head_result(object("v1")) end
    put = fn _bucket, _key, _replica, _object, _opts -> {:error, {:http_error, 403}} end

    assert {:error, {:http_error, 403}} =
             perform(put_job(), context, head: head, put: put)
  end

  test "DELETE reconciles the destination to the current local head", %{context: context} do
    parent = self()
    job = delete_job()

    put = fn bucket, key, replica, object, _opts ->
      send(parent, {:put, bucket, key, replica, object})
      :ok
    end

    delete = fn bucket, key, replica ->
      send(parent, {:delete, bucket, key, replica})
      :ok
    end

    deleted = fn _bucket, _key, _context -> {:error, :object_not_found} end
    assert :ok = perform(job, context, head: deleted, delete: delete)
    assert_received {:delete, "bucket", "key", %{endpoint: "https://dr.example"}}

    recreated = fn _bucket, _key, _context ->
      {:ok, %{delete_marker: false, metadata: %{content_hash: "new"}}}
    end

    assert :ok = perform(job, context, head: recreated, delete: delete, put: put)
    refute_received {:delete, _, _, _}

    assert_received {:put, "bucket", "key", %{endpoint: "https://dr.example"},
                     %{content_hash: "new"}}
  end

  test "removed destinations receive no later content from historical jobs", %{context: context} do
    parent = self()
    head = fn _, _, _ -> head_result(object("new-version")) end
    put = fn _, _, _, _, _ -> send(parent, :unexpected_put) end
    delete = fn _, _, _ -> send(parent, :unexpected_delete) end

    for job <- [put_job(), delete_job()] do
      assert :ok =
               perform(job, context, head: head, put: put, delete: delete, config: RemovedConfig)
    end

    refute_received :unexpected_put
    refute_received :unexpected_delete
  end

  test "configuration uncertainty retains work without sending current bytes", %{context: context} do
    parent = self()
    head = fn _, _, _ -> head_result(object("v2")) end
    put = fn _, _, _, _, _ -> send(parent, :unexpected_put) end

    assert {:error,
            {:replication_reconciliation_required, {:configuration_unavailable, :timeout}}} =
             perform(put_job(), context, head: head, put: put, config: FailedConfig)

    refute_received :unexpected_put
  end

  test "historical jobs resolve current credentials for an active effective destination", %{
    context: context
  } do
    parent = self()
    head = fn _, _, _ -> head_result(object("v2")) end

    put = fn _, _, replica, object, _ ->
      send(parent, {:put_current, replica, object})
      :ok
    end

    assert :ok = perform(put_job(), context, head: head, put: put, config: RotatedConfig)
    assert_received {:put_current, %{"access_key" => "current"}, %{version_id: "v2"}}
  end

  test "a directly queued explicit PUT retains authority over its matching snapshot", %{
    context: context
  } do
    parent = self()
    head = fn _, _, _ -> head_result(object("v1")) end

    put = fn _, _, _, current, _ ->
      send(parent, {:explicit_put, current})
      :ok
    end

    assert :ok = perform(put_job(), context, head: head, put: put, config: RemovedConfig)
    assert_received {:explicit_put, %{version_id: "v1"}}
  end

  test "a direct PUT without a pinned snapshot cannot forward later data to an inactive target",
       %{
         context: context
       } do
    head = fn _, _, _ -> head_result(object("v2")) end
    job = update_in(put_job().payload, &Map.delete(&1, :object))

    assert :ok =
             perform(job, context,
               head: head,
               config: RemovedConfig,
               put: fn _, _, _, _, _ -> flunk("unbound job forwarded current content") end
             )
  end

  test "a directly queued absent-key DELETE retains its one-time authorization", %{
    context: context
  } do
    parent = self()
    head = fn _, _, _ -> {:error, :object_not_found} end

    delete = fn _, _, _ ->
      send(parent, :explicit_delete)
      :ok
    end

    assert :ok = perform(delete_job(), context, head: head, delete: delete, config: RemovedConfig)
    assert_received :explicit_delete
  end

  test "Hook jobs always honor removal and current credentials even for matching bytes", %{
    context: context
  } do
    parent = self()
    head = fn _, _, _ -> head_result(object("v1")) end

    put = fn _, _, replica, _, _ ->
      send(parent, {:hook_put, replica})
      :ok
    end

    job = update_in(put_job().payload, &Map.put(&1, :operation_id, "hook-operation"))

    assert :ok = perform(job, context, head: head, put: put, config: RemovedConfig)
    refute_received {:hook_put, _}
    assert :ok = perform(job, context, head: head, put: put, config: RotatedConfig)
    assert_received {:hook_put, %{"access_key" => "current"}}
  end

  defp perform(job, context, opts) do
    Handler.perform(job, context, Keyword.put_new(opts, :config, ConfigStub))
  end

  defp assert_inverted_completion(context, initial, latest) do
    parent = self()
    {:ok, state} = Agent.start_link(fn -> %{head: initial, remote: initial} end)

    transfer = fn value ->
      if Process.delete(:hold_replication) do
        send(parent, {:transfer_waiting, self()})

        receive do
          :continue_replication -> :ok
        end
      end

      Agent.update(state, &Map.put(&1, :remote, value))
    end

    opts = [
      head: fn _, _, _ -> head_result(Agent.get(state, & &1.head)) end,
      put: fn _, _, _, object, _ -> transfer.(object) end,
      delete: fn _, _, _ -> transfer.(:absent) end
    ]

    original_job = if initial == :absent, do: delete_job(), else: put_job()

    original =
      Task.async(fn ->
        Process.put(:hold_replication, true)
        perform(original_job, context, opts)
      end)

    assert_receive {:transfer_waiting, worker}
    Agent.update(state, &Map.put(&1, :head, latest))
    latest_job = if latest == :absent, do: delete_job(), else: put_job(latest)
    assert :ok = perform(latest_job, context, opts)
    assert Agent.get(state, & &1.remote) == latest

    send(worker, :continue_replication)

    assert {:error, {:replication_reconciliation_required, :head_changed}} = Task.await(original)
    assert Agent.get(state, & &1.remote) == initial

    assert :ok = perform(original_job, context, opts)
    assert Agent.get(state, & &1.remote) == latest
  end

  defp object(version), do: %{content_hash: "hash-#{version}", version_id: version}
  defp head_result(:absent), do: {:error, :object_not_found}
  defp head_result(object), do: {:ok, %{delete_marker: false, metadata: object}}

  defp put_job(object \\ object("v1")) do
    %{
      kind: :cross_cluster_put,
      payload: %{
        bucket: "bucket",
        key: "key",
        replica: %{endpoint: "https://dr.example"},
        object: object
      }
    }
  end

  defp delete_job do
    %{
      kind: :cross_cluster_delete,
      payload: %{
        bucket: "bucket",
        key: "key",
        replica: %{endpoint: "https://dr.example"}
      }
    }
  end
end
