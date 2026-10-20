defmodule ExStorageService.CloudCache.WriteThroughTest do
  use ExUnit.Case, async: true
  alias ExStorageService.CloudCache.{Config, WriteThrough}
  alias ExStorageService.BlobStore.StagedBlob

  defmodule Backend do
    def put(key, record, _opts) do
      revision = Process.get(:revision, 0) + 1
      Process.put(:revision, revision)
      Process.put({__MODULE__, key}, %{value: record, mod_revision: revision})
      if String.starts_with?(key, "obj:"), do: Process.put(:local_metadata, record)
      :ok
    end

    def get(key, _opts), do: {:ok, Process.get({__MODULE__, key})}

    def delete(key, _opts) do
      Process.delete({__MODULE__, key})
      :ok
    end

    def list_page(prefix, _cursor, limit, _opts) do
      entries =
        for {{__MODULE__, key}, record} <- Process.get(),
            String.starts_with?(key, prefix),
            do: Map.put(record, :key, key)

      {:ok, %{entries: Enum.take(entries, limit), next_cursor: nil}}
    end

    def transaction(spec, _opts) do
      case Process.delete(:before_transaction_compare) do
        nil -> :ok
        hook -> hook.(spec)
      end

      passes =
        Enum.all?(spec.compare, fn
          {:exists, key, :==, value} ->
            Process.get({__MODULE__, key}) != nil == value

          {:mod_revision, key, :==, revision} ->
            case Process.get({__MODULE__, key}) do
              %{mod_revision: ^revision} -> true
              _ -> false
            end
        end)

      if passes do
        Enum.each(spec.success, fn {:put, key, value, _} -> put(key, value, []) end)
      end

      {:ok, %{succeeded: passes}}
    end
  end

  defmodule DelayedJournalBackend do
    defdelegate get(key, opts), to: Backend

    def put(key, record, _opts) do
      Process.put(:delayed_journal, {key, record})
      {:error, :timeout}
    end
  end

  defmodule Client do
    def put_file(_config, _key, path, _type, metadata, opts) do
      Process.put(:upstream_calls, Process.get(:upstream_calls, 0) + 1)
      Process.put(:upstream_identity, %{metadata: metadata, content_length: opts[:size]})
      assert_valid_stage(path, opts)
      before_result = Process.get(:before_upstream_result, fn -> :ok end)
      :ok = before_result.()
      Process.get(:upstream_result, :ok)
    end

    defp assert_valid_stage(path, opts) do
      bytes = File.read!(path)
      true = byte_size(bytes) == opts[:size]
      true = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == opts[:sha256]
    end

    def head_object(_config, _key), do: {:ok, Process.get(:upstream_identity)}
  end

  defmodule ContendedBackend do
    defdelegate put(key, value, opts), to: Backend
    defdelegate get(key, opts), to: Backend
    defdelegate delete(key, opts), to: Backend
    defdelegate list_page(prefix, cursor, limit, opts), to: Backend

    def transaction(spec, opts) do
      remaining = Process.get(:bucket_conflicts_remaining, 0)

      local_commit? =
        Enum.any?(spec.success, fn
          {:put, "obj:local:key", _, _} -> true
          _ -> false
        end)

      if local_commit? and remaining > 0 do
        {:ok, %{value: bucket, mod_revision: observed}} = Backend.get("bucket:local", [])
        :ok = Backend.put("bucket:local", bucket, [])
        {:ok, %{mod_revision: changed}} = Backend.get("bucket:local", [])
        Process.put(:bucket_conflicts_remaining, remaining - 1)
        send(self(), {:unrelated_bucket_update, observed, changed})
      end

      Backend.transaction(spec, opts)
    end
  end

  defmodule BusyCache do
    def put_file(_bucket, _key, _path, _hash, _size, _config, opts) do
      send(self(), {:cache_generation, opts[:bucket_generation]})
      {:error, :cache_busy}
    end
  end

  defmodule FailedCache do
    def put_file(_bucket, _key, _path, _hash, _size, _config),
      do: {:error, :injected_cache_failure}
  end

  setup %{tmp_dir: directory} do
    bytes = "validated bytes"
    path = Path.join(directory, "stage")
    File.write!(path, bytes)

    staged = %StagedBlob{
      path: path,
      hash: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
      etag: Base.encode16(:crypto.hash(:md5, bytes), case: :lower),
      size: byte_size(bytes)
    }

    config = %Config{
      enabled: true,
      provider: :s3_compat,
      endpoint: "http://isolated.invalid",
      bucket: "upstream",
      cache_enabled: false
    }

    :ok = Backend.put("bucket:local", %{generation: "original"}, [])

    :ok =
      Backend.put(
        ExStorageService.Metadata.ListIndex.root_key("local", "original"),
        ExStorageService.Metadata.ListIndex.empty("original"),
        []
      )

    opts = [
      backend: Backend,
      client: Client,
      before_local_commit: fn -> Process.get(:metadata_result, :ok) end,
      get_config: fn "local" -> {:ok, config} end
    ]

    {:ok, staged: staged, config: config, opts: opts}
  end

  @moduletag :tmp_dir

  test "busy optional cache cannot fail a durably acknowledged upstream write", context do
    context = %{
      context
      | config: %{context.config | cache_enabled: true},
        opts: Keyword.put(context.opts, :cache, BusyCache)
    }

    assert {:ok, meta} = put(context)
    assert meta.content_hash == context.staged.hash
    assert Process.get(:upstream_calls) == 1
    assert_received {:cache_generation, "original"}
    refute File.exists?(context.staged.path)
    assert {:ok, %{entries: []}} = Backend.list_page("ess:gateway-write:", nil, 10, [])
  end

  test "four unrelated bucket revision changes cannot fail an acknowledged write", context do
    Process.put(:bucket_conflicts_remaining, 4)
    context = %{context | opts: Keyword.put(context.opts, :backend, ContendedBackend)}

    result = put(context)

    assert Process.get(:bucket_conflicts_remaining) == 0

    for _ <- 1..4 do
      assert_received {:unrelated_bucket_update, observed, changed}
      assert changed > observed
    end

    assert Process.get(:upstream_calls) == 1
    assert {:ok, %{value: %{generation: "original"}}} = Backend.get("bucket:local", [])
    assert {:ok, meta} = result
    assert meta.content_hash == context.staged.hash
    assert meta.size == context.staged.size
    assert meta.etag == context.staged.etag
    assert meta.gateway_operation_id == "operation"
    assert {:ok, %{value: ^meta}} = Backend.get("obj:local:key", [])

    assert Process.get(:upstream_identity) == %{
             metadata: %{
               "ess-operation-id" => "operation",
               "ess-content-sha256" => context.staged.hash
             },
             content_length: context.staged.size
           }

    refute File.exists?(context.staged.path)
    assert {:ok, %{entries: []}} = Backend.list_page("ess:gateway-write:", nil, 10, [])
  end

  test "upstream success followed by local commit failure retains a recoverable stage", context do
    capture_uncertainty()
    Process.put(:metadata_result, {:error, :injected_commit_failure})
    assert {:error, {:local_reconciliation_required, :injected_commit_failure}} = put(context)

    assert_received {:operation_uncertainty,
                     %{source: :gateway_write, outcome: :reconciliation_required}}

    assert File.exists?(context.staged.path)
    Process.put(:metadata_result, :ok)

    assert {:ok, %{results: [{"operation", {:ok, meta}}], next_cursor: nil}} =
             WriteThrough.reconcile_page(nil, 10, context.opts)

    assert meta.content_hash == context.staged.hash
    assert Process.get(:upstream_calls) == 1
    refute File.exists?(context.staged.path)
  end

  test "unknown upstream outcomes are resolved by identity without repeating PUT", context do
    capture_uncertainty()
    Process.put(:upstream_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    assert_received {:operation_uncertainty, %{source: :gateway_write, outcome: :unknown}}

    assert {:ok, %{operation_id: "operation", state: :unknown}} =
             ExStorageService.Operations.OperationStatus.get(:gateway_write, "operation",
               backend: Backend
             )

    assert File.exists?(context.staged.path)

    assert {:ok, %{results: [{"operation", {:ok, _}}]}} =
             WriteThrough.reconcile_page(nil, 10, context.opts)

    assert Process.get(:upstream_calls) == 1

    assert {:ok, nil} =
             ExStorageService.Operations.OperationStatus.get(:gateway_write, "operation",
               backend: Backend
             )
  end

  test "late upstream timeout cannot resurrect a journal finalized by reconciliation", context do
    Process.put(:upstream_result, {:error, :timeout})

    Process.put(:before_upstream_result, fn ->
      assert {:ok, %{results: [{"operation", {:ok, meta}}], next_cursor: nil}} =
               WriteThrough.reconcile_page(nil, 10, context.opts)

      assert meta.content_hash == context.staged.hash
      assert {:ok, %{entries: []}} = Backend.list_page("ess:gateway-write:", nil, 10, [])
      refute File.exists?(context.staged.path)
      send(self(), {:reconciled_before_timeout, meta})
      :ok
    end)

    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    assert_received {:reconciled_before_timeout, meta}
    assert {:ok, %{value: ^meta}} = Backend.get("obj:local:key", [])
    assert {:ok, %{entries: []}} = Backend.list_page("ess:gateway-write:", nil, 10, [])
    refute File.exists?(context.staged.path)
    assert Process.get(:upstream_calls) == 1
  end

  test "late upstream timeout cannot downgrade a local commit retained after cache failure",
       context do
    config = %{context.config | cache_enabled: true}

    context = %{
      context
      | config: config,
        opts:
          context.opts
          |> Keyword.put(:cache, FailedCache)
          |> Keyword.put(:get_config, fn "local" -> {:ok, config} end)
    }

    Process.put(:upstream_result, {:error, :timeout})

    Process.put(:before_upstream_result, fn ->
      assert {:ok,
              %{
                results: [
                  {"operation",
                   {:error, {:local_reconciliation_required, :injected_cache_failure}}}
                ],
                next_cursor: nil
              }} = WriteThrough.reconcile_page(nil, 10, context.opts)

      assert {:ok, %{entries: [%{value: %{state: :local_committed}}]}} =
               Backend.list_page("ess:gateway-write:", nil, 10, [])

      send(self(), :local_commit_retained_before_timeout)
      :ok
    end)

    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    assert_received :local_commit_retained_before_timeout

    assert {:ok, %{entries: [%{value: %{state: :local_committed, path: path}}]}} =
             Backend.list_page("ess:gateway-write:", nil, 10, [])

    hash = context.staged.hash

    assert {:ok, %{value: %{gateway_operation_id: "operation", content_hash: ^hash}}} =
             Backend.get("obj:local:key", [])

    assert File.read!(path) == "validated bytes"
    assert Process.get(:upstream_calls) == 1
  end

  test "unknown marking loses its compare if reconciliation finishes after the journal read",
       context do
    Process.put(:upstream_result, {:error, :timeout})

    Process.put(:before_upstream_result, fn ->
      Process.put(:before_transaction_compare, fn _spec ->
        assert {:ok, %{results: [{"operation", {:ok, meta}}], next_cursor: nil}} =
                 WriteThrough.reconcile_page(nil, 10, context.opts)

        refute File.exists?(context.staged.path)
        send(self(), {:reconciled_before_unknown_compare, meta})
      end)

      :ok
    end)

    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    assert_received {:reconciled_before_unknown_compare, meta}
    assert {:ok, %{value: ^meta}} = Backend.get("obj:local:key", [])
    assert {:ok, %{entries: []}} = Backend.list_page("ess:gateway-write:", nil, 10, [])
    refute File.exists?(context.staged.path)
    assert Process.get(:upstream_calls) == 1
  end

  test "mismatching upstream identity remains unresolved", context do
    Process.put(:upstream_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = put(context)

    Process.put(:upstream_identity, %{
      metadata: %{"ess-operation-id" => "another", "ess-content-sha256" => context.staged.hash},
      content_length: context.staged.size
    })

    assert {:ok, %{results: [{"operation", {:error, :upstream_identity_changed}}]}} =
             WriteThrough.reconcile_page(nil, 10, context.opts)

    assert File.exists?(context.staged.path)
    refute Process.get(:local_metadata)
    assert Process.get(:upstream_calls) == 1
  end

  test "definitive upstream rejection cannot publish local state", context do
    Process.put(:upstream_result, {:error, {:http_error, 403}})
    assert {:error, {:http_error, 403}} = put(context)
    refute File.exists?(context.staged.path)
    refute Process.get(:local_metadata)
    assert {:ok, %{results: []}} = WriteThrough.reconcile_page(nil, 10, context.opts)
  end

  test "reconciliation cannot publish into a deleted and recreated bucket", context do
    Process.put(:upstream_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    :ok = Backend.put("bucket:local", %{generation: "new-generation"}, [])

    assert {:ok,
            %{
              results: [
                {"operation",
                 {:error, {:local_reconciliation_required, :bucket_generation_changed}}}
              ]
            }} = WriteThrough.reconcile_page(nil, 10, context.opts)

    refute Process.get(:local_metadata)
    assert File.exists?(context.staged.path)
  end

  test "reconciliation cannot replace a newer local object identity", context do
    Process.put(:upstream_result, {:error, :timeout})
    assert {:error, {:upstream_unknown, :timeout}} = put(context)
    :ok = Backend.put("obj:local:key", %{etag: "newer"}, [])

    assert {:ok,
            %{
              results: [
                {"operation",
                 {:error, {:local_reconciliation_required, :local_metadata_conflict}}}
              ]
            }} = WriteThrough.reconcile_page(nil, 10, context.opts)

    assert %{etag: "newer"} = Process.get(:local_metadata)
    assert File.exists?(context.staged.path)
  end

  test "failed protection data sync cannot journal or publish an upstream write", context do
    alias ExStorageService.BlobStore.LocalCAS

    blob_opts = [
      tmp_dir: context.tmp_dir,
      managed_staging: [instance: :failed_gateway_sync, node_id: "test"],
      faults: %{protection_data_sync: :injected_data_sync}
    ]

    {:ok, staged} = LocalCAS.stage("unpublished gateway bytes", blob_opts)

    context = %{
      context
      | staged: staged,
        opts:
          context.opts
          |> Keyword.put(:backend, DelayedJournalBackend)
          |> Keyword.put(:blob_store_opts, blob_opts)
    }

    assert {:error, {:staging_protection, :injected_data_sync}} = put(context)
    refute Process.get(:delayed_journal)
    refute Process.get(:upstream_calls)
    refute Process.get(:local_metadata)
    assert File.read!(staged.path) == "unpublished gateway bytes"

    assert Enum.sort(File.ls!(context.tmp_dir)) ==
             Enum.sort([Path.basename(staged.path), "stage"])
  end

  test "journal timeout retains protected bytes even when the submission commits later",
       context do
    alias ExStorageService.BlobStore.LocalCAS
    alias ExStorageService.Storage.StagingRecovery

    blob_opts = [
      tmp_dir: context.tmp_dir,
      managed_staging: [instance: :late_gateway, node_id: "test"]
    ]

    {:ok, staged} = LocalCAS.stage("late gateway bytes", blob_opts)

    context = %{
      context
      | staged: staged,
        opts:
          context.opts
          |> Keyword.put(:backend, DelayedJournalBackend)
          |> Keyword.put(:blob_store_opts, blob_opts)
    }

    assert {:error, :timeout} = put(context)
    {key, record} = Process.get(:delayed_journal)
    assert String.starts_with?(Path.basename(record.path), "ess1-g-")
    refute record.path == staged.path
    assert {:ok, nil} = Backend.get(key, [])
    {:ok, state} = StagingRecovery.new(blob_opts ++ [backend: Backend])
    assert {:done, state} = finish_recovery(state)
    assert state.deleted == 0
    :ok = Backend.put(key, record, [])
    assert {:ok, %{value: ^record}} = Backend.get(key, [])
    assert File.read!(record.path) == "late gateway bytes"
  end

  defp finish_recovery(%{candidate: candidate} = state) when candidate != nil,
    do: state |> ExStorageService.Storage.StagingRecovery.step() |> finish_recovery()

  defp finish_recovery(state) do
    receive do
      message ->
        case ExStorageService.Storage.StagingRecovery.message(state, message) do
          {:ok, state} -> finish_recovery(state)
          {:ignore, state} -> finish_recovery(state)
          other -> other
        end
    after
      3000 ->
        ExStorageService.Storage.StagingRecovery.close(state)
        flunk("recovery did not complete")
    end
  end

  defp put(context),
    do:
      WriteThrough.put(
        "local",
        "key",
        context.staged,
        context.config,
        %{content_type: "text/plain", metadata: %{}},
        "operation",
        context.opts
      )

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
