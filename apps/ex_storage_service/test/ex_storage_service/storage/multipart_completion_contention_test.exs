defmodule ExStorageService.Storage.MultipartCompletionContentionTest do
  use ExUnit.Case, async: false

  alias ExStorageService.Metadata
  alias ExStorageService.Metadata.Backend.Concord, as: Backend
  alias ExStorageService.Metadata.{BucketLifecycle, Keys, ListIndex, ObjectCommit}
  alias ExStorageService.Storage.Multipart

  defmodule InterleavingBackend do
    alias ExStorageService.Metadata.Backend.Concord, as: Backend

    for function <- [:get, :prefix_scan, :resolve_operation, :resolve_transaction] do
      def unquote(function)(key, opts),
        do: apply(Backend, unquote(function), [key, Keyword.delete(opts, :engine)])
    end

    def transaction(spec, opts) do
      action =
        Agent.get_and_update(Keyword.fetch!(opts, :engine), fn action -> {action, nil} end)

      if action, do: action.()
      Backend.transaction(spec, Keyword.delete(opts, :engine))
    end
  end

  defmodule CompletionConflictBackend do
    alias ExStorageService.Metadata.Backend.Concord, as: Backend

    def get(key, opts), do: Backend.get(key, Keyword.delete(opts, :engine))

    def transaction(%{success: [{:put, _key, value, _}]} = spec, opts) do
      phase = if Map.has_key?(value.completion, :hash), do: :composition, else: :freeze
      calls = if phase == :composition, do: :composition_calls, else: :freeze_calls
      remaining = if phase == :composition, do: :composition_remaining, else: :freeze_remaining

      action =
        Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
          state = Map.update!(state, calls, &(&1 + 1))

          cond do
            state.conflicts == 32 ->
              {:limit_exceeded, state}

            Map.fetch!(state, remaining) > 0 ->
              {:conflict,
               state
               |> Map.update!(remaining, &(&1 - 1))
               |> Map.update!(:conflicts, &(&1 + 1))}

            true ->
              {:commit, state}
          end
        end)

      case action do
        :limit_exceeded ->
          # Keep the original reset bug finite without cancellation/deadline changes.
          {:error, :test_conflict_budget_exceeded}

        :conflict ->
          {:mod_revision, bucket_key, :==, _} =
            Enum.find(spec.compare, fn
              {:mod_revision, "bucket:" <> _, :==, _} -> true
              _ -> false
            end)

          {:ok, %{value: bucket, mod_revision: observed}} = Backend.get(bucket_key)
          :ok = Backend.put(bucket_key, bucket)
          {:ok, %{mod_revision: changed}} = Backend.get(bucket_key)
          true = changed > observed
          {:ok, %{succeeded: false}} = Backend.transaction(spec, Keyword.delete(opts, :engine))

        :commit ->
          Backend.transaction(spec, Keyword.delete(opts, :engine))
      end
    end
  end

  setup do
    bucket = "completion-contention-#{System.unique_integer([:positive, :monotonic])}"
    :ok = Metadata.create_bucket(bucket)
    {:ok, upload} = Multipart.init_upload(bucket, "unrelated")
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "part")
    {:ok, %{value: bucket_meta}} = Backend.get("bucket:#{bucket}")
    index_key = ListIndex.root_key(bucket, BucketLifecycle.generation(bucket_meta))
    %{bucket: bucket, upload: upload, etag: etag, index_key: index_key}
  end

  for transition <- [:prepare, :release] do
    @transition transition
    test "unrelated #{@transition} preserves an already planned object publication", context do
      %{bucket: bucket, upload: upload, etag: etag, index_key: index_key} = context

      prepared =
        if @transition == :release do
          {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
          prepared
        end

      {:ok, %{uploads: summaries}} = Multipart.list_uploads(bucket)
      owner = self()

      action = fn ->
        {:ok, before_index} = Backend.get(index_key)
        {:ok, before_bucket} = Backend.get("bucket:#{bucket}")

        case @transition do
          :prepare ->
            assert {:ok, _} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
            assert {:error, :not_found} = Multipart.store_part(bucket, upload, 1, "late")

          :release ->
            assert :ok = Multipart.release_completion(bucket, upload, prepared)
        end

        {:ok, after_index} = Backend.get(index_key)
        {:ok, after_bucket} = Backend.get("bucket:#{bucket}")

        send(
          owner,
          {:completion_transition, before_index, after_index, before_bucket, after_bucket}
        )
      end

      {:ok, interleaving} = Agent.start_link(fn -> action end)
      on_exit(fn -> if Process.alive?(interleaving), do: Agent.stop(interleaving) end)

      assert {:ok, %{version_id: version_id}} =
               ObjectCommit.put(
                 bucket,
                 "publication",
                 %{
                   content_hash: "independent-publication",
                   size: 1,
                   etag: "independent-etag"
                 },
                 backend: InterleavingBackend,
                 engine: interleaving,
                 max_attempts: 1
               )

      assert {:ok, %{version_id: ^version_id, etag: "independent-etag"}} =
               ObjectCommit.get_head(bucket, "publication")

      assert_received {:completion_transition, before_index, after_index, before_bucket,
                       after_bucket}

      assert before_index == after_index
      assert before_bucket == after_bucket
      assert {:ok, %{uploads: ^summaries}} = Multipart.list_uploads(bucket)
      assert {:ok, [%{part_number: 1, etag: ^etag}]} = Multipart.list_parts(bucket, upload)
    end
  end

  test "completion preparation and release reject a rebuilding index", context do
    %{bucket: bucket, upload: upload, etag: etag, index_key: index_key} = context
    {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    {:ok, %{value: root}} = Backend.get(index_key)
    :ok = Backend.put(index_key, Map.put(root, :state, :building))

    assert {:error, :listing_index_unavailable} =
             Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])

    assert {:error, :listing_index_unavailable} =
             Multipart.release_completion(bucket, upload, prepared)

    assert {:ok, %{status: :completing}} = Multipart.get_upload(bucket, upload)
  end

  test "completion preparation and release cannot cross bucket generations", context do
    %{bucket: bucket, upload: upload, etag: etag} = context
    {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    {:ok, %{value: old_bucket}} = Backend.get("bucket:#{bucket}")
    :ok = Backend.put("bucket:#{bucket}", Map.put(old_bucket, :generation, "replacement"))

    :ok =
      Backend.put(ListIndex.root_key(bucket, "replacement"), ListIndex.empty("replacement"))

    assert {:error, :bucket_generation_changed} =
             Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])

    assert {:error, :bucket_generation_changed} =
             Multipart.release_completion(bucket, upload, prepared)

    assert {:ok, %{value: %{status: :completing}}} =
             Backend.get("mpu:#{bucket}:#{upload}")

    assert {:ok, nil} = Backend.get(Keys.multipart_upload(upload))
  end

  test "composition compare losses consume one finite preparation budget", context do
    engine = conflict_engine(0, 32)

    assert {:error, :multipart_completion_conflict} =
             Multipart.prepare_complete_upload(
               context.bucket,
               context.upload,
               [{1, context.etag}],
               backend: CompletionConflictBackend,
               engine: engine
             )

    assert %{freeze_calls: 1, composition_calls: 32, conflicts: 32} = Agent.get(engine, & &1)
    assert_retryable_without_publication(context)
  end

  test "successful freeze retains budget already consumed by compare losses", context do
    engine = conflict_engine(31, 1)

    assert {:error, :multipart_completion_conflict} =
             Multipart.prepare_complete_upload(
               context.bucket,
               context.upload,
               [{1, context.etag}],
               backend: CompletionConflictBackend,
               engine: engine
             )

    assert %{freeze_calls: 32, composition_calls: 1, conflicts: 32} = Agent.get(engine, & &1)
    assert_retryable_without_publication(context)
  end

  test "successful freeze and composition do not consume the last conflict allowance", context do
    engine = conflict_engine(31, 0)

    assert {:ok, prepared} =
             Multipart.prepare_complete_upload(
               context.bucket,
               context.upload,
               [{1, context.etag}],
               backend: CompletionConflictBackend,
               engine: engine
             )

    assert %{freeze_calls: 32, composition_calls: 1, conflicts: 31} = Agent.get(engine, & &1)
    assert prepared.content_hash == hash_part()
    assert {:ok, nil} = Backend.get(Keys.object_head(context.bucket, "unrelated"))
    assert {:ok, nil} = Backend.get("obj:#{context.bucket}:unrelated")
  end

  defp conflict_engine(freeze, composition) do
    {:ok, engine} =
      Agent.start_link(fn ->
        %{
          freeze_remaining: freeze,
          composition_remaining: composition,
          freeze_calls: 0,
          composition_calls: 0,
          conflicts: 0
        }
      end)

    on_exit(fn -> if Process.alive?(engine), do: Agent.stop(engine) end)
    engine
  end

  defp assert_retryable_without_publication(context) do
    assert {:ok, nil} = Backend.get(Keys.object_head(context.bucket, "unrelated"))
    assert {:ok, nil} = Backend.get("obj:#{context.bucket}:unrelated")

    assert {:ok, %{status: :completing, completion: completion} = upload} =
             Multipart.get_upload(context.bucket, context.upload)

    refute Map.has_key?(completion, :hash)
    refute Map.has_key?(upload, :completion_result)

    assert {:ok, [%{part_number: 1, etag: etag}]} =
             Multipart.list_parts(context.bucket, context.upload)

    assert etag == context.etag
    # Original public arity remains compatible and can retry the same upload.
    assert {:ok, prepared} =
             Multipart.prepare_complete_upload(context.bucket, context.upload, [{1, context.etag}])

    assert prepared.content_hash == hash_part()
    assert File.read!(ExStorageService.Storage.CAS.blob_path(prepared.content_hash)) == "part"
  end

  defp hash_part, do: Base.encode16(:crypto.hash(:sha256, "part"), case: :lower)
end
