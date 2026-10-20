defmodule ExStorageService.Storage.MultipartHardeningTest do
  use ExUnit.Case, async: false

  alias ExStorageService.{Metadata, ObjectService}
  alias ExStorageService.BlobStore.Source
  alias ExStorageService.Metadata.{Backend.Concord, BucketLifecycle, ListIndex}
  alias ExStorageService.Storage.{Multipart, Versioning}

  setup do
    bucket = "mpu-hardening-#{System.unique_integer([:positive])}"
    :ok = Metadata.create_bucket(bucket)
    %{bucket: bucket}
  end

  test "four concurrent initiations preserve every upload and exact index count", %{
    bucket: bucket
  } do
    owner = self()
    barrier = make_ref()

    tasks =
      for number <- 1..4 do
        Task.async(fn ->
          send(owner, {:initiation_ready, barrier, self()})

          receive do
            {:initiate, ^barrier} ->
              Multipart.init_upload(bucket, "concurrent-#{number}", %{metadata: %{"n" => number}})
          after
            5_000 -> raise "multipart initiation barrier timed out"
          end
        end)
      end

    for _ <- tasks do
      assert_receive {:initiation_ready, ^barrier, _pid}, 5_000
    end

    Enum.each(tasks, &send(&1.pid, {:initiate, barrier}))
    results = Task.await_many(tasks, 15_000)

    assert Enum.all?(results, &match?({:ok, _}, &1)), inspect(results)
    upload_ids = Enum.map(results, fn {:ok, id} -> id end)
    assert length(Enum.uniq(upload_ids)) == 4
    assert {:ok, %{active_upload_count: 4}} = ListIndex.status(bucket)
    assert {:ok, %{uploads: uploads, is_truncated: false}} = Multipart.list_uploads(bucket)
    assert Enum.sort(Enum.map(uploads, & &1.upload_id)) == Enum.sort(upload_ids)

    for {upload_id, number} <- Enum.zip(upload_ids, 1..4) do
      assert {:ok, %{key: key, attributes: %{metadata: %{"n" => ^number}}}} =
               Multipart.get_upload(bucket, upload_id)

      assert key == "concurrent-#{number}"
    end
  end

  test "multipart index counts and part roots follow overwrite, freeze, completion and abort", %{
    bucket: bucket
  } do
    assert {:ok, upload} = Multipart.init_upload(bucket, "indexed")
    assert {:ok, %{active_upload_count: 1}} = ListIndex.status(bucket)
    assert {:ok, _} = Multipart.store_part(bucket, upload, 1, "old")
    assert {:ok, etag} = Multipart.store_part(bucket, upload, 1, "new")

    assert {:ok, %{parts_count: 1, parts_index_complete: true}} =
             Multipart.get_upload(bucket, upload)

    assert {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    assert {:ok, %{active_upload_count: 1}} = ListIndex.status(bucket)
    assert :ok = Multipart.finalize_complete_upload(bucket, upload, prepared)
    assert {:ok, %{active_upload_count: 0}} = ListIndex.status(bucket)

    assert {:ok, %{parts_count: 0, parts_root: nil, status: :completed}} =
             Multipart.get_upload(bucket, upload)

    assert {:ok, []} = Multipart.list_parts(bucket, upload)

    assert {:ok, aborted} = Multipart.init_upload(bucket, "aborted")
    assert {:ok, _} = Multipart.store_part(bucket, aborted, 1, "part")
    assert :ok = Multipart.abort_upload(bucket, aborted)
    assert {:ok, %{active_upload_count: 0}} = ListIndex.status(bucket)
    assert {:error, :not_found} = Multipart.get_upload(bucket, aborted)
  end

  test "a rebuilding index rejects part streaming before consuming its source", %{bucket: bucket} do
    assert {:ok, upload} = Multipart.init_upload(bucket, "gated")

    assert {:ok, %{value: bucket_meta}} =
             Concord.get("bucket:#{bucket}", consistency: :linearizable)

    root_key = ListIndex.root_key(bucket, BucketLifecycle.generation(bucket_meta))
    assert {:ok, %{value: root}} = Concord.get(root_key, consistency: :linearizable)
    assert :ok = Concord.put(root_key, Map.put(root, :state, :building), [])
    on_exit(fn -> Concord.put(root_key, root, []) end)

    reader = fn state ->
      send(self(), :part_source_consumed)
      {:ok, "part", state + 1}
    end

    assert {:error, :listing_index_unavailable, 0} =
             Multipart.store_part_from_reader(bucket, upload, 1, reader, 0, max_size: 4)

    refute_received :part_source_consumed
  end

  test "parts paginate by marker with protocol bounds", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "key")

    for number <- [9, 1, 3],
        do: assert({:ok, _} = Multipart.store_part(bucket, upload, number, "part"))

    assert {:ok, %{parts: first, next_part_number_marker: 3, is_truncated: true}} =
             Multipart.list_parts(bucket, upload, max_parts: 2)

    assert Enum.map(first, & &1.part_number) == [1, 3]

    assert {:ok, %{parts: [%{part_number: 9}], is_truncated: false}} =
             Multipart.list_parts(bucket, upload, part_number_marker: 3, max_parts: 2)

    assert {:ok, %{parts: [], is_truncated: false}} =
             Multipart.list_parts(bucket, upload, max_parts: 0)

    assert {:error, :invalid_pagination} = Multipart.list_parts(bucket, upload, max_parts: 1001)

    assert {:error, :invalid_pagination} =
             Multipart.list_parts(bucket, upload, part_number_marker: -1)
  end

  test "upload pages preserve equal-key upload markers and common prefixes", %{bucket: bucket} do
    {:ok, a} = Multipart.init_upload(bucket, "same")
    {:ok, b} = Multipart.init_upload(bucket, "same")
    {:ok, _} = Multipart.init_upload(bucket, "tree/one")
    {:ok, _} = Multipart.init_upload(bucket, "tree/two")
    [first, second] = Enum.sort([a, b])

    assert {:ok,
            %{
              uploads: [%{upload_id: ^first}],
              is_truncated: true,
              next_key_marker: "same",
              next_upload_id_marker: ^first
            }} =
             Multipart.list_uploads(bucket, max_uploads: 1, delimiter: "/")

    assert {:ok,
            %{uploads: [%{upload_id: ^second}], common_prefixes: ["tree/"], is_truncated: false}} =
             Multipart.list_uploads(bucket,
               key_marker: "same",
               upload_id_marker: first,
               max_uploads: 2,
               delimiter: "/"
             )

    assert {:ok, %{uploads: [], common_prefixes: ["tree/"]}} =
             Multipart.list_uploads(bucket, prefix: "tree", delimiter: "/")
  end

  test "freeze fences late parts and abort, and a released condition can retry", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "key")
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "first")
    assert {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    assert {:error, :not_found} = Multipart.store_part(bucket, upload, 1, "late")
    assert {:error, :multipart_completion_conflict} = Multipart.abort_upload(bucket, upload)
    assert :ok = Multipart.release_completion(bucket, upload, prepared)
    assert {:ok, replacement} = Multipart.store_part(bucket, upload, 1, "replacement")
    assert replacement != etag
    assert {:ok, changed} = Multipart.prepare_complete_upload(bucket, upload, [{1, replacement}])
    assert changed.content_hash != prepared.content_hash
    assert changed.multipart_completion.token != prepared.multipart_completion.token
  end

  test "completion result and initiation attributes survive a finalization failure", %{
    bucket: bucket
  } do
    :ok = Versioning.set_versioning(bucket, :enabled)
    bytes = "durable bytes"
    hash = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)

    attrs = %{
      content_type: "application/x-init",
      content_encoding: "gzip",
      cache_control: "private",
      content_language: "fr",
      content_disposition: "attachment",
      expires: "Thu, 01 Jan 2037 00:00:00 GMT",
      metadata: %{"owner" => "initial"},
      tags: %{"stage" => "initial"}
    }

    {:ok, upload} = Multipart.init_upload(bucket, "key", attrs)
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, bytes)
    {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    assert prepared.content_hash == hash
    ready = %{hash: prepared.content_hash, size: prepared.size, etag: prepared.etag}

    opts = [
      metadata_opts: [
        operation_id: "multipart-complete:#{upload}:#{prepared.content_hash}",
        multipart_completion: prepared.multipart_completion
      ]
    ]

    assert {:ok, result} =
             ObjectService.commit_existing_blob(bucket, "key", ready, attrs, opts)

    version = result.version_id
    assert is_binary(version) and version != "null"

    assert {:error, :injected_finalize_failure} =
             Multipart.finalize_complete_upload(bucket, upload, prepared,
               before_finalize: fn -> {:error, :injected_finalize_failure} end
             )

    assert {:ok, [%{version_id: ^version}]} = Versioning.list_versions(bucket, "key")
    assert {:ok, %{source: before_recovery}} = ObjectService.get(bucket, "key", version, [])

    assert {:ok, ^bytes} =
             Source.reduce(before_recovery, "", fn chunk, acc -> {:cont, acc <> chunk} end)

    assert {:ok, recovered} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    assert recovered.completion_result.version_id == result.version_id

    assert {:ok, %{version_id: ^version}} =
             ObjectService.commit_existing_blob(bucket, "key", ready, attrs, opts)

    assert :ok = Multipart.finalize_complete_upload(bucket, upload, recovered)
    assert {:ok, %{metadata: metadata}} = ObjectService.head(bucket, "key")
    for {field, value} <- attrs, do: assert(Map.get(metadata, field) == value)
    assert metadata.content_hash == hash

    assert {:ok, %{version_id: ^version, source: source}} =
             ObjectService.get(bucket, "key", nil, [])

    assert {:ok, ^bytes} =
             Source.reduce(source, "", fn chunk, acc -> {:cont, acc <> chunk} end)

    assert {:ok, [%{version_id: ^version}]} = Versioning.list_versions(bucket, "key")
    assert {:ok, completed} = Multipart.get_upload(bucket, upload)
    assert completed.status == :completed
    assert completed.completion_result.version_id == version
  end

  test "invalid completion part lists never freeze an upload", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "key")
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "bytes")
    assert {:error, :invalid_part} = Multipart.prepare_complete_upload(bucket, upload, [])

    assert {:error, :invalid_part_order} =
             Multipart.prepare_complete_upload(bucket, upload, [{1, etag}, {1, etag}])

    assert {:error, :invalid_part_order} =
             Multipart.prepare_complete_upload(bucket, upload, [{2, etag}, {1, etag}])

    assert {:ok, %{status: :initiated}} = Multipart.get_upload(bucket, upload)
  end

  test "a reader started before completion cannot replace the frozen part", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "frozen")
    {:ok, etag} = Multipart.store_part(bucket, upload, 1, "original")
    parent = self()

    reader = fn :start ->
      send(parent, {:before_completion, self()})

      receive do
        :continue -> {:ok, "late replacement", :done}
      end
    end

    task =
      Task.async(fn ->
        Multipart.store_part_from_reader(bucket, upload, 1, reader, :start, max_size: 100)
      end)

    assert_receive {:before_completion, worker}
    assert {:ok, prepared} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
    send(worker, :continue)
    assert {:error, :not_found, :done} = Task.await(task)
    assert prepared.content_hash == Base.encode16(:crypto.hash(:sha256, "original"), case: :lower)
    assert {:ok, [%{etag: ^etag}]} = Multipart.list_parts(bucket, upload)
  end

  test "part number and count limits reject before freezing or publishing", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "limits")

    for number <- [0, -1, 10_001] do
      assert {:error, :invalid_part} = Multipart.store_part(bucket, upload, number, "invalid")
    end

    assert {:ok, []} = Multipart.list_parts(bucket, upload)

    assert {:error, :invalid_part} =
             Multipart.prepare_complete_upload(
               bucket,
               upload,
               List.duplicate({1, "etag"}, 10_001)
             )

    assert {:ok, %{status: :initiated}} = Multipart.get_upload(bucket, upload)
    assert {:ok, etag} = Multipart.store_part(bucket, upload, 1, "")
    assert {:ok, %{size: 0}} = Multipart.prepare_complete_upload(bucket, upload, [{1, etag}])
  end

  test "a reader finishing after abort cannot publish a part", %{bucket: bucket} do
    {:ok, upload} = Multipart.init_upload(bucket, "key")
    parent = self()

    reader = fn
      :start ->
        send(parent, {:reader_waiting, self()})

        receive do
          :continue -> {:ok, "late bytes", :done}
        end
    end

    task =
      Task.async(fn ->
        Multipart.store_part_from_reader(bucket, upload, 1, reader, :start, max_size: 100)
      end)

    assert_receive {:reader_waiting, worker}
    assert :ok = Multipart.abort_upload(bucket, upload)
    send(worker, :continue)
    assert {:error, :not_found, :done} = Task.await(task)
    assert {:error, :not_found} = Multipart.list_parts(bucket, upload)
  end
end
