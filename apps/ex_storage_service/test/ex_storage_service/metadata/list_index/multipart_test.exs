defmodule ExStorageService.Metadata.ListIndex.MultipartTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{Keys, ListIndex}
  alias ExStorageService.Metadata.ListIndex.Multipart

  defmodule ExactBackend do
    def get(key, opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :engine), fn state ->
        state = %{state | reads: state.reads + 1}

        state =
          if state.churn and String.contains?(key, ":node:"),
            do: update_in(state.records[state.root_key].mod_revision, &(&1 + 1)),
            else: state

        {{:ok, state.records[key]}, state}
      end)
    end

    def commit(engine, spec) do
      Agent.get_and_update(engine, fn state ->
        if Enum.all?(spec.compare, &matches?(&1, state.records)) do
          revision = state.revision + 1

          records =
            Enum.reduce(spec.success, state.records, fn
              {:put, key, value, _}, records ->
                Map.put(records, key, %{value: value, mod_revision: revision})

              {:delete, {:key, key}, _}, records ->
                Map.delete(records, key)
            end)

          {:ok, %{state | records: records, revision: revision}}
        else
          {{:error, :compare_failed}, state}
        end
      end)
    end

    defp matches?({:exists, key, :==, expected}, records),
      do: Map.has_key?(records, key) == expected

    defp matches?({:mod_revision, key, :==, expected}, records),
      do: match?(%{mod_revision: ^expected}, records[key])
  end

  setup do
    generation = "multipart-index-generation"
    root_key = ListIndex.root_key("bucket", generation)

    records = %{
      "bucket:bucket" => %{value: %{name: "bucket", generation: generation}, mod_revision: 1},
      root_key => %{value: ListIndex.empty(generation), mod_revision: 1}
    }

    {:ok, engine} =
      Agent.start_link(fn ->
        %{records: records, revision: 1, root_key: root_key, reads: 0, churn: false}
      end)

    %{engine: engine, root_key: root_key, opts: [backend: ExactBackend, engine: engine]}
  end

  test "legacy and v2 uploads initialize empty trees and parts page in numeric order", context do
    for {id, legacy?} <- [{"legacy", true}, {"current", false}] do
      key = upload_key(id, legacy?)
      create(context, id, "object", legacy?)
      upload = value(context, key)
      assert upload.parts_index_complete
      assert upload.parts_count == 0

      assert {:ok, %{parts: [], is_truncated: false}} =
               Multipart.parts_page("bucket", id, context.opts)

      part(context, id, key, 10, "ten", legacy?)
      part(context, id, key, 2, "two", legacy?)
      part(context, id, key, 2, "replacement", legacy?)
      assert value(context, key).parts_count == 2
      assert {:ok, page} = Multipart.parts_page("bucket", id, context.opts ++ [max_parts: 1])
      assert [%{part_number: 2, etag: "replacement"}] = page.parts
      assert page.is_truncated
      assert page.next_part_number_marker == 2
      assert page.checksum_algorithm == "SHA256"
      assert page.checksum_type == "COMPOSITE"

      assert {:ok, %{parts: [%{part_number: 10}], is_truncated: false}} =
               Multipart.parts_page("bucket", id, context.opts ++ [part_number_marker: 2])

      assert {:ok, %{parts: [], stats: %{reads: 0}}} =
               Multipart.parts_page("bucket", id, context.opts ++ [max_parts: 0])
    end

    assert value(context, context.root_key).active_upload_count == 2
  end

  test "active membership follows complete and abort without copying initiation metadata",
       context do
    create(context, "upload", "object", true)
    key = upload_key("upload", true)
    part(context, "upload", key, 1, "etag", true)
    upload = value(context, key)
    publish(context, [{:put, key, %{upload | status: :completing}, %{}}])
    assert value(context, context.root_key).active_upload_count == 1
    assert {:ok, %{uploads: [summary]}} = Multipart.uploads_page("bucket", context.opts)
    assert Map.keys(summary) |> Enum.sort() == [:bucket, :created_at, :key, :upload_id]
    refute Map.has_key?(summary, :attributes)

    publish(context, [
      {:put, key,
       Map.merge(value(context, key), %{
         status: :completed,
         completion_result: %{version_id: "one"}
       }), %{}}
    ])

    assert value(context, context.root_key).active_upload_count == 0
    assert value(context, key).parts_count == 1
    assert value(context, key).completion_result.version_id == "one"
    assert {:ok, %{uploads: []}} = Multipart.uploads_page("bucket", context.opts)
    assert {:error, :not_found} = Multipart.parts_page("bucket", "upload", context.opts)

    create(context, "abort", "object", false)
    abort_key = upload_key("abort", false)
    publish(context, [{:put, abort_key, %{value(context, abort_key) | status: :aborted}, %{}}])
    assert value(context, context.root_key).active_upload_count == 0
  end

  test "equal-key upload markers and common prefixes retain the protocol page contract",
       context do
    for {id, key} <- [{"1", "a"}, {"10", "a"}, {"2", "a"}, {"next", "a/child"}, {"last", "z"}],
        do: create(context, id, key, false)

    assert {:ok, page} = Multipart.uploads_page("bucket", context.opts ++ [max_uploads: 2])
    assert Enum.map(page.uploads, & &1.upload_id) == ["1", "10"]
    assert page.next_key_marker == "a"
    assert page.next_upload_id_marker == "10"

    assert {:ok, page} =
             Multipart.uploads_page(
               "bucket",
               context.opts ++ [key_marker: "a", upload_id_marker: "10"]
             )

    assert Enum.map(page.uploads, & &1.upload_id) == ["2", "next", "last"]
    assert {:ok, page} = Multipart.uploads_page("bucket", context.opts ++ [key_marker: "a"])
    assert Enum.map(page.uploads, & &1.upload_id) == ["next", "last"]

    assert {:ok, page} =
             Multipart.uploads_page("bucket", context.opts ++ [delimiter: "/", max_uploads: 4])

    assert page.common_prefixes == ["a/"]
    assert page.next_key_marker == "a/"
    assert page.next_upload_id_marker == ""

    assert {:ok, page} =
             Multipart.uploads_page("bucket", context.opts ++ [delimiter: "/", key_marker: "a/"])

    assert page.common_prefixes == []
    assert Enum.map(page.uploads, & &1.key) == ["z"]
  end

  test "large common-prefix groups and unrelated metadata do not require descendant reads",
       context do
    for number <- 1..1000, do: create(context, "child-#{number}", "group/#{number}", false)
    create(context, "outside", "outside", false)

    assert {:ok, page} =
             Multipart.uploads_page("bucket", context.opts ++ [delimiter: "/", max_uploads: 2])

    assert page.common_prefixes == ["group/"]
    assert [%{key: "outside"}] = page.uploads
    assert page.stats.reads < 40
    assert page.stats.visited < 40
    first_stats = page.stats

    Agent.update(context.engine, fn state ->
      records =
        for number <- 1..10_000,
            into: state.records,
            do: {"unrelated:#{number}", %{value: number, mod_revision: 1}}

      %{state | records: records}
    end)

    assert {:ok, %{stats: ^first_stats}} =
             Multipart.uploads_page("bucket", context.opts ++ [delimiter: "/", max_uploads: 2])
  end

  test "part metadata, roots and membership publish together and stale plans cannot apply",
       context do
    create(context, "upload", "object", false)
    key = upload_key("upload", false)
    upload = value(context, key)

    operations = [
      {:put, key, %{upload | parts_revision: 1}, %{}},
      {:put, Keys.multipart_part("upload", 1), part_value(1, "first"), %{}}
    ]

    assert {:ok, spec} = plan(context, operations)
    assert {:ok, %{parts: []}} = Multipart.parts_page("bucket", "upload", context.opts)
    assert :ok = ExactBackend.commit(context.engine, spec)
    assert {:error, :compare_failed} = ExactBackend.commit(context.engine, spec)

    assert {:ok, %{parts: [%{etag: "first"}]}} =
             Multipart.parts_page("bucket", "upload", context.opts)

    publish(context, [{:delete, {:key, Keys.multipart_part("upload", 1)}, %{}}])
    assert value(context, key).parts_count == 0
    assert {:ok, %{parts: []}} = Multipart.parts_page("bucket", "upload", context.opts)
  end

  test "unmigrated, conflicting and wrong-generation upload state never silently omits parts",
       context do
    key = upload_key("legacy", true)
    old = upload_value("legacy", "object")
    Agent.update(context.engine, &put_in(&1.records[key], %{value: old, mod_revision: 1}))

    assert {:error, :listing_index_migration_required} =
             plan(context, [{:put, key, %{old | status: :completing}, %{}}])

    assert {:error, :listing_index_migration_required} =
             Multipart.parts_page("bucket", "legacy", context.opts)

    create(context, "current", "object", false)
    current_key = upload_key("current", false)
    current = value(context, current_key)
    legacy_part = "mpu_part:bucket:current:1"

    operations = [
      {:put, current_key, current, %{}},
      {:put, Keys.multipart_part("current", 1), part_value(1, "one"), %{}},
      {:put, legacy_part, part_value(1, "different"), %{}}
    ]

    assert {:error, :multipart_index_identity_conflict} = plan(context, operations)

    assert {:error, :multipart_index_identity_conflict} =
             plan(context, [{:put, current_key, %{current | bucket_generation: "stale"}, %{}}])

    Agent.update(
      context.engine,
      &put_in(&1.records[current_key].value.parts_index_epoch, "stale")
    )

    assert {:error, :listing_index_corrupt} =
             Multipart.parts_page("bucket", "current", context.opts)
  end

  test "revision barriers reject churn and invalid pagination remains explicit", context do
    create(context, "upload", "object", false)
    part(context, "upload", upload_key("upload", false), 1, "first", false)
    Agent.update(context.engine, &%{&1 | churn: true, reads: 0})

    assert {:error, :listing_index_unavailable} =
             Multipart.parts_page("bucket", "upload", context.opts)

    assert Agent.get(context.engine, & &1.reads) < 100

    assert {:error, :invalid_pagination} =
             Multipart.parts_page("bucket", "upload", context.opts ++ [max_parts: 1001])

    assert {:error, :invalid_pagination} =
             Multipart.uploads_page("bucket", context.opts ++ [max_uploads: -1])
  end

  defp create(context, id, object_key, legacy?),
    do: publish(context, [{:put, upload_key(id, legacy?), upload_value(id, object_key), %{}}])

  defp upload_value(id, key) do
    %{
      bucket: "bucket",
      key: key,
      upload_id: id,
      status: :initiated,
      bucket_generation: "multipart-index-generation",
      parts_revision: 0,
      created_at: "2026-10-18T00:00:00Z",
      updated_at: "2026-10-18T00:00:00Z",
      attributes: %{
        checksum_algorithm: "SHA256",
        checksum_type: "COMPOSITE",
        user_metadata: %{private: String.duplicate("x", 16_384)}
      }
    }
  end

  defp part(context, id, upload_key, number, etag, legacy?) do
    upload = value(context, upload_key)
    key = if legacy?, do: "mpu_part:bucket:#{id}:#{number}", else: Keys.multipart_part(id, number)

    publish(context, [
      {:put, upload_key, %{upload | parts_revision: upload.parts_revision + 1}, %{}},
      {:put, key, part_value(number, etag), %{}}
    ])
  end

  defp part_value(number, etag),
    do: %{
      part_number: number,
      etag: etag,
      size: 5,
      hash: "hash",
      uploaded_at: "now",
      checksums: %{sha256: "checksum"}
    }

  defp upload_key(id, true), do: "mpu:bucket:#{id}"
  defp upload_key(id, false), do: Keys.multipart_upload(id)
  defp value(context, key), do: Agent.get(context.engine, & &1.records[key].value)

  defp publish(context, operations) do
    assert {:ok, spec} = plan(context, operations)
    assert :ok = ExactBackend.commit(context.engine, spec)
  end

  defp plan(context, operations) do
    root_record = Agent.get(context.engine, & &1.records[context.root_key])

    with {:ok, planned} <-
           Multipart.augment(ExactBackend, "bucket", root_record.value, operations, context.opts) do
      root = %{
        root_record.value
        | uploads_root: planned.root_id,
          active_upload_count: root_record.value.active_upload_count + planned.count_delta,
          commit_sequence: root_record.value.commit_sequence + 1
      }

      {:ok,
       %{
         compare: [
           {:mod_revision, context.root_key, :==, root_record.mod_revision} | planned.compare
         ],
         success:
           planned.metadata_operations ++
             planned.index_operations ++ [{:put, context.root_key, root, %{}}],
         failure: []
       }}
    end
  end
end
