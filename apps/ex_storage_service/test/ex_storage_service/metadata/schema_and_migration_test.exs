defmodule ExStorageService.Metadata.SchemaAndMigrationTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.{Keys, Migration, Schema}

  defmodule TestBackend do
    @behaviour ExStorageService.Metadata.Backend

    def start_link(opts \\ []) do
      Agent.start_link(fn ->
        %{
          records: %{},
          revision: 0,
          transactions: [],
          transaction_results: %{},
          compare_failures: Keyword.get(opts, :compare_failures, 0),
          timeout_after_commit: Keyword.get(opts, :timeout_after_commit, 0),
          transaction_count: 0,
          fail_transaction_at: Keyword.get(opts, :fail_transaction_at),
          mutate_source_on_transaction: Keyword.get(opts, :mutate_source_on_transaction)
        }
      end)
    end

    @impl true
    def get(key, opts), do: Agent.get(engine(opts), &read(&1, key))

    @impl true
    def put(key, value, opts) do
      Agent.update(engine(opts), &apply_operation({:put, key, value, %{}}, &1))
    end

    @impl true
    def delete(key, opts) do
      Agent.update(engine(opts), &apply_operation({:delete, {:key, key}, %{}}, &1))
    end

    @impl true
    def get_all(opts) do
      {:ok,
       Agent.get(engine(opts), fn state ->
         Enum.map(state.records, fn {key, record} -> {key, record.value} end)
       end)}
    end

    @impl true
    def prefix_scan(prefix, opts) do
      {:ok, records} = get_all(opts)
      {:ok, Enum.filter(records, &String.starts_with?(elem(&1, 0), prefix))}
    end

    @impl true
    def scan(prefix, opts), do: prefix_scan(prefix, opts)

    @impl true
    def list_page(prefix, cursor, limit, opts) do
      entries =
        Agent.get(engine(opts), fn state ->
          state.records
          |> Enum.filter(fn {key, _record} -> String.starts_with?(key, prefix) end)
          |> Enum.sort_by(&elem(&1, 0))
          |> Enum.drop_while(fn {key, _record} -> cursor && key <= cursor end)
        end)

      page = Enum.take(entries, limit)
      has_more = length(entries) > length(page)

      records =
        Enum.map(page, fn {key, record} ->
          %{key: key, value: record.value, mod_revision: record.mod_revision}
        end)

      next_cursor =
        if has_more,
          do: page |> List.last() |> elem(0),
          else: nil

      {:ok, %{entries: records, next_cursor: next_cursor}}
    end

    @impl true
    def transaction(spec, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        transaction_count = state.transaction_count + 1

        state = %{
          state
          | transactions: [spec | state.transactions],
            transaction_count: transaction_count
        }

        state =
          case {transaction_count, state.mutate_source_on_transaction} do
            {1, {key, value}} ->
              state
              |> then(&apply_operation({:put, key, value, %{}}, &1))
              |> Map.put(:mutate_source_on_transaction, nil)

            _other ->
              state
          end

        idempotency_key = Keyword.fetch!(opts, :idempotency_key)

        if state.fail_transaction_at == transaction_count do
          {{:error, :injected_interruption}, state}
        else
          case Map.get(state.transaction_results, idempotency_key) do
            %{spec: ^spec, result: result} ->
              {{:ok, result}, state}

            %{spec: _other} ->
              {{:error, :idempotency_conflict}, state}

            nil ->
              execute_transaction(spec, idempotency_key, state)
          end
        end
      end)
    end

    @impl true
    def resolve_transaction(idempotency_key, opts) do
      Agent.get(engine(opts), fn state ->
        case Map.get(state.transaction_results, idempotency_key) do
          %{result: result} -> {:ok, result}
          nil -> {:error, :not_found}
        end
      end)
    end

    @impl true
    def resolve_operation(key, opts), do: get(key, opts)

    def seed(engine, key, value) do
      Agent.update(engine, &apply_operation({:put, key, value, %{}}, &1))
    end

    def value(engine, key) do
      case Agent.get(engine, &Map.get(&1.records, key)) do
        nil -> nil
        record -> record.value
      end
    end

    def keys(engine), do: Agent.get(engine, &Map.keys(&1.records))
    def transactions(engine), do: Agent.get(engine, &Enum.reverse(&1.transactions))

    def clear_transaction_failure(engine) do
      Agent.update(engine, &%{&1 | fail_transaction_at: nil})
    end

    defp execute_transaction(spec, idempotency_key, %{compare_failures: failures} = state)
         when failures > 0 do
      state =
        spec.success
        |> Enum.find(fn
          {:put, "ess:v2:blob:" <> _, _value, _opts} -> true
          _operation -> false
        end)
        |> case do
          nil ->
            state

          {:put, key, _value, _opts} = operation ->
            case Map.get(state.records, key) do
              nil ->
                apply_operation(operation, state)

              %{value: current} ->
                apply_operation({:put, key, current, %{}}, state)
            end
        end
        |> Map.update!(:compare_failures, &(&1 - 1))

      result = %{succeeded: false}
      state = cache_result(state, idempotency_key, spec, result)
      {{:ok, result}, state}
    end

    defp execute_transaction(spec, idempotency_key, state) do
      result = %{succeeded: Enum.all?(spec.compare, &compare?(&1, state))}

      committed =
        if result.succeeded,
          do: Enum.reduce(spec.success, state, &apply_operation/2),
          else: state

      committed = cache_result(committed, idempotency_key, spec, result)

      if result.succeeded and committed.timeout_after_commit > 0 do
        {{:error, :timeout},
         %{committed | timeout_after_commit: committed.timeout_after_commit - 1}}
      else
        {{:ok, result}, committed}
      end
    end

    defp compare?({:exists, key, :==, expected}, state),
      do: Map.has_key?(state.records, key) == expected

    defp compare?({:mod_revision, key, :==, expected}, state) do
      state.records
      |> Map.get(key, %{mod_revision: 0})
      |> Map.fetch!(:mod_revision)
      |> Kernel.==(expected)
    end

    defp apply_operation({:put, key, value, _opts}, state) do
      revision = state.revision + 1

      %{
        state
        | revision: revision,
          records: Map.put(state.records, key, %{value: value, mod_revision: revision})
      }
    end

    defp apply_operation({:delete, {:key, key}, _opts}, state) do
      %{state | revision: state.revision + 1, records: Map.delete(state.records, key)}
    end

    defp cache_result(state, idempotency_key, spec, result) do
      %{
        state
        | transaction_results:
            Map.put(state.transaction_results, idempotency_key, %{spec: spec, result: result})
      }
    end

    defp read(state, key) do
      case Map.get(state.records, key) do
        nil -> {:ok, nil}
        record -> {:ok, record}
      end
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  test "schema status counts legacy and v2 records through bounded pages" do
    {:ok, engine} = TestBackend.start_link()

    records = [
      {"obj:bucket:a", %{content_hash: "hash-a", size: 1}},
      {"obj:bucket:b", %{content_hash: "hash-b", size: 2}},
      {"obj_ver:bucket:a:v1", %{version_id: "v1", content_hash: "hash-a", size: 1}},
      {"obj_ver_list:bucket:a", ["v1"]},
      {Keys.object_head("bucket", "a"),
       %{schema: 2, bucket: "bucket", key: "a", version_id: "v1"}},
      {Keys.object_version("bucket", "a", "v1"),
       %{
         schema: 2,
         bucket: "bucket",
         key: "a",
         version_id: "v1",
         parent_version_id: nil,
         content_hash: "hash-a",
         size: 1
       }},
      {Keys.blob("hash-a"),
       %{
         schema: 2,
         hash: "hash-a",
         algorithm: :sha256,
         size: 1,
         desired_replication_factor: 1
       }},
      {Keys.blob_location("hash-a", "node-a"),
       %{
         schema: 2,
         hash: "hash-a",
         node_id: "node-a",
         node_generation: 1,
         state: :ready,
         size: 1,
         verified_at: 1
       }}
    ]

    Enum.each(records, fn {key, value} -> TestBackend.seed(engine, key, value) end)

    assert {:ok,
            %{
              configured_schema: :v2,
              v1_only_objects: 1,
              migration_required: true,
              migration_ready: true,
              migration_complete: false,
              v2_writes_present: true,
              validation: %{invalid_record_count: 0},
              v1: %{objects: 2, versions: 1, version_lists: 1},
              v2: %{heads: 1, versions: 1, blobs: 1, blob_locations: 1}
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 1,
               page_size: 1
             )
  end

  test "schema status reports malformed records and blocks migration readiness" do
    {:ok, engine} = TestBackend.start_link()

    TestBackend.seed(engine, "obj:missing-bucket-separator", %{content_hash: "hash", size: 1})
    TestBackend.seed(engine, "obj_ver_list:bucket:key", ["missing-version"])
    TestBackend.seed(engine, Keys.blob("broken"), %{schema: 2, hash: "other", size: -1})

    assert {:ok,
            %{
              migration_ready: false,
              validation: %{
                invalid_record_count: 3,
                invalid_records: invalid_records,
                invalid_records_truncated: false
              }
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 1,
               page_size: 1
             )

    assert MapSet.new(Enum.map(invalid_records, & &1.key)) ==
             MapSet.new([
               "obj:missing-bucket-separator",
               "obj_ver_list:bucket:key",
               Keys.blob("broken")
             ])
  end

  test "schema status rejects dangling version parents and missing blob descriptors" do
    {:ok, engine} = TestBackend.start_link()
    linked_hash = "linked-hash"
    head_key = Keys.object_head("bucket", "key")
    linked_version_key = Keys.object_version("bucket", "key", "v2")
    missing_blob_version_key = Keys.object_version("bucket", "other", "v1")

    TestBackend.seed(engine, Keys.blob(linked_hash), %{
      schema: 2,
      hash: linked_hash,
      algorithm: :sha256,
      size: 4,
      desired_replication_factor: 1
    })

    TestBackend.seed(engine, linked_version_key, %{
      schema: 2,
      bucket: "bucket",
      key: "key",
      version_id: "v2",
      parent_version_id: "missing",
      content_hash: linked_hash,
      size: 4
    })

    TestBackend.seed(engine, head_key, %{
      schema: 2,
      bucket: "bucket",
      key: "key",
      version_id: "v2"
    })

    TestBackend.seed(engine, missing_blob_version_key, %{
      schema: 2,
      bucket: "bucket",
      key: "other",
      version_id: "v1",
      parent_version_id: nil,
      content_hash: "missing-hash",
      size: 8
    })

    assert {:ok,
            %{
              migration_ready: false,
              migration_complete: false,
              validation: %{invalid_record_count: 3, invalid_records: invalid_records}
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 1,
               page_size: 1
             )

    assert MapSet.new(Enum.map(invalid_records, & &1.key)) ==
             MapSet.new([head_key, linked_version_key, missing_blob_version_key])

    assert Enum.any?(invalid_records, fn
             %{
               key: ^head_key,
               reason: {:invalid_head_version, {:dangling_parent_version, "missing"}}
             } ->
               true

             _record ->
               false
           end)

    assert Enum.any?(invalid_records, fn
             %{key: ^missing_blob_version_key, reason: {:missing_blob_descriptor, "missing-hash"}} ->
               true

             _record ->
               false
           end)
  end

  test "fails closed on an unindexed legacy version with a colon and Unicode object key" do
    {:ok, engine} = TestBackend.start_link()
    bucket = "legacy"
    object_key = "目录/folder:object/☃"
    version_id = "version-orphan"
    legacy_key = "obj_ver:#{bucket}:#{object_key}:#{version_id}"

    TestBackend.seed(engine, legacy_key, %{
      version_id: version_id,
      content_hash: "orphan-hash",
      size: 7
    })

    assert {:ok,
            %{
              migration_ready: false,
              validation: %{
                invalid_record_count: 1,
                invalid_records: [%{key: ^legacy_key, reason: :missing_version_list}]
              }
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 2,
               page_size: 1
             )

    assert {:error, {:legacy_version_index_invalid, ^legacy_key, :missing_version_list}} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               replication_factor: 2,
               page_size: 1,
               local_blob_probe: fn _hash, _size -> :missing end
             )

    refute Enum.any?(TestBackend.keys(engine), &String.starts_with?(&1, "ess:v2:"))
  end

  test "promotes a v2-only descriptor to the explicit target RF" do
    {:ok, engine} = TestBackend.start_link()
    hash = "v2-only-hash"

    original = %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: 12,
      desired_replication_factor: 1,
      created_at: "2026-01-01T00:00:00Z",
      custom: :preserved
    }

    TestBackend.seed(engine, Keys.blob(hash), original)

    assert {:ok,
            %{
              migration_required: true,
              migration_ready: true,
              migration_complete: false,
              replication_ready: false,
              v2: %{under_target_descriptors: 1}
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 2,
               page_size: 1
             )

    assert {:ok,
            %{
              objects_scanned: 0,
              blob_descriptors_promoted: 1,
              target_replication_factor: 2
            }} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               replication_factor: 2,
               page_size: 1
             )

    assert %{original | desired_replication_factor: 2} ==
             TestBackend.value(engine, Keys.blob(hash))

    assert {:ok,
            %{
              migration_required: false,
              migration_ready: true,
              migration_complete: true,
              replication_ready: true,
              v2: %{under_target_descriptors: 0}
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 2,
               page_size: 1
             )
  end

  test "descriptor RF promotion retries CAS and resolves an ambiguous timeout" do
    Enum.each([compare_failures: 1, timeout_after_commit: 1], fn backend_option ->
      {:ok, engine} = TestBackend.start_link([backend_option])
      hash = "promotion-#{elem(backend_option, 0)}"

      TestBackend.seed(engine, Keys.blob(hash), %{
        schema: 2,
        hash: hash,
        algorithm: :sha256,
        size: 9,
        desired_replication_factor: 1
      })

      assert {:ok, %{blob_descriptors_promoted: 1}} =
               Migration.migrate_v2(
                 backend: TestBackend,
                 engine: engine,
                 replication_factor: 2,
                 page_size: 1
               )

      assert %{desired_replication_factor: 2} =
               TestBackend.value(engine, Keys.blob(hash))
    end)
  end

  test "descriptor RF promotion preserves an existing higher target" do
    {:ok, engine} = TestBackend.start_link()
    hash = "higher-rf"

    descriptor = %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: 5,
      desired_replication_factor: 3,
      custom: "untouched"
    }

    TestBackend.seed(engine, Keys.blob(hash), descriptor)

    assert {:ok, %{blob_descriptors_promoted: 0, target_replication_factor: 2}} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               replication_factor: 2,
               page_size: 1
             )

    assert TestBackend.value(engine, Keys.blob(hash)) == descriptor
    assert TestBackend.transactions(engine) == []
  end

  test "migrates a complete version chain atomically and retains every v1 record" do
    {:ok, engine} = TestBackend.start_link()
    bucket = "legacy"
    key = "目录/folder:object/☃"
    hash = String.duplicate("a", 64)

    old = %{
      version_id: "v1",
      content_hash: hash,
      size: 3,
      etag: "old",
      created_at: "2026-01-01T00:00:00Z"
    }

    marker = %{
      version_id: "v2",
      is_delete_marker: true,
      created_at: "2026-01-02T00:00:00Z"
    }

    current = %{
      content_hash: hash,
      size: 3,
      etag: "new",
      created_at: "2026-01-03T00:00:00Z"
    }

    legacy_records = [
      {"obj:#{bucket}:#{key}", current},
      {"obj_ver:#{bucket}:#{key}:v2", marker},
      {"obj_ver:#{bucket}:#{key}:v1", old},
      {"obj_ver_list:#{bucket}:#{key}", ["v2", "v1"]}
    ]

    Enum.each(legacy_records, fn {record_key, value} ->
      TestBackend.seed(engine, record_key, value)
    end)

    probe = fn ^hash, 3 ->
      {:ok, %{node_id: "data-a", node_generation: 7, verified_at: 123}}
    end

    opts = [
      backend: TestBackend,
      engine: engine,
      page_size: 1,
      replication_factor: 2,
      local_blob_probe: probe
    ]

    assert {:ok,
            %{
              objects_scanned: 1,
              objects_migrated: 1,
              objects_already_v2: 0,
              versions_migrated: 3,
              delete_markers_migrated: 1,
              blob_descriptors_created: 1,
              blob_locations_created: 1,
              missing_local_blobs: []
            }} = Migration.migrate_v2(opts)

    head = TestBackend.value(engine, Keys.object_head(bucket, key))
    assert head.version_id =~ "legacy-current-"

    current_version =
      TestBackend.value(engine, Keys.object_version(bucket, key, head.version_id))

    assert current_version.parent_version_id == "v2"
    assert current_version.content_hash == hash
    refute current_version.delete_marker

    delete_marker = TestBackend.value(engine, Keys.object_version(bucket, key, "v2"))
    assert delete_marker.parent_version_id == "v1"
    assert delete_marker.delete_marker
    assert delete_marker.is_delete_marker

    oldest = TestBackend.value(engine, Keys.object_version(bucket, key, "v1"))
    assert oldest.parent_version_id == nil
    assert oldest.content_hash == hash

    assert %{
             schema: 2,
             hash: ^hash,
             size: 3,
             desired_replication_factor: 2
           } = TestBackend.value(engine, Keys.blob(hash))

    assert %{
             hash: ^hash,
             node_id: "data-a",
             node_generation: 7,
             state: :ready,
             size: 3
           } = TestBackend.value(engine, Keys.blob_location(hash, "data-a"))

    Enum.each(legacy_records, fn {record_key, value} ->
      assert TestBackend.value(engine, record_key) == value
    end)

    transaction_count = length(TestBackend.transactions(engine))

    [transaction | _] = TestBackend.transactions(engine)
    version_list_key = "obj_ver_list:#{bucket}:#{key}"

    assert {:mod_revision, ^version_list_key, :==, _revision} =
             Enum.find(
               transaction.compare,
               &match?(
                 {:mod_revision, "obj_ver_list:" <> _, :==, _revision},
                 &1
               )
             )

    assert Enum.count(transaction.compare, fn
             {:mod_revision, "obj_ver:" <> _, :==, _revision} -> true
             _compare -> false
           end) == 2

    assert {:ok,
            %{
              objects_scanned: 1,
              objects_migrated: 0,
              objects_already_v2: 1,
              versions_migrated: 0
            }} = Migration.migrate_v2(opts)

    assert length(TestBackend.transactions(engine)) == transaction_count

    assert {:ok,
            %{
              v1: %{objects: 1, versions: 2, version_lists: 1},
              v1_only_objects: 0,
              migration_required: false,
              migration_ready: true,
              replication_ready: true,
              migration_complete: true
            }} =
             Schema.status(
               backend: TestBackend,
               engine: engine,
               metadata_schema: :v2,
               replication_factor: 2,
               page_size: 1
             )
  end

  test "rebuilds after a compare failure and resolves an ambiguous committed outcome" do
    hash = String.duplicate("b", 64)

    Enum.each([compare_failures: 1, timeout_after_commit: 1], fn backend_option ->
      {:ok, engine} = TestBackend.start_link([backend_option])

      TestBackend.seed(engine, "obj:bucket:key", %{
        content_hash: hash,
        size: 4,
        created_at: "2026-01-01T00:00:00Z"
      })

      assert {:ok, %{objects_migrated: 1, versions_migrated: 1}} =
               Migration.migrate_v2(
                 backend: TestBackend,
                 engine: engine,
                 replication_factor: 1,
                 local_blob_probe: fn ^hash, 4 -> :missing end
               )

      assert %{version_id: version_id} =
               TestBackend.value(engine, Keys.object_head("bucket", "key"))

      assert %{content_hash: ^hash} =
               TestBackend.value(engine, Keys.object_version("bucket", "key", version_id))

      assert %{hash: ^hash, size: 4} = TestBackend.value(engine, Keys.blob(hash))

      assert Enum.all?(
               TestBackend.keys(engine),
               &(not String.contains?(&1, "object_version_list"))
             )
    end)
  end

  test "source revision CAS prevents publishing a stale migration plan" do
    source_key = "obj:bucket:key"

    {:ok, engine} =
      TestBackend.start_link(
        mutate_source_on_transaction: {source_key, %{content_hash: "new-hash", size: 8}}
      )

    TestBackend.seed(engine, source_key, %{content_hash: "old-hash", size: 4})

    assert {:error, {:metadata_migration_failed, ^source_key, :compare_retry_exhausted}} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               replication_factor: 1,
               max_attempts: 2,
               local_blob_probe: fn _hash, _size -> :missing end
             )

    refute TestBackend.value(engine, Keys.object_head("bucket", "key"))
    assert TestBackend.value(engine, source_key) == %{content_hash: "new-hash", size: 8}
  end

  test "rejects conflicting blob descriptors without publishing v2 object metadata" do
    {:ok, engine} = TestBackend.start_link()
    hash = String.duplicate("c", 64)

    TestBackend.seed(engine, "obj:bucket:key", %{content_hash: hash, size: 5})

    TestBackend.seed(engine, Keys.blob(hash), %{
      schema: 2,
      hash: hash,
      algorithm: :sha256,
      size: 99,
      desired_replication_factor: 1
    })

    assert {:error,
            {:metadata_migration_failed, "obj:bucket:key", {:blob_descriptor_conflict, ^hash}}} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               replication_factor: 1,
               local_blob_probe: fn ^hash, 5 -> :missing end
             )

    assert TestBackend.value(engine, Keys.object_head("bucket", "key")) == nil
    assert TestBackend.value(engine, "obj:bucket:key") == %{content_hash: hash, size: 5}
  end

  test "reports missing blobs and rejects malformed blob identity without partial publication" do
    {:ok, engine} = TestBackend.start_link()
    missing_hash = String.duplicate("d", 64)

    TestBackend.seed(engine, "obj:bucket:missing", %{
      content_hash: missing_hash,
      size: 5
    })

    TestBackend.seed(engine, "obj:bucket:malformed", %{
      content_hash: String.duplicate("e", 64),
      size: "five"
    })

    opts = [
      backend: TestBackend,
      engine: engine,
      page_size: 1,
      replication_factor: 1,
      local_blob_probe: fn _hash, _size -> :missing end
    ]

    assert {:error,
            {:metadata_migration_failed, "obj:bucket:malformed",
             {:invalid_blob_identity, _hash, "five"}}} = Migration.migrate_v2(opts)

    refute TestBackend.value(engine, Keys.object_head("bucket", "malformed"))
    :ok = TestBackend.delete("obj:bucket:malformed", engine: engine)

    assert {:ok, %{missing_local_blobs: [^missing_hash], objects_migrated: 1}} =
             Migration.migrate_v2(
               backend: TestBackend,
               engine: engine,
               page_size: 1,
               replication_factor: 1,
               local_blob_probe: fn
                 ^missing_hash, 5 -> :missing
                 _hash, _size -> {:error, :should_not_probe}
               end,
               max_attempts: 1
             )
  end

  test "resumes idempotently after an interrupted paginated migration" do
    {:ok, engine} = TestBackend.start_link(fail_transaction_at: 2)

    TestBackend.seed(engine, "obj:bucket:a", %{content_hash: "hash-a", size: 1})
    TestBackend.seed(engine, "obj:bucket:b", %{content_hash: "hash-b", size: 2})

    opts = [
      backend: TestBackend,
      engine: engine,
      page_size: 1,
      replication_factor: 1,
      local_blob_probe: fn _hash, _size -> :missing end
    ]

    assert {:error, {:metadata_migration_failed, "obj:bucket:b", :injected_interruption}} =
             Migration.migrate_v2(opts)

    assert TestBackend.value(engine, Keys.object_head("bucket", "a"))
    refute TestBackend.value(engine, Keys.object_head("bucket", "b"))

    TestBackend.clear_transaction_failure(engine)

    assert {:ok,
            %{
              objects_scanned: 2,
              objects_migrated: 1,
              objects_already_v2: 1,
              missing_local_blobs: ["hash-b"]
            }} = Migration.migrate_v2(opts)

    assert TestBackend.value(engine, Keys.object_head("bucket", "b"))
  end

  test "fails closed when existing v2 metadata differs from the complete migration plan" do
    {:ok, engine} = TestBackend.start_link()

    TestBackend.seed(engine, "obj:bucket:key", %{content_hash: "hash", size: 4})

    opts = [
      backend: TestBackend,
      engine: engine,
      replication_factor: 1,
      local_blob_probe: fn _hash, _size -> :missing end
    ]

    assert {:ok, %{objects_migrated: 1}} = Migration.migrate_v2(opts)

    head_key = Keys.object_head("bucket", "key")
    original_head = TestBackend.value(engine, head_key)
    TestBackend.seed(engine, head_key, %{version_id: "different", schema: 2})

    assert {:error, {:metadata_migration_failed, "obj:bucket:key", :conflicting_v2_head}} =
             Migration.migrate_v2(opts)

    TestBackend.seed(engine, head_key, original_head)
    version_key = Keys.object_version("bucket", "key", original_head.version_id)
    version = TestBackend.value(engine, version_key)
    TestBackend.seed(engine, version_key, %{version | size: 999})

    assert {:error,
            {:metadata_migration_failed, "obj:bucket:key", {:conflicting_v2_version, version_id}}} =
             Migration.migrate_v2(opts)

    assert version_id == original_head.version_id

    TestBackend.seed(engine, version_key, version)
    :ok = TestBackend.delete(Keys.blob("hash"), engine: engine)

    assert {:error,
            {:metadata_migration_failed, "obj:bucket:key", {:missing_v2_blob_descriptor, "hash"}}} =
             Migration.migrate_v2(opts)
  end
end
