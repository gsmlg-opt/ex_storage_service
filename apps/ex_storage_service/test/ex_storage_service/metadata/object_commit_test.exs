defmodule ExStorageService.Metadata.ObjectCommitTest do
  use ExUnit.Case, async: true

  alias ExStorageService.Metadata.Keys
  alias ExStorageService.Metadata.ObjectCommit

  defmodule TestBackend do
    @behaviour ExStorageService.Metadata.Backend

    def start_link(opts \\ []) do
      Agent.start_link(fn ->
        %{
          records: %{},
          revision: 0,
          compare_failures: Keyword.get(opts, :compare_failures, 0),
          timeout_after_commit: Keyword.get(opts, :timeout_after_commit, 0),
          operation_resolution_misses_after_commit:
            Keyword.get(opts, :operation_resolution_misses_after_commit, 0),
          transactions: [],
          transaction_results: %{}
        }
      end)
    end

    @impl true
    def get(key, opts), do: Agent.get(engine(opts), &read(&1, key))

    @impl true
    def put(key, value, opts) do
      Agent.update(engine(opts), fn state -> apply_operation({:put, key, value, %{}}, state) end)
    end

    @impl true
    def delete(key, opts) do
      Agent.update(engine(opts), fn state ->
        apply_operation({:delete, {:key, key}, %{}}, state)
      end)
    end

    @impl true
    def get_all(opts) do
      {:ok,
       Agent.get(engine(opts), fn state ->
         Enum.map(state.records, fn {key, %{value: value}} -> {key, value} end)
       end)}
    end

    @impl true
    def scan(prefix, opts) do
      {:ok, entries} = get_all(opts)
      {:ok, Enum.filter(entries, fn {key, _} -> String.starts_with?(key, prefix) end)}
    end

    @impl true
    def prefix_scan(prefix, opts), do: scan(prefix, opts)

    @impl true
    def resolve_operation(key, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        if Map.has_key?(state.records, key) and state.operation_resolution_misses_after_commit > 0 do
          {{:ok, nil},
           %{
             state
             | operation_resolution_misses_after_commit:
                 state.operation_resolution_misses_after_commit - 1
           }}
        else
          {read(state, key), state}
        end
      end)
    end

    @impl true
    def transaction(spec, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        state = %{state | transactions: [spec | state.transactions]}
        idempotency_key = Keyword.get(opts, :idempotency_key)

        case Map.get(state.transaction_results, idempotency_key) do
          %{spec: ^spec, result: result} ->
            {{:ok, result}, state}

          %{spec: _other_spec} ->
            {{:error, :idempotency_conflict}, state}

          nil ->
            execute_transaction(spec, idempotency_key, state)
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

    def transactions(engine) do
      Agent.get(engine, &Enum.reverse(&1.transactions))
    end

    def transaction_results(engine) do
      Agent.get(engine, & &1.transaction_results)
    end

    def seed(engine, key, value) do
      Agent.update(engine, fn state -> apply_operation({:put, key, value, %{}}, state) end)
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)

    defp read(state, key) do
      case Map.get(state.records, key) do
        nil -> {:ok, nil}
        record -> {:ok, record}
      end
    end

    defp compare?({:mod_revision, key, :==, expected}, state) do
      revision = state.records |> Map.get(key, %{mod_revision: 0}) |> Map.fetch!(:mod_revision)
      revision == expected
    end

    defp compare?({:exists, key, :==, expected}, state),
      do: Map.has_key?(state.records, key) == expected

    defp compare?({:field, key, path, :==, expected}, state) do
      case Map.get(state.records, key) do
        %{value: value} -> get_in(value, path) == expected
        nil -> false
      end
    end

    defp execute_transaction(spec, idempotency_key, %{compare_failures: failures} = state)
         when failures > 0 do
      state =
        state
        |> advance_competing_head(spec)
        |> Map.update!(:compare_failures, &(&1 - 1))

      cache_result(state, idempotency_key, spec, %{succeeded: false})
      |> then(&{{:ok, %{succeeded: false}}, &1})
    end

    defp execute_transaction(spec, idempotency_key, state) do
      if Enum.all?(spec.compare, &compare?(&1, state)) do
        committed = Enum.reduce(spec.success, state, &apply_operation/2)
        committed = cache_result(committed, idempotency_key, spec, %{succeeded: true})

        if committed.timeout_after_commit > 0 do
          {{:error, :timeout},
           %{committed | timeout_after_commit: committed.timeout_after_commit - 1}}
        else
          {{:ok, %{succeeded: true}}, committed}
        end
      else
        state = cache_result(state, idempotency_key, spec, %{succeeded: false})
        {{:ok, %{succeeded: false}}, state}
      end
    end

    defp advance_competing_head(state, spec) do
      case Enum.find(spec.compare, fn
             {:mod_revision, key, :==, _revision} ->
               String.starts_with?(key, Keys.object_head_prefix())

             _other ->
               false
           end) do
        {:mod_revision, head_key, :==, _revision} ->
          apply_operation(
            {:put, head_key, %{version_id: "competing-#{state.revision}"}, %{}},
            state
          )

        nil ->
          state
      end
    end

    defp cache_result(state, nil, _spec, _result), do: state

    defp cache_result(state, key, spec, result) do
      %{
        state
        | transaction_results:
            Map.put(state.transaction_results, key, %{spec: spec, result: result})
      }
    end

    defp apply_operation({:put, key, value, _opts}, state) do
      revision = state.revision + 1

      %{state | revision: revision, records: Map.put(state.records, key, record(value, revision))}
    end

    defp apply_operation({:delete, {:key, key}, _opts}, state) do
      revision = state.revision + 1
      %{state | revision: revision, records: Map.delete(state.records, key)}
    end

    defp record(value, revision), do: %{value: value, mod_revision: revision}
  end

  defmodule Barrier do
    use GenServer

    def start_link(participants), do: GenServer.start_link(__MODULE__, participants)
    def wait(barrier), do: GenServer.call(barrier, :wait, :infinity)

    @impl true
    def init(participants), do: {:ok, %{remaining: participants, waiting: []}}

    @impl true
    def handle_call(:wait, _from, %{remaining: 0} = state), do: {:reply, :ok, state}

    def handle_call(:wait, _from, %{remaining: 1, waiting: waiting} = state) do
      Enum.each(waiting, &GenServer.reply(&1, :ok))
      {:reply, :ok, %{state | remaining: 0, waiting: []}}
    end

    def handle_call(:wait, from, state) do
      {:noreply, %{state | remaining: state.remaining - 1, waiting: [from | state.waiting]}}
    end
  end

  defmodule BarrierBackend do
    @behaviour ExStorageService.Metadata.Backend

    @impl true
    def get(key, opts) do
      if String.starts_with?(key, "ess:v2:object_head:"),
        do: Barrier.wait(Keyword.fetch!(opts, :barrier))

      TestBackend.get(key, opts)
    end

    @impl true
    defdelegate get_all(opts), to: TestBackend

    @impl true
    defdelegate put(key, value, opts), to: TestBackend

    @impl true
    defdelegate delete(key, opts), to: TestBackend

    @impl true
    defdelegate scan(prefix, opts), to: TestBackend

    @impl true
    defdelegate prefix_scan(prefix, opts), to: TestBackend

    @impl true
    defdelegate transaction(spec, opts), to: TestBackend

    @impl true
    defdelegate resolve_transaction(idempotency_key, opts), to: TestBackend

    @impl true
    defdelegate resolve_operation(key, opts), to: TestBackend
  end

  test "commits an immutable version, head, blob, and operation atomically" do
    {:ok, backend} = TestBackend.start_link()

    assert {:ok, %{version_id: "v1", operation_id: "op1", kind: :put}} =
             put(backend, "op1", "v1")

    assert {:ok, %{version_id: "v1"}} =
             ObjectCommit.get_head("bucket", "key", backend: TestBackend, engine: backend)

    assert {:ok, %{value: %{version_id: "v1"}}} =
             TestBackend.get(Keys.object_version("bucket", "key", "v1"), engine: backend)

    assert {:ok, %{value: %{hash: "hash-op1"}}} =
             TestBackend.get(Keys.blob("hash-op1"), engine: backend)
  end

  test "commits quorum durability, locations, and repair events atomically" do
    {:ok, backend} = TestBackend.start_link()

    placement =
      for node_id <- ["data-a", "data-b", "data-c"] do
        node = %{
          node_id: node_id,
          generation: 1,
          role: :data,
          enabled: true,
          draining: false
        }

        TestBackend.seed(backend, Keys.cluster_node(node_id), node)

        {:ok, %{mod_revision: revision}} =
          TestBackend.get(Keys.cluster_node(node_id), engine: backend)

        %{node: node, mod_revision: revision}
      end

    acknowledgements =
      for node_id <- ["data-a", "data-b"] do
        %{
          node_id: node_id,
          node_generation: 1,
          hash: "cluster-hash",
          size: 42,
          verified_at: "2026-07-27T00:00:00Z"
        }
      end

    durability = %{
      descriptor: %{
        schema: 2,
        hash: "cluster-hash",
        algorithm: :sha256,
        size: 42,
        desired_replication_factor: 3,
        created_at: "2026-07-27T00:00:00Z"
      },
      placement: placement,
      acknowledgements: acknowledgements,
      missing_node_ids: ["data-c"],
      configured_write_quorum: 2,
      required_write_quorum: 2,
      achieved_replica_count: 2,
      durability: :strict
    }

    metadata = %{
      content_hash: "cluster-hash",
      size: 42,
      etag: "cluster-etag",
      created_at: "2026-07-27T00:00:00Z"
    }

    assert {:ok, %{version_id: "cluster-v1"}} =
             ObjectCommit.put(
               "bucket",
               "key",
               metadata,
               commit_opts(backend, "cluster-op1", "cluster-v1") ++
                 [durability: durability]
             )

    assert {:ok, %{value: %{desired_replication_factor: 3}}} =
             TestBackend.get(Keys.blob("cluster-hash"), engine: backend)

    for node_id <- ["data-a", "data-b"] do
      assert {:ok, %{value: %{state: :ready, node_id: ^node_id}}} =
               TestBackend.get(
                 Keys.blob_location("cluster-hash", node_id),
                 engine: backend
               )
    end

    assert {:ok,
            %{
              value: %{
                durability: %{
                  acknowledged_replica_count: 2,
                  acknowledged_node_ids: ["data-a", "data-b"],
                  degraded: false
                }
              }
            }} =
             TestBackend.get(
               Keys.object_version("bucket", "key", "cluster-v1"),
               engine: backend
             )

    assert {:ok,
            %{
              value: %{
                events: [
                  %{
                    kind: :repair_blob,
                    state: :pending,
                    payload: %{hash: "cluster-hash", target_node_id: "data-c"}
                  }
                ]
              }
            }} = TestBackend.get(Keys.outbox("cluster-op1"), engine: backend)

    [transaction] = TestBackend.transactions(backend)
    success_keys = Enum.map(transaction.success, &operation_key/1)
    assert Keys.blob("cluster-hash") in success_keys
    assert Keys.blob_location("cluster-hash", "data-a") in success_keys
    assert Keys.blob_location("cluster-hash", "data-b") in success_keys
    assert Keys.object_version("bucket", "key", "cluster-v1") in success_keys
    assert Keys.object_head("bucket", "key") in success_keys
    assert Keys.outbox("cluster-op1") in success_keys

    retry_placement =
      placement
      |> Enum.reverse()
      |> Enum.map(fn record ->
        put_in(record, [:node, :generation], record.node.generation + 1)
      end)

    retry_durability = %{
      durability
      | placement: retry_placement,
        acknowledgements:
          Enum.map(acknowledgements, fn acknowledgement ->
            acknowledgement
            |> Map.update!(:node_generation, &(&1 + 1))
            |> Map.put(:verified_at, "2026-07-27T00:01:00Z")
          end)
    }

    assert {:ok, %{version_id: "cluster-v1"}} =
             ObjectCommit.put(
               "bucket",
               "key",
               metadata,
               commit_opts(backend, "cluster-op1", "ignored-retry-version") ++
                 [durability: retry_durability]
             )

    assert [_one_transaction] = TestBackend.transactions(backend)
  end

  test "rebuilds and retries after a compare failure" do
    {:ok, backend} = TestBackend.start_link(compare_failures: 1)

    assert {:ok, %{version_id: "v1"}} = put(backend, "op1", "v1")
    assert length(TestBackend.transactions(backend)) == 2
    assert backend |> TestBackend.transaction_results() |> Map.keys() |> length() == 2
  end

  test "uses the latest v1 version as migration context for the first v2 write" do
    {:ok, backend} = TestBackend.start_link()
    TestBackend.seed(backend, "obj_ver_list:bucket:key", ["legacy-v1"])

    assert {:ok, %{version_id: "v2"}} = put(backend, "op1", "v2")

    assert {:ok, %{value: version}} =
             TestBackend.get(Keys.object_version("bucket", "key", "v2"), engine: backend)

    assert version.parent_version_id == "legacy-v1"

    assert {:ok, %{value: ["legacy-v1"]}} =
             TestBackend.get("obj_ver_list:bucket:key", engine: backend)
  end

  test "resolves an ambiguous timeout by operation id without a second version" do
    {:ok, backend} = TestBackend.start_link(timeout_after_commit: 1)

    assert {:ok, %{version_id: "v1", operation_id: "op1"}} = put(backend, "op1", "v1")
    assert length(TestBackend.transactions(backend)) == 1
    assert {:ok, versions} = list(backend)
    assert Enum.map(versions, & &1.version_id) == ["v1"]
  end

  test "rejects reusing an operation id for a different logical request" do
    {:ok, backend} = TestBackend.start_link()
    assert {:ok, %{version_id: "v1"}} = put(backend, "op1", "v1")

    assert {:error, :operation_id_conflict} =
             ObjectCommit.put(
               "bucket",
               "other-key",
               metadata("other"),
               commit_opts(backend, "op1", "v2")
             )

    assert [_one_transaction] = TestBackend.transactions(backend)
  end

  test "resolves an ambiguous timeout by the exact transaction attempt" do
    {:ok, backend} =
      TestBackend.start_link(
        timeout_after_commit: 1,
        operation_resolution_misses_after_commit: 1
      )

    assert {:ok, %{version_id: "v1", operation_id: "op1"}} = put(backend, "op1", "v1")
    assert [_one_transaction] = TestBackend.transactions(backend)
    assert [_one_attempt] = backend |> TestBackend.transaction_results() |> Map.keys()
  end

  test "100 concurrent puts retain every immutable version and one valid head" do
    {:ok, backend} = TestBackend.start_link()
    {:ok, barrier} = Barrier.start_link(100)

    results =
      1..100
      |> Task.async_stream(
        fn n ->
          ObjectCommit.put("bucket", "key", metadata(n),
            backend: BarrierBackend,
            engine: backend,
            barrier: barrier,
            operation_id: "op#{n}",
            version_id: "v#{n}",
            max_attempts: 200
          )
        end,
        max_concurrency: 100,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))
    assert {:ok, versions} = list(backend)
    assert length(versions) == 100
    assert 100 == versions |> Enum.map(& &1.version_id) |> Enum.uniq() |> length()

    assert {:ok, heads} = TestBackend.scan(Keys.object_head_prefix(), engine: backend)
    assert length(heads) == 1

    assert {:ok, head} =
             ObjectCommit.get_head("bucket", "key", backend: TestBackend, engine: backend)

    assert Enum.any?(versions, &(&1.version_id == head.version_id))
  end

  test "concurrent puts and delete markers retain every operation" do
    {:ok, backend} = TestBackend.start_link()
    {:ok, barrier} = Barrier.start_link(100)

    results =
      1..100
      |> Task.async_stream(
        fn n ->
          opts = [
            backend: BarrierBackend,
            engine: backend,
            barrier: barrier,
            operation_id: "mixed-op#{n}",
            version_id: "mixed-v#{n}",
            max_attempts: 200
          ]

          if rem(n, 2) == 0 do
            ObjectCommit.put("bucket", "key", metadata(n), opts)
          else
            ObjectCommit.delete_marker("bucket", "key", opts)
          end
        end,
        max_concurrency: 100,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, {:ok, _}}, &1))
    assert {:ok, versions} = list(backend)
    assert length(versions) == 100
    assert Enum.count(versions, & &1.is_delete_marker) == 50

    assert {:ok, head} =
             ObjectCommit.get_head("bucket", "key", backend: TestBackend, engine: backend)

    assert Enum.any?(versions, &(&1.version_id == head.version_id))
  end

  test "never writes a v2 mutable object version list" do
    {:ok, backend} = TestBackend.start_link()
    assert {:ok, _} = put(backend, "op1", "v1")

    assert {:ok, _} =
             ObjectCommit.delete_marker("bucket", "key", commit_opts(backend, "op2", "v2"))

    keys =
      backend
      |> TestBackend.transactions()
      |> Enum.flat_map(& &1.success)
      |> Enum.map(&operation_key/1)

    refute Enum.any?(keys, &String.contains?(&1, "object_version_list"))
  end

  test "deleting the head protects its replacement from a concurrent delete" do
    {:ok, backend} = TestBackend.start_link()
    assert {:ok, _} = put(backend, "op1", "v1")
    assert {:ok, _} = put(backend, "op2", "v2")

    assert {:ok, %{kind: :deleted}} =
             ObjectCommit.delete_version(
               "bucket",
               "key",
               "v2",
               commit_opts(backend, "op3", "ignored")
             )

    delete_transaction = backend |> TestBackend.transactions() |> List.last()

    assert {:exists, Keys.object_version("bucket", "key", "v1"), :==, true} in delete_transaction.compare

    assert {:ok, %{version_id: "v1"}} =
             ObjectCommit.get_head("bucket", "key", backend: TestBackend, engine: backend)
  end

  test "v1 compatibility mode rejects mutations instead of using sequential writes" do
    {:ok, backend} = TestBackend.start_link()

    assert {:error, :v2_metadata_writes_disabled} =
             ObjectCommit.put(
               "bucket",
               "key",
               metadata("op1"),
               commit_opts(backend, "op1", "v1") ++ [metadata_schema: :v1]
             )

    assert TestBackend.transactions(backend) == []
  end

  defp put(backend, operation_id, version_id) do
    ObjectCommit.put(
      "bucket",
      "key",
      metadata(operation_id),
      commit_opts(backend, operation_id, version_id)
    )
  end

  defp list(backend),
    do: ObjectCommit.list_versions("bucket", "key", backend: TestBackend, engine: backend)

  defp metadata(id) do
    %{
      content_hash: "hash-#{id}",
      size: 10,
      etag: "etag-#{id}",
      created_at: "2026-07-18T00:00:00Z"
    }
  end

  defp commit_opts(backend, operation_id, version_id) do
    [
      backend: TestBackend,
      engine: backend,
      operation_id: operation_id,
      version_id: version_id
    ]
  end

  defp operation_key({:put, key, _value, _opts}), do: key
  defp operation_key({:delete, {:key, key}, _opts}), do: key
end
