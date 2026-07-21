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
          transactions: []
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
    def resolve_operation(key, opts), do: get(key, opts)

    @impl true
    def transaction(spec, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        state = %{state | transactions: [spec | state.transactions]}

        cond do
          state.compare_failures > 0 ->
            {{:ok, %{succeeded: false}}, %{state | compare_failures: state.compare_failures - 1}}

          not Enum.all?(spec.compare, &compare?(&1, state)) ->
            {{:ok, %{succeeded: false}}, state}

          true ->
            committed = Enum.reduce(spec.success, state, &apply_operation/2)

            if committed.timeout_after_commit > 0 do
              {{:error, :timeout},
               %{committed | timeout_after_commit: committed.timeout_after_commit - 1}}
            else
              {{:ok, %{succeeded: true}}, committed}
            end
        end
      end)
    end

    def transactions(engine) do
      Agent.get(engine, &Enum.reverse(&1.transactions))
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
    defdelegate transaction(spec, opts), to: TestBackend

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

  test "rebuilds and retries after a compare failure" do
    {:ok, backend} = TestBackend.start_link(compare_failures: 1)

    assert {:ok, %{version_id: "v1"}} = put(backend, "op1", "v1")
    assert length(TestBackend.transactions(backend)) == 2
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
