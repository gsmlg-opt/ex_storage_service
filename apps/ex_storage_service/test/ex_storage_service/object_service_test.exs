defmodule ExStorageService.ObjectServiceTest do
  use ExUnit.Case, async: true

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.{Context, InstanceConfig, ObjectService}

  defmodule MetadataStub do
    def head_bucket(_bucket), do: :ok
  end

  defmodule NoPack do
    def locate(_hash), do: {:error, :not_found}
  end

  defmodule CrossClusterHooksStub do
    def events_for_put(bucket, key, object, _opts) do
      {:ok,
       [
         %{
           id: "external-put",
           kind: :cross_cluster_put,
           state: :pending,
           payload: %{bucket: bucket, key: key, object: object}
         }
       ]}
    end

    def events_for_delete(bucket, key, _opts) do
      {:ok,
       [
         %{
           id: "external-delete",
           kind: :cross_cluster_delete,
           state: :pending,
           payload: %{bucket: bucket, key: key}
         }
       ]}
    end
  end

  defmodule OperationIntentsStub do
    def open(operation_id, hash, size, node_id, node_generation, _opts) do
      {:ok,
       %{
         operation_id: operation_id,
         hash: hash,
         size: size,
         node_id: node_id,
         node_generation: node_generation,
         state: :pending
       }}
    end

    def transition(operation_id, state, _opts),
      do: {:ok, %{operation_id: operation_id, state: state}}
  end

  defmodule UnknownIntents do
    defdelegate open(id, hash, size, node_id, generation, opts), to: OperationIntentsStub

    def transition(id, state, _) do
      send(self(), {:unknown_intent_transition, id, state})
      {:error, :timeout}
    end
  end

  defmodule ControlledAbortIntents do
    defdelegate open(id, hash, size, node_id, generation, opts), to: OperationIntentsStub

    def transition(id, state, opts) do
      case Keyword.fetch!(opts, :barrier).({:intent_transition, id, state}) do
        :ok -> OperationIntentsStub.transition(id, state, opts)
        error -> error
      end
    end
  end

  defmodule FailedCustomStore do
    defdelegate stage(data, opts), to: LocalCAS
    def commit(_staged, _opts), do: {:error, {:sync, :enospc}}

    def discard(staged, _opts) do
      send(self(), {:custom_discard, staged.path})
      :ok
    end
  end

  defmodule FailedClusterCoordinator do
    def ensure_blob(_context, staged, opts) do
      :ok = Keyword.fetch!(opts, :barrier).({:coordinator_return, staged.path})
      {:error, :blob_write_quorum_unavailable}
    end
  end

  defmodule ViewChangeEngine do
    @behaviour Concord.Engine

    @impl true
    def command(_command, _opts), do: {:error, :view_change}

    @impl true
    def query(_query, _opts), do: {:error, :view_change}

    @impl true
    def status(_opts), do: {:error, :view_change}

    @impl true
    def members(_opts), do: {:error, :view_change}
  end

  defmodule OperationIntentsFail do
    def open(_operation_id, _hash, _size, _node_id, _node_generation, _opts),
      do: {:error, :gc_lock_active}
  end

  defmodule ProtectedUnknownIntents do
    def open(_id, _hash, _size, _node, _generation, _opts) do
      [name] = File.ls!(Process.get(:protected_test_directory))
      true = String.starts_with?(name, "ess1-h-")
      Process.put(:intent_saw_protected_stage, name)
      {:error, :unknown_transaction_outcome}
    end
  end

  defmodule VanishingBlobStore do
    def verify(_hash, _opts) do
      if Process.get(:object_service_intent_opened),
        do: {:error, :not_found},
        else: :ok
    end

    def stat(hash, _opts),
      do: {:ok, %{hash: hash, size: 4, source: {:file, "/unused", 0, 4}}}
  end

  defmodule DeleteOnOpenIntents do
    def open(operation_id, hash, size, node_id, node_generation, _opts) do
      Process.put(:object_service_intent_opened, true)

      {:ok,
       %{
         operation_id: operation_id,
         hash: hash,
         size: size,
         node_id: node_id,
         node_generation: node_generation,
         state: :pending
       }}
    end

    def transition(operation_id, state, _opts),
      do: {:ok, %{operation_id: operation_id, state: state}}
  end

  defmodule VersioningStub do
    def child_spec(opts) do
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end

    def start_link(opts \\ []) do
      Agent.start_link(fn ->
        %{
          next_version: 1,
          current: %{},
          versions: %{},
          calls: [],
          put_failures: Keyword.get(opts, :put_failures, 0)
        }
      end)
    end

    def put_version(bucket, key, metadata, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        if state.put_failures > 0 do
          {{:error, :injected_versioning_failure},
           %{state | put_failures: state.put_failures - 1}}
        else
          {version_id, state} = next_version(state)
          version = Map.put(metadata, :version_id, version_id)

          state =
            state
            |> put_in([:current, {bucket, key}], version)
            |> put_in([:versions, {bucket, key, version_id}], version)
            |> Map.update!(:calls, &[{:put, bucket, key, metadata, opts} | &1])

          {{:ok, version_id}, state}
        end
      end)
    end

    def get_version(bucket, key, version_id, opts) do
      Agent.get(engine(opts), fn state ->
        version =
          if is_nil(version_id),
            do: Map.get(state.current, {bucket, key}),
            else: Map.get(state.versions, {bucket, key, version_id})

        if version, do: {:ok, version}, else: {:error, :not_found}
      end)
    end

    def delete_version(bucket, key, nil, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        {version_id, state} = next_version(state)

        marker = %{
          version_id: version_id,
          is_delete_marker: true,
          delete_marker: true,
          created_at: "2026-07-18T00:00:00Z"
        }

        state =
          state
          |> put_in([:current, {bucket, key}], marker)
          |> put_in([:versions, {bucket, key, version_id}], marker)
          |> Map.update!(:calls, &[{:delete, bucket, key, nil, opts} | &1])

        {{:ok, version_id, :delete_marker}, state}
      end)
    end

    def delete_version(bucket, key, version_id, opts) do
      Agent.get_and_update(engine(opts), fn state ->
        versions = Map.delete(state.versions, {bucket, key, version_id})
        current = Map.get(state.current, {bucket, key})

        current_by_key =
          if current && current.version_id == version_id,
            do: Map.delete(state.current, {bucket, key}),
            else: state.current

        state = %{
          state
          | versions: versions,
            current: current_by_key,
            calls: [{:delete, bucket, key, version_id, opts} | state.calls]
        }

        {{:ok, version_id, :deleted}, state}
      end)
    end

    def calls(engine), do: Agent.get(engine, &Enum.reverse(&1.calls))

    defp next_version(state) do
      version_id = "v#{state.next_version}"
      {version_id, %{state | next_version: state.next_version + 1}}
    end

    defp engine(opts), do: Keyword.fetch!(opts, :engine)
  end

  @tag :tmp_dir
  test "put and get return domain results with a send-file source", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine, operation_id: "put-op")
    body = "object-service-put"
    hash = sha256(body)

    assert {:ok,
            %{
              version_id: "v1",
              metadata: %{
                content_hash: ^hash,
                size: 18,
                etag: etag,
                content_type: "text/plain",
                metadata: %{"color" => "blue"},
                version_id: "v1"
              },
              ready_blob: %{hash: ^hash, source: {:file, path, 0, 18}}
            }} =
             ObjectService.put(
               "bucket",
               "key",
               ["object-", "service-", "put"],
               "text/plain",
               %{"color" => "blue"},
               opts
             )

    assert etag == md5(body)
    assert File.read!(path) == body

    assert {:ok,
            %{
              version_id: "v1",
              delete_marker: false,
              metadata: %{content_hash: ^hash},
              source: {:file, ^path, 0, 18}
            }} = ObjectService.get("bucket", "key", nil, opts)

    assert {:ok, %{source: {:file, ^path, 7, 7}}} =
             ObjectService.get("bucket", "key", nil, Keyword.put(opts, :range, {7, 7}))

    assert [{:put, "bucket", "key", %{content_hash: ^hash}, metadata_opts}] =
             VersioningStub.calls(engine)

    assert metadata_opts[:operation_id] == "put-op"
  end

  @tag :tmp_dir
  test "put_from_reader returns the final reader state after publishing the object", %{
    tmp_dir: tmp_dir
  } do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine, operation_id: "reader-put-op")

    reader = fn
      0 -> {:more, "state-", 1}
      1 -> {:ok, "threaded", 2}
    end

    assert {:ok, result, 2} =
             ObjectService.put_from_reader(
               "bucket",
               "reader-key",
               reader,
               0,
               "application/octet-stream",
               %{},
               opts
             )

    assert %{
             version_id: "v1",
             metadata: %{size: 14, content_hash: hash},
             ready_blob: %{path: path}
           } = result

    assert File.read!(path) == "state-threaded"
    assert hash == sha256("state-threaded")
  end

  @tag :tmp_dir
  test "attaches cross-cluster events before the object metadata commit", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)

    opts =
      service_opts(tmp_dir, engine,
        side_effects: %{},
        cross_cluster_hooks: CrossClusterHooksStub,
        operation_id: "external-op"
      )

    assert {:ok, %{version_id: "v1"}} =
             ObjectService.put("bucket", "key", "external", "text/plain", %{}, opts)

    assert [{:put, "bucket", "key", _metadata, metadata_opts}] =
             VersioningStub.calls(engine)

    assert [
             %{
               id: "external-put",
               kind: :cross_cluster_put,
               payload: %{bucket: "bucket", key: "key", object: %{content_hash: hash}}
             }
           ] = metadata_opts[:events]

    assert is_binary(hash)
  end

  @tag :tmp_dir
  test "context routes real object operations through embedded split roots", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)

    {:ok, config} =
      InstanceConfig.new(
        data_root: Path.join(tmp_dir, "data"),
        blob_root: Path.join(tmp_dir, "blobs"),
        tmp_root: Path.join(tmp_dir, "staging")
      )

    context = Context.new(config)

    opts = [
      context: context,
      metadata: MetadataStub,
      versioning: VersioningStub,
      metadata_opts: [engine: engine],
      blob_store: LocalCAS,
      blob_store_opts: [pack_module: NoPack],
      operation_intents: OperationIntentsStub,
      side_effects: false,
      timestamp: "2026-07-18T00:00:00Z"
    ]

    assert {:ok, %{ready_blob: %{path: path}}} =
             ObjectService.put("bucket", "key", "split-root", "text/plain", %{}, opts)

    assert String.starts_with?(path, Path.join(tmp_dir, "blobs"))
    assert File.read!(path) == "split-root"
    assert Path.wildcard(Path.join([tmp_dir, "staging", "uploads", "upload-*"])) == []

    assert {:ok, %{source: {:file, ^path, 0, 10}}} =
             ObjectService.get("bucket", "key", nil, opts)
  end

  @tag :tmp_dir
  test "ambiguous publication is observed even when persisting unknown state also fails", %{
    tmp_dir: tmp_dir
  } do
    capture_uncertainty()
    engine = start_supervised!(VersioningStub)

    opts =
      service_opts(tmp_dir, engine,
        operation_id: "private-unknown-operation",
        operation_intents: UnknownIntents,
        faults: [metadata_commit: fn _ -> {:error, :timeout} end]
      )

    assert {:error, :timeout} =
             ObjectService.put("bucket", "unknown", "retained", "text/plain", %{}, opts)

    assert_received {:unknown_intent_transition, "private-unknown-operation", :unknown}
    assert_received {:operation_uncertainty, %{source: :hosted, outcome: :unknown} = metadata}
    assert map_size(metadata) == 2
    assert VersioningStub.calls(engine) == []

    [path] =
      Path.wildcard(Path.join([opts[:blob_store_opts][:root], "objects", "sha256", "*", "*"]))

    assert File.read!(path) == "retained"
  end

  @tag :tmp_dir
  test "a metadata view change preserves an unknown intent and recoverable bytes", %{
    tmp_dir: tmp_dir
  } do
    capture_uncertainty()
    engine = start_supervised!(VersioningStub)

    opts =
      service_opts(tmp_dir, engine,
        operation_id: "view-change-operation",
        operation_intents: UnknownIntents,
        faults: [
          metadata_commit: fn _ ->
            ExStorageService.Metadata.Backend.Concord.transaction(
              %{compare: [], success: [], failure: []},
              engine: ViewChangeEngine
            )
          end
        ]
      )

    result = ObjectService.put("bucket", "view-change", "recoverable", "text/plain", %{}, opts)

    assert_received {:unknown_intent_transition, "view-change-operation", :unknown}
    refute_received {:unknown_intent_transition, "view-change-operation", :aborted}
    assert_received {:operation_uncertainty, %{source: :hosted, outcome: :unknown}}
    assert {:error, :cluster_not_ready} = result
    assert VersioningStub.calls(engine) == []

    [path] =
      Path.wildcard(Path.join([opts[:blob_store_opts][:root], "objects", "sha256", "*", "*"]))

    assert File.read!(path) == "recoverable"
  end

  @tag :tmp_dir
  test "metadata failure leaves one recoverable orphan and no visible object", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    parent = self()

    fault = fn context ->
      send(parent, {:metadata_commit, context})
      {:error, :injected_metadata_failure}
    end

    opts =
      service_opts(tmp_dir, engine,
        operation_id: "orphan-op",
        faults: [metadata_commit: fault]
      )

    assert {:error, :injected_metadata_failure} =
             ObjectService.put("bucket", "orphan", "durable orphan", "text/plain", %{}, opts)

    assert_received {:metadata_commit,
                     %{
                       bucket: "bucket",
                       key: "orphan",
                       operation_id: "orphan-op",
                       metadata: %{content_hash: hash},
                       ready_blob: %{path: path, hash: ready_hash}
                     }}

    assert ready_hash == hash
    assert File.read!(path) == "durable orphan"
    assert VersioningStub.calls(engine) == []
    assert {:error, :object_not_found} = ObjectService.get("bucket", "orphan", nil, opts)
  end

  @tag :tmp_dir
  test "versioning failure leaves a reusable orphan and retry publishes one version", %{
    tmp_dir: tmp_dir
  } do
    engine = start_supervised!({VersioningStub, put_failures: 1})
    opts = service_opts(tmp_dir, engine, operation_id: "retry-op")
    body = "retryable orphan"
    hash = sha256(body)

    assert {:error, :injected_versioning_failure} =
             ObjectService.put("bucket", "retry", body, "text/plain", %{}, opts)

    ready_path = LocalCAS.blob_path(hash, opts[:blob_store_opts])
    assert File.read!(ready_path) == body
    assert {:error, :object_not_found} = ObjectService.head("bucket", "retry", opts)

    assert {:ok, %{version_id: "v1", ready_blob: %{path: ^ready_path}}} =
             ObjectService.put("bucket", "retry", body, "text/plain", %{}, opts)

    assert [{:put, "bucket", "retry", %{content_hash: ^hash}, _metadata_opts}] =
             VersioningStub.calls(engine)

    assert [^ready_path] =
             Path.wildcard(
               Path.join([opts[:blob_store_opts][:root], "objects", "sha256", "*", "*"])
             )
  end

  @tag :tmp_dir
  test "stage and publish boundary failures never expose metadata", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    base_opts = service_opts(tmp_dir, engine)

    assert {:error, :after_stage_failure} =
             ObjectService.put(
               "bucket",
               "stage",
               "stage",
               "text/plain",
               %{},
               Keyword.put(base_opts, :faults, after_stage: {:error, :after_stage_failure})
             )

    assert {:error, :object_not_found} = ObjectService.head("bucket", "stage", base_opts)
    assert Path.wildcard(Path.join([base_opts[:blob_store_opts][:root], "**", "upload-*"])) == []

    for phase <- [:sync, :rename] do
      opts =
        Keyword.update!(base_opts, :blob_store_opts, fn blob_opts ->
          Keyword.put(blob_opts, :faults, %{phase => {:error, :injected}})
        end)

      assert {:error, {^phase, :injected}} =
               ObjectService.put("bucket", "failed-#{phase}", "failed", "text/plain", %{}, opts)

      assert {:error, :object_not_found} =
               ObjectService.head("bucket", "failed-#{phase}", base_opts)
    end
  end

  @tag :tmp_dir
  test "an operation-intent conflict discards staged bytes", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)

    opts =
      service_opts(tmp_dir, engine,
        operation_intents: OperationIntentsFail,
        operation_id: "intent-conflict"
      )

    assert {:error, :gc_lock_active} =
             ObjectService.put("bucket", "conflict", "discard-me", "text/plain", %{}, opts)

    assert VersioningStub.calls(engine) == []
    assert Path.wildcard(Path.join([opts[:blob_store_opts][:root], "**", "upload-*"])) == []
    refute File.exists?(LocalCAS.blob_path(sha256("discard-me"), opts[:blob_store_opts]))
  end

  @tag :tmp_dir
  test "confirmed abort reclaims four owned protected stages and preserves other bytes", %{
    tmp_dir: root
  } do
    versioning = start_supervised!(VersioningStub)
    owner = self()

    barrier = fn {:intent_transition, id, state} ->
      send(owner, {:abort_waiting, self(), id, state})

      receive do
        {:confirm_abort, ^id} -> :ok
      after
        5_000 -> {:error, :timeout}
      end
    end

    opts = managed_service_opts(root, versioning, barrier)
    blob_opts = Context.blob_store_options(opts[:context]) ++ [pack_module: NoPack]
    {:ok, existing} = LocalCAS.stage("existing CAS bytes", blob_opts)
    {:ok, ready} = LocalCAS.commit(existing, blob_opts)
    {:ok, other} = LocalCAS.stage("other owner's protected bytes", blob_opts)
    {:ok, other} = LocalCAS.protect(other, {:hosted, "shared-operation"}, blob_opts)
    uploads = blob_opts[:tmp_dir]
    baseline = temporary_bytes(uploads)
    body = :binary.copy("bounded fixture", 1_024)

    # This reproduces the confirmed-abort retention mechanism, not the unknown
    # filesystem error from the original CopyObject resource receipt.
    tasks =
      for n <- 1..4 do
        Task.async(fn ->
          failed =
            opts
            |> Keyword.update!(
              :metadata_opts,
              &Keyword.put(&1, :operation_id, "shared-operation")
            )
            |> Keyword.update!(
              :blob_store_opts,
              &Keyword.put(&1, :faults, %{sync: {:error, :enospc}})
            )

          ObjectService.put("bucket", "failed-#{n}", body, "text/plain", %{}, failed)
        end)
      end

    waiting =
      for _ <- 1..4 do
        assert_receive {:abort_waiting, pid, "shared-operation", :aborted}, 5_000
        pid
      end

    assert temporary_bytes(uploads) == baseline + 4 * byte_size(body)
    assert length(File.ls!(uploads)) == 5
    Enum.each(waiting, &send(&1, {:confirm_abort, "shared-operation"}))

    assert Enum.map(tasks, &Task.await(&1, 5_000)) ==
             List.duplicate({:error, {:sync, :enospc}}, 4)

    assert temporary_bytes(uploads) == baseline
    assert File.ls!(uploads) == [Path.basename(other.path)]
    assert File.read!(other.path) == "other owner's protected bytes"
    assert File.read!(ready.path) == "existing CAS bytes"
    assert VersioningStub.calls(versioning) == []
  end

  @tag :tmp_dir
  test "reader commit failure reclaims its stage only after confirmed abort", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)
    owner = self()
    barrier = fn event -> send(owner, event) && :ok end

    opts =
      managed_service_opts(root, versioning, barrier)
      |> Keyword.update!(:blob_store_opts, &Keyword.put(&1, :faults, %{sync: {:error, :enospc}}))

    reader = fn
      :initial -> {:more, "reader fixture", :delivered}
      :delivered -> {:done, :finished}
    end

    assert {:error, {:sync, :enospc}, :finished} =
             ObjectService.put_from_reader(
               "bucket",
               "reader",
               reader,
               :initial,
               "text/plain",
               %{},
               opts
             )

    assert_receive {:intent_transition, _, :aborted}
    assert temporary_bytes(opts[:blob_store_opts][:tmp_dir]) == 0
    assert File.ls!(opts[:blob_store_opts][:tmp_dir]) == []
    assert VersioningStub.calls(versioning) == []
  end

  @tag :tmp_dir
  test "nested storage uncertainty keeps protected stages and records unknown", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)
    owner = self()

    for reason <- [:timeout, :unknown, :unknown_transaction_outcome] do
      barrier = fn event -> send(owner, event) && :ok end

      opts =
        managed_service_opts(Path.join(root, Atom.to_string(reason)), versioning, barrier)
        |> Keyword.update!(:blob_store_opts, &Keyword.put(&1, :faults, %{sync: {:error, reason}}))

      assert {:error, {:sync, ^reason}} =
               ObjectService.put(
                 "bucket",
                 "uncertain",
                 "retain uncertain bytes",
                 "text/plain",
                 %{},
                 opts
               )

      assert_receive {:intent_transition, _, :unknown}
      refute_received {:intent_transition, _, :aborted}
      uploads = opts[:blob_store_opts][:tmp_dir]
      [name] = File.ls!(uploads)
      assert String.starts_with?(name, "ess1-h-")
      assert File.read!(Path.join(uploads, name)) == "retain uncertain bytes"
    end

    assert VersioningStub.calls(versioning) == []
  end

  @tag :tmp_dir
  test "unconfirmed abort retains its owned protected stage", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)

    for reason <- [:timeout, :operation_intent_compare_failed] do
      barrier = fn {:intent_transition, _id, :aborted} -> {:error, reason} end

      opts =
        managed_service_opts(Path.join(root, Atom.to_string(reason)), versioning, barrier)
        |> Keyword.update!(
          :blob_store_opts,
          &Keyword.put(&1, :faults, %{sync: {:error, :enospc}})
        )

      assert {:error, {:sync, :enospc}} =
               ObjectService.put(
                 "bucket",
                 "unconfirmed",
                 "retain aborted bytes",
                 "text/plain",
                 %{},
                 opts
               )

      uploads = opts[:blob_store_opts][:tmp_dir]
      [name] = File.ls!(uploads)
      assert String.starts_with?(name, "ess1-h-")
      assert File.read!(Path.join(uploads, name)) == "retain aborted bytes"
    end
  end

  @tag :tmp_dir
  test "custom blob stores retain their existing discard contract", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)
    barrier = fn _ -> :ok end

    opts =
      Keyword.put(managed_service_opts(root, versioning, barrier), :blob_store, FailedCustomStore)

    assert {:error, {:sync, :enospc}} =
             ObjectService.put("bucket", "custom", "custom owned bytes", "text/plain", %{}, opts)

    assert_receive {:custom_discard, path}
    refute_received {:custom_discard, _}
    assert File.read!(path) == "custom owned bytes"
    assert VersioningStub.calls(versioning) == []
  end

  @tag :tmp_dir
  test "cluster aggregate failure does not authorize protected stage deletion", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)
    owner = self()
    barrier = fn event -> send(owner, event) && :ok end
    opts = managed_service_opts(root, versioning, barrier)
    context = opts[:context]

    context = %{
      context
      | config: %{context.config | mode: :cluster, cluster_data_plane_enabled: true}
    }

    opts =
      opts
      |> Keyword.put(:context, context)
      |> Keyword.put(:write_coordinator, FailedClusterCoordinator)

    assert {:error, :blob_write_quorum_unavailable} =
             ObjectService.put(
               "bucket",
               "cluster",
               "retained cluster bytes",
               "text/plain",
               %{},
               opts
             )

    assert_receive {:coordinator_return, path}
    assert File.read!(path) == "retained cluster bytes"
    assert String.starts_with?(Path.basename(path), "ess1-h-")
    assert VersioningStub.calls(versioning) == []
  end

  @tag :tmp_dir
  test "after-blob-commit failure leaves one ready orphan and no metadata", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)

    opts =
      service_opts(tmp_dir, engine,
        faults: [after_blob_commit: {:error, :after_blob_commit_failure}]
      )

    assert {:error, :after_blob_commit_failure} =
             ObjectService.put("bucket", "ready-orphan", "ready", "text/plain", %{}, opts)

    ready_path = LocalCAS.blob_path(sha256("ready"), opts[:blob_store_opts])
    assert File.read!(ready_path) == "ready"
    assert {:error, :object_not_found} = ObjectService.head("bucket", "ready-orphan", opts)
    assert VersioningStub.calls(engine) == []
  end

  test "invalid ready blobs and object metadata return structured errors" do
    engine = start_supervised!(VersioningStub)

    opts = [
      metadata: MetadataStub,
      versioning: VersioningStub,
      metadata_opts: [engine: engine],
      side_effects: false
    ]

    assert {:error, :invalid_ready_blob} =
             ObjectService.commit_existing_blob("bucket", "key", :invalid, %{}, opts)

    assert {:error, :invalid_ready_blob} =
             ObjectService.commit_existing_blob(
               "bucket",
               "key",
               %{hash: sha256("negative"), size: -1},
               %{},
               opts
             )

    assert {:ok, _version_id} =
             VersioningStub.put_version("bucket", "broken", %{}, engine: engine)

    assert {:error, :invalid_object_metadata} = ObjectService.get("bucket", "broken", nil, opts)
  end

  test "commit_existing_blob revalidates the blob after opening its GC intent" do
    engine = start_supervised!(VersioningStub)
    hash = sha256("gone")

    opts = [
      metadata: MetadataStub,
      versioning: VersioningStub,
      metadata_opts: [engine: engine, operation_id: "revalidate-after-intent"],
      blob_store: VanishingBlobStore,
      operation_intents: DeleteOnOpenIntents,
      side_effects: false
    ]

    assert {:error, :blob_not_found} =
             ObjectService.commit_existing_blob(
               "bucket",
               "key",
               %{hash: hash, size: 4},
               %{},
               opts
             )

    assert VersioningStub.calls(engine) == []
  end

  @tag :tmp_dir
  test "delete returns a marker result and latest GET exposes no blob source", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine)

    assert {:ok, %{version_id: "v1"}} =
             ObjectService.put("bucket", "key", "delete-me", "text/plain", %{}, opts)

    assert {:ok, %{version_id: "v2", kind: :delete_marker}} =
             ObjectService.delete("bucket", "key", nil, opts)

    assert {:ok,
            %{
              version_id: "v2",
              delete_marker: true,
              metadata: %{is_delete_marker: true},
              source: nil
            }} = ObjectService.get("bucket", "key", nil, opts)

    assert File.exists?(LocalCAS.blob_path(sha256("delete-me"), opts[:blob_store_opts]))
  end

  @tag :tmp_dir
  test "copy reuses the immutable blob and returns destination metadata", %{tmp_dir: tmp_dir} do
    engine = start_supervised!(VersioningStub)
    opts = service_opts(tmp_dir, engine)
    body = "copy-without-buffering"
    hash = sha256(body)

    assert {:ok, %{version_id: "v1", ready_blob: source_ready}} =
             ObjectService.put("source", "key", body, "text/plain", %{"origin" => "source"}, opts)

    assert {:ok,
            %{
              version_id: "v2",
              metadata: %{
                content_hash: ^hash,
                size: 22,
                content_type: "text/plain",
                metadata: %{"origin" => "source"}
              },
              ready_blob: %{hash: ^hash, source: {:file, path, 0, 22}}
            }} = ObjectService.copy("source", "key", "destination", "copy", opts)

    assert path == source_ready.path

    assert [^path] =
             Path.wildcard(
               Path.join([opts[:blob_store_opts][:root], "objects", "sha256", "*", "*"])
             )

    assert {:ok, %{source: {:file, ^path, 0, 22}, metadata: %{content_hash: ^hash}}} =
             ObjectService.get("destination", "copy", nil, opts)
  end

  @tag :tmp_dir
  test "failed protection data sync submits no hosted intent or object metadata", %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)

    opts =
      service_opts(root, versioning,
        operation_id: "failed-protection-data-sync",
        operation_intents: ProtectedUnknownIntents
      )
      |> Keyword.update!(:blob_store_opts, fn blob_opts ->
        Keyword.put(blob_opts, :faults, %{protection_data_sync: :injected_data_sync})
      end)

    uploads = opts[:blob_store_opts][:tmp_dir]
    Process.put(:protected_test_directory, uploads)

    assert {:error, {:staging_protection, :injected_data_sync}} =
             ObjectService.put(
               "bucket",
               "key",
               "retained anonymous bytes",
               "text/plain",
               %{},
               opts
             )

    refute Process.get(:intent_saw_protected_stage)
    [name] = File.ls!(uploads)
    assert String.starts_with?(name, "ess1-a-")
    assert File.read!(Path.join(uploads, name)) == "retained anonymous bytes"

    refute File.exists?(
             LocalCAS.blob_path(sha256("retained anonymous bytes"), opts[:blob_store_opts])
           )
  end

  @tag :tmp_dir
  test "unknown intent submission retains the protected path; failed protection submits nothing",
       %{tmp_dir: root} do
    versioning = start_supervised!(VersioningStub)

    opts =
      service_opts(root, versioning,
        operation_id: "late-intent",
        operation_intents: ProtectedUnknownIntents
      )

    uploads = opts[:blob_store_opts][:tmp_dir]
    Process.put(:protected_test_directory, uploads)

    assert {:error, :unknown_transaction_outcome} =
             ObjectService.put("bucket", "key", "recoverable", "text/plain", %{}, opts)

    name = Process.get(:intent_saw_protected_stage)
    assert File.read!(Path.join(uploads, name)) == "recoverable"
    File.rm!(Path.join(uploads, name))
    Process.delete(:intent_saw_protected_stage)

    failed =
      Keyword.update!(
        opts,
        :blob_store_opts,
        &Keyword.put(&1, :faults, %{protection_directory_sync: :injected})
      )

    assert {:error, {:staging_protection, :injected}} =
             ObjectService.put("bucket", "key", "retained", "text/plain", %{}, failed)

    refute Process.get(:intent_saw_protected_stage)
    [name] = File.ls!(uploads)
    assert String.starts_with?(name, "ess1-h-")
    assert File.read!(Path.join(uploads, name)) == "retained"
  end

  defp managed_service_opts(root, versioning, barrier) do
    {:ok, config} =
      InstanceConfig.new(
        data_root: Path.join(root, "data"),
        blob_root: Path.join(root, "cas"),
        tmp_root: Path.join([root, "cas", "tmp"])
      )

    service_opts(root, versioning,
      context: Context.new(config),
      operation_intents: ControlledAbortIntents,
      metadata_opts: [engine: versioning, barrier: barrier]
    )
  end

  defp temporary_bytes(directory) do
    directory
    |> File.ls!()
    |> Enum.map(&File.stat!(Path.join(directory, &1)).size)
    |> Enum.sum()
  end

  defp service_opts(tmp_dir, engine, extra \\ []) do
    root = Path.join(tmp_dir, "cas")

    [
      metadata: MetadataStub,
      versioning: VersioningStub,
      metadata_opts: [engine: engine],
      blob_store: LocalCAS,
      blob_store_opts: [
        root: root,
        tmp_dir: Path.join([root, "tmp", "uploads"]),
        pack_module: NoPack
      ],
      operation_intents: OperationIntentsStub,
      side_effects: false,
      timestamp: "2026-07-18T00:00:00Z"
    ]
    |> Keyword.merge(extra)
    |> then(fn opts ->
      operation_id = Keyword.get(extra, :operation_id)

      if operation_id,
        do: Keyword.update!(opts, :metadata_opts, &Keyword.put(&1, :operation_id, operation_id)),
        else: opts
    end)
  end

  defp sha256(data),
    do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)

  defp md5(data),
    do: :md5 |> :crypto.hash(data) |> Base.encode16(case: :lower)

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
