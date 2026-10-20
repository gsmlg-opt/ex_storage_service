defmodule ExStorageService.Storage.StagingRecoveryTest do
  use ExUnit.Case, async: true
  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Storage.StagingRecovery, as: Recovery
  @moduletag :tmp_dir

  defmodule Backend do
    def get(key, opts) do
      Process.put(:guard_reads, [{key, opts} | Process.get(:guard_reads, [])])
      Process.get(:guard_result, {:ok, nil})
    end
  end

  test "fresh BEAM partial and completed anonymous stages are reclaimed", %{tmp_dir: root} do
    assert {_, 137} =
             beam(root, ~S"""
             opts = [tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"), managed_staging: [instance: :recovery_test, node_id: "test"]]
             {:ok, _} = ExStorageService.BlobStore.LocalCAS.stage("completed", opts)
             stream = Stream.map([:bytes, :crash], fn
               :bytes -> "partial"
               :crash -> System.halt(137)
             end)
             ExStorageService.BlobStore.LocalCAS.stage(stream, opts)
             """)

    assert [_, _] = File.ls!(root)
    state = sweep(root)
    assert state.deleted == 2
    assert [] = File.ls!(root)
  end

  test "live, foreign, legacy, malformed and symlink paths are retained", %{tmp_dir: root} do
    {:ok, live} = LocalCAS.stage("live", opts(root))

    {:ok, foreign} =
      LocalCAS.stage(
        "foreign",
        Keyword.put(opts(root), :managed_staging, instance: :other, node_id: "test")
      )

    File.write!(Path.join(root, "upload-1"), "legacy")
    File.write!(Path.join(root, "ess1-a-malformed"), "malformed")
    File.ln_s!(live.path, Path.join(root, "ess1-a-symlink"))
    state = sweep(root)
    assert state.deleted == 0
    assert File.read!(live.path) == "live"
    assert File.read!(foreign.path) == "foreign"
    assert Enum.count(File.ls!(root)) == 5
  end

  test "all existing guards and uncertain reads veto deletion", %{tmp_dir: root} do
    for result <- [
          {:ok, %{value: %{expires_at: 0}}},
          {:ok, %{value: :malformed}},
          {:error, :timeout},
          :malformed
        ] do
      dead_stage(root)
      Process.put(:guard_result, result)
      state = sweep(root)
      assert state.deleted == 0
    end

    assert Enum.count(File.ls!(root)) == 4

    assert [{"ess:v2:gc_guard:" <> _, [consistency: :linearizable]} | _] =
             Process.get(:guard_reads)
  end

  test "protected late hosted and gateway submissions remain after absent reads", %{tmp_dir: root} do
    assert {_, 0} =
             beam(root, ~S"""
             opts = [tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"), managed_staging: [instance: :recovery_test, node_id: "test"]]
             for kind <- [{:hosted, "late-hosted"}, {:gateway, "late-gateway"}, {:gateway, String.duplicate("long", 200)}] do
               {:ok, stage} = ExStorageService.BlobStore.LocalCAS.stage("late commit bytes", opts)
               {:ok, _} = ExStorageService.BlobStore.LocalCAS.protect(stage, kind, opts)
             end
             """)

    assert sweep(root).deleted == 0
    assert [_, _, _] = File.ls!(root)

    for name <- File.ls!(root),
        do: assert(File.read!(Path.join(root, name)) == "late commit bytes")
  end

  test "retained protected entries cannot starve a later anonymous candidate", %{tmp_dir: root} do
    assert {_, 0} =
             beam(root, ~S"""
             opts = [tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"), managed_staging: [instance: :recovery_test, node_id: "test"]]
             for _ <- 1..65 do
               {:ok, stage} = ExStorageService.BlobStore.LocalCAS.stage("retain", opts)
               {:ok, _} = ExStorageService.BlobStore.LocalCAS.protect(stage, {:hosted, "op"}, opts)
             end
             {:ok, _} = ExStorageService.BlobStore.LocalCAS.stage("remove", opts)
             """)

    state = sweep(root)
    assert state.deleted == 1
    assert Enum.count(File.ls!(root)) == 65
    assert state.decisions == 66
  end

  test "large candidate yields after at most four MiB and closes scanner on shutdown", %{
    tmp_dir: root
  } do
    dead_stage(root, 9 * 1024 * 1024)
    {:ok, state} = Recovery.new(opts(root) ++ [backend: Backend])
    state = next_candidate(state)
    state = Recovery.step(state)
    assert state.last_turn_bytes == 4 * 1024 * 1024
    assert state.candidate != nil
    assert state.deleted == 0
    {_, scanner_pid} = Port.info(state.port, :os_pid)
    Recovery.close(state)
    refute File.exists?("/proc/#{scanner_pid}")
    assert [_] = File.ls!(root)
  end

  test "unavailable proc identity disables reclamation", %{tmp_dir: root} do
    dead_stage(root)

    assert {:disabled, :owner_scope_unavailable} =
             Recovery.new(opts(root) ++ [proc_root: Path.join(root, "missing-proc")])

    assert [_] = File.ls!(root)
  end

  test "direct context options remain unmanaged", %{tmp_dir: root} do
    {:ok, config} =
      ExStorageService.InstanceConfig.new(data_root: root, blob_root: root, tmp_root: root)

    context = ExStorageService.Context.new(config)

    assert Keyword.has_key?(
             ExStorageService.Context.blob_store_options(context),
             :managed_staging
           )

    refute Keyword.has_key?(
             ExStorageService.Context.direct_blob_store_options(context),
             :managed_staging
           )
  end

  test "a valid anonymous symlink and a changed candidate are never unlinked", %{tmp_dir: root} do
    dead_stage(root, 5 * 1024 * 1024)
    [name] = File.ls!(root)
    path = Path.join(root, name)
    {:ok, state} = Recovery.new(opts(root) ++ [backend: Backend])
    state = next_candidate(state)
    state = Recovery.step(state)
    File.write!(path, "changed candidate")
    state = drive(state)
    assert state.deleted == 0
    assert File.read!(path) == "changed candidate"
    File.rm!(path)
    target = Path.join(root, "unmanaged-target")
    File.write!(target, "symlink target")
    File.ln_s!(target, path)
    assert sweep(root).deleted == 0
    assert {:ok, %{type: :symlink}} = File.lstat(path)
    assert File.read!(target) == "symlink target"
  end

  test "root replacement between hash turns retains both roots", %{tmp_dir: root} do
    uploads = Path.join(root, "uploads")
    File.mkdir_p!(uploads)
    dead_stage(uploads, 5 * 1024 * 1024)
    {:ok, state} = Recovery.new(opts(uploads) ++ [backend: Backend])
    state = next_candidate(state) |> Recovery.step()
    moved = Path.join(root, "old-uploads")
    File.rename!(uploads, moved)
    File.mkdir_p!(uploads)
    state = drive(state)
    assert state.deleted == 0
    assert [_] = File.ls!(moved)
  end

  test "one unacknowledged candidate bounds the queue and shutdown reaps its helper", %{
    tmp_dir: root
  } do
    for _ <- 1..100, do: LocalCAS.stage("live", opts(root))
    {:ok, state} = Recovery.new(opts(root) ++ [backend: Backend])
    port = state.port

    receive do
      {^port, {:data, {:eol, _}}} -> :ok
    after
      3000 -> flunk("missing cursor candidate")
    end

    {_, scanner_pid} = Port.info(port, :os_pid)
    children = File.read!("/proc/#{scanner_pid}/task/#{scanner_pid}/children") |> String.split()
    assert [_] = children
    Process.sleep(50)
    assert {:message_queue_len, 0} = Process.info(self(), :message_queue_len)
    Recovery.close(state)

    for pid <- [Integer.to_string(scanner_pid) | children],
        do: refute(File.exists?("/proc/#{pid}"))
  end

  defmodule ProtectionCollisionFS do
    alias ExStorageService.BlobStore.LocalCAS.FileSystem

    def open(path, modes) do
      if :exclusive in modes do
        File.write!(path, "another owner")
        Process.put(:protection_collision_path, path)
      end

      FileSystem.open(path, modes)
    end

    defdelegate sync(io), to: FileSystem
    defdelegate close(io), to: FileSystem
  end

  defmodule ProtectionRecordingFS do
    alias ExStorageService.BlobStore.LocalCAS.FileSystem

    def open(path, modes) do
      role = if :exclusive in modes, do: :reservation, else: :data
      record({:open, role})

      if role == :data and Process.get(:protection_fs_failure) == :data_open do
        {:error, :injected_open}
      else
        remember(FileSystem.open(path, modes), role)
      end
    end

    def open_directory(path) do
      record({:open, :directory})
      remember(FileSystem.open_directory(path), :directory)
    end

    def sync(io) do
      role = Process.get({:protection_handle, io})
      record({:sync, role})

      if role == :data and Process.get(:protection_fs_failure) == :data_sync,
        do: {:error, :injected_sync},
        else: FileSystem.sync(io)
    end

    def close(io) do
      role = Process.delete({:protection_handle, io})
      record({:close, role})
      result = FileSystem.close(io)

      if role == :data and Process.get(:protection_fs_failure) == :data_close,
        do: {:error, :injected_close},
        else: result
    end

    def rename(source, destination) do
      record({:rename, source, destination})
      FileSystem.rename(source, destination)
    end

    defp remember({:ok, io} = result, role) do
      Process.put({:protection_handle, io}, role)
      result
    end

    defp remember(error, _role), do: error

    defp record(event),
      do: Process.put(:protection_fs_events, [event | Process.get(:protection_fs_events, [])])
  end

  test "managed protection syncs and closes completed bytes before reserving or renaming", %{
    tmp_dir: root
  } do
    for kind <- [{:hosted, "ordered-hosted"}, {:gateway, "ordered-gateway"}] do
      {:ok, staged} = LocalCAS.stage("ordered protection bytes", opts(root))
      Process.delete(:protection_fs_events)

      assert {:ok, protected} =
               LocalCAS.protect(staged, kind, opts(root) ++ [fs_module: ProtectionRecordingFS])

      assert [
               {:open, :data},
               {:sync, :data},
               {:close, :data},
               {:open, :reservation},
               {:close, :reservation},
               {:rename, source, destination},
               {:open, :directory},
               {:sync, :directory},
               {:close, :directory}
             ] = Process.get(:protection_fs_events) |> Enum.reverse()

      assert source == staged.path
      assert destination == protected.path
      assert Recovery.protected?(protected.path)
      refute File.exists?(staged.path)
      assert File.read!(protected.path) == "ordered protection bytes"
      assert protected.hash == staged.hash
      assert protected.size == staged.size
    end
  end

  for {failure, reason, events} <- [
        {:data_open, :injected_open, [{:open, :data}]},
        {:data_sync, :injected_sync, [{:open, :data}, {:sync, :data}, {:close, :data}]},
        {:data_close, :injected_close, [{:open, :data}, {:sync, :data}, {:close, :data}]}
      ] do
    test "protection #{failure} failure preserves anonymous bytes without a reservation", %{
      tmp_dir: root
    } do
      {:ok, staged} = LocalCAS.stage("unsent protection bytes", opts(root))
      Process.put(:protection_fs_failure, unquote(failure))

      assert {:error, {:staging_protection, unquote(reason)}} =
               LocalCAS.protect(
                 staged,
                 {:hosted, "never-submit"},
                 opts(root) ++ [fs_module: ProtectionRecordingFS]
               )

      assert Process.get(:protection_fs_events) |> Enum.reverse() ==
               unquote(Macro.escape(events))

      assert File.ls!(root) == [Path.basename(staged.path)]
      refute Recovery.protected?(staged.path)
      assert File.read!(staged.path) == "unsent protection bytes"
    end
  end

  test "protection data-sync fault closes the source and cannot return a submit-ready stage", %{
    tmp_dir: root
  } do
    {:ok, staged} = LocalCAS.stage("faulted protection bytes", opts(root))

    assert {:error, {:staging_protection, :injected_data_sync}} =
             LocalCAS.protect(
               staged,
               {:gateway, "never-send-upstream"},
               opts(root) ++
                 [
                   fs_module: ProtectionRecordingFS,
                   faults: %{protection_data_sync: :injected_data_sync}
                 ]
             )

    assert [{:open, :data}, {:close, :data}] =
             Process.get(:protection_fs_events) |> Enum.reverse()

    assert File.ls!(root) == [Path.basename(staged.path)]
    assert File.read!(staged.path) == "faulted protection bytes"
  end

  test "unmanaged protection retains its existing no-sync contract", %{tmp_dir: root} do
    {:ok, staged} = LocalCAS.stage("unmanaged bytes", tmp_dir: root)

    assert {:ok, ^staged} =
             LocalCAS.protect(staged, {:hosted, "unmanaged"},
               tmp_dir: root,
               fs_module: ProtectionRecordingFS
             )

    assert Process.get(:protection_fs_events, []) == []
  end

  test "failed protected reservation preserves both owners' bytes", %{tmp_dir: root} do
    {:ok, staged} = LocalCAS.stage("source owner", opts(root))

    assert {:error, {:staging_protection, :eexist}} =
             LocalCAS.protect(
               staged,
               {:hosted, "op"},
               opts(root) ++ [fs_module: ProtectionCollisionFS]
             )

    assert File.read!(staged.path) == "source owner"
    assert File.read!(Process.get(:protection_collision_path)) == "another owner"
  end

  test "failed protection sync retains renamed bytes and never returns a submit-ready stage", %{
    tmp_dir: root
  } do
    {:ok, staged} = LocalCAS.stage("uncertain protection", opts(root))

    assert {:error, {:staging_protection, :injected_sync}} =
             LocalCAS.protect(
               staged,
               {:hosted, "op"},
               opts(root) ++ [faults: %{protection_directory_sync: :injected_sync}]
             )

    refute File.exists?(staged.path)
    [protected] = File.ls!(root)
    assert String.starts_with?(protected, "ess1-h-")
    assert File.read!(Path.join(root, protected)) == "uncertain protection"
  end

  test "Engine startup reclaims dead stages with explicit context and respects disabled maintenance",
       %{tmp_dir: root} do
    uploads = Path.join(root, "tmp/uploads")
    File.mkdir_p!(uploads)
    dead_stage(uploads)

    {:ok, config} =
      ExStorageService.InstanceConfig.new(
        instance: :recovery_test,
        node_id: "test",
        data_root: Path.join(root, "data"),
        blob_root: Path.join(root, "blobs"),
        tmp_root: Path.join(root, "tmp"),
        workers: [cas_gc: false]
      )

    context = ExStorageService.Context.new(config)

    settings = [
      data_root: context.data_root,
      blob_root: context.blob_root,
      tmp_root: context.tmp_root,
      recovery_context: context,
      name: {:global, {__MODULE__, make_ref()}}
    ]

    {:ok, engine} = ExStorageService.Storage.Engine.start_link(settings)
    assert :sys.get_state(engine).staging_recovery == nil
    assert [_] = File.ls!(uploads)
    GenServer.stop(engine)
    context = %{context | config: %{config | workers: Map.put(config.workers, :cas_gc, true)}}

    {:ok, engine} =
      ExStorageService.Storage.Engine.start_link(
        Keyword.put(settings, :recovery_context, context)
      )

    assert eventually(fn -> File.ls!(uploads) == [] end)
    GenServer.stop(engine)
  end

  test "restore quarantine never starts staging recovery", %{tmp_dir: root} do
    uploads = Path.join(root, "tmp/uploads")
    File.mkdir_p!(uploads)
    dead_stage(uploads)
    File.mkdir_p!(Path.join(root, "data"))
    File.write!(Path.join(root, "data/.ess-recovery.json"), "{}")

    {:ok, config} =
      ExStorageService.InstanceConfig.new(
        instance: :recovery_test,
        node_id: "test",
        data_root: Path.join(root, "data"),
        blob_root: Path.join(root, "blobs"),
        tmp_root: Path.join(root, "tmp"),
        workers: [cas_gc: true]
      )

    context = ExStorageService.Context.new(config)

    {:ok, engine} =
      ExStorageService.Storage.Engine.start_link(
        data_root: context.data_root,
        blob_root: context.blob_root,
        tmp_root: context.tmp_root,
        recovery_context: context,
        name: {:global, {__MODULE__, make_ref()}}
      )

    assert :sys.get_state(engine).staging_recovery == nil
    assert [_] = File.ls!(uploads)
    GenServer.stop(engine)
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.(),
      do: true,
      else:
        (
          Process.sleep(20)
          eventually(fun, attempts - 1)
        )
  end

  defp opts(root),
    do: [tmp_dir: root, managed_staging: [instance: :recovery_test, node_id: "test"]]

  defp dead_stage(root, size \\ 12) do
    assert {_, 0} =
             beam(
               root,
               ~S"""
               opts = [tmp_dir: System.fetch_env!("ESS_STAGE_TEST_ROOT"), managed_staging: [instance: :recovery_test, node_id: "test"]]
               {:ok, _} = ExStorageService.BlobStore.LocalCAS.stage(String.duplicate("x", String.to_integer(System.fetch_env!("ESS_STAGE_TEST_SIZE"))), opts)
               """,
               [{"ESS_STAGE_TEST_SIZE", Integer.to_string(size)}]
             )
  end

  defp sweep(root) do
    {:ok, state} = Recovery.new(opts(root) ++ [backend: Backend])
    drive(state)
  end

  defp next_candidate(state) do
    receive do
      message ->
        case Recovery.message(state, message) do
          {:ok, %{candidate: nil} = state} -> next_candidate(state)
          {:ok, state} -> state
          other -> flunk("unexpected scanner message #{inspect(other)}")
        end
    after
      3000 -> flunk("scanner did not supply candidate")
    end
  end

  defp drive(%{candidate: candidate} = state) when candidate != nil,
    do: drive(Recovery.step(state))

  defp drive(state) do
    receive do
      message ->
        case Recovery.message(state, message) do
          {:ok, state} -> drive(state)
          {:done, state} -> state
          {:ignore, state} -> drive(state)
          other -> flunk("scanner failed: #{inspect(other)}")
        end
    after
      3000 ->
        Recovery.close(state)
        flunk("scanner did not finish")
    end
  end

  defp beam(root, script, extra \\ []) do
    paths =
      Enum.flat_map([:ex_storage_service, :telemetry], fn app ->
        ["-pa", app |> :code.lib_dir() |> to_string() |> Path.join("ebin")]
      end)

    System.cmd(System.find_executable("elixir"), paths ++ ["-e", script],
      env: [{"ERL_FLAGS", "+S 1:1 +SDcpu 1 +SDio 1"}, {"ESS_STAGE_TEST_ROOT", root}] ++ extra,
      stderr_to_stdout: true
    )
  end
end
