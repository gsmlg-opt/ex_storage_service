defmodule ExStorageService.BlobStore.LocalCASTelemetryTest do
  use ExUnit.Case, async: false
  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.{Metrics, Telemetry}
  @moduletag :tmp_dir

  defmodule FullFileSystem do
    defdelegate mkdir_p(path), to: LocalCAS.FileSystem
    defdelegate open(path, modes), to: LocalCAS.FileSystem
    defdelegate close(io), to: LocalCAS.FileSystem
    defdelegate rm(path), to: LocalCAS.FileSystem
    defdelegate stat(path), to: LocalCAS.FileSystem
    def write(_io, "fail"), do: {:error, :enospc}
    def write(io, bytes), do: LocalCAS.FileSystem.write(io, bytes)
  end

  setup %{tmp_dir: root} do
    :ok = Metrics.setup()
    :ok = Metrics.reset()
    %{opts: [root: root, tmp_dir: Path.join(root, "stages"), pack_module: nil]}
  end

  test "paced real writes expose activity and bytes until commit or discard", %{opts: opts} do
    parent = self()

    task =
      Task.async(fn ->
        LocalCAS.stage_from_reader(
          fn
            :initial ->
              {:more, "abc", :paused}

            :paused ->
              send(parent, {:paused, self()})

              receive do
                :release -> {:ok, "def", :consumed}
              end
          end,
          :initial,
          opts
        )
      end)

    assert_receive {:paused, writer}
    assert eventually(fn -> Metrics.format_metrics() =~ ~s(transfer_active{kind="stage"} 1) end)

    assert %{tracked_bytes: 3, tracked_files: 1, complete_since_setup: true} =
             Metrics.staging_snapshot()

    send(writer, :release)
    assert {:ok, staged, :consumed} = Task.await(task)
    assert staged.size == 6
    assert Metrics.format_metrics() =~ ~s(transfer_active{kind="stage"} 0)
    assert Metrics.format_metrics() =~ "storage_staging_bytes_written_total 6"
    assert Metrics.staging_snapshot().tracked_bytes == 6
    assert {:ok, ready} = LocalCAS.commit(staged, opts)
    assert File.read!(ready.path) == "abcdef"
    assert %{tracked_bytes: 0, tracked_files: 0} = Metrics.staging_snapshot()
    assert {:ok, second} = LocalCAS.stage("discarded", opts)
    assert Metrics.staging_snapshot().tracked_bytes == 9
    assert :ok = LocalCAS.discard(second, opts)
    assert Metrics.staging_snapshot().tracked_files == 0
  end

  test "reader and disk errors balance activity and observe actual cleanup", %{opts: opts} do
    reader = fn
      :initial -> {:more, "abc", :next}
      :next -> {:error, :client_disconnected, :consumed}
    end

    assert {:error, :client_disconnected, :consumed} =
             LocalCAS.stage_from_reader(reader, :initial, opts)

    assert {:error, {:stage, :enospc}, :consumed} =
             LocalCAS.stage_from_reader(
               fn
                 :initial -> {:more, "abc", :next}
                 :next -> {:ok, "fail", :consumed}
               end,
               :initial,
               Keyword.put(opts, :fs_module, FullFileSystem)
             )

    assert File.ls!(opts[:tmp_dir]) == []

    assert %{tracked_bytes: 0, tracked_files: 0, complete_since_setup: true} =
             Metrics.staging_snapshot()

    output = Metrics.format_metrics()
    assert output =~ ~s(transfer_active{kind="stage"} 0)
    assert output =~ ~s(transfers_total{kind="stage",result="error"} 2)
    assert output =~ "storage_staging_bytes_written_total 6"
  end

  test "untrappable owner exit closes activity without deleting or hiding retained stage bytes",
       %{opts: opts} do
    parent = self()

    {writer, monitor} =
      spawn_monitor(fn ->
        LocalCAS.stage_from_reader(
          fn
            :initial ->
              {:more, "abc", :paused}

            :paused ->
              send(parent, :paused)

              receive do
                :never -> {:done, :done}
              end
          end,
          :initial,
          opts
        )
      end)

    assert_receive :paused
    assert eventually(fn -> Metrics.format_metrics() =~ ~s(transfer_active{kind="stage"} 1) end)
    [name] = File.ls!(opts[:tmp_dir])
    path = Path.join(opts[:tmp_dir], name)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :killed}

    assert eventually(fn ->
             Metrics.format_metrics() =~ ~s(transfers_total{kind="stage",result="owner_exit"} 1)
           end)

    assert Metrics.format_metrics() =~ ~s(transfer_active{kind="stage"} 0)
    assert File.read!(path) == "abc"

    assert %{tracked_bytes: 3, tracked_files: 1, complete_since_setup: false} =
             Metrics.staging_snapshot()

    # Only an explicit, confirmed physical removal may release this observation.
    assert :ok = File.rm(path)
    Telemetry.observe_stage(path, LocalCAS.FileSystem)

    assert %{tracked_bytes: 0, tracked_files: 0, complete_since_setup: false} =
             Metrics.staging_snapshot()
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.(),
      do: true,
      else:
        (
          Process.sleep(10)
          eventually(fun, attempts - 1)
        )
  end
end
