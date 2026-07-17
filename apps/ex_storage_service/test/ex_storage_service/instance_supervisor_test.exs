defmodule ExStorageService.InstanceSupervisorTest do
  use ExUnit.Case, async: false

  alias ExStorageService.{Context, InstanceConfig, Names}
  alias ExStorageService.BlobStore.LocalCAS

  @disabled_workers [
    multipart_gc: false,
    content_gc: false,
    cas_gc: false,
    packer: false,
    lifecycle: false,
    cross_cluster_replication: false,
    repair: false,
    scrub: false
  ]

  @tag :tmp_dir
  test "host can stop and restart an embedded instance with optional workers absent", %{
    tmp_dir: tmp_dir
  } do
    instance = "embedded-#{System.unique_integer([:positive])}"
    opts = instance_opts(instance, tmp_dir)

    assert {:ok, pid} = ExStorageService.start_link(opts)
    assert Process.alive?(pid)
    assert [{engine, _value}] = Registry.lookup(Names.registry(), {instance, :engine})
    assert Process.alive?(engine)

    for worker <- [
          :multipart_gc,
          :content_gc,
          :cas_gc,
          :packer,
          :lifecycle,
          :replication_job_queue,
          :replication_sync
        ] do
      assert Registry.lookup(Names.registry(), {instance, worker}) == []
    end

    assert File.dir?(Path.join(tmp_dir, "data"))
    assert File.dir?(Path.join([tmp_dir, "blobs", "objects", "sha256"]))
    assert File.dir?(Path.join([tmp_dir, "staging", "uploads"]))

    :ok = Supervisor.stop(pid)
    refute Process.alive?(pid)
    assert Process.alive?(self())

    assert {:ok, restarted} = ExStorageService.start_link(opts)
    assert Process.alive?(restarted)
    :ok = Supervisor.stop(restarted)
  end

  @tag :tmp_dir
  test "context routes staging and committed blobs to independent roots", %{tmp_dir: tmp_dir} do
    instance = "roots-#{System.unique_integer([:positive])}"
    {:ok, config} = InstanceConfig.new(instance_opts(instance, tmp_dir))
    context = Context.new(config)
    blob_opts = Context.blob_store_options(context)

    assert {:ok, staged} = LocalCAS.stage(["split", "-", "roots"], blob_opts)
    assert String.starts_with?(staged.path, Path.join(tmp_dir, "staging"))

    assert {:ok, ready} = LocalCAS.commit(staged, blob_opts)
    assert String.starts_with?(ready.path, Path.join(tmp_dir, "blobs"))
    assert File.read!(ready.path) == "split-roots"
  end

  test "child metadata roots must match the running one-Concord infrastructure" do
    assert {:error, {:invalid_instance_config, message}} =
             ExStorageService.start_link(
               instance: "bad-roots",
               auto_start: false,
               ra_root: "/different/ra",
               metadata_root: "/different/concord",
               workers: @disabled_workers
             )

    assert message =~ "cannot differ per child"
  end

  test "application child list omits only the default instance when auto_start is false" do
    assert {:ok, disabled} = InstanceConfig.new(auto_start: false)

    assert [registry, pubsub, task_supervisor] =
             ExStorageService.Application.children(disabled)

    assert match?({Registry, _opts}, registry)
    assert match?({Phoenix.PubSub, _opts}, pubsub)
    assert match?({Task.Supervisor, _opts}, task_supervisor)

    assert {:ok, enabled} = InstanceConfig.new(auto_start: true)

    assert [_registry, _pubsub, _task_supervisor, {ExStorageService, ^enabled}] =
             ExStorageService.Application.children(enabled)
  end

  test "names use Registry keys without creating dynamic atoms" do
    assert {:via, Registry, {ExStorageService.Registry, {"tenant-a", :engine}}} =
             Names.via("tenant-a", :engine)

    assert Names.process(:default, :engine, ExStorageService.Storage.Engine) ==
             ExStorageService.Storage.Engine

    assert Names.process("tenant-a", :engine, ExStorageService.Storage.Engine) ==
             Names.via("tenant-a", :engine)
  end

  defp instance_opts(instance, tmp_dir) do
    [
      instance: instance,
      auto_start: false,
      data_root: Path.join(tmp_dir, "data"),
      blob_root: Path.join(tmp_dir, "blobs"),
      tmp_root: Path.join(tmp_dir, "staging"),
      ra_root: Application.fetch_env!(:ex_storage_service, :ra_root),
      metadata_root: Application.fetch_env!(:ex_storage_service, :metadata_root),
      workers: @disabled_workers
    ]
  end
end
