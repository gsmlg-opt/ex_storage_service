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
    child_id = {ExStorageService, instance}

    assert {:ok, host} = Supervisor.start_link([{ExStorageService, opts}], strategy: :one_for_one)
    Process.unlink(host)

    on_exit(fn ->
      if Process.alive?(host), do: Supervisor.stop(host)
    end)

    assert [{^child_id, pid, :supervisor, [ExStorageService]}] = Supervisor.which_children(host)
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

    :ok = Supervisor.terminate_child(host, child_id)
    refute Process.alive?(pid)
    assert Process.alive?(host)
    assert eventually(fn -> Registry.lookup(Names.registry(), {instance, :engine}) == [] end)

    assert {:ok, restarted} = Supervisor.restart_child(host, child_id)
    assert Process.alive?(restarted)
    assert restarted != pid
    assert Process.alive?(host)
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

    assert {:error, context_message} =
             ExStorageService.context(
               instance: "bad-context-roots",
               auto_start: false,
               metadata_root: "/different/concord",
               workers: @disabled_workers
             )

    assert context_message =~ "cannot differ per child"
  end

  @tag :tmp_dir
  test "custom roots reject root-sensitive workers that still use application infrastructure", %{
    tmp_dir: tmp_dir
  } do
    workers = Keyword.put(@disabled_workers, :content_gc, true)

    assert {:error, {:invalid_instance_config, message}} =
             ExStorageService.start_link(
               instance_opts("unsafe-workers", tmp_dir)
               |> Keyword.put(:workers, workers)
             )

    assert message =~ "filesystem workers"
    assert message =~ ":content_gc"
  end

  @tag :tmp_dir
  test "replication sync receives its instance-specific job queue", %{tmp_dir: tmp_dir} do
    workers = Keyword.put(@disabled_workers, :cross_cluster_replication, true)

    {:ok, config} =
      InstanceConfig.new(
        instance_opts("replication-instance", tmp_dir)
        |> Keyword.put(:workers, workers)
      )

    children = config |> Context.new() |> ExStorageService.InstanceSupervisor.children()

    assert [
             {ExStorageService.Storage.Engine, _engine_opts},
             {ExStorageService.Replication.JobQueue, queue_opts},
             {ExStorageService.Replication.Sync, sync_opts}
           ] = children

    assert sync_opts[:job_queue] == queue_opts[:name]
    assert match?({:via, Registry, _}, sync_opts[:job_queue])
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

  defp eventually(fun, attempts \\ 20)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
