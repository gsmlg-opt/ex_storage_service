defmodule ExStorageService.InstanceSupervisor do
  @moduledoc """
  Supervises the local data-plane services for one storage instance.

  Concord/Ra and PubSub are application infrastructure and intentionally live
  outside this supervisor. Stopping this supervisor therefore restarts local
  storage lifecycle without terminating the host application or metadata
  system.
  """

  use Supervisor

  alias ExStorageService.{Context, InstanceConfig, Names}
  alias ExStorageService.Replication.{JobQueue, Sync}

  alias ExStorageService.Storage.{
    CasGC,
    ContentGC,
    Engine,
    Lifecycle,
    MultipartGC,
    Packer
  }

  @spec start_link(Context.t()) :: Supervisor.on_start()
  def start_link(%Context{} = context) do
    Supervisor.start_link(__MODULE__, context, name: Names.instance_supervisor(context.instance))
  end

  @impl true
  def init(%Context{} = context) do
    with :ok <- prepare_roots(context) do
      Supervisor.init(children(context), strategy: :one_for_one)
    else
      {:error, reason} -> {:stop, {:root_initialization_failed, reason}}
    end
  end

  @doc false
  @spec children(Context.t()) :: [Supervisor.child_spec()]
  def children(%Context{} = context) do
    config = context.config

    [
      {Engine,
       [
         data_root: context.data_root,
         blob_root: context.blob_root,
         tmp_root: context.tmp_root,
         name: name(context, :engine, Engine)
       ]}
    ] ++
      optional(config, :multipart_gc, {
        MultipartGC,
        [name: name(context, :multipart_gc, MultipartGC)]
      }) ++
      optional(config, :content_gc, {
        ContentGC,
        [name: name(context, :content_gc, ContentGC)]
      }) ++
      optional(config, :cas_gc, {
        CasGC,
        [name: name(context, :cas_gc, CasGC)]
      }) ++
      optional(config, :packer, {
        Packer,
        [name: name(context, :packer, Packer)]
      }) ++
      replication_children(context) ++
      optional(config, :lifecycle, {
        Lifecycle,
        [name: name(context, :lifecycle, Lifecycle)]
      })
  end

  defp replication_children(%Context{config: config} = context) do
    if InstanceConfig.worker_enabled?(config, :cross_cluster_replication) do
      [
        {JobQueue, [name: name(context, :replication_job_queue, JobQueue)]},
        {Sync, [name: name(context, :replication_sync, Sync)]}
      ]
    else
      []
    end
  end

  defp optional(config, worker, child) do
    if InstanceConfig.worker_enabled?(config, worker), do: [child], else: []
  end

  defp name(context, component, legacy),
    do: Names.process(context.instance, component, legacy)

  defp prepare_roots(context) do
    [context.data_root, context.blob_root, context.tmp_root]
    |> Enum.reduce_while(:ok, fn root, :ok ->
      case File.mkdir_p(root) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {root, reason}}}
      end
    end)
  end
end
