defmodule ExStorageService.Cluster.Outbox.Supervisor do
  @moduledoc """
  Supervises one instance's durable outbox dispatcher and bounded tasks.

  The children share a `:one_for_all` failure domain: if the dispatcher
  restarts, its in-flight tasks are cancelled and their durable leases are
  reclaimed only after expiry with a higher fencing token.
  """

  use Supervisor

  alias ExStorageService.Cluster.Outbox.Dispatcher
  alias ExStorageService.Cluster.Repair.Planner
  alias ExStorageService.{InstanceConfig, Names}

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    context = Keyword.fetch!(opts, :context)
    name = Names.process(context.instance, :outbox_supervisor, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    context = Keyword.fetch!(opts, :context)

    children =
      [
        {Task.Supervisor,
         name: context.outbox_task_supervisor,
         max_children: context.config.replica_concurrency + context.config.repair_concurrency}
      ] ++
        planner_children(context) ++
        [
          {Dispatcher,
           [
             context: context,
             task_supervisor: context.outbox_task_supervisor,
             name: Names.process(context.instance, :outbox_dispatcher, Dispatcher)
           ]}
        ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp planner_children(context) do
    if context.config.mode == :cluster and
         (InstanceConfig.worker_enabled?(context.config, :repair) or
            InstanceConfig.worker_enabled?(context.config, :scrub)) do
      [
        {Planner,
         [
           context: context,
           name: Names.process(context.instance, :repair_planner, Planner)
         ]}
      ]
    else
      []
    end
  end
end
