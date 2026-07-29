defmodule ExStorageService.Cluster.Outbox.Dispatcher do
  @moduledoc """
  Polls durable outbox pages and executes fenced jobs with bounded concurrency.

  The GenServer owns only local scheduling state. Job ownership, retry timing,
  lease expiry, and fencing live in Concord and survive process/node restarts.
  """

  use GenServer
  require Logger

  alias ExStorageService.Cluster.Repair.Handler, as: RepairHandler
  alias ExStorageService.CrossClusterReplication.Handler, as: CrossClusterHandler
  alias ExStorageService.Metadata.{JobStore, Outbox}
  alias ExStorageService.Metadata.Models.Job
  alias ExStorageService.{Context, InstanceConfig}

  @default_poll_interval 1_000
  @default_lease_ms 30_000
  @default_page_size 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec process_jobs(GenServer.server()) :: :ok
  def process_jobs(server), do: GenServer.cast(server, :poll)

  @impl true
  def init(opts) do
    context = Keyword.fetch!(opts, :context)
    lease_ms = Keyword.get(opts, :lease_ms, @default_lease_ms)

    state = %{
      context: context,
      task_supervisor: Keyword.fetch!(opts, :task_supervisor),
      outbox: Keyword.get(opts, :outbox, Outbox),
      job_store: Keyword.get(opts, :job_store, JobStore),
      processor: Keyword.get(opts, :processor, &perform/2),
      metadata_opts: Keyword.get(opts, :metadata_opts, []),
      owner_node: context.config.node_id,
      owner_generation: context.config.node_generation,
      kinds: eligible_kinds(context),
      concurrency: concurrency(context),
      poll_interval: Keyword.get(opts, :poll_interval, @default_poll_interval),
      lease_ms: lease_ms,
      renew_interval: Keyword.get(opts, :renew_interval, max(div(lease_ms, 3), 100)),
      page_size: Keyword.get(opts, :page_size, @default_page_size),
      outbox_cursor: nil,
      job_cursor: nil,
      running: %{},
      clock: Keyword.get(opts, :clock, fn -> System.system_time(:millisecond) end)
    }

    {:ok, state, {:continue, :schedule}}
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(:poll, 0)
    schedule(:renew, state.renew_interval)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:poll, state), do: {:noreply, poll(state)}

  @impl true
  def handle_info(:poll, state) do
    state = poll(state)
    schedule(:poll, state.poll_interval)
    {:noreply, state}
  end

  def handle_info(:renew, state) do
    state = renew_running(state)
    schedule(:renew, state.renew_interval)
    {:noreply, state}
  end

  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    %{job: job} = Map.fetch!(running, ref)
    finish_job(state, job, result)
    {:noreply, %{state | running: Map.delete(running, ref)}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    %{job: job} = Map.fetch!(running, ref)
    finish_job(state, job, {:error, {:task_down, reason}})
    {:noreply, %{state | running: Map.delete(running, ref)}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp poll(state) do
    state
    |> materialize_page()
    |> claim_page()
  end

  defp materialize_page(state) do
    case state.outbox.materialize_page(
           state.outbox_cursor,
           state.page_size,
           metadata_opts(state)
         ) do
      {:ok, %{next_cursor: cursor}} ->
        %{state | outbox_cursor: cursor}

      {:error, reason} ->
        Logger.warning("Outbox materialization failed: #{inspect(reason)}")
        %{state | outbox_cursor: nil}
    end
  end

  defp claim_page(%{kinds: []} = state), do: state

  defp claim_page(state) do
    available = total_concurrency(state) - map_size(state.running)

    if available <= 0 do
      state
    else
      case state.job_store.list_page(state.job_cursor, state.page_size, metadata_opts(state)) do
        {:ok, %{jobs: jobs, next_cursor: cursor}} ->
          jobs
          |> Enum.filter(&(&1.kind in state.kinds))
          |> Enum.filter(&eligible_job?(&1, state))
          |> Enum.reduce_while(%{state | job_cursor: cursor}, fn job, acc ->
            cond do
              map_size(acc.running) >= total_concurrency(acc) ->
                {:halt, acc}

              kind_capacity?(acc, job) ->
                {:cont, claim_and_start(acc, job)}

              true ->
                {:cont, acc}
            end
          end)

        {:error, reason} ->
          Logger.warning("Durable job scan failed: #{inspect(reason)}")
          %{state | job_cursor: nil}
      end
    end
  end

  defp claim_and_start(state, %Job{} = candidate) do
    now_ms = state.clock.()

    case state.job_store.claim(
           candidate.job_id,
           state.owner_node,
           now_ms,
           state.lease_ms,
           metadata_opts(state)
         ) do
      {:ok, job} ->
        start_job(state, job)

      {:error, {:not_claimable, current}} ->
        emit_lease_contention(current, candidate, state, now_ms)
        state

      {:error, reason} ->
        Logger.warning("Durable job claim failed for #{candidate.job_id}: #{inspect(reason)}")
        state
    end
  end

  defp start_job(state, job) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        state.processor.(job, state.context)
      end)

    put_in(state.running[task.ref], %{task: task, job: job})
  catch
    :exit, reason ->
      finish_job(state, job, {:error, {:task_start_failed, reason}})
      state
  end

  defp renew_running(state) do
    now_ms = state.clock.()

    running =
      Enum.reduce(state.running, %{}, fn {ref, entry}, acc ->
        job = entry.job

        case state.job_store.renew(
               job.job_id,
               state.owner_node,
               job.fencing_token,
               now_ms,
               state.lease_ms,
               metadata_opts(state)
             ) do
          {:ok, renewed} ->
            Map.put(acc, ref, %{entry | job: renewed})

          {:error, {:stale_lease, _current}} ->
            Task.shutdown(entry.task, :brutal_kill)
            acc

          {:error, reason} ->
            Logger.warning("Lease renewal failed for #{job.job_id}: #{inspect(reason)}")
            Map.put(acc, ref, entry)
        end
      end)

    %{state | running: running}
  end

  defp finish_job(state, job, :ok) do
    _ =
      state.job_store.complete(
        job.job_id,
        state.owner_node,
        job.fencing_token,
        state.clock.(),
        metadata_opts(state)
      )

    :ok
  end

  defp finish_job(state, job, {:error, reason}) do
    _ =
      state.job_store.fail(
        job.job_id,
        state.owner_node,
        job.fencing_token,
        reason,
        state.clock.(),
        metadata_opts(state)
      )

    :ok
  end

  defp finish_job(state, job, other),
    do: finish_job(state, job, {:error, {:invalid_handler_result, other}})

  defp eligible_kinds(%Context{config: config}) do
    []
    |> maybe_add(
      InstanceConfig.worker_enabled?(config, :cross_cluster_replication),
      [:cross_cluster_put, :cross_cluster_delete]
    )
    |> maybe_add(InstanceConfig.worker_enabled?(config, :repair), [:repair_blob, :cleanup])
    |> maybe_add(InstanceConfig.worker_enabled?(config, :scrub), [:scrub])
  end

  defp concurrency(%Context{config: config}) do
    %{
      replica:
        if(InstanceConfig.worker_enabled?(config, :cross_cluster_replication),
          do: config.replica_concurrency,
          else: 0
        ),
      maintenance:
        if(
          InstanceConfig.worker_enabled?(config, :repair) or
            InstanceConfig.worker_enabled?(config, :scrub),
          do: config.repair_concurrency,
          else: 0
        )
    }
  end

  defp total_concurrency(state), do: state.concurrency.replica + state.concurrency.maintenance

  defp kind_capacity?(state, job) do
    group = kind_group(job.kind)

    running =
      Enum.count(state.running, fn {_ref, entry} -> kind_group(entry.job.kind) == group end)

    running < Map.fetch!(state.concurrency, group)
  end

  defp kind_group(kind) when kind in [:cross_cluster_put, :cross_cluster_delete], do: :replica
  defp kind_group(_kind), do: :maintenance

  defp eligible_job?(%Job{kind: :scrub, payload: payload}, state) do
    target_node_id = Map.get(payload, :target_node_id, Map.get(payload, "target_node_id"))

    target_generation =
      Map.get(payload, :target_node_generation, Map.get(payload, "target_node_generation"))

    target_node_id == state.owner_node and target_generation == state.owner_generation
  end

  defp eligible_job?(%Job{}, _state), do: true

  defp perform(%Job{kind: kind} = job, context)
       when kind in [:cross_cluster_put, :cross_cluster_delete],
       do: CrossClusterHandler.perform(job, context)

  defp perform(%Job{} = job, context), do: RepairHandler.perform(job, context)

  defp maybe_add(kinds, true, additions), do: kinds ++ additions
  defp maybe_add(kinds, false, _additions), do: kinds

  defp metadata_opts(state) do
    Keyword.put(state.metadata_opts, :owner_generation, state.owner_generation)
  end

  defp emit_lease_contention(
         %Job{
           state: :running,
           lease_until_ms: lease_until_ms,
           owner_node: owner_node,
           owner_generation: owner_generation
         },
         candidate,
         state,
         now_ms
       )
       when is_integer(lease_until_ms) and lease_until_ms > now_ms do
    if {owner_node, owner_generation} != {state.owner_node, state.owner_generation} do
      :telemetry.execute(
        [:ex_storage_service, :cluster, :lease, :contention],
        %{count: 1},
        %{kind: candidate.kind, reason: :leased_by_other_owner}
      )
    end
  end

  defp emit_lease_contention(_current, _candidate, _state, _now_ms), do: :ok

  defp schedule(message, delay), do: Process.send_after(self(), message, delay)
end
