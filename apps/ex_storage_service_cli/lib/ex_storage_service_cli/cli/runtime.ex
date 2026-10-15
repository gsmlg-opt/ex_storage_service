defmodule ExStorageServiceCli.CLI.Runtime do
  @moduledoc "A small invocation-owned supervisor; starts HTTP dependencies and no storage servers."
  alias ExStorageServiceCli.{Context, Error}
  alias ExStorageServiceCli.Credentials.{DirectorySession, Provider}
  alias ExStorageServiceCli.Transfer.{Coordinator, RateLimiter}
  alias ExStorageServiceCli.CLI.Signals

  defstruct [
    :supervisor,
    :tasks,
    :credentials,
    :coordinator,
    :directory_sessions,
    :signals,
    :retry_state,
    :rate_limiter
  ]

  def with_invocation(callback, opts \\ []) when is_function(callback, 1) do
    with {:ok, _started} <- Application.ensure_all_started(:http_fetch),
         {:ok, supervisor} <-
           Supervisor.start_link(
             [
               {Task.Supervisor, []},
               {DynamicSupervisor, strategy: :one_for_one},
               {Coordinator, Keyword.take(opts, [:max_requests, :memory_budget, :max_waiters])},
               {RateLimiter, [owner: self(), max_waiters: opts[:max_requests] || 8]},
               {ExStorageServiceCli.S3.Retry.State, [owner: self()]},
               {DirectorySession, Keyword.put(opts, :invocation_owner, self())}
             ],
             strategy: :one_for_one
           ) do
      children =
        Map.new(Supervisor.which_children(supervisor), fn {id, pid, _type, _modules} ->
          {id, pid}
        end)

      runtime = %__MODULE__{
        supervisor: supervisor,
        tasks: children[Task.Supervisor],
        credentials: children[DynamicSupervisor],
        coordinator: children[Coordinator],
        directory_sessions: children[DirectorySession],
        retry_state: children[ExStorageServiceCli.S3.Retry.State],
        rate_limiter: children[RateLimiter]
      }

      cancel_reference = make_ref()

      try do
        with {:ok, runtime} <- signals(runtime, cancel_reference, opts) do
          result = callback.(runtime)

          case Coordinator.check(runtime.coordinator) do
            :ok -> result
            error -> error
          end
        end
      rescue
        error in ExStorageServiceCli.Transfer.Source.ReadError -> {:error, error.error}
      after
        if Process.alive?(supervisor),
          do: Supervisor.stop(supervisor, :normal, opts[:shutdown_timeout] || 5000)

        receive do
          {:ess_cancel, {^cancel_reference, _signal}} -> :ok
        after
          0 -> :ok
        end
      end
    else
      _ ->
        {:error,
         Error.new(:client, "CLI HTTP runtime could not start", code: :runtime_start_failed)}
    end
  end

  defp signals(runtime, reference, opts) do
    if opts[:handle_signals] || Process.get(:ess_handle_signals, false) do
      spec =
        {Signals, [coordinator: runtime.coordinator, owner: self(), cancel_reference: reference]}

      case Supervisor.start_child(runtime.supervisor, spec) do
        {:ok, pid} ->
          {:ok, %{runtime | signals: pid}}

        _ ->
          {:error,
           Error.new(:client, "Invocation signal handling could not start",
             code: :signal_start_failed
           )}
      end
    else
      {:ok, runtime}
    end
  end

  def provider(%__MODULE__{} = runtime, options, opts \\ []) do
    opts = opts |> Keyword.delete(:runtime) |> Keyword.put(:invocation_owner, self())

    spec = %{
      id: make_ref(),
      start: {Provider, :start_link, [options, opts]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    case DynamicSupervisor.start_child(runtime.credentials, spec) do
      {:ok, owner} ->
        {:ok, owner}

      _ ->
        {:error,
         Error.new(:credentials, "Credential refresh owner could not start",
           code: :credential_refresh_unavailable
         )}
    end
  catch
    :exit, _ ->
      {:error,
       Error.new(:credentials, "Credential refresh owner is unavailable",
         code: :credential_refresh_unavailable
       )}
  end

  def aws_context(%__MODULE__{} = runtime, options, opts \\ []),
    do: Context.aws(options, Keyword.put(opts, :runtime, runtime))

  def task_supervisor(%__MODULE__{tasks: tasks}), do: tasks
end
