defmodule ExStorageService.Cluster.DataReadiness do
  @moduledoc """
  Verifies that a cluster data node can satisfy its configured write quorum.

  Membership is read strongly. The local node is checked against its storage
  roots and engine process; remote eligible data nodes are checked through the
  authenticated internal transport health endpoint.
  """

  alias ExStorageService.Cluster.Membership
  alias ExStorageService.{Context, Names}
  alias ExStorageService.Storage.Engine

  @spec check(Context.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def check(%Context{} = context, opts \\ []) do
    with :ok <- local_storage_ready(context, opts),
         {:ok, members} <- membership(opts).members(context.config, metadata_opts(opts)) do
      eligible =
        Enum.filter(members, fn %{node: node} ->
          node.role == :data and node.enabled and not node.draining and
            is_binary(node.internal_endpoint) and node.internal_endpoint != ""
        end)

      {local, remote} =
        Enum.split_with(eligible, &(&1.node.node_id == context.config.node_id))

      healthy =
        Enum.count(local, &local_member_healthy?(&1.node, context)) +
          healthy_remote_count(remote, context, opts)

      required = context.config.write_quorum

      if healthy >= required do
        {:ok,
         %{
           role: :data,
           eligible_nodes: length(eligible),
           healthy_nodes: healthy,
           required_write_quorum: required
         }}
      else
        {:error, {:insufficient_healthy_nodes, %{healthy: healthy, required: required}}}
      end
    end
  end

  defp local_member_healthy?(node, context),
    do: node.generation == context.config.node_generation

  defp healthy_remote_count([], _context, _opts), do: 0

  defp healthy_remote_count(remote, context, opts) do
    timeout = Keyword.get(opts, :probe_timeout, 1_500)

    context.replica_task_supervisor
    |> Task.Supervisor.async_stream_nolink(
      remote,
      fn %{node: node} -> healthy_remote?(node, context, opts) end,
      ordered: false,
      max_concurrency: length(remote),
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.count(&match?({:ok, true}, &1))
  rescue
    _error -> 0
  catch
    _kind, _reason -> 0
  end

  defp healthy_remote?(node, context, opts),
    do: transport(opts).health(context, node, transport_opts(opts)) == :ok

  defp local_storage_ready(context, opts) do
    checker = Keyword.get(opts, :local_storage_checker, &default_local_storage_ready/1)
    checker.(context)
  end

  defp default_local_storage_ready(context) do
    engine = Names.process(context.instance, :engine, Engine)

    with true <- process_alive?(engine),
         [] <- Enum.reject([context.blob_root, context.tmp_root], &File.dir?/1) do
      :ok
    else
      false -> {:error, :storage_engine_not_running}
      invalid when is_list(invalid) -> {:error, {:invalid_storage_roots, length(invalid)}}
    end
  end

  defp process_alive?(name) when is_atom(name), do: Process.whereis(name) != nil

  defp process_alive?({:via, module, term}) do
    case module.whereis_name(term) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _other -> false
    end
  end

  defp metadata_opts(opts) do
    opts
    |> Keyword.get(:metadata_opts, [])
    |> Keyword.take([:backend, :consistency, :timeout, :engine, :barrier])
    |> Keyword.put_new(:consistency, :strong)
    |> Keyword.merge(Keyword.get(opts, :membership_opts, []))
  end

  defp transport_opts(opts) do
    opts
    |> Keyword.get(:transport_opts, [])
    |> Keyword.put_new(:timeout, Keyword.get(opts, :probe_timeout, 1_000))
  end

  defp membership(opts), do: Keyword.get(opts, :membership, Membership)

  defp transport(opts),
    do:
      Keyword.get_lazy(opts, :transport, fn ->
        Application.fetch_env!(:ex_storage_service, :cluster_transport)
      end)
end
