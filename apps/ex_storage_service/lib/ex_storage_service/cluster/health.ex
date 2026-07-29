defmodule ExStorageService.Cluster.Health do
  @moduledoc """
  Role-aware liveness and readiness checks.

  Metadata readiness remains owned by `Cluster.Readiness`. Data-node health is
  injected so the private transport and repair planner can supply live cluster
  evidence without coupling this core module to an HTTP adapter.
  """

  alias ExStorageService.Cluster.{DataReadiness, Readiness}
  alias ExStorageService.{Context, Names}
  alias ExStorageService.Storage.Engine

  @type check_result :: {:ok, map()} | {:error, term()}

  @spec liveness(keyword()) :: {:ok, map()} | {:error, map()}
  def liveness(opts \\ []) do
    checker = Keyword.get(opts, :liveness_checker, &default_liveness/1)

    case safe_check(checker, opts) do
      {:ok, details} -> {:ok, %{status: :ok, details: details}}
      {:error, reason} -> {:error, %{status: :failed, reason: reason_string(reason)}}
    end
  end

  @spec readiness(keyword()) :: {:ok, map()} | {:error, map()}
  def readiness(opts \\ []) do
    metadata =
      opts
      |> Keyword.get(:metadata_checker, &Readiness.check/1)
      |> safe_check(opts)
      |> component()

    data =
      opts
      |> Keyword.get(:data_checker, &default_data_readiness/1)
      |> safe_check(opts)
      |> component()

    result = %{checks: %{metadata: metadata, data: data}}

    if metadata.ready and data.ready,
      do: {:ok, Map.put(result, :status, :ready)},
      else: {:error, Map.put(result, :status, :not_ready)}
  end

  defp default_liveness(_opts) do
    if Process.whereis(ExStorageService.Supervisor),
      do: {:ok, %{process: :alive}},
      else: {:error, :application_supervisor_not_running}
  end

  defp default_data_readiness(opts) do
    with {:ok, context} <- context(opts) do
      case context.config.node_role do
        :metadata ->
          {:ok, %{role: :metadata, required: false}}

        :data ->
          data_node_readiness(context, opts)
      end
    end
  end

  defp data_node_readiness(%Context{config: %{mode: :cluster}} = context, opts) do
    provider =
      Keyword.get(
        opts,
        :data_health_provider,
        Application.get_env(:ex_storage_service, :data_health_provider, DataReadiness)
      )

    invoke_provider(provider, context, opts)
  end

  defp data_node_readiness(%Context{} = context, _opts) do
    engine = Names.process(context.instance, :engine, Engine)

    with true <- process_alive?(engine),
         :ok <- valid_roots(context) do
      {:ok, %{role: :data, healthy_nodes: 1, required_write_quorum: 1}}
    else
      false -> {:error, :storage_engine_not_running}
      {:error, reason} -> {:error, reason}
    end
  end

  defp valid_roots(context) do
    invalid =
      [context.blob_root, context.tmp_root]
      |> Enum.reject(&File.dir?/1)

    if invalid == [], do: :ok, else: {:error, {:invalid_storage_roots, length(invalid)}}
  end

  defp context(opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context -> {:ok, context}
      nil -> Context.default()
    end
  end

  defp invoke_provider(provider, context, opts) when is_function(provider, 2),
    do: provider.(context, opts)

  defp invoke_provider(provider, context, _opts) when is_function(provider, 1),
    do: provider.(context)

  defp invoke_provider(provider, context, opts) when is_atom(provider),
    do: provider.check(context, opts)

  defp invoke_provider(_provider, _context, _opts), do: {:error, :invalid_data_health_provider}

  defp safe_check(checker, opts) when is_function(checker, 1) do
    checker.(opts)
  rescue
    _error -> {:error, :check_failed}
  catch
    _kind, _reason -> {:error, :check_failed}
  end

  defp component({:ok, _details}), do: %{ready: true}
  defp component({:error, reason}), do: %{ready: false, reason: reason_string(reason)}
  defp component(_other), do: %{ready: false, reason: "invalid_check_result"}

  defp reason_string(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_string({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_string(_reason), do: "unavailable"

  defp process_alive?(name) when is_atom(name), do: Process.whereis(name) != nil

  defp process_alive?({:via, module, term}) do
    case module.whereis_name(term) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end
end
