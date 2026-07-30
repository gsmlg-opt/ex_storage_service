defmodule Mix.Tasks.Ess.Cluster.Bootstrap do
  use Mix.Task

  @shortdoc "Wait for Concord and register this configured cluster node"
  @moduledoc """
  Waits for Concord readiness and persistently registers the configured node:

      mix ess.cluster.bootstrap
  """
  @requirements ["app.start"]

  @impl Mix.Task
  def run([]) do
    with {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <- ExStorageService.Operations.Cluster.bootstrap(context) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      {:error, reason} -> Mix.raise("cluster bootstrap failed: #{inspect(reason)}")
    end
  end

  def run(_args), do: Mix.raise("usage: mix ess.cluster.bootstrap")
end
