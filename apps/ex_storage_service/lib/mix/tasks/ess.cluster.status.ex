defmodule Mix.Tasks.Ess.Cluster.Status do
  use Mix.Task

  @shortdoc "Show Concord, membership, and durability status"
  @moduledoc """
  Shows sanitized Concord, configured membership, and durability status:

      mix ess.cluster.status
  """
  @requirements ["app.start"]

  @impl Mix.Task
  def run([]) do
    with {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <- ExStorageService.Operations.Cluster.status(context) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      {:error, reason} -> Mix.raise("cluster status failed: #{inspect(reason)}")
    end
  end

  def run(_args), do: Mix.raise("usage: mix ess.cluster.status")
end
