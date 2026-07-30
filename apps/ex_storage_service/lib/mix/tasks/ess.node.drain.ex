defmodule Mix.Tasks.Ess.Node.Drain do
  use Mix.Task

  @shortdoc "Persistently exclude a data node from new replica placement"
  @moduledoc """
  Persistently marks a data node as draining:

      mix ess.node.drain NODE_ID
  """
  @requirements ["app.start"]

  @impl Mix.Task
  def run([node_id]) do
    with {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <- ExStorageService.Operations.Node.drain(context, node_id) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      {:error, reason} -> Mix.raise("node drain failed: #{inspect(reason)}")
    end
  end

  def run(_args), do: Mix.raise("usage: mix ess.node.drain NODE_ID")
end
