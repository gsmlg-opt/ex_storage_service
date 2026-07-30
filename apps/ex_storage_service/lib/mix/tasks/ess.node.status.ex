defmodule Mix.Tasks.Ess.Node.Status do
  use Mix.Task

  @shortdoc "Show one bounded page of node-drain progress"
  @moduledoc """
  Shows one bounded page of node-drain progress:

      mix ess.node.status NODE_ID [--cursor CURSOR] [--limit N]
  """
  @requirements ["app.start"]

  @switches [cursor: :string, limit: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [node_id], []} <- OptionParser.parse(args, strict: @switches),
         {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <-
           ExStorageService.Operations.Node.status(
             context,
             node_id,
             opts[:cursor],
             opts[:limit] || 100
           ) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      {_opts, _args, invalid} ->
        Mix.raise("invalid arguments or options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("node status failed: #{inspect(reason)}")
    end
  end
end
