defmodule Mix.Tasks.Ess.Repair.Plan do
  use Mix.Task

  @shortdoc "Preview one owned repair-shard page without enqueueing work"
  @moduledoc """
  Previews one locally owned repair-shard page without enqueueing work:

      mix ess.repair.plan --shard HEX [--cursor CURSOR] [--limit N]
  """
  @requirements ["app.start"]

  @switches [shard: :string, cursor: :string, limit: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: @switches),
         shard when is_binary(shard) <- opts[:shard],
         {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <-
           ExStorageService.Operations.Repair.plan_page(
             context,
             shard,
             opts[:cursor],
             opts[:limit] || 100
           ) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      nil ->
        Mix.raise("usage: mix ess.repair.plan --shard HEX [--cursor CURSOR] [--limit N]")

      {_opts, _args, invalid} ->
        Mix.raise("invalid options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("repair plan failed: #{inspect(reason)}")
    end
  end
end
