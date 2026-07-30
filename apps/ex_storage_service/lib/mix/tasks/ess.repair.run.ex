defmodule Mix.Tasks.Ess.Repair.Run do
  use Mix.Task

  @shortdoc "Enqueue one owned repair-shard page through the durable outbox"
  @moduledoc """
  Enqueues one locally owned repair-shard page through the durable outbox:

      mix ess.repair.run --shard HEX [--cursor CURSOR] [--limit N]
  """
  @requirements ["app.start"]

  @switches [shard: :string, cursor: :string, limit: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: @switches),
         shard when is_binary(shard) <- opts[:shard],
         {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <-
           ExStorageService.Operations.Repair.run_page(
             context,
             shard,
             opts[:cursor],
             opts[:limit] || 100
           ) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      nil ->
        Mix.raise("usage: mix ess.repair.run --shard HEX [--cursor CURSOR] [--limit N]")

      {_opts, _args, invalid} ->
        Mix.raise("invalid options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("repair run failed: #{inspect(reason)}")
    end
  end
end
