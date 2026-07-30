defmodule Mix.Tasks.Ess.Blob.Audit do
  use Mix.Task

  @shortdoc "Audit one bounded page of cluster blob durability metadata"
  @moduledoc """
  Audits one bounded page of cluster blob durability metadata without writing:

      mix ess.blob.audit [--cursor CURSOR] [--limit N]
  """
  @requirements ["app.start"]

  @switches [cursor: :string, limit: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: @switches),
         {:ok, context} <- ExStorageService.Context.default(),
         {:ok, result} <-
           ExStorageService.Operations.Blob.audit_page(
             context,
             opts[:cursor],
             opts[:limit] || 100
           ) do
      Mix.shell().info(inspect(result, pretty: true, limit: :infinity))
    else
      {_opts, _args, invalid} ->
        Mix.raise("invalid options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("blob audit failed: #{inspect(reason)}")
    end
  end
end
