defmodule Mix.Tasks.Ess.Metadata.SchemaStatus do
  @shortdoc "Report legacy and v2 metadata schema record counts"

  @moduledoc """
  Prints a read-only, paginated inventory of legacy and v2 metadata:

      mix ess.metadata.schema_status --replication-factor N
  """

  use Mix.Task

  @requirements ["app.start"]
  @switches [replication_factor: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: @switches),
         replication_factor when is_integer(replication_factor) and replication_factor > 0 <-
           opts[:replication_factor],
         {:ok, status} <-
           ExStorageService.Metadata.Schema.status(replication_factor: replication_factor) do
      Mix.shell().info("Configured schema: #{status.configured_schema}")
      Mix.shell().info("Target replication factor: #{status.target_replication_factor}")
      Mix.shell().info("V1-only objects: #{status.v1_only_objects}")
      Mix.shell().info("Migration required: #{status.migration_required}")
      Mix.shell().info("Migration safe to run: #{status.migration_ready}")
      Mix.shell().info("Migration complete: #{status.migration_complete}")
      Mix.shell().info("Replication ready: #{status.replication_ready}")
      print_counts("v1", status.v1)
      print_counts("v2", status.v2)
      Mix.shell().info("Invalid records: #{status.validation.invalid_record_count}")

      Enum.each(status.validation.invalid_records, fn invalid ->
        Mix.shell().error("  invalid: key=#{invalid.key} reason=#{inspect(invalid.reason)}")
      end)
    else
      nil ->
        Mix.raise("usage: mix ess.metadata.schema_status --replication-factor POSITIVE_INTEGER")

      replication_factor when is_integer(replication_factor) ->
        Mix.raise("replication factor must be a positive integer")

      {_opts, _args, invalid} ->
        Mix.raise("invalid options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("Metadata schema status failed: #{inspect(reason)}")
    end
  end

  defp print_counts(schema, counts) do
    Enum.each(counts, fn {kind, count} ->
      Mix.shell().info("#{schema}.#{kind}: #{count}")
    end)
  end
end
