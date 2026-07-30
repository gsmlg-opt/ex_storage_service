defmodule Mix.Tasks.Ess.Metadata.MigrateV2 do
  @shortdoc "Migrate legacy object metadata to immutable v2 records"

  @moduledoc """
  Migrates legacy object metadata without deleting or modifying v1 records:

      mix ess.metadata.migrate_v2 --replication-factor N

  Run with object writes disabled after backing up Concord metadata and the
  blob root. The operation is idempotent and may be resumed after interruption.
  """

  use Mix.Task

  @requirements ["app.start"]
  @switches [replication_factor: :integer]

  @impl Mix.Task
  def run(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: @switches),
         replication_factor when is_integer(replication_factor) and replication_factor > 0 <-
           opts[:replication_factor],
         {:ok, report} <-
           ExStorageService.Metadata.Migration.migrate_v2(replication_factor: replication_factor) do
      Mix.shell().info("Target replication factor: #{report.target_replication_factor}")
      Mix.shell().info("Objects scanned: #{report.objects_scanned}")
      Mix.shell().info("Objects migrated: #{report.objects_migrated}")
      Mix.shell().info("Objects already v2: #{report.objects_already_v2}")
      Mix.shell().info("Versions migrated: #{report.versions_migrated}")
      Mix.shell().info("Delete markers migrated: #{report.delete_markers_migrated}")
      Mix.shell().info("Blob descriptors created: #{report.blob_descriptors_created}")
      Mix.shell().info("Blob descriptors promoted: #{report.blob_descriptors_promoted}")
      Mix.shell().info("Blob locations created: #{report.blob_locations_created}")
      Mix.shell().info("Missing local blobs: #{length(report.missing_local_blobs)}")

      Enum.each(report.missing_local_blobs, fn hash ->
        Mix.shell().error("  missing: #{hash}")
      end)
    else
      nil ->
        Mix.raise("usage: mix ess.metadata.migrate_v2 --replication-factor POSITIVE_INTEGER")

      replication_factor when is_integer(replication_factor) ->
        Mix.raise("replication factor must be a positive integer")

      {_opts, _args, invalid} ->
        Mix.raise("invalid options: #{inspect(invalid)}")

      {:error, reason} ->
        Mix.raise("Metadata v2 migration failed: #{inspect(reason)}")
    end
  end
end
