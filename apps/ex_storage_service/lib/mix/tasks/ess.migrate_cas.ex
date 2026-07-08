defmodule Mix.Tasks.Ess.MigrateCas do
  @shortdoc "Migrate legacy bucket-local content files into the global CAS"

  @moduledoc """
  Moves all content files referenced by object metadata from the legacy
  bucket-local layout into the global CAS. Run with the service stopped
  or in maintenance mode:

      mix ess.migrate_cas

  Idempotent. Prints a report; metadata entries whose content is missing
  in both layouts are listed for manual repair. Legacy duplicate files
  (dedup hits) are left in place — delete the legacy layout only after
  verifying the report (PRD §19.3).
  """

  use Mix.Task

  @requirements ["app.start"]

  @impl Mix.Task
  def run(_args) do
    case ExStorageService.Storage.Migration.migrate_to_global_cas() do
      {:ok, report} ->
        Mix.shell().info("Migrated: #{report.migrated}")
        Mix.shell().info("Already global: #{report.already_global}")
        Mix.shell().info("Missing content: #{length(report.missing)}")

        Enum.each(report.missing, fn {bucket, hash} ->
          Mix.shell().error("  missing: bucket=#{bucket} hash=#{hash}")
        end)

      {:error, reason} ->
        Mix.raise("CAS migration failed: #{inspect(reason)}")
    end
  end
end
