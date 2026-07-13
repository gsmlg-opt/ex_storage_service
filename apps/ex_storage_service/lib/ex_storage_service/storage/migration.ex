defmodule ExStorageService.Storage.Migration do
  @moduledoc """
  One-shot migration of legacy bucket-local content files
  (`{data_root}/{bucket}/objects/...`) into the global CAS
  (`{data_root}/cas/objects/sha256/...`).

  Physical files only: object metadata (`obj:*`, `obj_ver:*`) is not
  rewritten in Phase 1 (see docs/prd/git-style-data-model.md §19–20).
  Run in maintenance mode (no concurrent writes). Idempotent: re-running
  counts already-migrated blobs under `:already_global`.
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.{CAS, Engine}

  @doc """
  Migrates every content hash referenced by `obj:*` / `obj_ver:*`
  metadata. Returns `{:ok, report}` with `:migrated`, `:already_global`,
  and `:missing` (list of `{bucket, hash}` whose content exists in
  neither layout — repair-worker input).
  """
  def migrate_to_global_cas do
    case Concord.get_all() do
      {:ok, all} ->
        report =
          all
          |> Enum.flat_map(&referenced_hash/1)
          |> Enum.uniq()
          |> Enum.reduce(%{migrated: 0, already_global: 0, missing: []}, &migrate_one/2)

        Logger.info(
          "CAS migration: #{inspect(Map.delete(report, :missing))}, missing: #{length(report.missing)}"
        )

        {:ok, report}

      error ->
        error
    end
  end

  # Key formats: "obj:{bucket}:{key}" and "obj_ver:{bucket}:{key}:{vid}".
  # In both, the segment after the first colon is the bucket.
  defp referenced_hash({key, value}) do
    with true <- String.starts_with?(key, "obj:") or String.starts_with?(key, "obj_ver:"),
         [_ns, bucket, _rest] <- String.split(key, ":", parts: 3),
         hash when is_binary(hash) <- Map.get(value, :content_hash) do
      [{bucket, hash}]
    else
      _ -> []
    end
  end

  defp migrate_one({bucket, hash}, acc) do
    cond do
      CAS.has_blob?(hash) ->
        ensure_meta_from_disk(hash)
        %{acc | already_global: acc.already_global + 1}

      true ->
        case Engine.promote_to_global(bucket, hash) do
          :ok ->
            %{acc | migrated: acc.migrated + 1}

          {:error, :not_found} ->
            Logger.warning("CAS migration: content missing for #{bucket} hash #{hash}")
            %{acc | missing: [{bucket, hash} | acc.missing]}
        end
    end
  end

  defp ensure_meta_from_disk(hash) do
    case File.stat(CAS.blob_path(hash)) do
      {:ok, %File.Stat{size: size}} -> Metadata.ensure_blob_meta(hash, size)
      {:error, _} -> :ok
    end
  end
end
