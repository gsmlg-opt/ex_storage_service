defmodule ExStorageService.Storage.Multipart do
  @moduledoc """
  Multipart upload storage operations.

  Manages the lifecycle of S3 multipart uploads: initiating uploads,
  storing individual parts, completing (concatenating) uploads, and
  aborting/cleaning up.

  Metadata is stored in Concord KV:
  - Upload record: `"mpu:{bucket}:{upload_id}"` — key, status, timestamps
  - Part record:   `"mpu_part:{bucket}:{upload_id}:{part_number}"` — hash, etag, size

  Part data is stored as global CAS blobs; the part record stores the blob hash.
  """

  require Logger

  @doc """
  Initiate a new multipart upload. Returns `{:ok, upload_id}`.
  """
  def init_upload(bucket, key) do
    upload_id = generate_upload_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    meta = %{
      bucket: bucket,
      key: key,
      upload_id: upload_id,
      status: :initiated,
      created_at: now,
      updated_at: now
    }

    case Concord.put(mpu_key(bucket, upload_id), meta) do
      :ok -> {:ok, upload_id}
      {:ok, _} -> {:ok, upload_id}
      error -> error
    end
  end

  @doc """
  Store a single part for a multipart upload.

  Writes the part data to the global CAS and records metadata in Concord.
  Returns `{:ok, etag}` on success.
  """
  def store_part(bucket, upload_id, part_number, data) do
    case ExStorageService.Storage.Engine.put_object(bucket, "mpu-part", data) do
      {:ok, {hash, etag, size}} ->
        part_meta = %{
          part_number: part_number,
          etag: etag,
          size: size,
          hash: hash,
          uploaded_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        Concord.put(mpu_part_key(bucket, upload_id, part_number), part_meta)
        {:ok, etag}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Complete a multipart upload.

  Concatenates all parts in order, computes final SHA-256 content hash
  and the S3 multipart etag (MD5-of-MD5s-partcount), moves the result
  to the content-addressed store, and cleans up part files.

  `parts` is a list of `{part_number, etag}` tuples from the client XML.

  Returns `{:ok, {content_hash, etag, size, manifest_hash}}`.
  """
  def complete_upload(bucket, upload_id, parts) do
    # Verify upload exists
    case get_upload(bucket, upload_id) do
      {:ok, upload_meta} ->
        # Sort parts by part number
        sorted_parts = Enum.sort_by(parts, fn {pn, _etag} -> pn end)

        # All parts except the last must meet the minimum part size (S3 rule).
        min_part_size =
          Application.get_env(:ex_storage_service, :min_part_size, 5 * 1024 * 1024)

        last_index = length(sorted_parts) - 1

        result =
          with {:ok, part_records} <- resolve_part_records(bucket, upload_id, sorted_parts),
               :ok <- validate_parts(part_records, min_part_size, last_index) do
            concatenate_parts(part_records)
          end

        case result do
          {:ok, {content_hash, etag, total_size, manifest_hash}} ->
            # Clean up part files and metadata
            cleanup_parts(bucket, upload_id)

            # Update upload metadata to completed
            now = DateTime.utc_now() |> DateTime.to_iso8601()

            completed_meta = %{
              bucket: bucket,
              key: get_in_upload(upload_meta, :key),
              upload_id: upload_id,
              status: :completed,
              content_hash: content_hash,
              manifest_hash: manifest_hash,
              etag: etag,
              size: total_size,
              created_at: get_in_upload(upload_meta, :created_at),
              updated_at: now
            }

            Concord.put(mpu_key(bucket, upload_id), completed_meta)

            {:ok, {content_hash, etag, total_size, manifest_hash}}

          error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Abort a multipart upload. Deletes all part files and metadata.
  """
  def abort_upload(bucket, upload_id) do
    cleanup_parts(bucket, upload_id)
    Concord.delete(mpu_key(bucket, upload_id))
    :ok
  end

  @doc """
  List all parts for a multipart upload with their etags and sizes.

  Returns `{:ok, parts}` where parts is a sorted list of part metadata maps.
  """
  def list_parts(bucket, upload_id) do
    case get_upload(bucket, upload_id) do
      {:ok, _upload_meta} ->
        prefix = "mpu_part:#{bucket}:#{upload_id}:"

        case Concord.get_all() do
          {:ok, all} ->
            parts =
              all
              |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
              |> Enum.map(fn {_k, v} -> v end)
              |> Enum.sort_by(fn p -> p.part_number end)

            {:ok, parts}

          error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Get upload metadata from Concord.
  """
  def get_upload(bucket, upload_id) do
    case Concord.get(mpu_key(bucket, upload_id)) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} -> {:ok, value}
      error -> error
    end
  end

  @doc """
  List all active (non-completed) multipart uploads. Used by the GC.
  """
  def list_active_uploads do
    case Concord.get_all() do
      {:ok, all} ->
        uploads =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "mpu:") end)
          |> Enum.map(fn {_k, v} -> v end)
          |> Enum.filter(fn v -> v.status == :initiated end)

        {:ok, uploads}

      error ->
        error
    end
  end

  # Private helpers

  defp generate_upload_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  defp mpu_key(bucket, upload_id), do: "mpu:#{bucket}:#{upload_id}"

  defp mpu_part_key(bucket, upload_id, part_number),
    do: "mpu_part:#{bucket}:#{upload_id}:#{part_number}"

  defp get_in_upload(meta, key) when is_map(meta), do: Map.get(meta, key)

  # Look up the Concord part record for each client-requested part and
  # check the client-supplied etags, preserving the historical error shapes.
  defp resolve_part_records(bucket, upload_id, sorted_parts) do
    sorted_parts
    |> Enum.reduce_while({:ok, []}, fn {pn, client_etag}, {:ok, acc} ->
      case Concord.get(mpu_part_key(bucket, upload_id, pn)) do
        {:ok, nil} ->
          {:halt, {:error, {:missing_part, pn, :not_found}}}

        {:ok, record} ->
          if client_etag != "" and record.etag != client_etag do
            {:halt, {:error, {:etag_mismatch, pn, client_etag, record.etag}}}
          else
            {:cont, {:ok, [record | acc]}}
          end

        {:error, reason} ->
          {:halt, {:error, {:missing_part, pn, reason}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp validate_parts(part_records, min_part_size, last_index) do
    part_records
    |> Enum.with_index()
    |> Enum.find(fn {record, idx} -> idx < last_index and record.size < min_part_size end)
    |> case do
      nil ->
        :ok

      {record, _idx} ->
        {:error, {:entity_too_small, record.part_number, record.size, min_part_size}}
    end
  end

  # Streams each CAS part blob into a tmp file (constant memory), computing
  # the whole-object SHA-256 on the way, then commits the result as a blob
  # and records the manifest.
  defp concatenate_parts(part_records) do
    alias ExStorageService.Storage.{CAS, Manifest}

    tmp_path = CAS.tmp_upload_path()
    out = File.open!(tmp_path, [:write, :raw, :binary])

    try do
      {sha_ctx, total_size} =
        Enum.reduce(part_records, {:crypto.hash_init(:sha256), 0}, fn record, {ctx, size} ->
          part_blob = CAS.blob_path(record.hash)

          ctx =
            part_blob
            |> File.stream!(262_144)
            |> Enum.reduce(ctx, fn chunk, c ->
              :ok = IO.binwrite(out, chunk)
              :crypto.hash_update(c, chunk)
            end)

          {ctx, size + record.size}
        end)

      File.close(out)

      content_hash = sha_ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)

      # S3 multipart etag: MD5 of the concatenated raw part-MD5 digests,
      # suffixed with the part count; part etags are the hex MD5s.
      md5_digests = Enum.map(part_records, &Base.decode16!(&1.etag, case: :mixed))
      combined_md5 = :crypto.hash(:md5, IO.iodata_to_binary(md5_digests))
      etag = "#{Base.encode16(combined_md5, case: :lower)}-#{length(part_records)}"

      :ok = CAS.commit_blob(tmp_path, content_hash)
      ExStorageService.Metadata.ensure_blob_meta(content_hash, total_size)

      manifest_parts =
        Enum.map(part_records, fn r ->
          %{number: r.part_number, hash: r.hash, size: r.size, etag: r.etag}
        end)

      {:ok, manifest_hash} = Manifest.create_manifest(manifest_parts, total_size, etag)

      {:ok, {content_hash, etag, total_size, manifest_hash}}
    rescue
      e ->
        File.close(out)
        File.rm(tmp_path)
        {:error, Exception.message(e)}
    end
  end

  defp cleanup_parts(bucket, upload_id) do
    # Delete part files from disk
    dir = Path.join([ExStorageService.Storage.CAS.data_root(), bucket, "multipart", upload_id])

    if File.dir?(dir) do
      File.rm_rf!(dir)
    end

    # Delete part metadata from Concord
    prefix = "mpu_part:#{bucket}:#{upload_id}:"

    case Concord.get_all() do
      {:ok, all} ->
        all
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
        |> Enum.each(fn {k, _v} -> Concord.delete(k) end)

      _ ->
        :ok
    end
  end
end
