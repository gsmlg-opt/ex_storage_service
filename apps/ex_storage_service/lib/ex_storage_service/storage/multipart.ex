defmodule ExStorageService.Storage.Multipart do
  @moduledoc """
  Multipart upload storage operations.

  Manages the lifecycle of S3 multipart uploads: initiating uploads,
  storing individual parts, completing (concatenating) uploads, and
  aborting/cleaning up.

  Metadata is stored in Concord KV:
  - Upload record: `"mpu:{bucket}:{upload_id}"` — key, status, timestamps
  - Part record:   `"mpu_part:{bucket}:{upload_id}:{part_number}"` — etag, size

  Part data is stored on disk at:
    `{data_root}/{bucket}/multipart/{upload_id}/part.NNNNN`
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

  Writes the part data to disk and records metadata in Concord.
  Returns `{:ok, etag}` on success.
  """
  def store_part(bucket, upload_id, part_number, data) do
    part_dir = part_dir(bucket, upload_id)
    File.mkdir_p!(part_dir)

    part_path = part_path(bucket, upload_id, part_number)

    case File.write(part_path, data) do
      :ok ->
        md5 = :crypto.hash(:md5, data)
        etag = Base.encode16(md5, case: :lower)
        size = byte_size(data)

        part_meta = %{
          part_number: part_number,
          etag: etag,
          size: size,
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

  Returns `{:ok, {content_hash, etag, size}}`.
  """
  def complete_upload(bucket, upload_id, parts) do
    data_root = data_root()

    # Verify upload exists
    case get_upload(bucket, upload_id) do
      {:ok, upload_meta} ->
        # Sort parts by part number
        sorted_parts = Enum.sort_by(parts, fn {pn, _etag} -> pn end)

        # Concatenate parts, computing hashes
        tmp_dir = Path.join([data_root, bucket, "tmp"])
        File.mkdir_p!(tmp_dir)
        tmp_path = Path.join(tmp_dir, "mpu_complete_#{:erlang.unique_integer([:positive])}")

        result =
          try do
            file = File.open!(tmp_path, [:write, :raw, :binary])
            sha256_ctx = :crypto.hash_init(:sha256)
            md5_digests = []
            total_size = 0

            {sha256_ctx, md5_digests, total_size} =
              Enum.reduce(sorted_parts, {sha256_ctx, md5_digests, total_size}, fn {pn,
                                                                                   client_etag},
                                                                                  {sha_ctx, md5s,
                                                                                   size} ->
                part_file = part_path(bucket, upload_id, pn)

                case File.read(part_file) do
                  {:ok, part_data} ->
                    part_md5 = :crypto.hash(:md5, part_data)
                    computed_etag = Base.encode16(part_md5, case: :lower)

                    if client_etag != "" and computed_etag != client_etag do
                      throw({:etag_mismatch, pn, client_etag, computed_etag})
                    end

                    :ok = IO.binwrite(file, part_data)
                    sha_ctx = :crypto.hash_update(sha_ctx, part_data)
                    {sha_ctx, md5s ++ [part_md5], size + byte_size(part_data)}

                  {:error, reason} ->
                    throw({:part_error, pn, reason})
                end
              end)

            File.close(file)

            sha256 = :crypto.hash_final(sha256_ctx)
            content_hash = Base.encode16(sha256, case: :lower)

            # S3 multipart etag: MD5 of concatenated MD5 digests, suffixed with -partcount
            combined_md5 = :crypto.hash(:md5, IO.iodata_to_binary(md5_digests))
            etag = "#{Base.encode16(combined_md5, case: :lower)}-#{length(sorted_parts)}"

            # Move to content-addressed storage
            dest = ExStorageService.Storage.Engine.content_path(data_root, bucket, content_hash)
            File.mkdir_p!(Path.dirname(dest))
            File.rename!(tmp_path, dest)

            {:ok, {content_hash, etag, total_size, upload_meta}}
          catch
            {:part_error, pn, reason} ->
              File.rm(tmp_path)
              {:error, {:missing_part, pn, reason}}

            {:etag_mismatch, pn, expected, actual} ->
              File.rm(tmp_path)
              {:error, {:etag_mismatch, pn, expected, actual}}
          end

        case result do
          {:ok, {content_hash, etag, total_size, _upload_meta}} ->
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
              etag: etag,
              size: total_size,
              created_at: get_in_upload(upload_meta, :created_at),
              updated_at: now
            }

            Concord.put(mpu_key(bucket, upload_id), completed_meta)

            {:ok, {content_hash, etag, total_size}}

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

  defp data_root do
    Application.get_env(:ex_storage_service, :data_root, "/tmp/ex_storage_service/data")
  end

  defp generate_upload_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  defp mpu_key(bucket, upload_id), do: "mpu:#{bucket}:#{upload_id}"

  defp mpu_part_key(bucket, upload_id, part_number),
    do: "mpu_part:#{bucket}:#{upload_id}:#{part_number}"

  defp part_dir(bucket, upload_id) do
    Path.join([data_root(), bucket, "multipart", upload_id])
  end

  defp part_path(bucket, upload_id, part_number) do
    padded = part_number |> Integer.to_string() |> String.pad_leading(5, "0")
    Path.join(part_dir(bucket, upload_id), "part.#{padded}")
  end

  defp get_in_upload(meta, key) when is_map(meta), do: Map.get(meta, key)

  defp cleanup_parts(bucket, upload_id) do
    # Delete part files from disk
    dir = part_dir(bucket, upload_id)

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
