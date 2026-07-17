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

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.Storage.{CAS, Engine, Manifest}

  @part_chunk_size 262_144

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
    case Engine.put_object(bucket, "mpu-part", data) do
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
    with {:ok, prepared} <- prepare_complete_upload(bucket, upload_id, parts),
         :ok <- finalize_complete_upload(bucket, upload_id, prepared) do
      {:ok, {prepared.content_hash, prepared.etag, prepared.size, prepared.manifest_hash}}
    end
  end

  @doc """
  Builds and durably commits the final multipart blob without publishing object
  metadata or cleaning the upload.

  The caller must publish the object through `ExStorageService.ObjectService`
  and call `finalize_complete_upload/3` only after that metadata commit
  succeeds. A failure therefore leaves a retryable upload and a recoverable CAS
  orphan rather than a falsely completed upload.
  """
  def prepare_complete_upload(bucket, upload_id, parts) do
    with {:ok, upload_meta} <- get_upload(bucket, upload_id) do
      sorted_parts = Enum.sort_by(parts, fn {part_number, _etag} -> part_number end)

      min_part_size =
        Application.get_env(:ex_storage_service, :min_part_size, 5 * 1024 * 1024)

      last_index = length(sorted_parts) - 1

      with {:ok, part_records} <- resolve_part_records(bucket, upload_id, sorted_parts),
           :ok <- validate_parts(part_records, min_part_size, last_index),
           {:ok, {content_hash, etag, size, manifest_hash}} <-
             concatenate_parts(bucket, part_records) do
        {:ok,
         %{
           content_hash: content_hash,
           etag: etag,
           size: size,
           manifest_hash: manifest_hash,
           upload: upload_meta
         }}
      end
    end
  end

  @doc """
  Marks a prepared multipart upload complete and removes its part records.
  """
  def finalize_complete_upload(bucket, upload_id, prepared) do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    completed_meta = %{
      bucket: bucket,
      key: get_in_upload(prepared.upload, :key),
      upload_id: upload_id,
      status: :completed,
      content_hash: prepared.content_hash,
      manifest_hash: prepared.manifest_hash,
      etag: prepared.etag,
      size: prepared.size,
      created_at: get_in_upload(prepared.upload, :created_at),
      updated_at: now
    }

    case Concord.put(mpu_key(bucket, upload_id), completed_meta) do
      result when result in [:ok, {:ok, nil}] ->
        cleanup_parts(bucket, upload_id)
        :ok

      {:ok, _result} ->
        cleanup_parts(bucket, upload_id)
        :ok

      {:error, _reason} = error ->
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

  # Streams each CAS part blob through LocalCAS, then records the manifest.
  defp concatenate_parts(bucket, part_records) do
    with {:ok, streams} <- part_streams(bucket, part_records),
         {:ok, staged} <- LocalCAS.stage(Stream.concat(streams), blob_store_opts()) do
      case LocalCAS.commit(staged, blob_store_opts()) do
        {:ok, ready} ->
          finish_concatenation(ready, part_records)

        {:error, _reason} = error ->
          _ = LocalCAS.discard(staged, blob_store_opts())
          error
      end
    end
  end

  defp part_streams(bucket, part_records) do
    part_records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, streams} ->
      case Engine.get_object_location(bucket, record.hash) do
        {:ok, {:file, path}} ->
          {:cont, {:ok, [file_slice_stream(path, 0, record.size) | streams]}}

        {:ok, {:pack, path, offset, size}} when size == record.size ->
          {:cont, {:ok, [file_slice_stream(path, offset, size) | streams]}}

        {:ok, {:pack, _path, _offset, size}} ->
          {:halt,
           {:error, {:multipart_part_size_mismatch, record.part_number, record.size, size}}}

        {:error, reason} ->
          {:halt, {:error, {:multipart_part_unavailable, record.part_number, reason}}}
      end
    end)
    |> case do
      {:ok, streams} -> {:ok, Enum.reverse(streams)}
      {:error, _reason} = error -> error
    end
  end

  defp finish_concatenation(ready, part_records) do
    with {:ok, etag} <- multipart_etag(part_records) do
      ExStorageService.Metadata.ensure_blob_meta(ready.hash, ready.size)

      manifest_parts =
        Enum.map(part_records, fn record ->
          %{
            number: record.part_number,
            hash: record.hash,
            size: record.size,
            etag: record.etag
          }
        end)

      case Manifest.create_manifest(manifest_parts, ready.size, etag) do
        {:ok, manifest_hash} ->
          {:ok, {ready.hash, etag, ready.size, manifest_hash}}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp multipart_etag(part_records) do
    part_records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, digests} ->
      case Base.decode16(record.etag, case: :mixed) do
        {:ok, digest} -> {:cont, {:ok, [digest | digests]}}
        :error -> {:halt, {:error, {:invalid_part_etag, record.part_number}}}
      end
    end)
    |> case do
      {:ok, digests} ->
        combined_md5 = digests |> Enum.reverse() |> IO.iodata_to_binary() |> md5()

        {:ok, "#{Base.encode16(combined_md5, case: :lower)}-#{length(part_records)}"}

      {:error, _reason} = error ->
        error
    end
  end

  defp md5(data), do: :crypto.hash(:md5, data)

  defp file_slice_stream(path, offset, size) do
    Stream.resource(
      fn ->
        case File.open(path, [:read, :raw, :binary]) do
          {:ok, file} -> {:open, file, offset, size}
          {:error, reason} -> {:error, {:open, reason}}
        end
      end,
      &read_file_chunk/1,
      &close_file_slice/1
    )
  end

  defp read_file_chunk({:error, reason}), do: throw({:error, {:multipart_read, reason}})

  defp read_file_chunk({:open, _file, _offset, 0} = state), do: {:halt, state}

  defp read_file_chunk({:open, file, offset, remaining}) do
    bytes_to_read = min(remaining, @part_chunk_size)

    case :file.pread(file, offset, bytes_to_read) do
      {:ok, data} when byte_size(data) == bytes_to_read ->
        {[data], {:open, file, offset + bytes_to_read, remaining - bytes_to_read}}

      {:ok, data} ->
        throw({:error, {:multipart_read, {:short_read, bytes_to_read, byte_size(data)}}})

      :eof ->
        throw({:error, {:multipart_read, :unexpected_eof}})

      {:error, reason} ->
        throw({:error, {:multipart_read, reason}})
    end
  end

  defp close_file_slice({:open, file, _offset, _remaining}), do: File.close(file)
  defp close_file_slice({:error, _reason}), do: :ok

  defp blob_store_opts do
    [root: Path.join(CAS.data_root(), CAS.reserved_root())]
  end

  defp cleanup_parts(bucket, upload_id) do
    # Delete part files from disk
    dir = Path.join([ExStorageService.Storage.CAS.data_root(), bucket, "multipart", upload_id])

    if File.dir?(dir) do
      _ = File.rm_rf(dir)
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
