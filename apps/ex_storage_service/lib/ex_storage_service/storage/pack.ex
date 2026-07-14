defmodule ExStorageService.Storage.Pack do
  @moduledoc """
  Immutable, content-addressed pack files for cold blobs.

  A pack is an **uncompressed concatenation** of blob contents at
  `{data_root}/cas/packs/pack-{sha256-of-pack-bytes}.pack`, so a packed
  blob is served with `send_file(path, offset, size)` — zero-copy, exact
  Content-Length, and Range = pack_offset + range_offset. CAS identity is
  preserved: blobs stay addressed by their SHA-256; `blob:sha256:{hash}`
  metadata carries `state: :packed` and `pack: %{hash:, offset:}`.

  The index lives twice: a `pack:{pack_hash}` Concord record and a JSON
  `.idx` sidecar for repair. Packs are never mutated; reclaiming dead
  entries (repack) is a future follow-up.

  Crash-safe write order: tmp pack → rename → sidecar → pack record →
  per-blob metadata. The loose blob is retained as a fallback until a later
  Packer cleanup pass, after readers that resolved its old location have had
  time to finish.
  """

  require Logger

  alias ExStorageService.Metadata
  alias ExStorageService.Storage.CAS

  def pack_path(pack_hash) do
    Path.join([CAS.data_root(), CAS.reserved_root(), "packs", "pack-#{pack_hash}.pack"])
  end

  @doc """
  Pack the given loose blobs. Skips hashes whose loose file is missing or
  whose metadata is already `:packed`. Returns the new pack's hash and the
  number of blobs packed.
  """
  def pack_blobs(hashes) do
    entries =
      hashes
      |> Enum.uniq()
      |> Enum.filter(&packable?/1)

    if entries == [] do
      {:ok, %{pack_hash: nil, packed: 0}}
    else
      write_pack(entries)
    end
  end

  @doc "Location of a packed blob: `{:ok, {pack_path, offset, size}}`."
  def locate(hash) do
    with {:ok, %{state: :packed, pack: pack_info} = meta} <- Metadata.get_blob_meta(hash),
         pack_hash when is_binary(pack_hash) <- get_field(pack_info, :hash),
         offset when is_integer(offset) and offset >= 0 <- get_field(pack_info, :offset),
         size when is_integer(size) and size >= 0 <- get_field(meta, :size),
         path = pack_path(pack_hash),
         {:ok, %File.Stat{type: :regular, size: pack_size}} <- File.stat(path),
         true <- offset + size <= pack_size do
      {:ok, {path, offset, size}}
    else
      _ -> {:error, :not_found}
    end
  end

  @doc "Read a packed blob's bytes."
  def read(hash) do
    with {:ok, {path, offset, size}} <- locate(hash),
         {:ok, fd} <- File.open(path, [:read, :raw, :binary]) do
      try do
        case :file.pread(fd, offset, size) do
          {:ok, data} when byte_size(data) == size -> {:ok, data}
          {:ok, _short_data} -> {:error, :corrupt_pack}
          :eof -> {:error, :corrupt_pack}
          {:error, reason} -> {:error, reason}
        end
      after
        File.close(fd)
      end
    end
  end

  ## Private

  defp packable?(hash) do
    File.exists?(CAS.blob_path(hash)) and no_gc_candidate?(hash) and
      case Metadata.get_blob_meta(hash) do
        {:ok, %{state: :packed}} -> false
        _ -> true
      end
  end

  # Fail closed: a metadata lookup failure must not let direct callers race
  # an in-progress CasGC candidate through to packed state.
  defp no_gc_candidate?(hash) do
    case Concord.get("gc:candidate:#{hash}") do
      {:ok, nil} -> true
      {:error, :not_found} -> true
      _candidate_or_error -> false
    end
  end

  defp write_pack(hashes) do
    tmp_dir = Path.join([CAS.data_root(), CAS.reserved_root(), "tmp"])
    File.mkdir_p!(tmp_dir)
    tmp_path = Path.join(tmp_dir, "pack-#{:erlang.unique_integer([:positive])}.tmp")

    out = File.open!(tmp_path, [:write, :raw, :binary])

    try do
      {index, _offset, sha_ctx} =
        Enum.reduce(hashes, {[], 0, :crypto.hash_init(:sha256)}, fn hash, {idx, offset, ctx} ->
          data = File.read!(CAS.blob_path(hash))
          :ok = IO.binwrite(out, data)
          size = byte_size(data)
          entry = %{blob_hash: hash, offset: offset, size: size}
          {[entry | idx], offset + size, :crypto.hash_update(ctx, data)}
        end)

      File.close(out)

      index = Enum.reverse(index)
      pack_hash = sha_ctx |> :crypto.hash_final() |> Base.encode16(case: :lower)
      dest = pack_path(pack_hash)
      File.mkdir_p!(Path.dirname(dest))
      File.rename!(tmp_path, dest)

      write_sidecar(dest, pack_hash, index)

      total_size = Enum.reduce(index, 0, &(&1.size + &2))

      Concord.put("pack:#{pack_hash}", %{
        hash: pack_hash,
        entries: index,
        size: total_size,
        blob_count: length(index),
        created_at: DateTime.utc_now() |> DateTime.to_iso8601()
      })

      Enum.each(index, fn %{blob_hash: hash, offset: offset, size: size} ->
        mark_packed(hash, pack_hash, offset, size)
      end)

      Logger.info(
        "Pack: packed #{length(index)} blobs into pack-#{pack_hash} (#{total_size} bytes)"
      )

      {:ok, %{pack_hash: pack_hash, packed: length(index)}}
    rescue
      e ->
        File.close(out)
        File.rm(tmp_path)
        {:error, Exception.message(e)}
    end
  end

  defp write_sidecar(dest, pack_hash, index) do
    sidecar =
      JSON.encode!(%{
        format: "ess-pack-v1",
        hash: pack_hash,
        entries: Enum.map(index, fn e -> [e.blob_hash, e.offset, e.size] end)
      })

    File.write!(dest <> ".idx", sidecar)
  end

  defp mark_packed(hash, pack_hash, offset, size) do
    case Metadata.get_blob_meta(hash) do
      {:ok, meta} ->
        meta
        |> Map.put(:state, :packed)
        |> Map.put(:pack, %{hash: pack_hash, offset: offset})
        |> Map.put(:physical_path, Path.join(["cas", "packs", "pack-#{pack_hash}.pack"]))
        |> Map.put(:size, size)
        |> Map.put(:packed_at, System.os_time(:second))
        |> then(&Metadata.put_blob_meta(hash, &1))

      {:error, :not_found} ->
        Metadata.put_blob_meta(hash, %{
          hash: "sha256:#{hash}",
          size: size,
          physical_path: Path.join(["cas", "packs", "pack-#{pack_hash}.pack"]),
          state: :packed,
          pack: %{hash: pack_hash, offset: offset},
          packed_at: System.os_time(:second),
          created_at: DateTime.utc_now() |> DateTime.to_iso8601(),
          last_seen_at: DateTime.utc_now() |> DateTime.to_iso8601()
        })
    end
  end

  defp get_field(map, key) when is_map(map), do: map[key] || map[to_string(key)]
  defp get_field(_, _), do: nil
end
