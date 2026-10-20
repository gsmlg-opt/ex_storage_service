defmodule ExStorageService.Operations.BackupManifest do
  @moduledoc false

  # Concord's existing logical export materializes metadata. Blob I/O below is
  # sequential and bounded independently of object size; no object is read whole.
  @chunk 262_144
  @format "ess-quiesced-backup-v1"
  @hash ~r/^[a-f0-9]{64}$/

  def format, do: @format

  def digest(path), do: digest(path, 0, nil)

  defp digest(path, offset, length) do
    with :ok <- regular(path), {:ok, file} <- File.open(path, [:read, :raw, :binary]) do
      try do
        :file.position(file, offset)
        hash_file(file, :crypto.hash_init(:sha256), 0, length)
      after
        File.close(file)
      end
    end
  end

  defp hash_file(_file, hash, size, 0), do: {:ok, hex(:crypto.hash_final(hash)), size}

  defp hash_file(file, hash, size, remaining) do
    count = if is_nil(remaining), do: @chunk, else: min(@chunk, remaining)

    case IO.binread(file, count) do
      :eof when is_nil(remaining) ->
        {:ok, hex(:crypto.hash_final(hash)), size}

      :eof ->
        {:error, :short_file}

      {:error, reason} ->
        {:error, reason}

      data ->
        next = if is_nil(remaining), do: nil, else: remaining - byte_size(data)
        hash_file(file, :crypto.hash_update(hash, data), size + byte_size(data), next)
    end
  end

  def copy(source, destination) do
    with :ok <- regular(source),
         :ok <- directory(Path.dirname(destination)),
         {:ok, input} <- File.open(source, [:read, :raw, :binary]) do
      try do
        with {:ok, output} <- File.open(destination, [:write, :exclusive, :raw, :binary]) do
          try do
            with :ok <- File.chmod(destination, 0o600),
                 :ok <- copy_chunks(input, output),
                 :ok <- :file.sync(output) do
              :ok
            end
          after
            File.close(output)
          end
        end
      after
        File.close(input)
      end
    end
  end

  def sync_file(path) do
    with :ok <- regular(path), {:ok, file} <- File.open(path, [:read, :raw, :binary]) do
      try do
        :file.sync(file)
      after
        File.close(file)
      end
    end
  end

  # Only restore-owned temporary files are replaceable. The final path remains
  # untouched until a complete synced copy has its expected digest and size.
  def install(source, destination, expected_hash, expected_size) do
    temporary = destination <> ".ess-restore-" <> expected_hash

    with :ok <- directory(Path.dirname(destination)),
         :ok <- discard_partial(temporary),
         :ok <- copy(source, temporary),
         {:ok, ^expected_hash, ^expected_size} <- digest(temporary),
         {:error, :enoent} <- File.lstat(destination),
         :ok <- File.rename(temporary, destination),
         :ok <- sync_directory(Path.dirname(destination)) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, :restore_destination_conflict}
    end
  end

  defp discard_partial(path) do
    case regular(path) do
      :ok -> File.rm(path)
      {:error, :enoent} -> :ok
      error -> error
    end
  end

  defp copy_chunks(input, output) do
    case IO.binread(input, @chunk) do
      :eof ->
        :ok

      {:error, reason} ->
        {:error, reason}

      data ->
        with :ok <- IO.binwrite(output, data), do: copy_chunks(input, output)
    end
  end

  def write_json(path, value) do
    temporary = path <> ".tmp-" <> random_id()

    with :ok <- directory(Path.dirname(path)),
         :ok <- File.write(temporary, JSON.encode!(value), [:exclusive, :sync]),
         :ok <- File.chmod(temporary, 0o600),
         :ok <- File.rename(temporary, path),
         :ok <- sync_directory(Path.dirname(path)) do
      :ok
    end
  end

  def sync_directory(path) do
    case :file.open(String.to_charlist(path), [:read, :raw, :directory]) do
      {:ok, fd} ->
        try do
          :file.sync(fd)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def directory(path) do
    with :ok <- File.mkdir_p(path), do: no_symlinks(path)
  end

  def regular(path) do
    with :ok <- no_symlinks(path), {:ok, %{type: :regular}} <- File.lstat(path) do
      :ok
    else
      {:ok, _} -> {:error, :not_regular_file}
      error -> error
    end
  end

  def no_symlinks(path) do
    path
    |> Path.expand()
    |> Path.split()
    |> Enum.reduce_while({:ok, ""}, fn component, {:ok, prefix} ->
      # Path.join("", "/") normalizes the root to an empty string. Preserve
      # the anchor so lstat checks absolute ancestors instead of the cwd.
      current = if prefix == "", do: component, else: Path.join(prefix, component)

      case File.lstat(current) do
        {:ok, %{type: :symlink}} -> {:halt, {:error, :symlink_artifact}}
        {:ok, _} -> {:cont, {:ok, current}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def safe_path(root, relative) when is_binary(relative) do
    expanded = Path.expand(root)
    path = Path.expand(relative, expanded)

    if Path.type(relative) == :relative and path != expanded and
         String.starts_with?(path, expanded <> "/") and
         not Enum.any?(Path.split(relative), &(&1 in [".", ".."])) do
      {:ok, path}
    else
      {:error, :unsafe_artifact_path}
    end
  end

  def safe_path(_, _), do: {:error, :unsafe_artifact_path}

  def files(root, excluded \\ []) do
    with :ok <- no_symlinks(root), do: walk(root, root, excluded, [])
  end

  defp walk(path, root, excluded, entries) do
    cond do
      path in excluded ->
        {:ok, entries}

      true ->
        case File.lstat(path) do
          {:ok, %{type: :regular}} ->
            {:ok, [Path.relative_to(path, root) | entries]}

          {:ok, %{type: :directory}} ->
            with {:ok, children} <- File.ls(path) do
              Enum.reduce_while(Enum.sort(children), {:ok, entries}, fn child, {:ok, acc} ->
                case walk(Path.join(path, child), root, excluded, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  error -> {:halt, error}
                end
              end)
            end

          {:ok, _} ->
            {:error, :unsafe_artifact_type}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def read_snapshot(path) do
    with :ok <- regular(path),
         {:ok, bytes} <- File.read(path),
         %{metadata: metadata, data: %{version: 2, kv_data: entries} = snapshot} <-
           :erlang.binary_to_term(bytes),
         {:ok, options} <- checksum_options(metadata),
         true <-
           metadata.checksum == :crypto.hash(:sha256, :erlang.term_to_binary(snapshot, options)),
         true <- is_list(entries) do
      {:ok, snapshot, Enum.map(entries, fn {key, stored} -> {key, unwrap(stored)} end)}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_metadata_snapshot}
    end
  rescue
    _ -> {:error, :invalid_metadata_snapshot}
  end

  defp checksum_options(%{checksum_scheme: :sha256_deterministic_etf_v1}),
    do: {:ok, [:deterministic]}

  defp checksum_options(%{checksum_scheme: _}), do: {:error, :invalid_metadata_snapshot}
  defp checksum_options(_metadata), do: {:ok, []}

  defp unwrap(%{value: value, expires_at: _}), do: value
  defp unwrap({value, expiry}) when is_integer(expiry), do: value
  defp unwrap(value), do: value

  def references(entries) do
    sizes =
      Map.new(
        for {key, value} <- entries,
            is_map(value) and is_binary(key) and
              String.starts_with?(key, ["ess:v2:blob:", "blob:sha256:"]),
            hash =
              String.replace_prefix(
                String.replace_prefix(key, "ess:v2:blob:", ""),
                "blob:sha256:",
                ""
              ),
            do: {hash, Map.get(value, :size)}
      )

    Enum.reduce_while(entries, {:ok, %{}}, fn {key, value}, {:ok, refs} ->
      value =
        if is_map(value) and String.starts_with?(key, "ess:v2:job:") do
          payload = Map.get(value, :payload, %{})
          object = Map.get(payload, :object) || %{}

          hash =
            Map.get(payload, :hash) || Map.get(payload, :content_hash) ||
              Map.get(object, :content_hash)

          size = Map.get(payload, :size) || Map.get(object, :size) || sizes[hash]
          Map.put(value, :payload, Map.merge(payload, %{content_hash: hash, size: size}))
        else
          value
        end

      case reference(key, value) do
        nil ->
          {:cont, {:ok, refs}}

        {:error, _} = error ->
          {:halt, error}

        {hash, size} ->
          case Map.fetch(refs, hash) do
            {:ok, ^size} -> {:cont, {:ok, refs}}
            {:ok, _} -> {:halt, {:error, :conflicting_blob_sizes}}
            :error -> {:cont, {:ok, Map.put(refs, hash, size)}}
          end
      end
    end)
  end

  defp reference(key, value) when is_binary(key) and is_map(value) do
    cond do
      String.starts_with?(key, ["obj:", "obj_ver:", "ess:v2:object_version:"]) and
          not Map.get(value, :is_delete_marker, false) ->
        ref(Map.get(value, :content_hash), Map.get(value, :size))

      String.starts_with?(key, "ess:v2:multipart_part:") ->
        ref(Map.get(value, :hash), Map.get(value, :size))

      String.starts_with?(key, "ess:v2:operation_intent:") and
          Map.get(value, :state) in [:pending, :unknown] ->
        ref(Map.get(value, :hash), Map.get(value, :size))

      String.starts_with?(key, "ess:v2:job:") and
          Map.get(value, :state) in [:pending, :running] ->
        payload = Map.get(value, :payload, %{})
        hash = Map.get(payload, :hash) || Map.get(payload, :content_hash)
        if hash, do: ref(hash, Map.get(payload, :size)), else: nil

      String.starts_with?(key, "ess:gateway-write:") ->
        ref(Map.get(value, :hash), Map.get(value, :size))

      true ->
        nil
    end
  end

  defp reference(_, _), do: nil

  defp ref("sha256:" <> hash, size), do: ref(hash, size)

  defp ref(hash, size) when is_binary(hash) and is_integer(size) and size >= 0 do
    if Regex.match?(@hash, hash), do: {hash, size}, else: {:error, :invalid_blob_reference}
  end

  defp ref(nil, _), do: nil
  defp ref(_, _), do: {:error, :invalid_blob_reference}

  def validate_boundary(entries, mode) do
    each(entries, fn {key, value} ->
      cond do
        String.starts_with?(key, "ess:v2:list_index:") and String.ends_with?(key, ":root") and
          is_map(value) and Map.get(value, :state) not in [:ready, "ready"] ->
          {:error, :listing_index_not_quiesced}

        mode in [:cluster, "cluster"] and String.starts_with?(key, "blob:sha256:") and
          is_map(value) and Map.get(value, :state) == :packed ->
          {:error, :packed_cluster_restore_unsupported}

        true ->
          :ok
      end
    end)
  end

  def verify(root, master_key) do
    with {:ok, manifest_path} <- safe_path(root, "manifest.json"),
         :ok <- regular(manifest_path),
         {:ok, encoded} <- File.read(manifest_path),
         {:ok, digest_path} <- safe_path(root, "manifest.sha256"),
         :ok <- regular(digest_path),
         {:ok, expected} <- File.read(digest_path),
         true <- hex(:crypto.hash(:sha256, encoded)) == String.trim(expected),
         {:ok, manifest} <- JSON.decode(encoded),
         true <- manifest["format"] == @format,
         :ok <- key_matches(manifest, master_key),
         :ok <- verify_artifacts(root, manifest["artifacts"]),
         {:ok, metadata} <- safe_path(root, manifest["metadata"]),
         true <- Enum.any?(manifest["artifacts"], &(&1["path"] == manifest["metadata"])),
         {:ok, _snapshot, entries} <- read_snapshot(metadata),
         :ok <- validate_boundary(entries, manifest["mode"]),
         {:ok, refs} <- references(entries),
         true <- manifest["references"] == refs,
         :ok <- verify_references(root, manifest, entries, refs),
         :ok <- verify_journals(manifest, entries) do
      {:ok, manifest, entries}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_backup_manifest}
    end
  rescue
    _ -> {:error, :invalid_backup_manifest}
  end

  def key_matches(manifest, key) when is_binary(key) and byte_size(key) > 0 do
    if manifest["master_key_sha256"] == hex(:crypto.hash(:sha256, key)),
      do: :ok,
      else: {:error, :master_key_mismatch}
  end

  def key_matches(_, _), do: {:error, :master_key_required}

  defp verify_artifacts(root, artifacts) when is_list(artifacts) do
    paths = Enum.map(artifacts, & &1["path"])

    if length(paths) == MapSet.size(MapSet.new(paths)) do
      each(artifacts, fn artifact ->
        with {:ok, path} <- safe_path(root, artifact["path"]),
             {:ok, hash, size} <- digest(path),
             true <- hash == artifact["sha256"] and size == artifact["size"] do
          :ok
        else
          {:error, _} = error -> error
          _ -> {:error, :artifact_checksum_mismatch}
        end
      end)
    else
      {:error, :duplicate_artifact}
    end
  end

  defp verify_artifacts(_, _), do: {:error, :invalid_artifact_inventory}

  defp verify_journals(manifest, entries) do
    each(entries, fn {key, value} ->
      if is_binary(key) and String.starts_with?(key, "ess:gateway-write:") do
        if Enum.any?(manifest["artifacts"], fn artifact ->
             artifact["kind"] == "journal" and artifact["source_path"] == value.path and
               artifact["sha256"] == value.hash and artifact["size"] == value.size
           end),
           do: :ok,
           else: {:error, :journal_artifact_missing}
      else
        :ok
      end
    end)
  end

  def verify_references(root, manifest, entries, refs) do
    packed = Map.new(for {"blob:sha256:" <> hash, value} <- entries, do: {hash, value})

    each(refs, fn {hash, size} ->
      case Map.get(packed, hash) do
        %{state: :packed, pack: %{hash: pack_hash, offset: offset}} ->
          verify_packed(root, manifest, entries, hash, size, pack_hash, offset)

        _ ->
          if Enum.any?(manifest["artifacts"], fn a ->
               a["sha256"] == hash and a["size"] == size and
                 a["kind"] in ["blob", "tmp", "journal", "legacy"]
             end),
             do: :ok,
             else: {:error, {:referenced_blob_missing, hash}}
      end
    end)
  end

  defp verify_packed(root, manifest, entries, hash, size, pack_hash, offset) do
    pack = Enum.find(manifest["artifacts"], &(&1["sha256"] == pack_hash and &1["kind"] == "pack"))

    with true <- is_map(pack) and is_integer(offset) and offset >= 0,
         true <- offset + size <= pack["size"],
         {:ok, path} <- safe_path(root, pack["path"]),
         sidecar when is_map(sidecar) <-
           Enum.find(manifest["artifacts"], &(&1["path"] == pack["path"] <> ".idx")),
         {:ok, index_path} <- safe_path(root, sidecar["path"]),
         {:ok, bytes} <- File.read(index_path),
         {:ok, %{"format" => "ess-pack-v1", "hash" => ^pack_hash, "entries" => index}} <-
           JSON.decode(bytes),
         true <- [hash, offset, size] in index,
         {_, %{entries: metadata_index}} <-
           Enum.find(entries, fn {key, _} -> key == "pack:" <> pack_hash end),
         true <-
           Enum.map(metadata_index, fn entry -> [entry.blob_hash, entry.offset, entry.size] end) ==
             index,
         {:ok, ^hash, ^size} <- digest(path, offset, size) do
      :ok
    else
      _ -> {:error, {:invalid_packed_blob, hash}}
    end
  end

  def each(enumerable, fun) do
    Enum.reduce_while(enumerable, :ok, fn item, :ok ->
      case fun.(item) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  def hex(bytes), do: Base.encode16(bytes, case: :lower)
  def random_id, do: hex(:crypto.strong_rand_bytes(12))
end
