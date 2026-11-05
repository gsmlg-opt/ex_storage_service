defmodule ExStorageService.Metadata.GatewayIndex do
  @moduledoc """
  Verifies the selected upstream-owned object during offline index conversion.

  Historical gateway records describe retained local metadata, not hosted CAS
  replicas or an upstream version API. Only the selected current body is read;
  a selected delete marker instead requires upstream absence. Verification stages
  bounded raw bytes without publishing CAS locations or filling the cache.
  """

  alias ExStorageService.BlobStore.LocalCAS
  alias ExStorageService.CloudCache.{Client, Config}

  defmodule PrivateFileSystem do
    @moduledoc false
    alias ExStorageService.BlobStore.LocalCAS.FileSystem
    defdelegate mkdir_p(path), to: FileSystem
    defdelegate write(io, data), to: FileSystem
    defdelegate sync(io), to: FileSystem
    defdelegate close(io), to: FileSystem
    defdelegate rm(path), to: FileSystem
    defdelegate stat(path), to: FileSystem
    defdelegate lstat(path), to: FileSystem

    def open(path, modes) do
      with {:ok, io} <- FileSystem.open(path, modes) do
        case File.chmod(path, 0o600) do
          :ok ->
            {:ok, io}

          {:error, reason} ->
            FileSystem.close(io)
            FileSystem.rm(path)
            {:error, reason}
        end
      end
    end
  end

  @doc false
  def validate_version(version) when is_map(version) do
    valid =
      field(version, :schema) == 2 and field(version, :cloud_backed) == true and
        is_binary(field(version, :bucket)) and field(version, :bucket) != "" and
        is_binary(field(version, :key)) and field(version, :key) != "" and
        is_binary(field(version, :version_id)) and field(version, :version_id) != ""

    cond do
      not valid ->
        {:error, :invalid_gateway_object_identity}

      marker?(version) ->
        :ok

      valid_hash?(field(version, :content_hash)) and is_integer(field(version, :size)) and
          field(version, :size) >= 0 ->
        :ok

      true ->
        {:error, :invalid_gateway_blob_identity}
    end
  end

  def validate_version(_), do: {:error, :invalid_gateway_object_identity}

  @doc false
  def validate_configuration(record) do
    case configuration(record) do
      {:ok, _config} -> :ok
      {:error, reason} -> error(reason)
    end
  end

  @doc "Checks one selected cloud head against its frozen gateway configuration."
  def verify_current(bucket, metadata, config_record, opts \\ []) do
    with true <-
           field(metadata, :bucket) == bucket and is_binary(field(metadata, :key)) and
             field(metadata, :cloud_backed) == true,
         {:ok, config} <- configuration(config_record) do
      client = Keyword.get(opts, :gateway_client, Client)
      verify(client, config, metadata, opts)
    else
      false -> error(:invalid_object_identity)
      {:error, reason} -> error(reason)
    end
  rescue
    _ -> error(:verification_unavailable)
  catch
    _, _ -> error(:verification_unavailable)
  end

  defp verify(client, config, metadata, opts) do
    if marker?(metadata) do
      case client.head_object(config, field(metadata, :key)) do
        {:error, :not_found} -> :ok
        {:ok, _} -> error(:upstream_present)
        _ -> error(:upstream_unavailable)
      end
    else
      with :ok <- validate_version(metadata),
           {:ok, head} <- client.head_object(config, field(metadata, :key)),
           :ok <- head_identity(head, metadata) do
        verify_body(client, config, metadata, head, opts)
      else
        {:error, :not_found} -> error(:upstream_missing)
        {:error, reason} when is_atom(reason) -> error(reason)
        _ -> error(:upstream_unavailable)
      end
    end
  end

  defp verify_body(client, config, metadata, head, opts) do
    tmp = Keyword.get(opts, :gateway_tmp_dir, System.tmp_dir!())

    download = [
      if_match: "\"#{head.etag}\"",
      max_size: field(metadata, :size),
      tmp_dir: tmp,
      fs_module: PrivateFileSystem
    ]

    case client.get_file(config, field(metadata, :key), download) do
      {:ok, staged, received} ->
        result =
          try do
            cond do
              staged.hash != field(metadata, :content_hash) or
                  staged.size != field(metadata, :size) ->
                error(:content_mismatch)

              head.etag != Map.get(received, :etag) or
                  Map.get(head, :version_id) != Map.get(received, :version_id) ->
                error(:upstream_changed)

              true ->
                :ok
            end
          rescue
            _ -> error(:verification_unavailable)
          catch
            _, _ -> error(:verification_unavailable)
          end

        case LocalCAS.discard(staged, fs_module: PrivateFileSystem) do
          :ok -> result
          _ -> error(:verification_cleanup_failed)
        end

      {:error, :not_found} ->
        error(:upstream_changed)

      {:error, {:http_error, 412}} ->
        error(:upstream_changed)

      _ ->
        error(:upstream_unavailable)
    end
  end

  defp head_identity(head, metadata) when is_map(head) do
    etag = Map.get(head, :etag)

    valid =
      is_binary(etag) and etag != "" and not String.starts_with?(etag, "W/") and
        not String.contains?(etag, ["\"", "\r", "\n"]) and
        Map.get(head, :content_length) == field(metadata, :size) and
        valid_remote_version?(Map.get(head, :version_id))

    cond do
      not valid ->
        {:error, :invalid_upstream_identity}

      not stored_identity?(metadata, :upstream_etag, etag) or
          not stored_identity?(metadata, :upstream_version_id, Map.get(head, :version_id)) ->
        {:error, :stored_upstream_identity_changed}

      true ->
        :ok
    end
  end

  defp head_identity(_, _), do: {:error, :invalid_upstream_identity}

  defp stored_identity?(metadata, key, actual) do
    if Map.has_key?(metadata, key) or Map.has_key?(metadata, Atom.to_string(key)) do
      expected = field(metadata, key)
      is_binary(expected) and expected != "" and expected == actual
    else
      true
    end
  end

  defp configuration(%{value: stored}) when is_map(stored) do
    config = struct(Config, stored)
    uri = URI.parse(Config.endpoint_url(config))

    valid =
      config.enabled == true and config.provider in [:aws, :r2, :minio, :s3_compat] and
        is_binary(config.bucket) and config.bucket != "" and
        is_binary(config.region) and config.region != "" and
        is_binary(config.access_key_id) and config.access_key_id != "" and
        uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" and
        is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment) and
        Config.plaintext_secret(config) != ""

    if valid, do: {:ok, config}, else: {:error, :gateway_configuration_unavailable}
  rescue
    _ -> {:error, :gateway_configuration_unavailable}
  end

  defp configuration(_), do: {:error, :gateway_configuration_unavailable}

  defp marker?(value),
    do: field(value, :delete_marker, field(value, :is_delete_marker, false)) == true

  defp valid_remote_version?(nil), do: true

  defp valid_remote_version?(value) when is_binary(value),
    do: value != "" and not String.contains?(value, ["\r", "\n"])

  defp valid_remote_version?(_), do: false

  defp valid_hash?(value) when is_binary(value) and byte_size(value) == 64,
    do: Regex.match?(~r/\A[0-9a-f]{64}\z/, value)

  defp valid_hash?(_), do: false
  defp field(value, key, default \\ nil)

  defp field(value, key, default) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  defp field(_, _, default), do: default
  defp error(reason), do: {:error, {:gateway_index_verification_failed, reason}}
end
