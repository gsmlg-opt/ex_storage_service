defmodule ExStorageService.CloudCache.Config do
  @moduledoc """
  Per-bucket cloud cache configuration backed by Concord KV.

  When enabled, a bucket acts as a gateway to a remote S3-compatible store
  (AWS S3 or Cloudflare R2). Writes go directly to the remote; reads are
  served from a local LRU disk cache that is populated on demand.

  Configuration is stored under `"cloud_cache:{bucket}"` in Concord.
  The secret access key is AES-256-CTR encrypted using the same
  `ESS_MASTER_KEY` as IAM secrets.
  """

  @type provider :: :aws | :r2

  @type t :: %__MODULE__{
          enabled: boolean(),
          provider: provider(),
          endpoint: String.t() | nil,
          region: String.t(),
          bucket: String.t(),
          access_key_id: String.t(),
          encrypted_secret: String.t(),
          cache_max_bytes: non_neg_integer(),
          cache_enabled: boolean()
        }

  defstruct enabled: false,
            provider: :aws,
            endpoint: nil,
            region: "us-east-1",
            bucket: "",
            access_key_id: "",
            encrypted_secret: "",
            cache_max_bytes: 10_737_418_240,
            cache_enabled: true

  @doc """
  Get cloud cache config for a bucket.

  Returns `{:ok, %Config{}}` if configured, `{:error, :not_found}` otherwise.
  """
  @spec get_config(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def get_config(bucket) do
    case Concord.get("cloud_cache:#{bucket}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, stored} -> {:ok, from_map(stored)}
      error -> error
    end
  end

  @doc """
  Set (create or update) cloud cache config for a bucket.

  `params` is a map or keyword list with the config fields.
  If `secret_access_key` is provided in plaintext, it will be encrypted.
  """
  @spec set_config(String.t(), map() | keyword()) :: :ok | {:error, term()}
  def set_config(bucket, params) do
    params = normalize(params)

    # Encrypt the secret if given as plaintext
    {secret_field, encrypted} =
      cond do
        Map.has_key?(params, :secret_access_key) and params[:secret_access_key] != "" ->
          enc = encrypt_secret(params[:secret_access_key])
          {:encrypted_secret, enc}

        Map.has_key?(params, :encrypted_secret) ->
          {:encrypted_secret, params[:encrypted_secret]}

        true ->
          # Preserve existing encrypted secret on update
          existing_enc =
            case get_config(bucket) do
              {:ok, existing} -> existing.encrypted_secret
              _ -> ""
            end

          {:encrypted_secret, existing_enc}
      end

    stored =
      %{
        enabled: Map.get(params, :enabled, false),
        provider: normalize_provider(Map.get(params, :provider, :aws)),
        endpoint: Map.get(params, :endpoint),
        region: Map.get(params, :region, "us-east-1"),
        bucket: Map.get(params, :bucket, ""),
        access_key_id: Map.get(params, :access_key_id, ""),
        cache_max_bytes: Map.get(params, :cache_max_bytes, 10_737_418_240),
        cache_enabled: Map.get(params, :cache_enabled, true)
      }
      |> Map.put(secret_field, encrypted)

    case Concord.put("cloud_cache:#{bucket}", stored) do
      :ok -> :ok
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Delete cloud cache config for a bucket.
  """
  @spec delete_config(String.t()) :: :ok | {:error, term()}
  def delete_config(bucket) do
    case Concord.delete("cloud_cache:#{bucket}") do
      :ok -> :ok
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Check if cloud cache is enabled for a bucket.
  Returns the config if enabled, `:disabled` otherwise.
  """
  @spec get_active_config(String.t()) :: {:ok, t()} | :disabled
  def get_active_config(bucket) do
    case get_config(bucket) do
      {:ok, %__MODULE__{enabled: true} = config} -> {:ok, config}
      _ -> :disabled
    end
  end

  @doc """
  Derive the S3 endpoint URL from the config.

  For AWS: `https://s3.{region}.amazonaws.com`
  For R2: uses the `endpoint` field directly (account-specific)
  For custom: uses `endpoint` field directly
  """
  @spec endpoint_url(t()) :: String.t()
  def endpoint_url(%__MODULE__{provider: :aws, endpoint: nil, region: region}) do
    "https://s3.#{region}.amazonaws.com"
  end

  def endpoint_url(%__MODULE__{provider: :aws, endpoint: ep}) when is_binary(ep) and ep != "" do
    ep
  end

  def endpoint_url(%__MODULE__{endpoint: ep}) when is_binary(ep) and ep != "" do
    ep
  end

  def endpoint_url(%__MODULE__{}) do
    raise "Cloud cache endpoint is not configured"
  end

  @doc "Decrypt the stored secret and return the plaintext secret access key."
  @spec plaintext_secret(t()) :: String.t()
  def plaintext_secret(%__MODULE__{encrypted_secret: enc}) when is_binary(enc) and enc != "" do
    decrypt_secret(enc)
  end

  def plaintext_secret(_), do: ""

  @doc "Encrypt a plaintext secret using the master key."
  @spec encrypt_secret(String.t()) :: String.t()
  def encrypt_secret(plaintext) do
    key = master_key()
    iv = :crypto.strong_rand_bytes(16)
    ciphertext = :crypto.crypto_one_time(:aes_256_ctr, key, iv, plaintext, true)
    Base.encode64(iv <> ciphertext)
  end

  @doc "Decrypt an AES-256-CTR encrypted secret."
  @spec decrypt_secret(String.t()) :: String.t()
  def decrypt_secret(encrypted_b64) do
    key = master_key()

    case Base.decode64(encrypted_b64) do
      {:ok, <<iv::binary-16, ciphertext::binary>>} ->
        :crypto.crypto_one_time(:aes_256_ctr, key, iv, ciphertext, false)

      _ ->
        raise "Failed to decrypt cloud cache secret: invalid encrypted data"
    end
  end

  ## Private

  defp master_key do
    case Application.get_env(:ex_storage_service, :master_key) do
      nil ->
        raise "ESS_MASTER_KEY is not configured. Set :master_key in :ex_storage_service config."

      key_b64 when is_binary(key_b64) ->
        case Base.decode64(key_b64) do
          {:ok, key} when byte_size(key) >= 32 -> binary_part(key, 0, 32)
          {:ok, key} -> :crypto.hash(:sha256, key)
          :error -> :crypto.hash(:sha256, key_b64)
        end
    end
  end

  defp from_map(m) do
    %__MODULE__{
      enabled: map_get(m, :enabled, false),
      provider: normalize_provider(map_get(m, :provider, :aws)),
      endpoint: map_get(m, :endpoint),
      region: map_get(m, :region, "us-east-1"),
      bucket: map_get(m, :bucket, ""),
      access_key_id: map_get(m, :access_key_id, ""),
      encrypted_secret: map_get(m, :encrypted_secret, ""),
      cache_max_bytes: map_get(m, :cache_max_bytes, 10_737_418_240),
      cache_enabled: map_get(m, :cache_enabled, true)
    }
  end

  defp map_get(m, key, default \\ nil) do
    Map.get(m, key) || Map.get(m, to_string(key)) || default
  end

  defp normalize(params) when is_list(params), do: Map.new(params)
  defp normalize(params) when is_map(params), do: params

  defp normalize_provider(:aws), do: :aws
  defp normalize_provider(:r2), do: :r2
  defp normalize_provider("aws"), do: :aws
  defp normalize_provider("r2"), do: :r2
  defp normalize_provider(_), do: :aws
end
