defmodule ExStorageService.IAM.AccessKey do
  @moduledoc """
  IAM Access Key management backed by Concord key-value store.

  Access keys are stored with the key pattern: "access_key:{access_key_id}"
  The secret access key is encrypted at rest using AES-256-CTR with
  the master key from application config.
  """

  alias ExStorageService.IAM.User

  @type status :: :active | :inactive

  @type t :: %{
          access_key_id: String.t(),
          secret_access_key: String.t(),
          user_id: String.t(),
          status: status(),
          created_at: String.t()
        }

  @doc """
  Creates a new access key pair for the given user.
  Returns the access key with the plaintext secret (only time it's available).
  """
  @spec create_access_key(String.t()) :: {:ok, t()} | {:error, term()}
  def create_access_key(user_id) do
    case User.get_user(user_id) do
      {:ok, _user} ->
        access_key_id = generate_access_key_id()
        secret_access_key = generate_secret_access_key()
        now = DateTime.utc_now() |> DateTime.to_iso8601()

        encrypted_secret = encrypt_secret(secret_access_key)

        stored = %{
          access_key_id: access_key_id,
          encrypted_secret: encrypted_secret,
          user_id: user_id,
          status: :active,
          created_at: now
        }

        case Concord.put("access_key:#{access_key_id}", stored) do
          :ok ->
            {:ok,
             %{
               access_key_id: access_key_id,
               secret_access_key: secret_access_key,
               user_id: user_id,
               status: :active,
               created_at: now
             }}

          error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Retrieves an access key by its access key ID, with the secret decrypted.
  """
  @spec get_access_key(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def get_access_key(access_key_id) do
    case Concord.get("access_key:#{access_key_id}") do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, stored} ->
        secret = decrypt_secret(stored.encrypted_secret)

        {:ok,
         %{
           access_key_id: stored.access_key_id,
           secret_access_key: secret,
           user_id: stored.user_id,
           status: stored.status,
           created_at: stored.created_at
         }}

      error ->
        error
    end
  end

  @doc """
  Lists all access keys for a given user.
  Secrets are NOT included in the listing (masked).
  """
  @spec list_user_keys(String.t()) :: {:ok, [map()]} | {:error, term()}
  def list_user_keys(user_id) do
    case Concord.get_all() do
      {:ok, all} ->
        keys =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "access_key:") end)
          |> Enum.map(fn {_k, v} -> v end)
          |> Enum.filter(fn key -> key.user_id == user_id end)
          |> Enum.map(fn stored ->
            %{
              access_key_id: stored.access_key_id,
              user_id: stored.user_id,
              status: stored.status,
              created_at: stored.created_at
            }
          end)

        {:ok, keys}

      error ->
        error
    end
  end

  @doc """
  Activates an access key.
  """
  @spec activate_key(String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def activate_key(access_key_id) do
    update_key_status(access_key_id, :active)
  end

  @doc """
  Deactivates an access key.
  """
  @spec deactivate_key(String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def deactivate_key(access_key_id) do
    update_key_status(access_key_id, :inactive)
  end

  @doc """
  Deletes an access key.
  """
  @spec delete_key(String.t()) :: :ok | {:error, :not_found | term()}
  def delete_key(access_key_id) do
    case Concord.get("access_key:#{access_key_id}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, _} -> Concord.delete("access_key:#{access_key_id}")
      error -> error
    end
  end

  @doc """
  Looks up an access key by its ID and returns the key data with user_id.
  Used by the authentication plug to verify signatures.
  """
  @spec lookup_by_access_key_id(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def lookup_by_access_key_id(access_key_id) do
    get_access_key(access_key_id)
  end

  # Private helpers

  defp update_key_status(access_key_id, new_status) do
    case Concord.get("access_key:#{access_key_id}") do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, stored} ->
        updated = %{stored | status: new_status}

        case Concord.put("access_key:#{access_key_id}", updated) do
          :ok ->
            {:ok,
             %{
               access_key_id: updated.access_key_id,
               user_id: updated.user_id,
               status: updated.status,
               created_at: updated.created_at
             }}

          error ->
            error
        end

      error ->
        error
    end
  end

  defp generate_access_key_id do
    suffix =
      :crypto.strong_rand_bytes(12)
      |> Base.encode32(case: :upper, padding: false)
      |> String.slice(0, 16)

    "AKIA#{suffix}"
  end

  defp generate_secret_access_key do
    :crypto.strong_rand_bytes(30)
    |> Base.encode64()
    |> String.slice(0, 40)
  end

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

  defp encrypt_secret(plaintext) do
    key = master_key()
    iv = :crypto.strong_rand_bytes(16)
    ciphertext = :crypto.crypto_one_time(:aes_256_ctr, key, iv, plaintext, true)
    Base.encode64(iv <> ciphertext)
  end

  defp decrypt_secret(encrypted_b64) do
    key = master_key()

    case Base.decode64(encrypted_b64) do
      {:ok, <<iv::binary-16, ciphertext::binary>>} ->
        :crypto.crypto_one_time(:aes_256_ctr, key, iv, ciphertext, false)

      _ ->
        raise "Failed to decrypt secret: invalid encrypted data"
    end
  end
end
