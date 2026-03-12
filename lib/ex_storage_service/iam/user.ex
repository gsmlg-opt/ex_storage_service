defmodule ExStorageService.IAM.User do
  @moduledoc """
  IAM User management backed by Concord key-value store.

  Users are stored with the key pattern: "user:{user_id}"
  """

  @type status :: :active | :suspended

  @type t :: %{
          id: String.t(),
          name: String.t(),
          status: status(),
          created_at: String.t(),
          updated_at: String.t()
        }

  @doc """
  Creates a new IAM user with the given name.
  Generates a unique user ID in the format "usr_xxxx".
  """
  @spec create_user(String.t()) :: {:ok, t()} | {:error, term()}
  def create_user(name) do
    user_id = generate_user_id()
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    user = %{
      id: user_id,
      name: name,
      status: :active,
      created_at: now,
      updated_at: now
    }

    case Concord.put("user:#{user_id}", user) do
      :ok -> {:ok, user}
      error -> error
    end
  end

  @doc """
  Retrieves a user by their user ID.
  """
  @spec get_user(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def get_user(user_id) do
    case Concord.get("user:#{user_id}") do
      {:ok, nil} -> {:error, :not_found}
      {:ok, user} -> {:ok, user}
      error -> error
    end
  end

  @doc """
  Lists all IAM users.
  """
  @spec list_users() :: {:ok, [t()]} | {:error, term()}
  def list_users do
    case Concord.get_all() do
      {:ok, all} ->
        users =
          all
          |> Enum.filter(fn {k, _v} -> String.starts_with?(k, "user:") end)
          |> Enum.map(fn {_k, v} -> v end)

        {:ok, users}

      error ->
        error
    end
  end

  @doc """
  Suspends a user, preventing them from authenticating.
  """
  @spec suspend_user(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def suspend_user(user_id) do
    update_status(user_id, :suspended)
  end

  @doc """
  Activates a suspended user.
  """
  @spec activate_user(String.t()) :: {:ok, t()} | {:error, :not_found | term()}
  def activate_user(user_id) do
    update_status(user_id, :active)
  end

  @doc """
  Deletes a user by their user ID.
  """
  @spec delete_user(String.t()) :: :ok | {:error, :not_found | term()}
  def delete_user(user_id) do
    case get_user(user_id) do
      {:ok, _user} -> Concord.delete("user:#{user_id}")
      error -> error
    end
  end

  # Private helpers

  defp update_status(user_id, new_status) do
    case get_user(user_id) do
      {:ok, user} ->
        now = DateTime.utc_now() |> DateTime.to_iso8601()
        updated = %{user | status: new_status, updated_at: now}

        case Concord.put("user:#{user_id}", updated) do
          :ok -> {:ok, updated}
          error -> error
        end

      error ->
        error
    end
  end

  defp generate_user_id do
    suffix =
      :crypto.strong_rand_bytes(8)
      |> Base.encode32(case: :lower, padding: false)
      |> String.slice(0, 12)

    "usr_#{suffix}"
  end
end
