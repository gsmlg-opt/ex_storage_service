alias ExStorageService.IAM.AccessKey
alias ExStorageService.IAM.Policy
alias ExStorageService.IAM.User

Application.ensure_all_started(:ex_storage_service)

defmodule E2ESeed do
  @moduledoc false

  def run do
    {:ok, rw_user} = User.create_user(unique_name("e2e-rw"))
    {:ok, ro_user} = User.create_user(unique_name("e2e-readonly"))

    {:ok, rw_policy} =
      Policy.create_policy(unique_name("e2e-full-access"), Policy.full_access_statements())

    {:ok, ro_policy} =
      Policy.create_policy(unique_name("e2e-readonly"), Policy.read_only_statements())

    :ok = Policy.attach_policy(rw_user.id, rw_policy.id)
    :ok = Policy.attach_policy(ro_user.id, ro_policy.id)

    {:ok, rw_key} = AccessKey.create_access_key(rw_user.id)
    {:ok, ro_key} = AccessKey.create_access_key(ro_user.id)

    env = %{
      "E2E_ACCESS_KEY_ID" => rw_key.access_key_id,
      "E2E_SECRET_ACCESS_KEY" => rw_key.secret_access_key,
      "E2E_READONLY_ACCESS_KEY_ID" => ro_key.access_key_id,
      "E2E_READONLY_SECRET_ACCESS_KEY" => ro_key.secret_access_key
    }

    write_env(env)
    mask_secrets(env)
    assert_seeded!([rw_key.access_key_id, ro_key.access_key_id])
    stop_storage_apps()

    IO.puts("Seeded e2e IAM users #{rw_user.id} and #{ro_user.id}")
  end

  defp unique_name(prefix) do
    suffix =
      :crypto.strong_rand_bytes(6)
      |> Base.encode32(case: :lower, padding: false)
      |> String.slice(0, 10)

    "#{prefix}-#{suffix}"
  end

  defp write_env(env) do
    case System.get_env("E2E_GITHUB_ENV") do
      nil ->
        Enum.each(env, fn {name, value} -> IO.puts("export #{name}=#{shell_escape(value)}") end)

      path ->
        body =
          env
          |> Enum.map(fn {name, value} -> "#{name}=#{value}\n" end)
          |> Enum.join()

        File.write!(path, body, [:append])
    end
  end

  defp mask_secrets(env) do
    env
    |> Enum.filter(fn {name, _value} -> String.contains?(name, "SECRET") end)
    |> Enum.each(fn {_name, value} -> IO.puts("::add-mask::#{value}") end)
  end

  defp assert_seeded!(access_key_ids) do
    Enum.each(access_key_ids, fn access_key_id ->
      {:ok, _key} = AccessKey.get_access_key(access_key_id)
    end)

    Process.sleep(1_000)
  end

  defp stop_storage_apps do
    Enum.each([:ex_storage_service, :concord, :ra], fn app ->
      Application.stop(app)
    end)
  end

  defp shell_escape(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end
end

E2ESeed.run()
