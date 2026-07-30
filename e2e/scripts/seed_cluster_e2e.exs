target = :"ess_data_a@127.0.0.1"
deadline = System.monotonic_time(:millisecond) + 30_000

connect = fn connect ->
  if Node.connect(target) do
    :ok
  else
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(250)
      connect.(connect)
    else
      raise "could not connect to #{inspect(target)}"
    end
  end
end

:ok = connect.(connect)

source = """
alias ExStorageService.IAM.AccessKey
alias ExStorageService.IAM.Policy
alias ExStorageService.IAM.User

suffix = System.unique_integer([:positive, :monotonic])
{:ok, user} = User.create_user("cluster-e2e-\#{suffix}")
{:ok, policy} = Policy.create_policy("cluster-e2e-full-\#{suffix}", Policy.full_access_statements())
:ok = Policy.attach_policy(user.id, policy.id)
{:ok, key} = AccessKey.create_access_key(user.id)
%{access_key_id: key.access_key_id, secret_access_key: key.secret_access_key}
"""

credentials =
  case :rpc.call(target, Code, :eval_string, [source], 30_000) do
    {%{} = result, _binding} -> result
    {:badrpc, reason} -> raise "cluster credential seed RPC failed: #{inspect(reason)}"
    _other -> raise "cluster credential seed returned an unexpected result"
  end

env = %{
  "E2E_ACCESS_KEY_ID" => credentials.access_key_id,
  "E2E_SECRET_ACCESS_KEY" => credentials.secret_access_key
}

path =
  System.get_env("E2E_GITHUB_ENV") ||
    raise "E2E_GITHUB_ENV is required; credentials are written only to that file"

File.write!(path, Enum.map_join(env, "", fn {name, value} -> "#{name}=#{value}\n" end), [
  :append
])

File.chmod!(path, 0o600)

if System.get_env("GITHUB_ACTIONS") == "true" do
  IO.puts("::add-mask::#{credentials.secret_access_key}")
end
