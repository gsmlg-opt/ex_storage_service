alias ExStorageService.IAM.{User, AccessKey, Policy}
{:ok, user} = User.create_user("hardening-#{System.unique_integer([:positive])}")
{:ok, policy} = Policy.create_policy("hardening-full", Policy.full_access_statements())
:ok = Policy.attach_policy(user.id, policy.id)
{:ok, key} = AccessKey.create_access_key(user.id)
path = System.fetch_env!("E2E_GITHUB_ENV")
File.write!(path, "E2E_ACCESS_KEY_ID=#{key.access_key_id}\nE2E_SECRET_ACCESS_KEY=#{key.secret_access_key}\n")
File.chmod!(path, 0o600)
IO.puts("Disposable hardening identity seeded")
