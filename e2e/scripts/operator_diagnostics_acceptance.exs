# Disposable actual-Concord proof of the documented failed-job lookup recipe.
root = System.fetch_env!("ESS_DATA_ROOT")
true = Regex.match?(~r|^/tmp/ess-hardening-operator-[A-Za-z0-9_-]+$|, root)

false =
  ExStorageService.InstanceConfig.from_application_env() |> elem(1) |> Map.fetch!(:auto_start)

alias ExStorageService.Metadata.{JobStore, Keys}
alias ExStorageService.Metadata.Models.Job

now = System.system_time(:millisecond)
id = "operator-diagnostic-" <> Integer.to_string(System.unique_integer([:positive]))
secret_fixture = "private-fixture-payload-never-print"

{:ok, job} =
  Job.new(id, %{id: id, kind: :scrub, payload: %{private: secret_fixture}}, now, max_attempts: 1)

:ok = Concord.put(Keys.job(id), Map.from_struct(job))
{:ok, claimed} = JobStore.claim(id, "operator-fixture", now, 60_000, owner_generation: 1)

{:ok, %{state: :failed}} =
  JobStore.fail(id, "operator-fixture", claimed.fencing_token, secret_fixture, now + 1,
    owner_generation: 1
  )

# Same allowlist and result handling as the release-RPC operator documentation.
{:ok, failed} = JobStore.get(id, timeout: 1000)

summary =
  Map.take(failed, [
    :job_id,
    :kind,
    :state,
    :attempts,
    :max_attempts,
    :fencing_token,
    :lease_until_ms,
    :next_attempt_at_ms
  ])

true = summary.state == :failed and summary.attempts == 1 and summary.fencing_token == 1
false = inspect(summary) =~ secret_fixture
false = Map.has_key?(summary, :payload)
false = Map.has_key?(summary, :last_error)
{:error, :not_found} = JobStore.get(id <> "-absent", timeout: 1000)

IO.puts(
  JSON.encode!(%{failed_job: summary, absent_job: :not_found, private_fields_omitted: true})
)
