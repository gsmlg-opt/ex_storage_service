# Disposable standalone durable-dispatch acceptance. Never print credentials.
[action, request_path, result_path] = System.argv()
target = String.to_atom(System.fetch_env!("E2E_GATEWAY_CONTROL_TARGET"))
request = JSON.decode!(File.read!(request_path))
root = Path.expand(request["root"])
true = Regex.match?(~r|^/tmp/ess-hardening-backlog-[A-Za-z0-9_-]+$|, root)
true = Path.dirname(Path.expand(request_path)) == root
true = Path.dirname(Path.expand(result_path)) == root
true = Node.connect(target)

source = """
alias ExStorageService.{Context, Metadata}
alias ExStorageService.Metadata.JobStore
request = #{inspect(request)}
{:ok, context} = Context.default()
true = context.config.mode == :standalone
true = context.data_root == Path.join(request["root"], "data")
true = context.blob_root == Path.join([request["root"], "data", "blob"])
case #{inspect(action)} do
  "seed" ->
    alias ExStorageService.IAM.{AccessKey, Policy, User}
    {:ok, user} = User.create_user("backlog-acceptance")
    {:ok, policy} = Policy.create_policy("backlog-acceptance", Policy.full_access_statements())
    :ok = Policy.attach_policy(user.id, policy.id)
    {:ok, key} = AccessKey.create_access_key(user.id)
    %{access_key: key.access_key_id, secret_key: key.secret_access_key}
  "configure" ->
    true = Regex.match?(~r|^http://127\\.0\\.0\\.1:[0-9]+$|, request["endpoint"])
    true = request["bucket"] == "backlog-acceptance"
    :ok = ExStorageService.CrossClusterReplication.Config.set_bucket_replicas(request["bucket"],
      [%{endpoint: request["endpoint"], bucket: "remote", access_key: request["token"]}])
    %{configured: true}
  "status" ->
    {:ok, page} = JobStore.list_page(nil, 100)
    true = is_nil(page.next_cursor)
    jobs = Enum.filter(page.jobs, &(&1.kind in [:cross_cluster_put, :cross_cluster_delete]))
    {:ok, backlog} = ExStorageService.CrossClusterReplication.Status.snapshot(page_size: 100, max_pages: 1)
    true = backlog.complete
    %{jobs: Enum.map(jobs, fn job -> %{id: job.job_id, state: to_string(job.state),
      attempts: job.attempts, fencing_token: job.fencing_token,
      owner_generation: job.owner_generation, created_at_ms: job.created_at_ms} end),
      backlog: backlog, metrics: ExStorageService.Metrics.format_metrics()}
end
"""

result =
  case :rpc.call(target, Code, :eval_string, [source], 30_000) do
    {:badrpc, reason} -> raise "backlog RPC failed: #{inspect(reason)}"
    {result, binding} when is_list(binding) -> result
  end

File.write!(result_path, JSON.encode!(result), [:exclusive])
File.chmod!(result_path, 0o600)
IO.puts("replication backlog control #{action}: ok")
