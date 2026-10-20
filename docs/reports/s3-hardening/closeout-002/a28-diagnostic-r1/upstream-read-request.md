**Requesting repo:** gsmlg-opt/ex_storage_service on branch `main`

**Severity:** blocker

Coordinated Concord/VSR/ExTurso **3.1.4** fixes the singleton-read allocation reported in #100, but retained-log traversal still makes a fixed ESS listing proportional to unrelated writes. This is a separate remaining CPU/work boundary; the 3.1.4 changelog explicitly retains that traversal. ESS cannot certify its mandatory A28 unrelated-growth independence while it remains present. No full 10k/100k acceptance run is claimed, and this does not establish the cause of the historical 10,800-second timeout.

Actual isolated production ObjectCommit fixture: create 1,000 target objects normally, measure ten unchanged delimiter pages, then create 1,000 objects in a second bucket normally and repeat the same target page. All index, generation, visible/version counters and numbered-version/delete-marker invariants pass. Command version remains the existing-database default `0`; no format switch, forced snapshot, log mutation or validation bypass is used.

| Ten delimiter pages, max_keys=1 | Before unrelated writes | After unrelated writes |
|---|---:|---:|
| ESS exact-key backend reads | 100 | 100 |
| Returned exact-record serialized bytes | 26,620 | 26,620 |
| Retained applied log entries | 1,003 | 2,003 |
| `assert_applied_requests_are_unique!/1` checks | 100 | 100 |
| `applied_requests_indexed?/3` calls including one base case/check | 100,400 | 200,400 |
| That function's native call_time microseconds | 19,538 | 39,140 |
| Wall microseconds for the ten pages, including diagnostic overhead | 22,793 | 42,445 |

An absent-prefix page independently remains 50 ESS reads/11,600 returned bytes, while native recursive visits grow 50,200 -> 100,200. Native `materialize/1` and `Core.snapshot/1` have zero calls inside both measured read groups. The native log's full serialized inventory is 5,870,028 -> 11,865,907 bytes; that is offline inventory size, **not** a claim that all operation payload bytes are touched by identity validation. Silent OTP call_time profiling is intrusive and these timings are diagnostic, not acceptance latencies.

Minimal pure reproduction on the actual released 3.1.4 modules (no applications/storage/effects started):

```elixir
alias ViewstampedReplication.{Configuration, Log, LogEntry, Member, Protocol}
alias ViewstampedReplication.Protocol.State

for count <- [1_000, 2_000, 4_000] do
  configuration = Configuration.new!(group_id: :probe, replica_id: :replica,
    members: [%Member{id: :replica, endpoint: :pure_fixture}])
  entries = for n <- 1..count do
    %LogEntry{view_number: 0, op_number: n, client_id: :writer,
      request_number: n, operation: {:put, "constant", <<0>>}}
  end
  {state, _effects} = Protocol.step(State.new(configuration), {:storage_recovered, %{
    replica_id: :replica, log: %Log{base_op_number: 0, entries: entries},
    commit_number: count, applied_number: count,
    client_table: %{writer: %{request_number: count, status: :applied, result: :ok}},
    hard_state: %{view_number: 0, last_normal_view: 0, status: :normal}
  }})
  # Genuine recovery builds the new derived index; do not omit it and measure fallback.
  ^count = map_size(state.applied_request_index.writer)
  samples = for _ <- 1..30 do
    {:reductions, before} = Process.info(self(), :reductions)
    {^state, [{:read, :route, {:get, "constant"}}]} =
      Protocol.step(state, {:read_request, :route, {:get, "constant"}})
    {:reductions, after_read} = Process.info(self(), :reductions)
    after_read - before
  end
  IO.inspect({count, samples})
end
```

Executed source-fenced probe retaining exact results/effects and unchanged application set: 1k/2k/4k pure singleton-read p50 reductions **2,560/5,060/10,060**, p50 **17/44/91 us**, 30 samples each. Toolchain Elixir 1.18.5/OTP 28, `+S 4:4 +SDcpu 2 +SDio 2`. This isolates CPU scaling independently of ESS planning, disk synchronization and observer queues.

Expected behavior: provide a supported read protocol whose actual visited work is bounded independently of unrelated retained log growth, while retaining corruption, duplicate-request, committed-prefix, recovery and snapshot safety. Do not weaken assertions or require ESS to compact snapshots merely to conceal the steady-state dependency cost.

ESS evidence: `docs/reports/s3-hardening/closeout-002/a28-diagnostic-r1/{execution,probe,upstream-minimal}.json`; production-path script `e2e/scripts/index_setup_probe.exs`; standalone dependency reproduction `e2e/scripts/index_upstream_probe.exs`. Starting HEAD `b17282d96a58eff962a35d807c9e7adb39af4774`, uncommitted production-path diagnostic source fingerprint `20351587f71da6b8ba72d53bf9e46af149a33c4c767049c06e03a765b2104350` (unchanged across its 64.03-second run). Later minimal probe source is separately retained; none of this is full A28 acceptance.
