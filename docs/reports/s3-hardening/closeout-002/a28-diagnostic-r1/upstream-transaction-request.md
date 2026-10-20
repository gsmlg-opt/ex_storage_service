**Requesting repo:** gsmlg-opt/ex_storage_service on branch `main`

**Severity:** blocker

Coordinated Concord/VSR/ExTurso **3.1.4** improves per-write materialization from #99 for direct puts/deletes, but ordinary atomic `{:txn, specification}` commands still return `changes: :all`. ESS ObjectCommit publishes object/head/version/index/outcome records atomically with `Concord.Txn.commit/2`; splitting those mutations would violate ESS correctness. The required 10k/100k setup therefore still takes the full compatibility-ETS rebuild and observer diff on every object transaction. This is an unhandled command class, separate from retained-log read traversal and current-command representation admission.

Minimal released-3.1.4 reproduction, with no applications or materialized ETS started:

```elixir
alias Concord.StateMachine.Core
alias Concord.StateMachine.Core.Context
context = %Context{op_number: 1, timestamp_ms: 1_761_000_000_000}
initial = Core.init()
{:ok, direct, direct_changes} =
  Core.apply_with_changes(0, context, {:put, "target", "value"}, initial)
spec = %{compare: [], success: [{:put, "target", "value", %{}}], failure: []}
{{:ok, %{succeeded: true}}, transaction, transaction_changes} =
  Core.apply_with_changes(0, context, {:txn, spec}, initial)
true = direct.store == transaction.store
IO.inspect(direct_changes)      # [current: "target", store: "target"]
IO.inspect(transaction_changes) # :all
```

This runs successfully against the published 3.1.4 modules and retains the exact same store result. `Core.incremental_command?/1` recognizes put/delete/put_if/delete_if/touch but excludes `{:txn, _}`. `Core.apply_with_changes/4` therefore takes its fallback branch, `StateMachine.materialize(state, :all)` invokes full `materialize/1`, and `Observer.changed_keys(:all, previous, current)` builds the full current-key union.

Independent actual ESS ObjectCommit production-path diagnostic (normal indexed commits, singleton file-backed Concord, command version `0`, no dependency patches or forced snapshots):

| Target writes in stage | 1-100 | 101-500 | 501-1000 |
|---|---:|---:|---:|
| Number of transactions | 100 | 400 | 500 |
| Stage wall us | 880,053 | 5,213,951 | 12,708,278 |
| `ListIndex.augment/4` native call_time us | 15,192 | 47,857 | 76,564 |
| `StateMachine.materialize/2` native call_time us | 52,958 | 1,269,918 | 4,682,216 |
| `Observer.committed/6` native call_time us | 18,744 | 576,662 | 2,314,231 |
| File.persist/2 native call_time us | 692,457 | 2,700,965 | 3,997,366 |

The larger stages contain different numbers of operations; per-write comparisons must account for that. Selected OTP call_time profiling is intrusive; nested function rows must not be blindly summed as independent wall-time shares. No checkpoint calls and no compare conflicts were observed in these stages. All resulting index links, generation/counter invariants, version removal restoring its predecessor and delete marker selection passed independently. Existing DB command format was preserved; current-version admission is not removed or bypassed.

Expected behavior: track exactly the affected rows for supported atomic transactions, including the chosen branch and idempotent/outcome metadata, while preserving compare atomicity, legacy replay, error/rollback semantics and recovery. Retain full rebuild only for operations that genuinely require it. ESS cannot replace atomic object/index publication with separate writes or an asynchronous local workaround.

This blocks ESS A28 scale closeout; no unmeasured 100k timing or causal attribution to the historical 10,800-second timeout is claimed. Toolchain Elixir 1.18.5/OTP 28, `+S 4:4 +SDcpu 2 +SDio 2`. Actual 2,000 normal target/unrelated object commits plus version/marker checks completed in 64.03 seconds, with source unchanged across the run. No server application remains running.

ESS evidence: `docs/reports/s3-hardening/closeout-002/a28-diagnostic-r1/{execution,probe,upstream-minimal}.json`; production-path `e2e/scripts/index_setup_probe.exs`; minimal `e2e/scripts/index_upstream_probe.exs`. Starting HEAD `b17282d96a58eff962a35d807c9e7adb39af4774`; production diagnostic source fingerprint `20351587f71da6b8ba72d53bf9e46af149a33c4c767049c06e03a765b2104350`.
