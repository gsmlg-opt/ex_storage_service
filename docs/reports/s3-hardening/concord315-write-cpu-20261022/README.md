# Concord/VSR 3.1.5 retained-history write CPU diagnostic

The published VSR 3.1.5 pure protocol performs increasing CPU work per write as
retained history grows. This bounded diagnostic separates that work from fsync,
network transport, Concord materialization and ESS listing-tree work. It does not
establish the cause of complete ESS latency or a failed acceptance criterion.
The authoritative 100k index workload was still running when this report was
prepared; no result or extrapolated completion time is claimed for it.

Requesting repository: `gsmlg-opt/ex_storage_service`, branch `main`, observed
HEAD `0f347902a2e5c0c649bcec4c14d904c5281a3bd6`. Both published packages'
`mix.exs` source links identify `gsmlg-dev/concord` as upstream, including its
`apps/viewstamped_replication` application. Upstream issue: [gsmlg-dev/concord#104](https://github.com/gsmlg-dev/concord/issues/104), severity **needed**.

## Measured result

The final command exited 0 in **0.771101 seconds**, below its 30-second bound.
Each size has 30 sequential completed writes in an untraced run and a separate
call-time-profiled run: 180 completed writes total. Recovery/setup and explicit
effect/guard checks are outside the measured `Protocol.step/2` windows.

| Retained entries at start | Untraced median submit + apply acknowledgement | Median reductions | Profiled index recursion calls, 30 writes | Profiled prefix recursion calls, 30 writes |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 483.5 microseconds | 27,898 | 60,960 | 30,465 |
| 2,000 | 1,229.5 microseconds | 58,537 | 120,960 | 60,465 |
| 4,000 | 2,484 microseconds | 115,543.5 | 240,960 | 120,465 |

Every profiled 30-write run recorded 120 complete invariant checks and 60
applied-request-index refreshes. Profiling affects elapsed time; the wall-time
column therefore uses the separate untraced runs. Reductions and recursion counts
provide additional evidence independent of shared-host scheduling noise. These
small-sample medians are diagnostics, not production latency guarantees.

## Source mechanism and boundaries

Published `ViewstampedReplication.Protocol.step/2` validates input state and the
resulting transition. Ordinary singleton writes take two steps: client request
and successful state-machine acknowledgement. They retain the following work:

- `Log.append/2` obtains the last operation through `List.last/1`, then copies
  the forward list using `entries ++ [entry]`.
- Invariant checks walk contiguous retained entries and validate applied-request
  uniqueness against the derived index.
- Changed-log transitions compare the retained committed prefix.
- `refresh_applied_request_index/2` rebuilds nested maps from all retained
  applied entries after both append and acknowledgement; no incremental update
  is used for these two ordinary write transitions.
- Apply-start and acknowledgement each fetch the newest operation through a
  forward-list `Enum.at/2`.

The implementation therefore performs repeated retained-history scans and
derived-map construction per write. Individual persistent `Map.put/3` calls are
not described as copying the entire application state.

This is distinct from the released #101 singleton-read runtime fast path and
#102 incremental atomic-transaction materialization repairs. File-backed VSR also
has synchronous WAL writes; they were not invoked or timed here. A separate source
finding, not measured here, is Concord's keyed-transaction result-cache pruning
(`cache_txn_request/5`: `Enum.reject/2` then `Map.new/1`, bounded by its retention
policy). ESS supplies transaction idempotency keys. That cost and ESS index depth
must not be folded into this pure-protocol measurement.

## Preserved correctness checks

Each size starts through the genuine `{:storage_recovered, recovered}` protocol
event with a contiguous log, a valid latest applied client record, and unique
`{client_id, request_number}` identities. One stable client and monotonically
increasing request numbers avoid a growing client-table serialization confound.

The probe checks append-before-commit-before-apply effect ordering, applied-state
persistence before reply, matching reply identities/results, final operation/
commit/applied counters, and an empty pending-client table. Repeating the exact
latest request produces a cached reply without another append/apply. A forged
duplicate retained request is rejected by the ordinary invariant check. No guard
or dependency implementation was patched or disabled.

Effects were inspected in their original order. Persistence, broadcast and
application effects were **not executed**; the probe supplies a deterministic
successful apply acknowledgement. It tests the pure protocol transition and
effect contract, not durable storage, transport, real state-machine mutation,
cluster quorum, or acceptance.

## Exact execution and evidence

The executed probe is preserved byte-for-byte in `probe.exs`. The command was:

```sh
PAGER=cat ERL_FLAGS='+S 1:1 +SDcpu 1 +SDio 1' \
  elixir -pa /home/gao/Workspace/gsmlg-opt/ex_storage_service/_build/prod/lib/viewstamped_replication/ebin \
  /tmp/ess-vsr-write-cpu-iU3PleL2/probe.exs
```

The Python driver enforced `subprocess.run(..., timeout=30)` and captured the
terminal exit and elapsed time in `execution.json`. No Mix, services or heavy
workloads were started. Runtime: Elixir 1.18.5, OTP 28, ERTS 16.4.0.5, one online
scheduler. Before/after started-application inventories are identical:
`compiler`, `elixir`, `kernel`, `logger`, `stdlib`. Concord, VSR, ExTurso and ESS
applications were absent. `shutdown.json` confirms no process with the exact
probe argv path remained; unrelated processes were untouched.

For reproduction after obtaining the published compiled VSR 3.1.5 package, use
the corresponding ebin path and a copy of `probe.exs`. The exact retained script
writes its result to `/tmp/ess-vsr-write-cpu-iU3PleL2/result.json`; that disposable
directory must exist. Preserve these report files before any reproduction.

- `source-manifest.json`: observed checkout, exact argv/environment, published
  application version, source hashes, executed probe hash and loaded BEAM hashes.
- `result.json`: all raw per-event time/reduction samples, call-time profiles,
  guard/effect results, toolchain and started-application inventories.
- `summary.json`: medians derived from the raw samples, with the same boundary.
- `execution.json`, `stdout.log`, `stderr.log`: final actual command outcome.
- `shutdown.json`: post-execution owned-process check.
- `SHA256SUMS`: hashes of every other file in this directory.

All copied receipts retain their original bytes and original diagnostic scope.
No historical result, production source, acceptance ledger or scenario status
was changed by this report.
