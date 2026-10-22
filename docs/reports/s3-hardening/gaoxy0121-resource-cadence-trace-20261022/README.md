# Resource cadence trace: failed diagnostic, 2026-10-22

This compact report preserves a **failed, incomplete diagnostic**. It does not
establish production filesystem, fsync, GIL or VM causation, certify resource
acceptance, or authorize publication. No existing failure is promoted to a pass.

The [hosted run 39779530866](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39779530866)
and [job 119826918910](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39779530866/job/119826918910)
failed. Its tested source was `fc85bafd9706731192d1da66cc1dcee023268ee3`, with
executable fingerprint
`1bb4eb2b4359a672d56e5df788b1b41ca385b08fc4d31dd76db98f2dab83f2e9`.
This is the diagnostic source identity, not the current checkout or a final
acceptance candidate. `original/diagnostic.json` retains the actual command,
return code 1, 60.997-second duration, source and diagnostic-tool hashes.

## What executed

The first six original cluster resource groups ran with 64 MiB fixtures,
RF=2/W=2, the original 25 ms observation schedule and unchanged 100 ms coverage
limit. Groups 7–20 were excluded deliberately; the original 20-case manifest is
retained in `original/results.json` and the exclusion list in
`original/diagnostic-scope.json`.

| Group | Operation | Concurrency | Original outcome |
| --- | --- | ---: | --- |
| 1 | strict quorum replica PUT | 1 | No group failures |
| 2 | authenticated peer GET | 1 | No group failures |
| 3 | fenced repair pull | 1 | No group failures |
| 4 | fenced repair push | 1 | No group failures |
| 5 | cross-cluster worker paced sink | 1 | No group failures |
| 6 | strict quorum replica PUT | 4 | Cadence failed on both data nodes |

Group 6 retained verified size/hash and two ready replicas for each of four
fixtures. That byte/replica observation does not override its cadence failures.
Data-a failed six metric coverage checks and the temporary-probe duration;
data-b failed all six metric coverage checks. No later group was run.

| Group 6 metric | Data-a maximum gap, ms | Data-b maximum gap, ms |
| --- | ---: | ---: |
| binary / total | 292.618615 | 176.709959 |
| RSS | 210.669225 | 178.970658 |
| queue maximum / total | 294.604715 | 178.036161 |
| temporary bytes | 318.087880 | 289.480061 |

The maximum original temporary probe was 107.762455 ms on data-a and
7.985478 ms on data-b. `derived/*-original-sample-pairs.json` retains the actual
original rows bounding each reported worst gap. No samples are interpolated.

## What the extra tracing shows, and changes

Both data observers ran under continuous `strace -D -f`, with raw pointer
arguments rather than payload strings. Per-probe procfs/thread snapshots and
external scheduler/cgroup/pressure monitors added substantial work. The original
diagnostic retained 325,154,370 bytes, approximately 310 MiB. These conditions
materially perturb collection and differ from an ordinary acceptance run.

Selected exact syscall entry/resume lines substantiate these traced waits:

| Node / thread | Syscall | Traced elapsed, ms | Boundary interpretation |
| --- | --- | ---: | --- |
| data-a / 23061 | write | 93.384 | Inside original export write, measured 93.432731 ms |
| data-a / 23061 | futex | 173.447 | Concurrent export-thread wait |
| data-a / 23046 | fstat | 148.583 | Diagnostic RSS-before snapshot, outside original RSS probe |
| data-a / 23046 | getdents64 | 106.771 | Inside original 107.762455 ms temporary probe |
| data-b / 23050 | pselect6 | 161.619 | Concurrent main-thread wait |
| data-b / 23050 | pselect6 | 156.174 | Concurrent main-thread wait |
| data-b / 23062 | futex | 111.782 | Concurrent export-thread wait |

The fstat location matters: data-a's original RSS probe lasted 1.103427 ms,
while its diagnostic before-snapshot lasted 155.099488 ms. Another data-a
temporary probe lasted only 2.313834 ms inside a 171.422285 ms instrumented
bracket, including a 167.123653 ms before-snapshot. Data-b's original encoding
lasted 0.057777 ms with a 162.444197 ms diagnostic before-snapshot. Slow
instrumented brackets therefore cannot all be attributed to the original
measurement functions. `derived/*-probe-windows.json` preserves these windows,
CPU brackets and scheduling counters.

External monitor start intervals themselves reached 184.513883 ms on data-a
and 170.302943 ms on data-b. Across their approximately 10.6-second group-6
windows, recorded host steal and cgroup throttle deltas were zero. Host I/O
pressure totals increased by 1,349,068 microseconds for `some` and 606,844 for
`full`. These are aggregate observations, not attribution to this observer or
proof of a filesystem/fsync fault. Tracee runqueue counters do not account for
every observed wall-time wait or measure tracer-side delay. The selected
scheduler endpoints and maximum monitor-gap pairs remain available.

The tracing supports locating particular waits **in this instrumented run**.
It does not explain the earlier untraced 111–139 ms gaps or establish the same
cause. A narrow follow-up without ptrace and bulky per-probe snapshots would be
a separate experiment; none was run while curating this report.

## Shutdown has two independent results

`original/diagnostic.json` records ten tracked owned identities stopped and no
remaining members of the owned process group. These ten are the diagnostic's
tracked set, not an inventory of every process ever started. Both trace monitors
finished complete, reported zero discarded trace/probe/scheduler bytes, matched
retained sizes, and confirmed their tracers stopped. Delivered probe events
matched sent counts: 7,720 for data-a and 7,716 for data-b, with zero drops.

The original resource export contract still failed independently on data-b:
its terminal export manifest is absent and its stop verification records
`FileNotFoundError`. Original shutdown stderr records
`RuntimeError: observed process has no resident memory` after BEAM SIGTERM.
Data-a retained a successful terminal export for 1,930 rows; data-b retained
1,929 contiguous rows without a successful original terminal export. Complete
diagnostic telemetry and OS termination cannot replace that missing manifest.

The separate `rss-regression/` receipts preserve a deterministic local shutdown
race reproducer: one failure against the byte-exact observer from `e754ad8`,
then one passing test against the dirty intermediate observer repair. The old
observer hash is `6d594fbf1b3cd0158eef2f03c7e2ef517f227671bdd80ddbd96d9308412e7b5d`,
the same observer bytes used by the hosted `fc85bafd` diagnostic. The repaired
hash was `7899bd0ef66f527af2840db7740d6290dd4a03f6c12bbf91a21cf4f0a75cf343`.
These are separate root-run red/green fixture receipts, not a rerun of the
hosted groups and not retroactive completion of data-b's original export.
The full 34-test observer suite was reported passed by the coordinating agent;
its complete output was not retained here and is not independently rerun or
claimed as a saved receipt by this report.

## Portability and selection limits

`original/` files are byte-identical small receipts. `selection-manifest.json`
records original paths, full original SHA256 hashes, sizes, source identities,
URLs and exact selection rules. Derived files retain source line numbers and
original row hashes. Original absolute runner and `/tmp` paths are historical
provenance; consumers must read the local copied files, not dereference those
old paths.

Only eight probe boundaries, the union of the original worst-gap sample pairs,
four scheduler rows per data node, seven selected syscall pairs and bounded
failure stderr excerpts are included. Probe/scheduler thread projections omit
bulk procfs `sched`, `status` and `stat` text; relevant clocks, identity,
schedstat and parsed context-switch counters remain. Exact syscall lines contain
raw pointer arguments and no request bodies or credentials. No complete large
telemetry file, environment dump or private payload is copied.

These selected windows substantiate the stated numbers. They cannot reproduce
the full raw-series audit, prove that no other wait occurred, certify global
trace completeness independently, or establish the full resource acceptance
manifest. Those broader checks require the original retained artifact. Python
and BEAM monotonic timestamps remain separate clocks; only durations within
each clock and approximate wall-time overlap are compared.

`SHA256SUMS` covers every other file in this compact report. Verification from
the repository root:

```sh
PAGER=cat python3 - <<'PY'
import hashlib, pathlib
p = pathlib.Path('docs/reports/s3-hardening/gaoxy0121-resource-cadence-trace-20261022')
for row in (p / 'SHA256SUMS').read_text().splitlines():
    expected, relative = row.split('  ', 1)
    assert hashlib.sha256((p / relative).read_bytes()).hexdigest() == expected, relative
print('All compact report checksums match')
PY
```

Curating this report ran no Mix, observer, services or workloads and changed no
production source, tests, ledger, workflow or shared notes.
