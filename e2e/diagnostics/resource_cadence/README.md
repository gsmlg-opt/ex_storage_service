# Incomplete resource cadence diagnostic

This manually dispatched Ubuntu 26.04 diagnostic executes the original cluster
resource prefix (groups 1–16), stopping after the 1 GiB, concurrency-four strict
RF2/W2 PUT. It cannot certify acceptance or authorize publication, even when the
selected groups and diagnostic lifecycle pass. It changes no default acceptance
workflow, resource envelope, quorum, 25 ms sampling or conservative 100 ms coverage
calculation. An original failed group remains failed and stops the prefix.
The prefix includes the failed case in full run 42386733186; the earlier six-group
prefix did not reach that case.

The launcher transparently forwards ordinary Python invocations, and runs the
original resource program with one AST-inserted stop after its existing measured
group assertion. Both `data-a` and `data-b` observers retain their original Port
PID and stdin/stdout pipes through an in-place exec. The observer's original probe
functions and returned windows remain authoritative. The manual mode choice is:

- `probe-only` (default): no ptrace or full per-probe procfs snapshots. Each
  unchanged probe is bracketed by thread CPU clocks, `RUSAGE_THREAD` CPU/context
  counters and one `/proc/thread-self/schedstat` read on each side. Compact
  datagrams are bounded at 2 KiB and serialized/sent after the original window.
- `syscall-trace`: retains `strace -D -f`, full per-probe snapshots and the
  original raw syscall stream. This is an intrusive diagnostic mode.
- `filesystem-trace`: uses the same compact brackets as `probe-only`, plus
  `strace -D -f` restricted to `openat`, `openat2`, `close`, `newfstatat`,
  `statx`, `fstat`, `getdents64`, `lseek`, `statfs` and `fstatfs`. It excludes
  read/write, synchronization and futex calls. Trace arguments remain raw
  addresses/numbers; paths and payloads are never decoded.

All modes use a separate bounded monitor for contemporaneous scheduler, cgroup
CPU/throttling, pressure and aggregate host CPU (including steal) observations.
Its full snapshots run outside the observer process. Extra diagnostic CPU, GIL,
procfs and output overhead remains included in the unchanged resource evaluation.
Bracket accounting includes boundary instrumentation; it must not be treated as
an exact decomposition of the narrower original probe window. Snapshot start/end
clocks retain that distinction.

When enabled, trace syscall arguments use `raw=all`; buffers, filenames, headers,
cookies, credentials and protocol data are never decoded. No `execve`, environment dump,
network payload dump, `-yy`, ptrace-policy change, affinity change or quota change
is used. Optional `kernel.sched_schedstats=1` enables accounting only, on the
disposable hosted VM, with before/after readback retained. A syscall's elapsed
duration includes descheduling; it alone cannot establish a filesystem wait or
fsync cause. Compare aligned original probe windows with per-thread executing and
runqueue-wait deltas, context switches, host steal and cgroup throttle counters.
Disabled/unavailable accounting remains explicit and limits causal conclusions.

The workflow allows 30 minutes including setup; the controller bounds the single
diagnostic workload to 900 seconds plus owned shutdown. The original fixture owns
normal observer shutdown and identity verification. The controller fences any
emergency process cleanup by PID/start ticks, records it as a diagnostic failure,
and retains observer/tracer/monitor cleanup receipts. Fixture roots are retained;
the artifact excludes FIFOs/sockets and credential files. Workflow artifacts are
uploaded on success or failure, beginning with an incomplete marker before setup.

In either traced mode, each observer monitor continuously drains the full-lifetime
trace without detaching at a syscall count. Retention caps per observer are 1 GiB
syscalls (absent in probe-only), 512 MiB probe boundaries and 256 MiB scheduler
rows. After exhaustion, draining continues but discarded bytes are counted and
diagnostic coverage fails. Probe
datagrams are bounded at 2 KiB in probe-only/filesystem-trace and 32 KiB in syscall-trace; any drop
or count/mode mismatch fails coverage. Probe-only omits syscall/tracer-presence
expectations and fails if an unexpected tracer is observed. The monitor's
fail-closed safety lifetime is 960 seconds; the controller owns earlier
900-second workload cancellation. Full-lifetime coverage requires both monitor
terminal receipts, complete datagram delivery, original export verification and
stopped original identities; both traced modes also require natural trace completion.

Internal instrumentation variables, set by the controller/dispatcher only:

- `ESS_DIAGNOSTIC_MODE`: manual workflow choice `probe-only` (default),
  `syscall-trace` or `filesystem-trace`, fenced across initialization, launch
  and terminal receipts.
- `ESS_DIAGNOSTIC_PINNED_PYTHON`: absolute pinned virtualenv Python forwarded by
  the transparent launcher, also used as the original observer's Python.
- `ESS_DIAGNOSTIC_EVIDENCE`: unique disposable diagnostic report directory,
  outside the original resource output directory.
- `ESS_OBSERVER_DIAGNOSTIC_ROOT`: private per-observer FIFO/socket/trace directory
  under that fixture's original cluster root.

These variables do not configure production behavior or acceptance thresholds.
`DIAGNOSTIC_LABEL` and `ENABLE_SCHEDULER_ACCOUNTING` are workflow-local values.

Pure fixtures (no BEAM, service, observer, tracer or workload launch):

```sh
PAGER=cat python3 -B e2e/diagnostics/resource_cadence/test_diagnostic.py
```

Performance diagnostics run only on a confirmed local performance host, never on
hosted CI. Run one bounded diagnostic for a specific source revision (mode is
`probe-only`, `filesystem-trace` or `syscall-trace`):

```sh
MIX_ENV=prod ESS_DIAGNOSTIC_MODE=filesystem-trace \
  "$PINNED_PYTHON" -B e2e/diagnostics/resource_cadence/run.py \
  --label "cadence-diagnostic-$(date -u +%Y%m%dT%H%M%SZ)" --python "$PINNED_PYTHON"
```

Retain the output, original failed resource receipts and source identity
together; a non-reproduction adds diagnostic evidence only.
