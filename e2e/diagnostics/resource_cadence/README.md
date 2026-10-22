# Incomplete resource cadence diagnostic

This manually dispatched Ubuntu 24.04 diagnostic executes the original cluster
resource prefix (groups 1–6), stopping after the 64 MiB, concurrency-four strict
RF2/W2 PUT. It cannot certify acceptance or authorize publication, even when the
selected groups and diagnostic lifecycle pass. It changes no default acceptance
workflow, resource envelope, quorum, 25 ms sampling or conservative 100 ms coverage
calculation. An original failed group remains failed and stops the prefix.

The launcher transparently forwards ordinary Python invocations, and runs the
original resource program with one AST-inserted stop after its existing measured
group assertion. Both `data-a` and `data-b` observers retain their original Port
PID and stdin/stdout pipes through `strace -D -f`. The observer's original probe
functions and returned windows remain authoritative. Diagnostic decorators add
thread CPU/runqueue/context-switch accounting around each probe, and a separate
monitor records contemporaneous scheduler, cgroup CPU/throttling, pressure and
aggregate host CPU (including steal) observations. Extra diagnostic overhead is
included in the unchanged resource evaluation.

Trace syscall arguments use `raw=all`; buffers, filenames, headers, cookies,
credentials and protocol data are never decoded. No `execve`, environment dump,
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

Each observer monitor continuously drains the full-lifetime trace without
detaching at a syscall count. Retention caps per observer are 1 GiB syscalls,
512 MiB probe boundaries and 256 MiB scheduler rows. After exhaustion, draining
continues but discarded bytes are counted and diagnostic coverage fails. Probe
datagrams are bounded at 32 KiB; any drop or count mismatch fails coverage. The
monitor's fail-closed safety lifetime is 960 seconds; the controller owns earlier
900-second workload cancellation. Full-lifetime coverage requires natural trace
completion, both monitor terminal receipts and stopped original identities.

Internal instrumentation variables, set by the controller/dispatcher only:

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

Dispatch `.github/workflows/resource-cadence-diagnostic.yml` once for a specific
source revision. Retain the artifact, original failed resource receipts and
source identity together; a non-reproduction adds diagnostic evidence only.
