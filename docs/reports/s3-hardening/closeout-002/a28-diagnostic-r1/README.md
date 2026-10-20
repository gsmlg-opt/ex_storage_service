# A28 diagnostic: residual Concord 3.1.4 blockers

This is diagnosis, not A28 acceptance. The mandatory 10k/100k datasets were not
run. No historical result was promoted. The historical 10,800-second timeout and
2,958-put observation remain observations; this run establishes two current
mechanisms without claiming they explain every historical delay.

`execution.json` records a fresh isolated production-path run, exit 0 in
64.029636897 seconds, reaped. Starting HEAD was
`b17282d96a58eff962a35d807c9e7adb39af4774`; the executable-source fingerprint was
`20351587f71da6b8ba72d53bf9e46af149a33c4c767049c06e03a765b2104350` before and
after that run. Later diagnostic/preflight additions have a different fingerprint.
Runtime was Elixir 1.18.5/OTP 28, coordinated Concord/VSR/ExTurso 3.1.4, command
version 0, `+S 4:4 +SDcpu 2 +SDio 2`. Affinity, baseline load, disk availability
and unavailable cgroup observations are explicit in `execution.json`; no CPU or
memory cgroup limit was inferred from missing files.

The fixture used normal ObjectCommit transactions, independent buckets and
generation-scoped index invariants. `progress.jsonl` records successful progress,
backend timings, returned bytes, retries/conflicts, scheduler queues, GC, VM and
process memory/reductions, and persistence files. `probe.json` separates setup,
offline inventory and real page operations. Silent call-time profiling is
intrusive diagnostic instrumentation, not acceptance latency or allocation proof.

For a fixed 1,000-key target, adding 1,000 unrelated normal commits produced:

| Ten delimiter pages, max-keys 1 | Before | After unrelated growth |
| --- | ---: | ---: |
| ESS exact reads | 100 | 100 |
| Returned serialized bytes | 26,620 | 26,620 |
| Native uniqueness validations | 100 | 100 |
| Native recursive history checks, including base | 100,400 | 200,400 |
| Native validation call time, us | 19,538 | 39,140 |
| Page wall time including diagnostic overhead, us | 22,793 | 42,445 |

Retained native entries grew 1,003 to 2,003. Offline serialized retained-log size
grew 5,870,028 to 11,865,907 bytes; this is inventory size, not a claim that each
payload byte is visited by validation. The native recursive visits demonstrate
proportional unrelated-history work despite bounded ESS tree visits/returned
bytes. Absent-page native calls likewise grew 50,200 to 100,200. No read-group
materialize/1 or Core.snapshot/1 calls occurred. This preserves the scope of
3.1.4's allocation fix while identifying its remaining traversal defect.

Setup had zero transaction conflicts and no checkpoint calls. At stages 1–100,
101–500 and 501–1000, wall time per operation increased approximately 8.8, 13.0
and 25.4 ms. For the final 500 operations, ListIndex.augment/4 accounted for
76,564 us, materialize/2 4,682,216 us, Observer.committed/6 2,314,231 us and
File.persist/2 3,997,366 us. These nested call-time values must not be summed as
independent wall-time shares. Measurements do not support blaming root conflicts;
native materialization, observation and persistence dominate this run.

`upstream-minimal.json` is a separate pure dependency reproducer, exit 0, with no
service applications started. Equivalent direct put and one-key atomic txn have
identical stores, but direct put reports changed keys and txn reports `:all`.
The native singleton-read probe at 1k/2k/4k retained entries has p50 reductions
2,560/5,060/10,060 and p50 time 17/44/91 us. It uses the genuine recovery-derived
request index rather than a forged cache.

Both defects are upstream blockers, verified open with type Bug and label
`internal request`:

- [concord#101](https://github.com/gsmlg-dev/concord/issues/101): singleton reads
  still traverse unrelated retained history in 3.1.4.
- [concord#102](https://github.com/gsmlg-dev/concord/issues/102): atomic transactions
  still trigger full-state materialization in 3.1.4.

Exact issue bodies and URLs are retained in `upstream-*-request.md` and
`upstream-issues.json`. No dependency patch, split transaction, forced checkpoint,
snapshot workaround, or replacement index was introduced. The isolated root
listed in `execution.json` is retained; its owning command has stopped.

`preflight.json` records the index entry point exiting 1 before fixture creation
with both blockers. The gate fails closed for unvalidated future/mismatched
dependencies; a version bump alone is not proof. Resume only after inspecting
released source/API fixes, updating dependencies, rerunning both probes and then
the unchanged full A28 datasets plus affected A29 checks. Preflight tests: 4/4;
setup-diagnostic helper tests: 4/4; shell syntax and git diff checks passed.
