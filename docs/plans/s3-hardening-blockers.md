# ESS-S3-HARDENING-001 dependency blockers

## Current dependency state — 2026-10-19

Concord #94–#97 are CLOSED and their fixes are published and installed as the
coordinated Concord/VSR/ExTurso 3.1.2 family. The release-wait prerequisites below
are historical and no longer block execution. Exact upstream/package evidence:
`docs/reports/s3-hardening/concord-3.1.2-update-20261019-r1/upstream-release.json`.
Native read-budget/transition probes and fully isolated A36/A37 recovery now
pass; complete-row restore audit and source boundaries are in that directory.

The strict S3 32-completion failure is locally repaired: private MPU state
transitions rewrote unchanged shared index records and could exhaust existing
object-publication compare attempts. The diagnostic503 is positively traced to
compare_retry_exhausted; the earlier409s were not individually classified. The
unchanged strict32-way test and full Core572/0/S3 357/0 now pass at27ad96fb…;
see completion-concurrency-20261019-r1/results.json. No dependency workaround or
retry/deadline increase. Mandatory scale/backfill/resource/rejoin and consolidated
verification remain unpassed; the source-frozen full lanes are now executing.
Current98bc matrix:30passed/8in_progress, no release-wait blocked rows. A01–A27
and A33–A35 have complete current-source assertion audits. Historical f495
restores remain evidence; A36/A37 require the queued current-source regression.

The register below preserves the original reproductions and previous unblock
conditions. References to OPEN/current/blocked there describe that historical
checkpoint, not the installed 3.1.2 state. No checksum/auth/quorum guard is waived.

## Historical blocker register

All three issues are Bug/internal-request/blocker reports in `gsmlg-dev/concord`.
The requesting checkout is `gsmlg-opt/ex_storage_service`, branch `main`, HEAD
`3477bc540c3732dba3d47a7e08779783f9be25fc`, with uncommitted hardening changes.
Do not publish or deploy this checkpoint as fully accepted.

| Issue | Reproduced defect | Required unblock |
|---|---|---|
| [Concord#94](https://github.com/gsmlg-dev/concord/issues/94) | VSR3.0.6 validates every committed log position with nested linear linked-list lookup on transitions. Actual32-completion requests miss original deadlines. Retained recovery replays through the same assertions by source trace; exact restart CPU attribution was not sampled. | A released correction preserving committed-prefix invariants, coordinated Concord/VSR dependency update, then original race,10k/100k, backfill/rejoin and complete-suite runs. Released3.1.1 source previously inspected still retained the loop. |
| [Concord#95](https://github.com/gsmlg-dev/concord/issues/95) | A byte-identical valid native backup can fail checksum verification in a fresh VM with another atom initialization history. Actual isolated restore stops at native verification. | Released stable backup serialization/checksum correction, dependency update, then independent standalone and clean-cluster restore with originals inaccessible and all data/configuration checks. |
| [Concord#96](https://github.com/gsmlg-dev/concord/issues/96) | Public VSR read timeout50ms leaks a raw primary-discovery timeout exit after5002ms; direct Replica.read control returns structured quorum_unavailable in59ms. Actual1GiBc4strict replica PUT returned HTTP500 and a repair planner terminated through that path. | Released discovery/read-budget and structured-error correction preserving linearizable barriers, dependency update, then original isolated API reproduction adapted to fixed behavior, large concurrent cluster resources, retained-root restart and complete suites. |

#94 and#95 were rechecked OPEN during this continuation;#96 was created after an
executed independent API reproduction. No dependency source was changed, no
checksum was bypassed, no quorum weakened, and no local timeout catch or deadline
increase was introduced to mask these failures.

Evidence under `docs/reports/s3-hardening/`:

- #94: `phase3-race-diagnostic-r1.log`, `phase3-vsr-log-cost.log`, current
  `phase3-standalone-r2.log` and final correctness log.
- #95: `concord-backup-checksum-repro.log`, `phase3-restore-r3.log` and the retained
  isolation/backup artifacts. Immutable native artifact SHA is recorded in the
  execution ledger.
- #96: `phase3-primary-timeout-repro.log`,
  `phase3-cluster-resource-r2/results.json`, its data-a log, and
  `phase3-standalone-r2-restarted-server.log`.

Reproducers live in `e2e/scripts/repro_concord_backup_checksum.exs` and
`e2e/scripts/repro_concord_primary_timeout.exs`. Their success exit currently means
**the defect was reproduced**, never that acceptance passed. Do not blindly reuse
that exit interpretation after upgrading dependencies.

Cluster resource r2 completed15groups within the original fixed bounds before
its failed16thgroup. In particular the progressing1GiBexternal replication repair
passed in33.408s; the complete20group profile did not pass. Full local/gateway r6
passed56groups before additional metrics instrumentation and is prior-source
evidence until its final regression is executed.

The detailed status ledger remains authoritative for independent work and the
current next runtime. A blocked row remains mandatory; no requirement has been
converted to optional or silently deferred.

## Needed upstream feature: operator backlog pagination

[Concord#97](https://github.com/gsmlg-dev/concord/issues/97) is a Feature/internal
request with severity `needed`. Concord3.0.6 `KV.list` materializes global metadata
before prefix filtering, sorting and limiting. The explicitly invoked replication
backlog diagnostic limits returned pages but cannot bound backend allocation.
Its callsite retains TODO/WORKAROUND annotations; it never runs on metrics scrapes
or as a background scan. This limitation is distinct from the transactional
per-bucket S3 listing index and is not represented as a passed bounded backend
scan. Adopt a released bounded range API before removing this caveat.

## Final continuation boundary — 2026-10-18

All four issues remain OPEN; see `upstream-blockers-final-r3.json`. Later
canonical resource r8 failed after 49/56 groups during 1 GiB/c4 gateway PUT.
Both subsequent diagnostic replays failed after 46/56 groups during local
UploadPartCopy (503), followed by an abort conflict (409). Their underlying
local failure remains unclassified; it is not attributed to an upstream issue
without matching evidence. V2 tracing stayed active but scoped gateway
WriteThrough, not that local call. Its sampler gap reached 46,861 ms.
See `resource-timeout-diagnostic-v2/run-reviewed-r1/summary.json`. No full
resource pass is claimed. Owned worker/server groups and container cleanup
were verified; no runtime remains active. Aggregate verification returns1.
