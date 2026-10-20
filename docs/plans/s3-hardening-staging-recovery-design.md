# A31 bounded staging recovery architecture

Accepted implementation boundary, 2026-10-18. Read-only architecture review by
`staging_recovery_design` (PROJECT_ARCHITECTURE); implementation remains Sol-owned.
This document is a design, not passing verification.

## Ownership and eligible files

Internal `Storage.StagingRecovery`; no new supervisor, job kind, database or
dependency. Managed filenames encode version, ownership-scope fingerprint, OS
PID and cryptographic nonce. Scope binds Linux boot identity, PID namespace,
effective UID, node/instance identity and exact configured staging root. Verify
the current `/proc/self` PID view matches `System.pid()`. Retain on uncertain
ownership/process visibility. Only matching-scope, demonstrably absent owner
PIDs permit reclamation. A live or reused PID is retained. Different boot or
PID namespace is foreign ownership, not proof of death. Hostname+PID is unsafe
with shared container volumes.

Explicit service-owned staging opts enable management. Direct LocalCAS callers
and `Context.direct_blob_store_options/1` remain unmanaged. Preserve legacy
upload-N, earlier random names, malformed names, caller paths, symlinks and
nonregular files.

## Protect before metadata submission

Three distinguishable filename classes in the existing uploads directory:

| Class | Meaning | Automatic cleanup |
|---|---|---|
| Anonymous | No intent/journal submission has been possible | Eligible after all guards |
| Hosted-protected | A hosted intent may have been submitted | Retain |
| Gateway-protected | A gateway journal may have been submitted | Retain |

Atomically rename and directory-sync the anonymous stage into hosted-protected
naming **before every staged hosted intent submission**: ObjectService's
standalone/cluster staged paths and both staged multipart part paths. Include
its SHA256 in the protected name. Before WriteThrough submits its journal,
perform the gateway-protected rename+directory-sync; journal the resulting exact
path. Encode the operation ID reversibly where filename bounds permit. Preserve
existing oversized operation-ID compatibility using a conservative protected
form instead of truncating/rejecting it.

Reserve destinations exclusively before replacement. Failed creation must
never delete another owner's file. Thread returned stage paths through every
following call. Failed/uncertain protection prevents metadata submission; once
submission is possible, preserve the protected file on uncertainty.

The ordering is the proof: anonymous dead-owner paths cannot have an outstanding
submission that references them. Neither an absent journal nor an absent guard
alone proves safety; an earlier unfinished request can still linearize later.

## Conservative guards and inventory

For eligible anonymous files, stream SHA256 and exact-read `Keys.gc_guard(hash)`
linearly. Every existing guard, including expired, is a veto. Malformed replies
or errors retain. Aggregate guards cannot prove all same-hash intents absent.

Protected-hosted inventory can exact-read its hash guard; protected gateway
inventory can exact-read its journal and compare the path. Absence, mismatch,
terminal-looking records or read failures **never authorize deletion** here.
Protected reclamation requires a separately proven terminal/tombstone fence.
Report retained recovery inventory honestly; it is not anonymous cleanup.

Immediately before unlink recheck owner/root identity, regular type, inode,
device, size and modification identity. Keep existing private-root assumptions;
this does not defend against privileged root rewriting.

## Bounded continuation and teardown

Use the existing instance Storage.Engine handle_info loop, with explicit Context
passed by InstanceSupervisor. Do not recover another instance's roots from a
global fallback. Start after directory initialization on data instances with
existing CAS maintenance enabled, outside restore/recovery quarantine. Do not
block readiness on a complete scan; disabled embedded maintenance remains off.

Maintain one non-following depth-one find traversal as an ephemeral cursor.
Each candidate executes a fixed shell helper which emits one bounded basename
then **waits for acknowledgement before find can advance**. Accept bounded
heartbeats while processing a candidate. EOF, invalid acknowledgement or read
inactivity terminates the helper and causes the fixed find expression to quit.
Pass roots/paths as literal argv; never interpolate them into shell source.
Validate candidate basenames against the managed grammar. Normal unacknowledged
Port output can grow a BEAM mailbox and is insufficient backpressure.

Initial per-turn limits: one candidate and file descriptor in progress, 256KiB
chunks, 4MiB hash bytes or 32 candidate decisions before yielding. Retain hash
context and scanner across turns. A find|head-N restarted each turn can starve
later files behind retained entries and is forbidden. No full File.ls or disk
inventory spool. Total traversal time scales with finite directory size; eventual
completion assumes sufficiently uninterrupted execution.

On completion close the scanner and schedule a later sweep. On stop/error or
protocol failure close descriptors and stdin; verify helper/scanner termination.
Missing executables or unsupported capabilities disable deletion with a bounded
reason. Backup.quiesce stops the instance, and scanner shutdown must complete
with it. Restore validation must never activate cleanup.

Concord#96 means exact reads do not yet have a proven wall-clock budget. Do not
claim such a bound or introduce a local timeout workaround. Preserve stages on
unavailable metadata; dependency blockers remain separate.

## Implementation ownership and required verification

Sol owns new Storage.StagingRecovery; LocalCAS naming/protection; explicit
Context and service staging callsites; ObjectService/Multipart intent boundaries;
WriteThrough journal boundary; Engine/InstanceSupervisor continuation/shutdown;
scoped tests, crash assertions and runbook. Public BlobStore behaviour, Concord,
GC intent semantics and RF/W remain intact.

Required tests: real fresh-BEAM partial/completed anonymous cleanup; collision
protection; live/reused PID and foreign namespace/root/instance retention;
unreadable proc, legacy/unmanaged/symlink retention; crashes around protected
rename/sync and intent/journal submissions; delayed submission after an absent
read retaining protected bytes; pending/unknown/expired-guard/path-mismatch/error
retention; more protected candidates than a batch cannot starve anonymous work;
large hashing yields; helper/consumer faults bound queues/processes; shutdown
leaves no helper; gateway reconciliation, embedding and restore quarantine
regressions pass. No source-only assertion is a verification pass.

Residual limits: protected and legacy paths are retained recovery inventory;
cross-boot/container namespace reclamation is unproven; Concord#94/#95/#96 remain
blocked. The preceding collision56/0 is not validation of this cleanup design.
