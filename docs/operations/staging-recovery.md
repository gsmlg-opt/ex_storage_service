# Anonymous staging recovery

Storage.Engine drives recovery for its explicit data-instance context after root
initialization. It runs only when existing `cas_gc` maintenance is enabled and
backup/restore quarantine is inactive. It does not delay readiness until an
inventory finishes. Direct LocalCAS callers and
`Context.direct_blob_store_options/1` remain unmanaged.

A managed anonymous filename binds a version, cryptographic nonce, OS PID and an
ownership fingerprint: Linux boot identity, PID namespace, effective UID, Erlang
node, configured instance/node ID and exact staging root. Reclamation requires
the same ownership scope and a positively absent `/proc/<owner-pid>`. A live or
reused PID, different boot/namespace/root/instance, unreadable process state,
legacy `upload-*`, malformed filename, symlink or nonregular file is retained.
Unsupported proc/utility capabilities disable recovery with a bounded log reason.
Cross-boot/container-namespace reclamation is deliberately unproven.

Before every staged hosted intent or gateway journal submission, the stage is
renamed into an exclusively reserved protected filename and its directory is
synced. Hosted names include the SHA256; gateway names include it plus a
reversible operation ID when it fits, otherwise an opaque protected marker.
Callers use the returned path. Failure to reserve, rename or sync prevents the
submission. A failed exclusive reservation never removes another owner's file.

Protected names are recovery inventory. They are always retained by this
cleanup, including absent/mismatched/terminal-looking metadata and unavailable
metadata. An absent record cannot rule out an earlier submission committing
later. Gateway reconciliation continues using its exact journal path. Failed
protection can leave a protected reservation or renamed file; retain it for
review. Do not delete such files solely by age, an absent read, or a successful
service restart. Existing operation-status diagnostics expose the relevant
hosted/gateway operation state without returning credentials or stage paths.

For an eligible anonymous file, recovery streams SHA256 and performs one exact
linearizable GC-guard read. Every existing guard (including expired/malformed
values), read failure or unexpected response vetoes unlink. Immediately before
unlink it rechecks owner scope, root identity, regular-file type, device/inode,
size and modification identity. Private service-owned root permissions remain
required; this is not a defense against privileged root rewriting.

One retained depth-one, non-following `find` cursor emits one bounded basename
and waits for an acknowledgement. Recovery holds one candidate and one file
descriptor, reads 256 KiB chunks, and yields after at most 4 MiB per turn; it
makes at most one candidate decision per event (below the 32-decision ceiling).
There is no complete directory list or disk inventory spool, and retained
entries do not restart/starve the traversal. Helpers accept heartbeat messages
while a candidate is processed and stop on invalid acknowledgement, EOF or
inactivity. Engine shutdown closes the file and stops its cursor/helper;
`Backup.quiesce/1` stops the instance and therefore this maintenance. A completed
sweep schedules another in 60 seconds.

Total scan work scales with directory size. Concord 3.1.2 applies the native read
budget to primary discovery and the exact metadata read; an unavailable guard
retains the file. This does not bound total scan or filesystem latency. Protected,
foreign and legacy files can remain indefinitely and must not be described as
reclaimed anonymous stages. Recovery of these classes requires a separately
proved terminal/fencing procedure, which this cleanup does not supply.

Executed checks and source identities are recorded in
[staging-recovery evidence](../reports/s3-hardening/staging-recovery/README.md).
