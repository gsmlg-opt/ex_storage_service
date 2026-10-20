# ESS-S3-HARDENING-001: P3.4 implementation handoff

Status: implementation underway, 2026-10-18. The investigation below defines
the supported boundary. This document is not passing evidence for A36 or A37.
Baseline: `3477bc540c3732dba3d47a7e08779783f9be25fc`.

Progress checkpoint:

- New `operations/backup.ex` and `operations/backup_manifest.ex` implement the
  boundary; root coordinates the application startup/private-listener hooks.
- Parent strict test compilation passed. Initial 11 contracts reproduced a path
  anchor bug (`Path.join("", "/")` normalized the root to empty). Repair 1
  preserves the anchor while retaining ancestor symlink checks; all 11 contracts
  then passed in `docs/reports/s3-hardening/phase3-backup-r1.log`.
- Three additional contracts cover interrupted atomic installation, non-ready
  listing roots/cluster packing restrictions, and pinned job-body references.
  Parent coordinates the current 14-contract execution and retained logs.
- New real runner: `bash e2e/run_hardening_restore.sh /absolute/pinned/python
  unique-label all`. Shell syntax, Python AST syntax and `git diff --check`
  passed. Standalone real attempts reached configuration seeding (r1) and then
  the backup boundary/namespace launch (r2), but neither completed restore.
- r1 exposed an unsupported bucket-policy fixture. The fixture now uses the
  supported attached ESS IAM policy with explicit allow/deny and version/tag
  reader permissions; all IAM records remain part of exact metadata verification.
- r2 exposed missing mount destinations under a read-only root. Namespace mounts
  now live beneath the writable private `/tmp`: `/tmp/recovery`, `/tmp/backup`,
  `/tmp/fixture`, `/tmp/evidence`. Original `/tmp` source roots remain hidden,
  backup/fixture are read-only, and isolated mount/PID assertions remain required.
- Publication GC guards persist after normal commits; they do not indicate
  unresolved work. Actual locks/journals/nonterminal intents still block enabled
  workers. Explicit `intent_resolutions` permit reviewed terminal intent changes
  only while preserving all content identity, owner/generation and fence fields.
  Two new pure policy contracts are pending coordinated parent execution.
- No worker runtime/server remains active. Contract repair 1 passed; the first
  real restore attempt was initial implementation validation, its first repair
  reached the independent namespace prerequisite failure described above. The
  second restore harness repair is source-ready, runtime pending. A36/A37 remain
  unpassed. Do not count namespace launch or backup creation as restored recovery.

Confirmed independent-recovery blocker after r3: Concord 3.0.6 computes its inner
backup checksum from noncanonical `:erlang.term_to_binary(data)`. A synthetic
1000-atom-key map verifies in the creating VM and fails native
`Concord.Backup.verify/1` in a fresh VM with reversed atom initialization, despite
an identical backup-file SHA256. Pure two-process reproduction:
`e2e/scripts/repro_concord_backup_checksum.exs`; sanitized output:
`docs/reports/s3-hardening/concord-backup-checksum-repro.log`. The actual private
snapshot likewise passes source checks but fails fresh native verification before
restore. No private snapshot or credentials were added to the repository.
Tracked blocker: [gsmlg-dev/concord#95](https://github.com/gsmlg-dev/concord/issues/95),
type Bug, label `internal request`, severity blocker. The callsite retains an
upstream TODO; checksum verification remains enforced. Do not
patch the dependency, disable inner/outer checksums, rewrite the snapshot locally,
or claim A36/A37 from source-side backup verification. Resume only after a
published upstream fix and coordinated dependency update.

## Reuse the supported mechanism

Extend [the existing upgrade runbook](../operations/cluster-upgrade.md). Use a
quiesced logical Concord 3 backup plus immutable blob artifacts. Restore a
logical snapshot into a fresh standalone VSR group or the existing supported
fixed three-voter/two-data-node topology. Do not copy VSR directories between
groups, claim an online consistency boundary, migrate Concord 2 files, enable
cluster packing/lifecycle, change RF=2/W=2, or introduce a second metadata
storage mechanism.

The smallest implementation is an `ExStorageService.Operations.Backup` module
with create, verify, restore-preflight, restore, and explicit activation
operations, called by release RPC. Thin Mix tasks may expose the same operations
for source checkouts. Keep transport/runtime ownership in the existing
operations and supervisor APIs. Do not require Mix in a release image.

The supported cluster restore profile can retain the same logical data-node
IDs with strictly higher generations, new distributed Erlang node names and
isolated roots/network. Arbitrary node-ID remapping or a different voter count
is outside this initial profile. This still requires explicit mapping validation;
it must not clone the source runtime identity or import its VSR state.

## Source facts that affect implementation

| Source | Verified behavior | Consequence |
| --- | --- | --- |
| `deps/concord/lib/concord/backup.ex` | `create(path: ..., timeout: ...)`; `verify(path)` returns `{:ok, :valid}`; restore overwrites the complete KV/index snapshot. `force` is documented but not enforced in the implementation. | The ESS wrapper must check destination emptiness and quarantine itself; `force: true` is not a safety guard. |
| `deps/concord/lib/concord/state_machine/core.ex` | Logical snapshot contains KV and indexes. Restore clears Concord leases, history, and request deduplication state. | Do not promise those are preserved. ESS job owner/token fields are ordinary KV and need their own fencing transition. |
| `application.ex`, `instance_supervisor.ex`, `instance_config.ex` | `ESS_AUTO_START=false` omits the local storage instance. Worker defaults include GC, packing, lifecycle, and cross-cluster dispatch; disabling the cluster data plane alone does not disable all workers. | Require listeners off and no instance during import; start a validation instance only with every worker explicitly false. |
| `metadata/job_store.ex`, `metadata/models/job.ex` | Owner node, owner generation, fencing token, state, and unexpired lease are compared transactionally. Supported states are pending/running/completed/failed. | Higher node generations alone do not revoke imported job leases. Do not invent a new job state to avoid fencing. |
| `cluster/node_registrar.ex`, `cluster/membership.ex`, `operations/cluster.ex` | Registration is startup/explicit, rejects lower generations, and is overwritten by a logical restore. | Re-register every target voter after import using `Operations.Cluster.bootstrap/2`. |
| `cluster/read_coordinator.ex`, `metadata/blob_locations.ex` | Usable locations require matching member/location generations; ready-location transitions have metadata/topology fences. | Old location evidence cannot be trusted for new roots. Verify destination bytes before recording higher-generation locations. |
| `iam/access_key.ex`, `cloud_cache/config.ex` | Secret encryption uses AES-CTR; decryption with a wrong key need not error. | Require a key binding from the private backup manifest before import; successful decryption is insufficient. |
| `storage/pack.ex` | Pack files live under `CAS.blob_root()/packs/pack-<hash>.pack`; JSON `.idx` sidecars and `pack:<hash>` metadata duplicate the index. | Capture pack file, sidecar, and metadata; verify whole-pack and referenced segment hashes/lengths. |
| `context.ex` | One Concord metadata root per BEAM; tmp/blob roots and worker-root validation are explicit. | Use separate OS processes and roots for source/target, retaining existing same-filesystem staging checks. |

The existing Concord API materializes the complete metadata snapshot and backup
file in memory. Blob copy and verification can be chunked and inventories paged,
but this implementation cannot claim constant-memory metadata export. If a
bounded streaming metadata export becomes a mandatory acceptance condition,
route that missing feature to the owning Concord repository; do not patch the
dependency or silently replace its snapshot protocol.

## Quiesced backup boundary

1. Close public S3/admin mutations on all source nodes; drain active requests,
   replica transfers, notifications, and background tasks. Stop the instance
   children through their owning supervisor, not by killing permanently
   restarted processes. Keep Concord infrastructure alive for export.
2. Assert every source instance and mutating/outbound task is stopped. Record
   the participating node IDs/generations, topology, metadata schema, application
   version/source SHA, and a consistency-point timestamp/revision when the
   underlying API exposes it. Do not label an application clock as a Concord
   revision. No node may resume writers/maintenance until bundle publication.
3. Run `Concord.Backup.create(path: ..., timeout: 120_000)` once, then require
   `Concord.Backup.verify/1 == {:ok, :valid}`. Retain the exact returned path,
   avoiding a latest-file glob when multiple backups exist.
4. Copy immutable required bytes from the quiesced nodes into a private staging
   bundle. Use bounded chunks and bounded file concurrency. Deduplicate by
   content hash; fail when any reference lacks a verified representation. For
   cluster backup, visit every required source node and verify at least one
   recoverable copy for every referenced hash; record original replica evidence.
5. Write and fsync checksummed inventory/manifest files, verify the staged
   bundle, then atomically publish it. An interrupted staging directory is not a
   successful backup. Record quiescence start/end and backup ID. Source activation
   after publication is an explicit operation, not an error-handler side effect.

Bundle contents and policy:

| Artifact/state | Required policy |
| --- | --- |
| Logical metadata snapshot | Preserve all KV/indexes, including legacy records, versions/delete markers, object attributes/checksums/tags, bucket policies/CORS/lifecycle, IAM, gateway configuration, multipart, outbox/jobs, intents and uncertain gateway journals. No destructive migration/filtering during backup. |
| Loose CAS | Copy each hash needed by heads, historical versions, multipart parts, intents, or retained jobs. Verify SHA-256 and exact size while copying and again at destination. Include a complete inventory so omissions fail verification. |
| Packed objects | Copy full immutable packs and sidecars. Verify pack SHA-256, sidecar format/index agreement, segment bounds, sizes and per-blob hashes; include packed historical versions. Do not require a loose fallback to exist. |
| Multipart | Preserve open uploads, initiation attributes/checksum settings, retained parts and completed results. Completing records are retained and diagnosed, not silently converted into new uploads or replayed into duplicate versions. |
| Operation intents / gateway journals | Preserve unknown/active records and their required bytes, including journal-referenced staging files. Journal paths are absolute in the current write-through record: validate their source-root containment, inventory/copy them, then revision-fence the rewrite to verified target paths. Unknown remote outcomes remain quarantined for reconciliation; restore must not replay an upstream mutation merely to make the local state look complete. |
| Jobs/outbox | Preserve payloads and terminal results. Fence imported running claims before activation; all imported outbound and destructive work remains inactive until explicit approval of the recovery state. |
| Cache | Exclude only entries proved reconstructible and label exclusions. Gateway configuration/cache backup does not contain upstream-only object bodies. Required staged journal bytes are not disposable cache. |
| Configuration/secrets | Record schema/version, mode, topology and nonsecret effective configuration required to interpret data. Retain encrypted secret records in the private snapshot. Provision actual master/internal/cookie secrets separately. Never print decrypted values or credentials into reports. |

A checksummed manifest should contain a format version, backup ID, source
version/SHA/schema, quiescence boundary, sanitized topology/generation map,
artifact relative paths/sizes/SHA-256, inventory digests/counts and explicit
exclusions. Reuse the runbook's private master-key fingerprint and require an
exact matching provisioned key before restore; never embed the key. Treat the
bundle as trusted private operator input, reject path traversal/symlink artifacts,
and do not export decrypted IAM material. Do not claim metadata checksums provide
authentication against a maliciously replaced bundle.

## Restore and activation sequence

1. Verify all artifact checksums, semantic reference coverage, supported format,
   required key fingerprint and target mapping before importing anything. Reject
   a missing key, metadata file, referenced loose blob, pack, sidecar or invalid
   segment. Reject nonempty target roots; do not remove user data to make them
   empty. A failed attempt stays quarantined and is not auto-activated.
2. Use `ESS_AUTO_START=false`, `ESS_PUBLIC_S3_ENABLED=false`,
   `ESS_WEB_ENABLED=false`, repair/scrub false and cluster data plane false.
   Validate actual process state as well as flags. Source and target processes
   must be isolated; the target must not reach the original metadata voters or
   data roots. Use one fresh standalone group or three fresh cluster voters.
3. For cluster, bootstrap all empty voters once using the existing ordered
   topology, then persist bootstrap=false for all subsequent starts. Import the
   verified logical backup exactly once through `Concord.Backup.restore/2`.
   Before activation, re-register every voter with the planned higher generation.
   New Erlang node names/internal URLs belong to this recovery topology.
4. Copy verified CAS/pack artifacts into empty target roots using bounded I/O.
   For cluster, install sufficient verified copies on the two data nodes to meet
   RF=2/W=2 before writer activation. Update location evidence only after hashing
   the destination bytes, using current node generations and revision/member
   comparisons. Do not merely rewrite generations on old ready locations.
5. Fence imported running jobs in bounded pages: compare each imported revision,
   increment its fencing token, clear owner/generation/lease and return it to
   pending with its payload/attempt history retained. Record the recovery marker
   and each transition so interruption can resume idempotently. Completed/failed
   results stay terminal. No new job state is necessary. Prove old-token renew,
   complete, fail and physical-cleanup authorization all reject. Reconcile or
   invalidate imported per-hash GC/cleanup locks under the recovery quarantine;
   never discard active protection and run deletion concurrently.
6. Start validation-only instances using an explicit complete worker map with
   multipart_gc/content_gc/cas_gc/packer/lifecycle/cross_cluster_replication/
   repair/scrub all false. Verify bytes, metadata, policies and restored signing
   credentials over real S3 HTTP. If loopback listeners are needed for validation,
   keep them isolated from external clients and gate writes until RF/index/schema
   and fencing checks pass. The standard public-listener safety guards remain.
7. Write a durable sanitized verification report bound to backup ID, target roots,
   generations, artifact/inventory digests and completed gates. Activation is a
   separate explicit operation that revalidates these bindings. Do not use a
   mutable report alone as authorization. Enable only reviewed worker categories;
   restoring cross-cluster configuration must not automatically resume historical
   outbound deletes/puts. Pending unknown operations require reconciliation.

Worker quarantine must survive process restart during import/verification. The
operator wrapper needs a persistent recovery marker plus startup configuration
checks, rather than a transient in-memory boolean or an undocumented environment
switch. If a new variable is necessary, document it in both README and AGENTS.

## Required real acceptance harness

Use a dedicated disposable runner, separate from normal standalone/cluster and
resource tests. Parent runtime ownership must be exclusive. Unique names/ports,
test-only secrets, process-group cleanup, bounded deadlines, retained per-node
logs and nonzero prerequisite failure are required. Never operate on production
paths. A small Python/Boto3 fixture can prepare/verify S3 state while Elixir
release-RPC helpers create bundles, fence state and inspect internal invariants.

1. Seed a standalone fixture with binary/raw gzip bodies, user/system attributes,
   checksum-enabled versions, delete markers, tags, policies/CORS/location,
   least-privilege IAM keys, an open multipart upload with parts, a completed
   multipart result and representative pending/running/terminal jobs. Include a
   real packed historical object and a shared hash. Keep the expected hashes,
   version IDs, nonsecret attributes and intended job/multipart policy in a
   separate small fixture file; do not store credentials there.
2. Quiesce, create and verify a bundle. Then resume only the disposable source,
   modify/delete originals and demonstrate source no longer matches the backup
   fixture. Stop every source process before recovery.
3. Run restore in a container or mount namespace that exposes only the read-only
   bundle, test fixture/separately provisioned keys and fresh destination roots.
   Do not mount source roots. Retain mount inspection and failed source-path
   access as isolation proof. Renaming the original directory by itself is not
   proof that originals are inaccessible.
4. Restore standalone, including quarantine-restart coverage. Verify every saved
   body SHA-256/version/marker/tag/header/checksum, IAM allow/deny behavior and
   bucket configuration over signed independent Boto3 requests. List/complete
   the retained upload and replay completed-upload internal recovery without a
   duplicate version. Assert exact job/token policy and no outbound/destructive
   canary activity before explicit activation. Measure restore/preflight/verify
   duration and record the backup consistency point. This is A36 evidence.
5. Repeat with a cluster-origin bundle and a separate clean fixed three-voter,
   two-data-node recovery topology. Require strictly higher data generations,
   fresh metadata roots, source-network isolation, restored RF2/W2 copies,
   successful voter registration and endpoint reads. Exercise stale source job
   tokens and old-generation replica/cleanup operations; each must reject.
   Observe outbound HTTP canaries and file/metadata inventories across import,
   validation and quarantine restart. Explicitly activate reviewed recovery work
   and assert only then that allowed work runs. This is A37 evidence.
6. For both modes, alter a copy of the bundle independently for each negative
   case: missing/corrupt logical metadata, loose body, pack file, sidecar/index,
   truncated segment, absent/wrong master key, unsupported format, nonempty
   destination and invalid generation/topology mapping. Require failure before
   activation, preserved originals/target quarantine and a sanitized diagnosis.

## Scoped checks before acceptance

Unit/contract coverage should exercise manifest/reference coverage, streaming
hash/copy failure and path handling; quiescence/empty-root guards; master-key
binding; idempotent revision-fenced job reset and ambiguous results; stale-token
rejection; validation marker binding; process-state checks and interrupted
restore re-entry. A missing original path or a passing restore helper alone is
not A36/A37. The real isolated standalone and three-voter lanes must pass, with
commands, exit status, client/toolchain versions, backup IDs, isolation proof,
timing and verification output retained in `docs/reports/s3-hardening/`.

The initial source-only investigation established no blocker. The subsequent
fresh-VM recovery run reproduced the checksum portability blocker #95 documented
at the top of this handoff; that verified result supersedes this initial scope.
The coordinator integrated the concrete wrappers/startup marker and began actual
acceptance. A36 is blocked before import on #95; A37 has not completed and depends
on the same native verification defect. Neither acceptance scenario is passed.
