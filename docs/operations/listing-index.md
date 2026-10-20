# Listing-index conversion and rebuild

Task: ESS-S3-HARDENING-001, P3.1. These are explicit offline operator operations.
The presence of this runbook does not establish A28/A29 acceptance. Consult the
task's verification evidence for the checks actually executed.

New buckets publish an empty ready descriptor atomically. Existing buckets with
no descriptor return `:listing_index_migration_required`; building and validating
buckets return an availability error. A ready descriptor is authoritative over
retained v1 object records, including persistent v2 deletion barriers. No request
silently scans or normalizes legacy history.

## Preconditions and safety boundary

Use only the intended disposable/test roots while validating these procedures.
For an operator conversion, first deploy a compatible writer binary on every
participant. Mixed older binaries and direct Concord mutations bypass the gate
and are unsupported. Preserve the documented fixed three-voter topology,
distributed node identities, cookie, data-plane guards, and configured RF/W.

Quiesce listeners, object producers, notifications and storage workers on every
source with `ExStorageService.Operations.Backup.quiesce/1`. The existing backup
procedure must produce a verified metadata-and-blob bundle before conversion.
Keep its source contexts, master-key provisioning and artifact protections. Never
include credentials, owner tokens, snapshot contents or object metadata in logs.

`ListingIndex.begin/2` verifies the bundle and checks actual source processes,
using RPC for remote sources. Default preflight requires exactly the configured
source node set. Its transaction compares the current bucket and descriptor
revisions and installs the per-bucket gate. Started writers using an earlier
descriptor cannot commit through that fence. After this succeeds, compatible
services may restart: another bucket may mutate, but this bucket's namespace
writes, MPU transitions and deletion remain gated until cutover.

Every converted non-marker version passes the existing migration identity,
descriptor, location and actual CAS checksum/size checks. Peer probes verify
source-node CAS roots through RPC. Fewer verified source locations than the
build's target RF blocks that version's publication; metadata cannot invent
durability. Existing descriptor RF is never reduced. Descriptor/location
operations, normalized version, index changes and checkpoint commit together.
Validation rechecks the evidence and requires the committed descriptor and
ready location metadata. The target RF is frozen at begin and cannot be below
the source configuration. Repair missing replicas through the documented repair
procedure before retrying; do not lower RF/W or substitute unverified probes.

## Release RPC entrypoints

Use `/app/bin/ess remote` for an interactive operator shell, or equivalent
`/app/bin/ess rpc` expressions. These entrypoints require neither runtime Mix
nor repository source files. Bind the real source contexts and verified backup
path using the backup runbook; in a cluster the list includes all three nodes.
The following Elixir illustrates the call sequence, not shell interpolation:

```elixir
alias ExStorageService.Operations.ListingIndex

opts = [sources: source_contexts, backup: verified_backup_path]
{:ok, build} = ListingIndex.begin(bucket, opts)
token = build.owner_token
{:ok, _} = ListingIndex.capture(bucket, token, private_manifest_path, opts)

# Repeat resume until its returned phase is :validating.
{:ok, progress} = ListingIndex.resume(bucket, token, private_manifest_path, opts)

# Repeat validate until its returned phase is :validated.
{:ok, progress} = ListingIndex.validate(bucket, token, private_manifest_path, opts)

{:ok, %{phase: :ready}} = ListingIndex.cutover(bucket, token, opts)
{:ok, %{ready: true}} = ListingIndex.readiness(bucket)
```

Supply the same source contexts when resuming after process or leader restart.
`batch_size: 1..100` bounds replay/validation work per call; the default is 100.
Each source publication persists its byte offset, ordered lower bound, counts
and chained digest in the same transaction. A failed compare changes neither
publication nor checkpoint. Each explicit retry has a new transaction attempt
identity; an ambiguous response resolves that attempt's original identity.
An unresolved transaction is an error, never a successful checkpoint claim.

`ListingIndex.status/2` and `readiness/2` return sanitized descriptor diagnostics.
To recover an interrupted owner, call
`ListingIndex.takeover(bucket, previous_token, opts)` and securely retain its
new token. The old token cannot checkpoint, validate or cut over. Direct
`Metadata.ListIndex.Backfill.status/2` exposes operator progress and its owner
token; keep that result private.

## Source manifest and bounds

Capture intentionally calls one linearizable Concord global export. With the
locked Concord API this materializes all live metadata, including unrelated
buckets. Budget and measure that peak separately from request memory. It is
never evidence of a bounded HTTP listing. Capture performs external sorting with
1 MiB payload runs and at most 16 merge inputs; oversized individual legacy
records are capped at a 16 MiB frame and fail explicitly. File-path bookkeeping
and the offline global snapshot grow with the source inventory. A frame's SHA-256
and length protect each record; the accepted final file has a persisted whole-file
digest and byte length. Files are mode 0600 and temporary sort directories 0700.

The retained `<path>.raw`, `<path>.source` and final manifest contain sensitive
metadata. Do not place them under public static roots or check them into Git.
Keep all artifacts with the verified backup. Only a fully written, synchronized,
atomically published final manifest can be bound to the build. Interrupted
capture remains `:capturing`; restart capture at a fresh private path. Partial
or existing files are never silently accepted or overwritten.

Reconciliation includes every `obj_ver:` record, even if absent from a legacy
version list. Legacy-only objects stage all immutable versions in bounded
transactions before publishing the selected head, so histories above 256 rows
are supported without request normalization. Original v1 records remain intact.
Existing v2 heads and deletion barriers suppress retained stale legacy records.
Malformed/conflicting identities stop capture with the exact source key and
reason. Both MPU namespaces are reconciled together; conflicting same-number
parts stop capture, rather than choosing one namespace and losing the other.

Validation verifies each normalized record and exact tree terminal, latest and
marker semantics, active and terminal MPU outcomes, part counts and descriptor
epochs. Part-tree validation keeps a radix path and visits at most the protocol's
10,000 parts for one upload; it does not materialize them. The final object,
version and active-upload tree scan proceeds through durable bounded steps and
compares actual reachable terminal counts with manifest and descriptor counts.
Unchanged corrupt nodes fail closed. Cutover compares the owner/build revision,
bucket generation, descriptor and accepted manifest before releasing the gate.

## Failure and rollback

Changed source revisions, altered manifests, old owner tokens, generation
replacement, metadata unavailability, missing replicas and validation errors
leave readiness false. Preserve the exact returned error and sanitized progress;
do not describe unexecuted or blocked validation as passed. On an interrupted
replay or validation, resume the same accepted manifest and checkpoint. A new
capture identity cannot resume an existing replay.

Old index epochs and the previous descriptor remain retained. Before cutover,
restore the verified source metadata/blob backup into isolated roots if rollback
is required. Do not simply reinstall an old descriptor: normalization may already
have written versions and new-epoch MPU descriptors. After cutover, use a fresh
gated rebuild or the isolated backup/restore procedure. This implementation does
not reclaim historical index epochs or delete v1 records.
