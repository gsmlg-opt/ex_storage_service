# P3.1 bounded metadata index — proposed, unimplemented

Task: ESS-S3-HARDENING-001 / P3.1 / A28–A29. Architecture-only preparatory
assignment, `astra_gate: PROJECT_ARCHITECTURE`. This document does not mark any
implementation, resource measurement, or acceptance scenario passed. Phase 3
implementation remains gated on the coordinating agent's Phase 2 verification.

Inspected checkout: `main`, `3477bc540c3732dba3d47a7e08779783f9be25fc`, with
concurrent, uncommitted Phase 1/2 changes. Source references below describe that
working tree on 2026-10-18, not just HEAD. This assignment changes only this file.

## Recommendation and constraints

Use a per-bucket compressed byte-radix tree stored in ordinary Concord records.
Read nodes by exact current key. Change tree nodes, a small bucket index root,
object heads/versions, operation results, and existing intent/outbox/quorum
records in the **same existing Concord transaction**. Preserve raw S3 key order,
bucket generation fencing, null-version replacement, delete markers, and legacy
deletion barriers. No ETS authority, new database, dependency patch, or weakened
read/write quorum is required.

Use mutable tree nodes and bracket each page with two linearizable reads of the
same root revision. Retry a changed root within a fixed budget; return a retryable
availability error if a coherent page cannot be obtained. This is a deliberate
availability tradeoff under sustained writes, not permission to return stale or
partially assembled listings. Existing `BucketLifecycle.guard/3` already
serializes mutations through the bucket record; this design does not claim to
remove that contention.

The minimum hosted object index has two ordered trees: visible current objects
and all retained versions/delete markers. The same primitive should cover active
multipart uploads and ordered part numbers to remove the existing public multipart
listing scans. A small bucket-name catalogue is a separate integration hook for
`ListBuckets`; otherwise that endpoint still scans unrelated metadata. These
hooks are named explicitly below rather than included in the A28 evidence by
assumption. Gateway upstream-authoritative listing continues to use upstream
pagination; an ESS hosted index must not turn cache residency into an upstream
object inventory.

Initial migration/rebuild uses a durable **per-bucket mutation gate** and a
checksummed offline source manifest. Same-bucket namespace mutations are blocked
until cutover. Safe reads and operations in other buckets remain permitted.
Online backfill concurrent with same-bucket writes is not proposed. The plan
allows offline rebuild; testing concurrent permitted operations must exercise
these exact restrictions.

## Evidence from the locked code

The lock pins Concord and its companion family to 3.0.6.

| Source / symbol | Observed behavior and consequence |
|---|---|
| `deps/concord/lib/concord/state_machine/core.ex`, `do_query({:list, ...})` | Builds `records_at_revision`, filters and sorts all matching state before taking `limit`. `Backend.Concord.list_page/4` is not a bounded ordered read. |
| Same file, `do_query({:prefix_scan, ...})` | Enumerates `state.store`; a narrow prefix does not bound work. |
| Same file, `find_record_at_revision/3` | Filters `state.history`; historical point reads can also scan global history. Do not use `revision:` for tree traversal. |
| Same file, `do_query({:get_record, key})`; `Concord.KV.get/2` with `metadata: true` | Current exact-key lookup uses `Map.get(state.current, key)`. This is the usable primitive. |
| `metadata/backend/concord.ex`, `transaction/2`; `Concord.Txn.commit/2` | Native compare/success/failure transactions and idempotency resolution already exist. Keep them. |
| `metadata.ex`, `list_objects/2` | Uses a global snapshot, now with keyed version lookup. The quadratic lookup repair is only an intermediate improvement. |
| `storage/version_listing.ex`, `list/2` | Builds a coherent global snapshot and preserves the Phase 2 page/filter contract. Replace its storage traversal, not its S3 semantics. |
| `metadata/legacy_object.ex`, `do_prepare/4` | Every new v2 head can trigger a legacy prefix scan. A completed migration marker must remove this from ready-bucket mutation paths. |
| `metadata/object_commit.ex`, `do_delete_version/9` and `list_versions/3` | Version deletion materializes history to choose a replacement. Use exact parent lookup and a bounded first-version seek. |
| `metadata/bucket_lifecycle.ex`, `delete/3` | Global emptiness scan includes retained legacy records. After validated cutover, use authoritative live-version/active-upload counts while retaining the generation and revision comparisons. |
| `storage/multipart.ex`, `list_uploads/2`, `list_parts/3` | Public pagination still starts with prefix scans. Register upload/part index hooks rather than declaring all listings bounded from object tests alone. |

Only the root `AGENTS.md` was found outside dependencies/build inputs. Agent Note
recall returned the three existing HTTP transport notes and no index/migration
decision; no shared note was changed. There is no prior index design inferred
from those unrelated notes.

## Records and ordering

Add key constructors in `Metadata.Keys`; do not change existing v1/v2 keys.
Illustrative namespaces (the coordinator may choose equivalent names):

```text
ess:v2:list_index:<encoded_bucket>:<encoded_generation>:root
ess:v2:list_index:<encoded_bucket>:<encoded_generation>:<epoch>:node:<node_id>
ess:v2:list_index:<encoded_bucket>:<encoded_generation>:build
```

The root is a fixed-size descriptor, never a list of keys:

```elixir
%{
  schema: 1, bucket_generation: generation, epoch: epoch,
  state: :ready, legacy_complete: true,
  current_root: node_id_or_nil, versions_root: node_id_or_nil,
  uploads_root: node_id_or_nil,
  visible_count: n, version_count: n, active_upload_count: n,
  commit_sequence: n, source_manifest_digest: digest_or_nil
}
```

`commit_sequence` is diagnostic. The actual consistency proof uses Concord's
`mod_revision`, epoch, and bucket generation. Empty new buckets create their ready
root atomically with `bucket:<name>`; an absent root on an existing bucket means
**migration required**, never empty. Generation namespaces prevent an old cursor,
worker, or index root from accessing a recreated bucket.

Each compressed radix node contains `{schema, label, terminal, children}`:

- `label` is the nonempty byte segment from its incoming edge; a root may have
  an empty label. Children are a sorted, unique byte-to-16-byte-ID collection,
  at most 256 entries. Compare bytes, not locale-aware strings.
- `terminal` is absent or a bounded listing summary and exact metadata pointer.
  Store key/version identity, ETag, size, timestamps, delete-marker/latest flags,
  and storage-class fields actually returned by listings. Do not copy tags,
  arbitrary user metadata, or object bodies into nodes.
- No non-root node may have neither terminal nor children. A nonterminal unary
  node must be merged with its child. Removing entries must prune empty paths;
  retained empty nodes would make sparse traversal unbounded.
- Start with a 32 KiB serialized node ceiling, verified against the existing
  maximum accepted key/version/timestamp lengths before implementation. A valid
  maximum-length record must fit without tightening the public API. If it does
  not, revise the representation before shipping; do not reject previously valid
  objects to meet an arbitrary node ceiling.
- Node IDs are opaque fixed-size random IDs. Check absence for new IDs. Read and
  mutation overlays deduplicate IDs and operations; never write the same node
  twice in one final transaction with contradictory values.

An insert splits only the first mismatching compressed edge. A delete removes
the terminal and merges/prunes as necessary. Planning may read an entire path,
whose depth is bounded by encoded key length; it never enumerates a subtree.
All dirty records are published in one transaction. Keep a bounded traversal
stack and request-local cache; do not retain all previously visited nodes.

Current tree keys are raw object-key bytes. Base64 is for safe **storage record
names**, not ordering.

Version tree keys encode the tuple:

```text
(object_key ascending, latest_rank ascending, created_at descending, version_id descending)
```

`latest_rank` is 0 only for the head's selected version, otherwise 1. This
preserves the working Phase 2 `VersionListing.sort_versions/1` behavior, including
the explicit head taking precedence over timestamps. Use exactly its canonical
timestamp accessor (`created_at`, then the existing `updated_at` fallback),
including its validated legacy default, rather than reordering legacy rows.
Use a tested memcomparable
tuple encoding, for example ascending bytes `b -> <<1,b>>`, terminated by `<<0>>`;
descending is the bytewise complement of that entire encoded component. A fixed
rank byte separates the first and remaining components. Test shorter strings,
zero/255 bytes in the internal codec, Unicode UTF-8 keys, colons, literal plus,
percent escapes, equal timestamps, and prefix boundaries. The full encoded key
is bounded by validated source-field lengths, not bucket size.

Changing the head moves at most one previous entry from rank 0 to rank 1 and the
selected replacement/new entry to rank 0. Null replacement deletes its previous
tuple and inserts the replacement; it never creates a second null version.
Deleted current objects disappear from the current tree while retained history
stays in the version tree. A final deletion barrier remains in the original
head record even when both index trees have no entry for that object.

Upload keys are `(object_key ascending, upload_id ascending)` with the final
ordering matched to Phase 2's tested marker contract. Per-upload part trees use
fixed-width unsigned part numbers; keep their root/revision on the upload record
or in a descriptor fenced by that record. Parts are bounded to the existing
10,000-part protocol limit, but scanning every part is still unnecessary for a
small `ListParts` page. Completed/aborted upload outcome records remain durable
while their entries are removed from the active-upload tree.

## Transaction integration

Introduce a core-only `Metadata.ListIndex` facade and pure
`Metadata.ListIndex.Radix` planner. Suggested interface:

```elixir
prepare_mutation(backend, bucket, generation, delta, opts)
# {:ok, %{compare: [...], success: [...], root: next_root}} | {:error, reason}
page(backend, bucket, generation, kind, query, opts)
# {:ok, bounded_page} | {:error, reason}
status(backend, bucket, generation, opts)
```

`delta` states exact old/new head, version, or upload identities from already
observed records. It is not an arbitrary callback with hidden writes. Reuse the
existing backend injection for deterministic fault tests. The facade never
commits an object publication independently.

For each logical mutation:

1. Read the current bucket and root with linearizable exact reads. Reject a
   mismatched generation, unknown schema, missing root, or non-ready state.
2. Plan tree changes from the observed exact records. Include root and observed
   node revision comparisons plus absence comparisons for new IDs. Preserve
   every existing precondition, source-identity, version-policy, upload token,
   bucket, intent, replica, and outbox comparison.
3. Append tree puts/deletes and the updated root to the same success branch as
   metadata publication. Keep failure empty. Update counters by exact old/new
   membership, not by request type; retries and part overwrites must not increment
   counts twice. Root revision changes for every index-visible mutation.
4. Use ObjectCommit's existing transaction fingerprint/idempotency outcome
   handling for the **whole** augmented specification. Unknown outcome recovery
   resolves the original transaction/operation; it never repairs the index in a
   second best-effort write. A new compare attempt must replan from fresh state.

Specific production hooks:

| File / hook | Required integration |
|---|---|
| `metadata/object_commit.ex`, `do_commit_new_version/11` | Current/version deltas; demote former latest; preserve atomic `MultipartCommit.completion_operations/5`. |
| Same file, `do_delete_version/9` | Remove exact version; select the stored parent by exact key if it survives, otherwise seek first remaining chronological version. Promote replacement and update current entry atomically. No `list_versions/3` in this request path. |
| Same file, `update_tags/7` | Preserve its existing fences. If the page reads metadata instead of entirely self-contained summaries, touch the root on these changes too; otherwise prove tags are not a listing dependency. Do not casually rewrite sort fields. |
| `metadata/bucket_lifecycle.ex`, `create/1`, `guard/3`, `delete/3` | Create descriptor; enforce durable build gate; use counts only after validated cutover and compare root plus bucket revisions before deletion. Active `:completing` uploads remain occupied. |
| `metadata/legacy_object.ex`, `prepare/4` | A ready, fully normalized bucket requires no prefix scan, including a never-before-seen key. Incomplete migration returns a clear availability/migration error; no hidden slow fallback. |
| `metadata/migration.ex` | Integrate staged normalization/index building, preserve existing RF promotion and source validation. Existing `list_page` loops are offline work, not bounded online-scan evidence. |
| `metadata.ex`, `list_objects/2`; `storage/version_listing.ex`, `list/2` | Delegate traversal; preserve page shape and current S3 validation/encoding behavior. Keep snapshot helpers only for offline validation/test oracles. |
| `storage/multipart.ex`; `metadata/multipart_commit.ex` | Initiation/abort/completion update active-upload membership and counts. Part writes update the part tree in their existing fenced transaction. Freeze/completion/retry semantics from Phase 2 remain intact. |
| `metadata.ex`, `put_object_meta/3`, `delete_object_meta/2`, and direct legacy callers | Audit and route production mutations through the guard/index transaction, or retain them as explicitly offline-only operations. Their current direct Concord writes bypass the proposed gate. |
| Core `operations/` and existing Mix/operator task entrypoints | Add status, begin/resume/validate/cutover RPC functions; runtime release support must not depend on Mix/source. |

Also audit lifecycle, migration, cloud-cache reconciliation, and any direct head
or version writer. An unregistered mutation invalidates the consistency proof.
Do not count cache-only legacy records as hosted live versions without applying
the established gateway ownership rules. Blob-location/repair changes that do
not alter listing-visible metadata need no index write.

## Page traversal, prefix skipping, and consistency proof

Implement exact seek, lower-bound seek, successor, and subtree skip against the
compressed tree. Cache only a bounded path/frontier; cap the public page at the
existing 1,000 result limit and retain at most one lookahead result.

For a delimiter request, find the first matching descendant, derive its common
prefix, emit the prefix once, then seek directly to the bytewise successor of
that **raw prefix**. Compute successor by incrementing the last non-255 byte
and truncating the suffix; no successor means end-of-range. Convert that bound
through the version tuple codec when traversing history. Do not repeatedly call
`next` to discard descendants. A group containing 100,000 keys or versions must
cost one emitted result plus bounded seeks, not 100,000 visited terminals.

For ListObjects v1/v2, use exclusive marker/start-after bounds and preserve
common-prefix filtering and combined key counts. `max_keys=0` returns an empty
page after validation without descending the tree. Preserve the Phase 2 opaque
token validation; add generation/format binding without making ordinary object
mutations invalidate every token. Tokens carry a logical lower bound, not node
IDs. Bind bucket, prefix, delimiter, and relevant query settings; never trust a
client-supplied physical pointer. Epoch changes may resume by logical bound if
format semantics are unchanged; otherwise fail explicitly.

For ListObjectVersions, resolve `(key_marker, version_id_marker)` by exact
version and head reads, derive its tuple rank, and seek strictly after it. An
absent required marker follows the Phase 2 tested error behavior. A key marker
without a version marker skips the entire object-key group. Never scan the
key's full history to find the marker or calculate `IsLatest`.

Each successful page must follow this protocol:

1. Read bucket identity and ready index root R0 linearly. Retain generation,
   epoch, and `mod_revision`.
2. Read current tree nodes and any bounded referenced metadata by exact keys.
   Validate shapes and enforce depth, byte, visited-record, and deadline bounds.
3. Read root R1 linearly, then confirm bucket identity still exists. Accept only
   if root identity/state/revision equals R0 and bucket generation matches.
   Missing nodes, unexpected pointers, or integrity errors also require this
   closing check: a changed root permits retry; an unchanged corrupt root fails
   closed and affects listing readiness.

Every relevant mutation changes the root in the same atomic transaction.
Therefore unchanged R0/R1 means all page dependencies were stable during the
read interval. Linearizable reads prevent a lagging replica from manufacturing
an unchanged old revision. A concurrent commit after the closing check is valid:
the page linearizes before that commit. This gives a coherent **single page**,
not a snapshot spanning multiple HTTP requests, consistent with ordinary S3
pagination under concurrent mutation.

Use at most 4 page attempts and the existing request deadline, with small bounded
backoff. Never stream partial XML before the closing check. Exhaustion returns
the existing retryable service-unavailable family, with no successful stale page.
Do not weaken consistency to improve benchmark latency. Historical `revision:`
reads, `get_all`, `prefix_scan`, `scan`, and `list_page` are forbidden inside this
ready-path implementation.

## Backfill, cutover, interruption, and rollback

State machine: `missing -> building -> validating -> ready`; failed validation
stays gated with a resumable error. Rebuild of a ready tree first gates mutations
and captures its existing epoch for recovery. The durable build record contains
bucket generation, new epoch, source snapshot/manifest identity and digest,
fenced owner token, phase, current manifest shard/offset, counts, and validation
digest. Keep checkpoint size independent of bucket size.

1. **Preflight:** all writers on all participating nodes must run the new guard.
   Mixed old writer binaries and arbitrary direct Concord writes are not safe.
   Quiesce namespace/background mutations for this bucket, fence new commits
   using the durable gate, and drain already-started operations. Existing public
   cluster guards and RF/W remain unchanged. Take a verified metadata/blob backup
   before conversion, following the existing migration/runbook requirements.
2. **Capture:** explicitly offline, export a coherent source snapshot after the
   gate. The locked API may materialize global metadata during export; disclose
   its peak memory and never run it inside an HTTP request or present it as a
   bounded scan. Produce length-delimited, checksummed, ordered manifest shards
   on disk with bounded external-sort buffers. Persist capture identity only
   after a complete durable manifest. Interrupted capture is restarted; partial
   files never become an accepted source. Sensitive backup metadata stays in
   protected disposable/operator storage, not verification logs.
3. **Reconcile the source:** use v2 heads, including deletion barriers, to
   suppress retained legacy values. Validate all `obj_ver:` records, including
   ones absent from legacy lists; never silently omit them. Corrupt/conflicting
   identities stop cutover with an exact report. Normalize legacy-only objects
   to v2 while retaining originals. For histories beyond the current 256-record
   request normalization bound, use bounded offline chunks and durable per-key
   progress; publish that key's head only after all versions are verified.
   Keep list readiness false during partially staged normalization, because
   current snapshot fallback semantics can observe staged v2 versions.
4. **Build:** consume the manifest in bounded batches; use exact metadata reads
   and revision checks to reject unexpected source changes. Commit tree changes
   and the next checkpoint together, comparing build owner token/epoch/revision.
   Put retries and response loss through idempotency resolution. A resume uses
   the same manifest digest; changed source/generation cannot resume it.
5. **Validate:** stream the manifest and tree in order; compare all normalized
   identities and listing summaries, latest/null/marker semantics, counts, tree
   shape, ordering, and referenced metadata. Check deletion barriers and active
   uploads explicitly. Retain a digest/report. Validation may visit all entries
   as an explicit maintenance operation, in bounded batches; it cannot replace
   the request-path bounded-work test.
6. **Cutover:** one transaction compares the build fence/checkpoint, bucket
   generation, old descriptor revision, and validated manifest digest; installs
   the ready roots/counts/`legacy_complete`; releases the mutation gate. All data
   nodes obtain readiness from the committed descriptor. There is no per-node
   asynchronous projection to catch up.
7. **Restart/rejoin:** retain and report building/validating state across process
   restart, leader change, and voter rejoin. A takeover changes a fenced owner
   token; the old owner cannot checkpoint or cut over. An isolated/stale voter
   must fail linearizable reads instead of serving cached index pages. A ready
   node probes descriptor plus roots through Concord; missing/corrupt pointers
   fail closed. Do not bootstrap an existing cluster again.
8. **Rollback:** before cutover, keep the prior ready epoch and source backup.
   Reverting staged normalization requires explicit operator recovery, not
   deleting the retained v1 records. After any post-cutover writes, the old index
   epoch is stale and must not simply be selected again. Keep the new writer
   contract and rebuild, or quiesce and restore the verified matching metadata
   and blob backup. Returning to pre-v2 binaries after v2 writes already requires
   restore; this design does not loosen that rule. Keep old epochs/artifacts
   until the documented rollback window ends; cleanup is explicit maintenance.

During rebuilding, existing immutable-object GET/HEAD and authenticated reads
that do not mutate cache/namespace may continue. LIST may return unavailable
until validated readiness; no successful partial index or implicit full-scan
fallback. Operations in unrelated ready buckets proceed. Tests must verify
same-bucket mutation rejection **before** staging/upstream effects where possible,
and again at metadata commit for races. Gateway cache fills and maintenance
writers need the same audit, not an exemption merely because they began as GET.

## Alternatives and tradeoffs

| Alternative | Assessment |
|---|---|
| Concord `list`/prefix with smaller limits | Rejected for steady state: hidden global scans remain. |
| One bucket key list or fixed hash shards containing lists | Rejected: hot/large buckets or adversarial prefixes still cause growing materialization; hashing loses S3 order. |
| B+tree pages | Feasible; fewer network reads and better packed space. More split/merge/byte-occupancy complexity for variable-length S3 keys. Prefer it only if measured radix overhead cannot meet declared latency/resource goals. |
| Immutable copy-on-write nodes | Easy coherent root snapshots, but reader pinning/retention and safe reclamation add another lifecycle protocol. Not needed for the minimum design. |
| ETS ordered projection | Requires an applied-revision barrier, durable replay, rejoin/rebuild handling, and a proof for every writer. Adds a second consistency mechanism without removing the durable-index need. |
| Historical Concord point reads | Rejected for bounds with this lock: history fallback scans globally. |
| Online dual-write backfill | Possible with a durable mutation log and bounded replay/checkpoint protocol; larger scope. Quiesced per-bucket rebuild is the proposed first delivery. |

The radix design may perform many quorum reads for long keys and high fanout.
Instrument before accepting it. Do not add a misleading “batch” API that hides
a global scan. A bounded exact-key batch could be a later backend extension only
after checking the actual API and demonstrating its barrier semantics.

## Bounded implementation work packages and acceptance

These are handoff packages for Sol after Phase 2; no package is implemented here.

1. **I1 — primitive and codec:** new `metadata/list_index/radix.ex`, facade, key
   constructors, and focused property/contract tests. Verify exact lower bounds,
   insert/split/delete/merge, maximum-length inputs, prefix skipping, and node
   byte limits. Fake backend rejects every scan and records reads/bytes. A pure
   map/sort oracle is test-only.
2. **I2 — atomic writer integration:** ObjectCommit, BucketLifecycle, LegacyObject,
   migration guard, and direct-writer audit. Verify create-only/If-Match races,
   null transitions, marker removal, tags, deleted legacy suppression, and atomic
   MPU finalization with injected compare failures and lost transaction responses.
   Assert no head/version/index/result combination can be partially committed.
3. **I3 — public readers:** Metadata/VersionListing plus S3 listing/token adapters;
   upload/part readers and writer hooks; bucket catalogue if included in the
   bounded public-listing claim. Preserve Phase 2 API tests and independent
   Boto3/AWS CLI/mc pages. Verify late-page prefix groups and version markers.
4. **I4 — operator state machine:** core operations RPC, existing migration
   integration, disk manifest tools, readiness, and runbook. Crash after every
   checkpoint/cutover boundary; resume without duplicates or skipped history.
   Retain rollback artifacts. Test permissions/gates rather than trusting an
   operator's “writers stopped” flag alone.
5. **I5 — actual A28/A29 evidence:** isolated standalone and real three-voter
   fixtures at 10k and 100k keys, unrelated metadata, and retained history.
   Exercise failover/rejoin and concurrent permitted operations. Publish measured
   bounds, p50/p95, index bytes/record count, and exact commands/logs. Provide an
   optional 1M profile without a pass claim unless executed.

Instrumentation and acceptance must include both request and dependency work:

- Instrument `Backend.get`, tree node decoding, visited terminals, seek count,
  serialized bytes returned, page heap/binary allocation, retries, and duration.
  Record index node count, serialized index size, source-metadata size, and
  Concord process memory separately from request allocations. Concord's resident
  metadata naturally grows with N; a single list must not allocate a fresh copy.
- In contract tests, fail any ready-path call to `get_all`, `prefix_scan`, `scan`,
  `list_page`, or revision reads. In the real backend lane, trace/query-count the
  actual Concord commands to expose hidden wrappers; `:get_record` is expected.
  Check dependent serializers/adapters as well as the facade.
- Declare structural budgets before running: page payload at most `P+1`, node
  size at most 32 KiB after compatibility validation, cache/frontier at most
  `D+2` nodes, at most 4 attempts, and conservative per-attempt node reads
  `4 * (P+1) * (D+1) + 8`, where D is the maximum encoded input length validated
  by I1, not a value derived from N. Count actual decoded bytes as well as reads.
  Derive/check in the numeric heap envelope from these constants and the chosen
  fixture maximum key length. This deliberately loose worst-case bound must be
  complemented by fixed-query measurements as N changes.
- Run page sizes 0, 1, 10, 100, and 1000; first/middle/final pages; absent prefixes;
  deep keys; mixed special keys; 10k/100k descendants under one common prefix;
  many versions of one key; and another bucket/unrelated namespace grown tenfold.
  Prefix descendants must be skipped structurally. Adding unrelated metadata
  must not increase visited index records for an otherwise identical query.
- Generate fixtures with the same transaction planner/invariants as production,
  using lightweight valid metadata and shared content where appropriate. A
  fixture that bypasses index transactions is not acceptance. Warm startup and
  backfill separately, retain their costs, then measure repeated request pages.
- Race a page against overwrite/delete/tag/head changes. It must return one
  coherent observed generation or an explicit retryable failure. Inject missing
  nodes during a changing root versus a stable root. Test response-loss replay,
  generation replacement, invalid tokens, and uninterrupted page bounds under
  repeated root changes.
- A29 must cover killed/resumed build, changed/corrupt manifest, obsolete owner,
  leader election, stopped/rejoined voter, stale-node LIST, post-cutover writes,
  another bucket's concurrent writes, and same-bucket rejected writes. Verify
  retained v1/v2 records and no restoration of deleted legacy keys. No production
  roots, deployments, publication, or dependency modifications are involved.

## Decisions and remaining verification

Coordinator acceptance is needed for the proposed quiesced per-bucket rebuild,
compressed radix representation, and retry-on-root-change availability tradeoff.
These are implementation design choices within the requested P3.1 scope, not a
request for another user phase approval. If online same-bucket writes during
backfill are required, return for a different design instead of silently adding
an unfenced dual-write path.

Source inspection and architectural handoff are complete. Node size, transaction
size/latency, runtime memory envelopes, production writer coverage, and all A28/A29
executions remain unverified. No Mix commands, services, benchmarks, or acceptance
tests were run for this architecture assignment.
