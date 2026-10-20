# ESS: Three-Phase S3 Hardening and Completion Plan

**Repository:** `gsmlg-opt/ex_storage_service`\
**Prepared:** 2026-10-13\
**Review baseline:** `42a9fe2b41faa82cf10c3f07840b890be18b876d` (`v0.6.7`)\
**Status:** Execution specification; implementation and acceptance results are not yet verified.

## 1. Objective and completion boundary

Complete all three phases below, including implementation, regression tests, independent client tests, documentation, migration safeguards, and recovery evidence. This is one delivery objective, not a request to stop after Phase 1 or produce another roadmap.

The outcome is a dependable, explicitly scoped S3-compatible Elixir object store: correct operations and authorization, verified upload integrity, predictable concurrency semantics, bounded request-path resource use, and independently demonstrated recoverability.

“Done” means every mandatory task and every required acceptance scenario in `ACCEPTANCE_MATRIX.md` has implementation evidence and a passing result. A missing prerequisite, unexecuted required test, or unresolved correctness failure is **blocked**, not passed. Retain useful completed work and report the exact remaining blocker; never convert a requirement into an optional item to claim completion.

The baseline review was static. First inspect the actual checkout and reproduce each suspected defect. An already-correct implementation should be retained and protected with tests, not rewritten to match an old review.

## 2. Constraints to preserve

Follow the repository's current `AGENTS.md` and applicable nested instructions. The inspected instructions require the existing umbrella boundaries, Concord/VSR metadata, content-addressed files, Plug/Bandit S3 API, and DuskMoon administration UI. They also require `http_fetch` for production outbound requests and preserve explicit cluster safety restrictions. [R1]

- Keep Elixir/OTP and functional domain boundaries. No Ecto, PostgreSQL, replacement object store, or replacement consensus implementation. Schema evolution here means versioned Concord records/indexes, not Ecto migrations.
- Reuse `ObjectService`, `LocalCAS`, existing transactions, operation intents, fenced jobs, cluster coordinators, and recovery mechanisms. Add no new dependency by default; investigate existing capabilities first.
- Keep core independent of Plug, Phoenix HTTP responses, and the cluster transport application. Use injectable boundaries for I/O and faults rather than mutable global test configuration.
- Preserve standalone and embedded operation and fixed, ordered three-voter cluster operation. Do not replace VSR membership with service discovery or change RF/W defaults to make tests pass.
- Preserve the existing guards on cluster cloud-backed writes, packing, lifecycle workers, and metadata-only nodes unless the guarded capability is fully implemented and independently verified. This objective does not require lifting these guards.
- Production object transport remains bounded, raw-byte `http_fetch`, with explicit lengths where required, `decode_body: false`, and no redirects on signed requests. Independent test clients may use existing test-only libraries. [R1]
- Never modify real buckets, live data roots, production credentials, or a running production cluster. Use disposable roots, unique test node names, and local test endpoints. Do not publish packages, deploy, force-push, or change unrelated repositories as part of this goal.

## 3. Invariants

**I1 — Operation identity:** A request resolves to exactly one supported operation or a deliberate error. An unsupported subresource never executes ordinary object CRUD.

**I2 — Authorization:** Permission checks cover every affected resource and explicit version. Authentication proves identity; neither a valid signature nor knowledge of an upload ID grants authorization.

**I3 — Integrity and visibility:** A newly visible hosted object references complete durable data whose required request checksums have passed. A rejected upload does not replace the previous object, create a visible version, or publish corrupt upstream content.

**I4 — Atomic preconditions:** Conditional writes and bucket deletion are decided within the same serialized metadata transition that commits their effects. Handler-level check-then-write is insufficient.

**I5 — Honest outcomes:** Success is returned only after the operation's documented success condition. A timeout with unknown commit outcome is not proof of rollback. An upstream availability failure is not object absence.

**I6 — Resource bounds:** At fixed concurrency, request memory and mailbox growth are bounded independently of object length. Steady-state listings do not materialize all metadata or all keys in a bucket.

**I7 — Recovery safety:** GC, repair, packing, migration, and restore preserve referenced versions, pinned readers, multipart state, and protected uncertain operations. Cross-cluster replication never counts toward local RF/W durability.

## 4. Baseline checkpoint — preparation, not a fourth phase

Record the actual branch/SHA, dirty working tree, toolchain, dependency versions, supported deployment modes, and baseline checks. Inspect routing, authentication, IAM, body readers, object and multipart commits, listing, cloud cache, lifecycle, packing, jobs, and recovery documentation.

Create or update these repository-local execution documents:

| Document | Purpose |
|---|---|
| `docs/plans/s3-hardening-plan.md` | This plan, adjusted to actual module names without reducing scope |
| `docs/plans/s3-hardening-status.md` | Task status, changed paths, exact commands, results, remaining blockers, next action |
| `docs/s3-compatibility.md` | Operation/parameter/header/mode support matrix tied to tests |
| `docs/reports/s3-hardening-verification.md` | Final evidence and remaining declared limitations |

Use statuses `pending`, `in_progress`, `passed`, and `blocked`. Keep baseline failures separate from regressions. Do not erase existing documentation or overwrite unrelated work.

## 5. Phase 1 — Security, data correctness, and truthful failure handling

### P1.1 — Resolve operations once; eliminate destructive fallback

**Starting points:** S3 `router.ex`, `plugs/authorize.ex`, and bucket/object/multipart handlers. [R2, R3]

Introduce a small operation descriptor and pure resolver shared by authorization and dispatch. It should identify bucket/key, explicit versions, multipart identity, copy source, relevant conditions, and required permissions. Preserve original request bytes for signature verification; do not conflate canonical signing representation with decoded domain keys.

Classify method, target shape, operation-selecting query parameters, and relevant headers. In particular, distinguish UploadPartCopy from UploadPart and subresource operations from CRUD. Reject conflicting selectors, malformed identifiers, and unsupported security-affecting features before mutation.

Do not reject every unfamiliar query parameter indiscriminately: signature parameters, response overrides, listing options, and harmless SDK parameters have separate classifications. Preserve duplicate query pairs for signing and define which duplicates are invalid for operation selection.

Use explicit S3-compatible errors: known unimplemented operations normally use `NotImplemented`; malformed combinations use a suitable client error. Apply authentication/disclosure rules deliberately. Never turn `?acl`, `?retention`, `?legal-hold`, `?tagging`, or `?versions` into another operation.

**Gate:** resolver/dispatch tests cover the complete operation table. Negative tests snapshot object content, versions, bucket configuration, and upstream requests and prove no unintended mutation. Later, supported Tagging operations change tags only.

### P1.2 — Authorize the real operation and all resources

Use the AWS operation-to-policy-action mapping, not operation names prefixed with `s3:`. For example, HeadBucket uses ListBucket; ordinary HeadObject uses GetObject; multipart initiation, part upload, and completion use PutObject. Version-specific permissions and source-copy permissions must be evaluated separately. [A1]

Implement source/destination authorization for CopyObject and UploadPartCopy, per-entry authorization for DeleteObjects, explicit-version actions, and strict binding of every multipart operation to `{bucket, key, uploadId}`. Resolve upload metadata without leaking its contents to unauthorized callers. Knowing an ID must not enable changing, completing, listing, or aborting another target's upload.

Keep presigned requests subject to active key/user checks and the same operation permissions. Ensure policy read failures fail closed, including a failure to read one attached deny policy. Test revocation and policy changes across cluster nodes under the promised consistency model.

Add canonical `arn:aws:s3:::` resource support while handling existing `arn:ess:::` policies through an explicit, tested compatibility layer. Plan legacy action migration carefully: an old HEAD-only grant must not silently become a full object-read grant. Deny statements must remain effective across canonical and legacy forms. Reject unsupported policy semantics at validation rather than silently ignoring restrictions; full IAM/STS is not in scope.

Update built-in policies and their documentation. Distinguish S3 operation names, policy action names, and ESS legacy aliases.

**Gate:** least-privilege positive/negative tests for standard and legacy policies, cross-bucket copy, mixed-result deletes, multipart key mismatch, versions, presigned requests, policy lookup faults, and revoked keys. No implicit privilege broadening.

### P1.3 — Enforce upload integrity before publication

**Starting points:** `auth/sig_v4.ex`, `presigned.ex`, shared body readers, local/cloud object handlers, multipart handlers, and LocalCAS staging. [R4]

Represent request integrity expectations explicitly and carry them through the state-threaded reader into staging and commit. Verify a declared payload SHA-256, Content-MD5 where supported/required, and supported algorithm-specific checksums against actual received bytes. Keep the CAS SHA-256, S3 ETag, and wire checksum as distinct concepts. [A2, A5]

Minimum acceptance profile: payload SHA-256; Content-MD5; CRC32, CRC32C, CRC64NVME, SHA1, and SHA256 wire checksum support with correctly declared applicable checksum types; all ordinary default upload modes exercised by the selected current AWS CLI, Boto3, and mc versions. Inventory the modes before implementation. Other explicit algorithms may be rejected and documented, never silently accepted. Do not disable SDK checksums to make the required default-client tests pass.

For accepted aws-chunked modes, validate framing, rolling signatures when applicable, terminal chunks, decoded length, and declared checksum trailers. Test truncation and bytes after completion. Unknown modes fail before publication; framing-only parsing is not authenticated streaming support. Ordinary UNSIGNED-PAYLOAD and presigned behavior must follow the documented transport/security policy rather than being confused with a declared digest.

Correct canonical request handling: percent encoding, spaces versus literal plus, repeated/empty query parameters, signed headers, credential scope/date/service, and path preservation. Do not normalize away meaningful repeated slashes or dot segments in object keys. Use independent signer fixtures and official rules, not only ESS signing and verification against each other. [A3]

For cloud write-through, prevent invalid late checksums from creating a visible upstream object. A bounded disk spool, followed by validation and upload, is acceptable; an unverified visible upstream PUT followed by attempted deletion is not.

**Gate:** damaged or truncated signed/checksummed requests leave no visible mutation locally or upstream; valid independent-client uploads succeed; reader cancellation and size failures release staging resources.

### P1.4 — Commit conditional mutations atomically

Implement PutObject and CompleteMultipartUpload preconditions within Concord transactions/serialized commit commands. Include current version identity and bucket generation in relevant comparisons. Support If-None-Match create-only behavior and If-Match ETag behavior according to the chosen general-purpose S3 profile. [A4, A5, A6]

Pin the copy source version and evaluate source conditions against that immutable identity. Honor the operation-specific condition precedence; do not infer identical semantics for unrelated APIs.

Distinguish precondition failure from a real concurrent conflict and from an unknown transaction outcome. Internal operation IDs support safe recovery; do not promise generic exactly-once S3 PUTs or deduplicate separate legitimate client PUT requests.

**Gate:** at least 32 synchronized create-only writers produce exactly one success; stale conditional replacements fail; repeat the relevant races across both cluster data endpoints. Inject an uncertain commit result and recover without losing a successful write or creating spurious internal completions.

### P1.5 — Make bucket deletion safe and version-aware

Check authoritative live objects, historical versions, and delete markers, not ordinary LIST output. S3 requires versions and delete markers to be removed before bucket deletion. [A7]

Serialize bucket deletion with new object commits, multipart initialization/completion, and bucket recreation. Use bucket lifecycle/generation fencing or an equivalent transactional invariant. Stale work from a deleted bucket must not become visible in a later bucket with the same name.

Define incomplete-upload treatment separately according to the verified API profile; do not infer it from visible objects or conflate it with historical versions. Update associated configuration and indexes consistently. Propagate deletion failures rather than replying with unconditional success.

**Gate:** a bucket containing only historical versions/delete markers cannot be deleted; concurrent puts/completions cannot orphan committed objects; failures never return success; recreating a bucket does not resurrect old metadata or jobs.

### P1.6 — Specify cloud authority and eliminate false success

Define two distinct profiles: hosted buckets whose authority is ESS, and cloud gateway/cache buckets whose object authority is the configured upstream. Cross-cluster replication remains a third, asynchronous concern. [R5, R1]

For supported gateway operations, perform authorization and validation before upstream mutation. Failed upstream deletion cannot be converted into a successful local deletion. Batch deletion returns per-entry outcomes. Distinguish not-found, access-denied, timeout, transient failure, and unknown mutation outcome.

Record a durable reconciliation intent before an external mutation when local state must subsequently follow it. After upstream success, retry/reconcile a failed local index update safely. Do not blindly repeat a destructive request whose upstream outcome is unknown, especially against versioned upstreams. Version identity and conditional operations must fence retries where available.

Ensure cache hits obey a documented freshness policy. Default to revalidation or explicitly bounded freshness; never silently serve indefinite stale data as current. A changed upstream object cannot be served using stale length/ETag/metadata. Do not serve stale data on error unless a separate explicit policy permits it and its limitations are documented.

Preserve the cluster cloud-write prohibition. For unsupported gateway versioning, multipart, or other feature combinations, fail before starting the operation; do not create an upload or write local versions that the gateway GET path cannot read. Existing genuinely supported gateway behavior must remain functional.

**Gate:** upstream faults, successful upstream mutation plus local metadata failure, restarts, cache invalidation, and external changes have truthful, recoverable outcomes. Unsupported mode combinations are tested negative cases, not hidden fallbacks.

### P1.7 — Bound control requests and normalize errors

Set explicit small, operation-specific limits for buffered XML/configuration bodies, including DeleteObjects. Enforce key/count/tag/metadata limits for supported APIs. Reject unsafe XML constructs and malformed/ambiguous input without unexpected exceptions. Fuzz parsing with bounded generated cases using existing tooling.

Map domain errors to stable S3 responses. Do not expose internal exception terms, secrets, credential material, or filesystem details. Preserve request IDs and bodyless HEAD response behavior. Treat a response stream failure after headers as a terminated transfer, never append an XML error into object bytes.

**Phase 1 exit:** all P1 tasks and A01–A18 in the matrix pass, plus baseline regressions. Continue directly to Phase 2.

## 6. Phase 2 — Complete a tested S3 compatibility profile

### P2.1 — Publish the support contract

Maintain a machine-readable or test-driven operation registry and a human-readable matrix. Include operation, method/subresource, parameters and headers, IAM actions, errors, standalone/cluster/gateway support, and evidence test IDs.

Mandatory hosted capabilities are bucket CRUD/location; ListObjects v1 and v2; object CRUD/copy/batch delete; versions; multipart and multipart-copy; object attributes listed below; conditional operations; object/bucket tagging; CORS; signing and checksums above. Preserve existing supported lifecycle/notification behavior and document their actual subset and mode guards.

Full AWS IAM/STS, ACL ownership semantics, Object Lock, SSE/KMS, website hosting, S3 Select, directory buckets, and erasure coding are outside this objective. They must be deliberately rejected where requested. Do not advertise full AWS parity. Path-style remains required; virtual-hosted addressing is not added unless already supported and needing preservation.

### P2.2 — Complete versions and listing semantics

Implement ListObjectVersions with versions, delete markers, IsLatest, ordering, and key/version markers. Honor explicit versionId on GET, HEAD, DELETE, copy sources, and tagging. Cover never-enabled, enabled, and suspended buckets, including the special null version and delete-marker responses. Normal GET/HEAD response version headers must match the version actually served. [R6, A8]

Implement ListObjects v1 markers and v2 prefix/delimiter/max-keys/start-after/continuation/encoding behavior and correct key counts. Test common-prefix-only pages, page size zero, empty results, maximum sizes, malformed tokens, Unicode, XML-sensitive keys, and repeated delimiters. Prevent legacy v1 data from reappearing beneath a v2 deletion/head. [R7, A9]

Phase 2 may use the existing metadata access implementation while correctness is completed; Phase 3 must replace full scans. Multi-page listings need not create a globally immutable snapshot during arbitrary concurrent writes unless the contract explicitly promises one. Each response must meet the declared consistency level.

### P2.3 — Complete multipart state and API behavior

Persist content type, system/user metadata, tags, checksum negotiation, target identity, and bucket generation at initiation. Completion uses the persisted attributes, not the XML request's Content-Type. [R8]

Add ListMultipartUploads, UploadPartCopy, and bounded ListParts pagination. Handle part overwrites, strictly parsed part numbers, part ordering/ETags, operation-specific size/count limits, empty/final parts, missing parts, and packed or remote part sources where supported. [A6]

Serialize UploadPart, Complete, and Abort so late part writes or lost responses do not corrupt completion state. Provide internal retry/recovery for a committed completion followed by response/finalization failure; return the documented S3 outcome on later client retries. Finalization must not publish additional versions accidentally. Cleanup removes only unreferenced parts after fences and in-flight protections are respected.

### P2.4 — Preserve object attributes and copy semantics

Round-trip Content-Type, Content-Encoding, Cache-Control, Content-Disposition, Content-Language, Expires, user metadata, tags, and supported checksums through PUT, multipart, COPY, GET, HEAD, and supported gateway requests. [A5, A10]

Implement COPY/REPLACE metadata and tag directives according to S3 semantics, source version handling, and copy preconditions. Do not assume a copied multipart object's ETag must remain identical; verify and document the correct resulting representation.

Support requested GET response-header overrides on signed/presigned requests without allowing header injection. Complete GET/HEAD conditional response precedence and valid single-range behavior, including cold-cache range reads. Do not claim multi-range support without implementing it.

### P2.5 — Implement CORS, location, and tagging

Add GetBucketLocation and consistent configured region responses. Add persisted bucket CORS PUT/GET/DELETE, correctly scoped OPTIONS preflight handling, actual-response CORS headers, exposed headers, and cache variance behavior. Preflight handling must not weaken authentication on the actual request. [R2]

Add bucket and version-aware object Tagging PUT/GET/DELETE, correct IAM checks and tag limits, and COPY/multipart interactions. Tags are metadata, not replacement object bytes. Define upstream support explicitly; unsupported gateway tagging must fail before mutation.

Use existing configuration/admin patterns when visibility or configuration is needed; no UI redesign and no relaxation of current cluster admin guards.

### P2.6 — Prove interoperability independently

Use real HTTP servers, current selected AWS CLI v2/Boto3/mc releases, and record/pin their exact versions for reproducibility. Preserve existing Boto3 standalone and mc/RustFS gateway tests. Use independent raw signed requests for cases ordinary SDKs do not emit. [R9]

Run basic operations, large/multipart transfers, versions, conditions, metadata, tags, CORS, presigned reads/writes, checksum modes, and special keys. Default-client checksum behavior is a required test lane. SDK-specific fallback settings may be documented as additional tests, never substitutes for that lane.

**Phase 2 exit:** P2 tasks, A19–A27, and all Phase 1 tests pass. The support matrix has no “supported” entry without evidence. Continue to Phase 3.

## 7. Phase 3 — Scale, recovery, and operational verification

### P3.1 — Replace full-scan listing with a bounded ordered index

Immediately remove nested linear version lookups by using keyed lookup within the current snapshot, but do not mark the task complete at that intermediate fix. The baseline implementation scans global metadata and then searches it again for individual versions. [R7]

Inspect the locked Concord capabilities. Prefer bounded ordered scans. Otherwise implement a bounded, versioned per-bucket index in existing Concord transactions, such as paged ordered records. Do not invent a dependency API, add an external database, or store every bucket key in a single ever-growing serialized list. The index must preserve S3 key order rather than assume the existing encoded metadata keys sort correctly.

Update heads/versions/index entries atomically. An optional local ordered projection is only acceptable with a durable source, a proven applied-revision barrier, rebuild/rejoin handling, and no stale successful LIST. Define efficient seeking for common prefixes so a page of directories does not scan every descendant.

Implement resumable backfill and validation, readiness/cutover rules, and non-destructive rollback boundaries. Legacy records and v2 deletions must remain correct during transition. Full scans may exist in explicit bounded maintenance or offline rebuild workflows, not steady-state request handling.

**Gate:** post-startup listings across 10k and 100k key fixtures perform no get_all/full-bucket materialization; page allocation and visited entries scale with requested results and index traversal, not unrelated metadata. Record throughput, p50/p95, memory, and index size. Provide a 1M-key operator profile without claiming results unless executed.

### P3.2 — Make every supported transfer path bounded

Audit local, remote replica, gateway, cache fill, copy, multipart composition, repair, and replication paths. Remove whole-object binaries, eager enumeration, and unbounded message queues. Keep connection state threaded correctly through body reads.

Gateway writes may use disk staging to validate and establish replayable lengths. Gateway reads need acknowledged backpressure or bounded temporary-file staging. Never publish a partial cache entry. Cache keys/metadata must identify the actual upstream version or validators; metadata and body cannot come from different upstream revisions. Range-on-cold-cache must return the correct range without an object-sized RAM buffer.

Bound transfer concurrency, memory buffers, temporary disk use, cache capacity, and control-body size. Handle client disconnect, upstream failure, disk-full, cancellation, and restart cleanup. Avoid evicting a file still being served without safe open-handle/pin semantics. Preserve raw encoded bytes; remove only transfer-specific aws-chunked encoding, not an object's genuine gzip encoding.

**Gate:** real HTTP transfers of 64 MiB and 1 GiB objects at concurrency 1 and 4, including throttled peers, stay within a predeclared memory envelope. Record process/BEAM binary memory, RSS, queue depth, and temporary disk occupancy; report page cache separately. The envelope is a test target derived from configured bounds, not a performance claim.

### P3.3 — Exercise crash consistency and maintenance safety

Inject failure at staging, file sync/rename, blob replication, metadata commit, outbox enqueue/claim, completion finalization, response delivery, cache publication, and physical deletion. Restart and check invariants rather than merely observing that the process starts.

Test leader loss/re-election/rejoin, data-node loss/recovery, blocked quorum, operation-intent recovery, node generation changes, lease expiry, duplicate job delivery, read repair, and corruption scrubbing. Keep RF=2/W=2 behavior: losing one of two data replicas may preserve reads but must not silently allow strict-quorum writes. [R1, R9]

Test GC/packing against live versions, multiple references to one hash, multipart parts, in-flight reads, and uncertain operations. Retain current packing/lifecycle mode restrictions. Fix failures uncovered in these paths that violate the mandatory invariants; do not expand the product into a new cluster design.

### P3.4 — Automate backup and independent restore

Extend existing operations/runbooks instead of creating a competing mechanism. Implement a supported recoverable backup boundary: initially, explicitly quiesced writers and maintenance with a verified metadata snapshot is acceptable. Do not claim online-consistent backup based on copying changing metadata and files independently.

Capture metadata, versions/delete markers, required CAS data and packed manifests/indexes, configuration/schema versions, IAM records, and defined handling for multipart state, operation intents, and durable jobs. Produce a checksummed manifest and fail incomplete/corrupt backups. Protect credentials; document separately provisioned master-key recovery without printing secrets into reports.

Restore into a fresh empty root and isolated instance, with the original data paths unavailable. Verify data hashes, keys/versions/tags/attributes, policies, configuration, and job state. Cache data may be excluded only when explicitly reconstructible; a gateway backup is not a backup of upstream-only object bodies.

Restore policy must fence old leases/node generations and leave destructive/outbound workers disabled until validation and explicit recovery activation. A cluster restore creates/rebuilds a supported clean cluster; never clone live node identities blindly. Prove a missing or corrupted referenced blob is detected, not silently accepted.

Record measured recovery duration and the backup consistency point. Replication is not independent backup: test recovery after changes/deletions made after the backup.

### P3.5 — Add actionable operational evidence

Expose bounded-cardinality metrics for transfer activity/errors, bytes, staged disk/capacity, integrity failures, precondition failures, listing latency/index readiness, upstream health/errors, cache outcomes, replication backlog/age, repair/scrub/GC, and unknown operations. Do not label metrics with raw object keys, request IDs, or credentials.

Keep liveness separate from readiness and mode-specific write availability. Provide diagnostics and supported release RPC/operator commands for index status, reconciliation, failed jobs, capacity, backup verification, and restore. Update existing administration views only where necessary and supported.

### P3.6 — Make acceptance reproducible and finish release preparation

Keep fast unit/contract tests separate from standalone real-HTTP, gateway/RustFS, three-voter, active-active failure/restart, resource-bound, and restore jobs. Use timeouts, unique roots/ports/nodes, reliable teardown, and retained logs. A harness prerequisite failure is not a successful skip.

Make correctness/E2E gates run for relevant changes; include resource-bound and restore profiles in mandatory completion verification even when a heavier CI lane is scheduled/manual. Provide one aggregate acceptance command or documented script that reports pass/fail/blocked per profile.

Run formatting, strict compilation, all application tests, existing and new integration profiles, production assets/release build, and release-container smoke tests. Capture changed configuration, protocol/policy compatibility changes, index backfill/upgrade steps, known unsupported features, and tested capacities in release notes. Do not publish or deploy.

**Phase 3 exit:** P3 tasks, A28–A38, all earlier tests, and aggregate completion checks pass. Only then report the goal complete.

## 8. Suggested commit sequence

Use small, bisectable commits in dependency order: operation resolver and negative routes; authorization/binding; integrity and signing; atomic conditions and deletion; truthful gateway errors; version/list correctness; multipart completion; attributes/tagging/CORS; bounded index and backfill; bounded gateway transfers; crash/maintenance tests; restore automation; metrics/docs/CI and final verification.

Keep task IDs in commit descriptions or the status ledger. Do not wait until the end to integrate shared contracts. Independent test work may proceed in parallel, but avoid simultaneous ownership of the same routing, transaction, or reader modules.

## 9. Overall definition of done

All task IDs are accounted for. All required acceptance rows have code/test evidence and a passing execution. No unresolved P0/P1 or violation of I1–I7 remains. No supported capability exists only as a route, mock, TODO, permissive fallback, or untested claim. There is no new full-object buffering, unbounded request listing, weakened quorum, or silently broadened permission.

The final report includes actual starting/ending SHA, changed modules, exact tool/client versions, executed commands/results, measured resource data, failure/recovery evidence, backup/restore evidence, migration/rollback instructions, and explicitly excluded capabilities. A blocked item stays visible with a reproducible explanation and the smallest next action.

## 10. Sources and implementation entry points

Repository references are pinned to the reviewed SHA. Revalidate against the working checkout. These are source locations, not evidence that the future changes or tests have run.

**Source root:** `https://github.com/gsmlg-opt/ex_storage_service/blob/42a9fe2b41faa82cf10c3f07840b890be18b876d/`

- **R1:** `AGENTS.md`; `README.md`; existing deployment, embedding, and cluster-upgrade runbooks.
- **R2:** `apps/ex_storage_service_s3/lib/ex_storage_service_s3/router.ex`.
- **R3:** S3 `plugs/authorize.ex`; core `iam/policy.ex`, `iam/access_key.ex`, and user lookup paths.
- **R4:** S3 `auth/sig_v4.ex`, `presigned.ex`, `handlers/shared.ex`; core `blob_store/local_cas.ex`.
- **R5:** S3 `handlers/object.ex`, `handlers/object/cloud_backend.ex`; core `cloud_cache/*`.
- **R6:** S3 `handlers/object.ex`, `handlers/object/local_backend.ex`, `handlers/bucket.ex`; core `object_service.ex` and versioning/commit modules.
- **R7:** `apps/ex_storage_service/lib/ex_storage_service/metadata.ex` and metadata key/index/commit modules.
- **R8:** S3 `multipart_handlers.ex`; core `storage/multipart.ex` and multipart transaction modules.
- **R9:** `e2e/README.md`, `e2e/s3_compat.py`, existing cluster/gateway harnesses and `.github/workflows/`.

Official general-purpose S3 references, checked while preparing this plan. Resolve current signing-specific links through official documentation when old links redirect. Verify exact edge cases rather than treating another S3 implementation as the specification.

- **A1 — Required API permissions:** `https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html`
- **A2 — Upload integrity and checksum types:** `https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html`
- **A3 — Signature canonicalization:** `https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html` (also consult S3-specific path and streaming rules).
- **A4 — Conditional writes:** `https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html`
- **A5 — PutObject:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html`
- **A6 — CompleteMultipartUpload:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html`
- **A7 — DeleteBucket:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html`
- **A8 — HeadObject:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html`
- **A9 — ListObjectsV2:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html`
- **A10 — CopyObject:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html`
- **A11 — DeleteObjects:** `https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html`
