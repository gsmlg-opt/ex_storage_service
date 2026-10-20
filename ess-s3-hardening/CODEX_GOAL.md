# Codex Goal: Complete All Three ESS S3 Hardening Phases

> Delivery authorization for ESS-S3-HARDENING-CLOSEOUT-002 (2026-10-21)
> supersedes this historical document's routine commit/push/release prohibition
> and the later daily-only delivery restriction. Publication now requires full
> current-candidate acceptance followed by the established root release workflow.
> Production operations, destructive actions, protected-branch approvals and
> immutable published tags retain their original safety boundaries.

**Goal ID:** `ESS-S3-HARDENING-001`\
**Repository:** `gsmlg-opt/ex_storage_service`\
**Review reference:** `42a9fe2b41faa82cf10c3f07840b890be18b876d` / `v0.6.7`\
**Required outcome:** Implement, test, document, and verify all three phases. Do not stop after the initial fixes.

## Your assignment

Work as the implementing Elixir/OTP and storage-systems engineer in this repository. Complete the accompanying `IMPLEMENTATION_PLAN.md` and all mandatory scenarios in `ACCEPTANCE_MATRIX.md`. This is an execution request, not a request for another assessment, scaffold, architecture proposal, or first-phase-only delivery.

The goal is a dependable, explicitly scoped S3-compatible object storage service with correct operation routing, complete authorization boundaries, verified upload integrity, atomic conditions, honest failure handling, a tested compatibility profile, bounded transfer/listing behavior, and demonstrated independent recovery.

Begin by inspecting the actual checkout and its instructions. The referenced review was static and may be superseded: reproduce findings, retain already-correct code, and fix actual causes. Do not reset the repository to the reference SHA or overwrite unrelated work.

## Inputs and durable execution state

Read `AGENTS.md`, applicable nested instructions, README, relevant implementation/tests/configuration, and existing operations/upgrade documents. Read the supplied plan and matrix, whether they are attached or already in the checkout.

Store the plan at `docs/plans/s3-hardening-plan.md` and the matrix at `docs/plans/s3-hardening-acceptance.md`, or reuse clear existing equivalents. Maintain:

- `docs/plans/s3-hardening-status.md`: phase/task/acceptance status, concrete evidence, changed modules, unresolved blockers, and next action.
- `docs/s3-compatibility.md`: implemented operation/parameter/header/mode coverage and deliberate exclusions, tied to test cases.
- `docs/reports/s3-hardening-verification.md`: final execution results, measured resource behavior, recovery evidence, and upgrade/rollback notes.

Persist useful checkpoints after each work unit and before context compaction. When resuming, inspect these files and git state rather than repeating the audit or assuming earlier commands succeeded. Keep notes concise and factual; never store secrets.

If only this goal file is supplied, its phase requirements below still authorize implementation. Reconstruct the detailed checklist from these requirements and current source rather than stopping because the companion document is absent. With companion documents available, all their mandatory tasks and acceptance rows are binding.

## Architectural and operational constraints

Preserve the five existing umbrella applications, Concord/VSR metadata, immutable content-addressed files, ObjectService/LocalCAS boundaries, Plug/Bandit S3 API, embedded deployment, and DuskMoon administration. Core remains independent of protocol response code and the transport app. Prefer functional transformations, explicit state machines, and injectable I/O/fault boundaries.

Do not introduce Ecto, PostgreSQL, another metadata authority, a replacement consensus implementation, or a substitute S3 server. Do not add dependencies merely for convenience. Inspect the locked Concord/http_fetch APIs; never invent capabilities or edit dependency source as an untracked workaround.

Keep production outbound traffic on `http_fetch` using bounded raw-byte transfers, `decode_body: false`, and no redirects for signed requests. Preserve real Content-Encoding and explicit lengths. Test-only independent clients remain independent.

Preserve standalone RF=1/W=1 and cluster defaults/guards, ordered three-voter membership, node generation fencing, operation intents, fenced maintenance jobs, and existing safety restrictions on gateway writes/packing/lifecycle/admin in cluster mode. Cross-cluster replication does not satisfy local write quorum. Do not enable a guarded capability simply to improve a feature matrix.

Use disposable data roots and local isolated services. Never operate on production data/credentials or run tests against real user buckets. Do not deploy, publish releases/packages, change another repository, push, or force-push. Keep small local commits when the checkout's workflow permits them; never erase unrelated changes. Follow the repository's command and asset-pipeline conventions.

## Phase 1 — Fix security and correctness completely

Execute P1.1–P1.7 in the plan and prove A01–A18:

1. **One operation resolver for auth and dispatch.** Resolve method, bucket/key, subresources, versions, upload identity, and copy headers once. Preserve raw signing data separately. Reject unsupported or contradictory operations before mutation. An ACL/retention/tagging/version request must never fall through to ordinary PUT/GET/DELETE. Distinguish benign SDK parameters from selectors; preserve duplicate query pairs for signing.
2. **Complete authorization.** Use real AWS action mappings, not invented `s3:<APIName>` permissions. Enforce Copy/UploadPartCopy source and destination permissions, per-entry batch-delete permissions, version-specific actions, and `{bucket,key,uploadId}` binding on every multipart request. Presigned requests still require active keys/users and policy checks. Fail closed on partial policy-read failures. Migrate canonical S3 ARNs and legacy ESS actions/resources without broadening old grants or losing denies.
3. **Integrity before visibility.** Carry declared hashes/checksums through a state-threaded stream into staging/commit. Verify payload SHA-256, applicable Content-MD5, and required wire checksums. Support the plan's default-client checksum/signing profile. Accepted aws-chunked streams must verify applicable signatures, framing, lengths, terminal chunks, and trailers; framing-only acceptance is forbidden. Never disable normal SDK checksums to pass. Canonicalize independently tested signed paths/queries/headers correctly. Invalid late checksums must not publish upstream data; disk staging is acceptable.
4. **Atomic conditions.** PutObject and multipart completion preconditions belong inside the serialized Concord mutation, not a separate HEAD check. Pin copy source identity. Test synchronized create-only and stale-ETag races on standalone and both cluster data endpoints. Preserve uncertain-outcome recovery without claiming exactly-once semantics for arbitrary independent S3 requests.
5. **Safe bucket deletion.** Account for all versions/delete markers, serialize deletion with writes/completions/recreation, and fence stale work with bucket identity/generation. Define incomplete-upload behavior deliberately. Propagate metadata failure; never return unconditional success.
6. **Truthful gateway behavior.** Document upstream-authoritative versus hosted semantics. Do not convert upstream deletion failures into local success or timeouts into NoSuchKey. Persist reconciliation for upstream-success/local-failure and unknown outcomes; retries must not blindly mutate a newer upstream state. Define cache freshness and metadata/body identity. Reject unsupported feature/mode combinations before initiating mutations, retaining cluster gateway-write guards.
7. **Bound control bodies and errors.** Explicit XML/configuration limits, hardened parsing, strict identifiers, stable S3 error mapping, sanitized diagnostics, bodyless HEAD errors, and correct response-stream termination after headers.

Validate Phase 1, record evidence, then continue directly. Do not ask for authorization to start Phase 2.

## Phase 2 — Finish the supported S3 profile

Execute P2.1–P2.6 and prove A19–A27:

- Create a test-backed compatibility matrix for operations, query parameters, headers, IAM actions, errors, and standalone/cluster/gateway modes. Keep deliberate exclusions explicit rather than silently accepting them.
- Complete ListObjectVersions; explicit version GET/HEAD/DELETE/COPY/tagging; null-version and delete-marker semantics; correct response version IDs; ListObjects v1/v2 markers, prefix/delimiter, start-after, max-keys, encoding, and counts. Prevent legacy record resurrection beneath newer deletions.
- Complete ListMultipartUploads, UploadPartCopy, and ListParts pagination. Persist initiation metadata/tags/checksum settings. Handle part validation, overwrites, abort/complete races, remote/packed sources where supported, finalization failure, and safe internal completion recovery.
- Round-trip system/user metadata, real content encoding, tags, and supported checksums across PUT/multipart/COPY/GET/HEAD. Implement metadata/tag COPY versus REPLACE, copy source conditions, signed response overrides, conditional reads, and single-range responses including cold caches.
- Implement GetBucketLocation, persisted CORS configuration and preflight/actual-response behavior, and bucket/object/version tagging with correct permissions. Actual requests must remain authenticated. Do not redesign the admin UI or relax cluster listener guards.
- Verify real independent AWS CLI v2, Boto3, and mc clients, recording/pinning their selected current versions. Preserve existing signed standalone and RustFS gateway checks. Use raw fixtures for unusual wire/signature cases and a required lane using default SDK checksum behavior.

The mandatory hosted profile must be implemented, not renamed “unsupported.” Gateway combinations that cannot preserve semantics remain explicitly guarded as specified in the plan. Full IAM/STS, ACL ownership, Object Lock, SSE/KMS, website hosting, S3 Select, directory buckets, and erasure coding are outside this delivery; requests for excluded behavior must fail deliberately. No claim of complete AWS feature parity.

Validate Phase 2 and keep Phase 1 green, then continue directly to Phase 3.

## Phase 3 — Prove resource bounds, resilience, and recovery

Execute P3.1–P3.6 and prove A28–A38:

- **Bounded listing index:** first remove nested linear lookup, then eliminate full metadata/full-bucket scans from steady-state listings. Use actual Concord ordered scans or a bounded transactional per-bucket index. Preserve key order, versions, prefixes, and consistency. An ETS projection needs a proven revision barrier and rebuild/rejoin behavior. Add resumable backfill/cutover/rollback safeguards without deleting legacy data. Run 10k/100k-key instrumented workloads; provide a larger optional profile without inventing results.
- **Bounded transfers everywhere supported:** local, gateway, cache miss, copy, multipart, replicas, repair, and replication. Preserve backpressure, raw bytes, connection state, and cancellation. Bound buffers/concurrency/temporary disk/cache capacity. Never publish partial cache entries or serve mismatched metadata/body. Run actual 64 MiB and 1 GiB transfers at concurrency 1 and 4 with slow peers, predeclared memory/queue bounds, and reported measurements.
- **Crash and maintenance verification:** test filesystem/metadata commit boundaries, unknown results, response loss, leader failover/rejoin, data-node outage, lease/generation fencing, repair/scrub, and GC/packing interleavings with versions, shared blobs, multipart, and readers. Preserve strict RF/W and unsupported-mode guards. Fix invariant violations uncovered by these tests.
- **Independent backup/restore:** extend existing runbooks/tooling. A quiesced, verified backup boundary is acceptable; unrelated copies of live metadata and blobs are not an online-consistency guarantee. Restore into empty isolated roots without originals and verify content hashes, versions/tags/attributes, policies/configuration, and defined job/multipart state. Handle packed artifacts, missing blobs, required master-key recovery, and stale leases/node identities safely. Test standalone and supported clean-cluster restoration. Keep outbound/destructive workers inactive until verification and explicit recovery activation. A gateway cache backup must not claim to include upstream-only bodies.
- **Operations and delivery gates:** bounded-cardinality metrics, readiness separate from liveness, release RPC/operator diagnostics, configuration docs, migration/backfill/rollback notes, tested limits, and release notes. Integrate separate correctness, standalone, gateway, cluster, resource, and restore lanes with reliable cleanup/log retention and one aggregate acceptance entry point. Build and smoke-test the production release without publishing/deploying.

## Verification discipline

For each bug, add a failing reproducer before the repair when practical. Test public behavior and transaction invariants, not implementation details alone. A helper passing its own generated signature is not independent interoperability evidence. A mock stream is not proof of bounded production transport. Successful restart is not evidence of correct restored data.

Run the current repository's authoritative checks, including formatting, strict compilation, all umbrella app tests, existing/new E2E lanes, production assets/release, and release-container smoke checks. Typical starting commands are `mix format --check-formatted`, `mix compile --warnings-as-errors`, and `mix test`; inspect the repo for exact environment/setup and integration commands. Do not claim a command ran when it did not.

No deleting/skipping required tests, lowering quorum, disabling auth/checksums, suppressing warnings, swallowing failures, or inflating resource thresholds to manufacture green results. Document baseline failures separately and repair those that block this goal's acceptance.

Use the existing development environment. When a prerequisite is missing, diagnose and use safe local setup where available. Record actual external blockers and continue independent tasks. Do not invent results or repeatedly retry an unchanged infrastructure failure.

## Completion rule and final handoff

Completion requires **all three phases**, all mandatory task IDs, and all 38 acceptance scenarios to pass with evidence. Documentation or unit tests alone are insufficient. Do not stop at the end of a phase merely because the task is large. Continue implementation, testing, and integration without asking for phase-by-phase approval.

If genuinely blocked, report partial completion honestly and preserve the execution ledger; do not say the goal is complete. State the failing command/evidence, missing capability, affected acceptance IDs, and smallest concrete unblock action. Do not silently defer Phase 2 or Phase 3.

The final handoff must include actual starting/ending SHA, concise implementation summary by phase, changed modules, exact test/client versions and results, resource measurements, crash/recovery and restore evidence, upgrade/rollback steps, and declared excluded features. List any remaining blocker separately. Never report full S3 parity or production validation beyond the evidence.

**Start by inspecting the checkout and recording the baseline. Then implement Phase 1, Phase 2, and Phase 3 through their verified completion gates.**
