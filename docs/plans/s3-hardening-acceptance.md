# ESS S3 Hardening — Mandatory Acceptance Matrix

**Goal:** `ESS-S3-HARDENING-001`\
**Prepared:** 2026-10-13\
**Initial status:** Every scenario below is **pending**. This file specifies future acceptance tests; it does not report executed results.

Use together with `IMPLEMENTATION_PLAN.md` and `CODEX_GOAL.md`.

## Execution rules

Every row requires a named automated test or harness case, actual execution, and evidence in the repository verification report. A row containing several related assertions passes only when all apply. Do not label a row passed because a helper exists or a route returns a success status.

Use the official API references in the plan to settle operation-specific responses, permissions, and edge cases. Test the declared support profile, including negative tests for explicitly excluded mode combinations. Correctly rejecting an excluded capability is not permission to exclude a mandatory hosted capability.

Lanes:

- **Contract:** ExUnit/domain tests with deterministic fault injection and concurrency barriers.
- **HTTP:** Actual listening server and independent signed client; Plug-only tests do not satisfy this lane.
- **Gateway:** Local ESS plus disposable upstream using the existing RustFS harness and upstream fault controls.
- **Cluster:** Existing isolated three-voter topology and both data-node endpoints, retaining RF/W guards.
- **Resource:** Instrumented real transfers and metadata workloads on a documented runner.
- **Restore:** Backup followed by a fresh isolated restoration with original data roots inaccessible.

## Phase 1: A01–A18

| ID | Scenario and required result | Required lane(s) | Tasks |
|---|---|---|---|
| A01 | Unsupported operation selectors never execute CRUD. ACL/retention/legal-hold and other unsupported requests leave object bytes, version history, bucket configuration, and upstream requests unchanged. Tagging changes only tags once supported. | Contract, HTTP | P1.1 |
| A02 | Conflicting subresources, duplicate operation selectors, malformed IDs/part numbers, and ambiguous copy/multipart requests are handled deliberately; harmless SDK parameters do not accidentally select another operation. | Contract, HTTP | P1.1, P1.7 |
| A03 | CopyObject and UploadPartCopy require appropriate source/version access and destination write access. A user with only destination access cannot copy a protected source. | Contract, HTTP | P1.2 |
| A04 | UploadPart, ListParts, Complete, and Abort cannot operate on an upload under the wrong bucket or key. Unauthorized callers cannot obtain part metadata or use an upload ID as a capability. | Contract, HTTP | P1.2 |
| A05 | Standard AWS action mappings and canonical S3 ARNs work. Legacy ESS policies retain their intended scope. An old HEAD-only allow is not broadened to GET; explicit deny remains effective across aliases. | Contract, HTTP | P1.2 |
| A06 | Missing/unreadable policy records, including an attached deny policy, fail closed. Revoked keys, suspended users, and changed policies are enforced under the declared cluster read consistency. | Contract, Cluster | P1.2 |
| A07 | DeleteObjects handles object/version entries separately, mixed authorization and per-entry failures, quiet responses, limits, and integrity requirements. A permitted entry does not authorize another key. | Contract, HTTP, Gateway | P1.2, P1.6, P1.7 |
| A08 | A correctly signed request with a declared payload hash but altered body is rejected before any visible hosted or upstream mutation. Previous bytes and versions remain intact. | HTTP, Gateway | P1.3 |
| A09 | Correct and incorrect Content-MD5 and supported algorithm-specific checksums are exercised independently of the CAS hash/ETag. Declared unsupported algorithms never receive an unvalidated success. | Contract, HTTP | P1.3 |
| A10 | Accepted signed and unsigned-trailer aws-chunked modes validate their applicable signature chain, length, framing, and checksum trailers. Corruption, truncation, malformed termination, or prohibited trailing bytes cannot publish an object. | Contract, HTTP, Gateway | P1.3 |
| A11 | Independent signature fixtures cover spaces, literal plus, percent escapes, Unicode, repeated/empty query values, duplicate headers, invalid credential scope, and meaningful path slashes/dot segments. ESS is not its only signing oracle. | Contract, HTTP | P1.3 |
| A12 | Presigned GET/PUT, signed response overrides, expiration, key revocation, and resource permissions behave correctly. Changing a signed parameter or constrained header fails. | HTTP | P1.2, P1.3 |
| A13 | At least 32 synchronized create-only writers yield exactly one success. Stale If-Match writes fail. Conditional multipart completion and races across both cluster endpoints preserve the same invariants. | Contract, HTTP, Cluster | P1.4 |
| A14 | Historical versions/delete markers prevent bucket deletion. Racing writes, completions, and delete/recreate cannot create orphan-visible objects or revive old state. Injected delete failure never returns success. | Contract, HTTP, Cluster | P1.5 |
| A15 | Upstream denied/unavailable/timeout responses are not returned as successful deletes or fabricated NoSuchKey. Failed writes/deletes preserve truthful cache/index state and per-entry batch outcomes. | Gateway | P1.6 |
| A16 | Upstream mutation success followed by local commit failure is recovered through durable reconciliation after restart. Unknown external outcomes do not cause blind destructive retry or unexpected additional versions. | Gateway | P1.6 |
| A17 | Out-of-band upstream replacement/deletion and stale cache entries obey the documented freshness policy. Served body, size, ETag, and attributes refer to one upstream identity. Excluded feature/mode combinations fail before mutation. | Gateway | P1.6 |
| A18 | Oversized control/XML requests and malformed XML/dates/ranges fail safely. No secret or internal exception detail leaks. HEAD errors have no object/XML body; late stream failures terminate rather than append error content. | Contract, HTTP | P1.7 |

## Phase 2: A19–A27

| ID | Scenario and required result | Required lane(s) | Tasks |
|---|---|---|---|
| A19 | Versions work across never-enabled/enabled/suspended buckets: null version, delete markers, ListObjectVersions pagination/IsLatest, explicit-version GET/HEAD/DELETE/COPY, and correct version/error headers. | Contract, HTTP, Cluster | P2.1, P2.2 |
| A20 | ListObjects v1/v2 handle prefixes, delimiters, start-after/markers, continuation, encoding, zero/max page sizes, common-prefix-only pages, and malformed tokens. Legacy records cannot reappear beneath newer v2 deletions. | Contract, HTTP, Cluster | P2.2 |
| A21 | ListMultipartUploads and ListParts paginate correctly. UploadPartCopy respects source versions/ranges and authorization, and completion validates parts/order/ETags/limits using supported local and remote sources. | Contract, HTTP, Cluster | P2.3 |
| A22 | Multipart initialization attributes survive completion. Concurrent part overwrite/abort/complete, late writes, finalization failure, response loss, and restart do not corrupt the final object or duplicate an internal completion. | Contract, HTTP, Cluster | P2.3 |
| A23 | Object system/user attributes and applicable checksums round-trip through PUT, multipart, COPY/REPLACE, GET, HEAD, and supported gateway operations. Genuine gzip bytes and Content-Encoding are preserved. | HTTP, Gateway | P2.4 |
| A24 | Bucket/object/version Tagging CRUD uses correct permissions and limits; COPY and multipart tag semantics are correct. Tag deletion never removes object bytes or a version. | Contract, HTTP, Cluster | P2.5 |
| A25 | GetBucketLocation and configured region behavior are consistent. CORS rules persist; permitted/denied preflights and actual response headers work without bypassing real-request authentication or overbroad caching. | Contract, HTTP | P2.5 |
| A26 | Special keys and XML escaping round-trip. GET/HEAD conditions, signed response overrides, full/ranged reads, and cold-cache ranges return correct content and headers. | HTTP, Gateway | P2.2, P2.4 |
| A27 | Recorded current selected AWS CLI v2, Boto3, and mc versions pass the declared compatibility profiles using normal signing/checksum behavior. Supported matrix entries map to passing tests; existing core/web/CLI workflows remain intact. | HTTP, Gateway, Cluster | P2.1, P2.6 |

## Phase 3: A28–A38

| ID | Scenario and required result | Required lane(s) | Tasks |
|---|---|---|---|
| A28 | With 10k and 100k keys plus unrelated metadata, steady-state listing uses no global or full-bucket materialization. Instrumented visited records and allocation follow page/index bounds, including large common-prefix groups. Record p50/p95 and index size. | Contract, Resource | P3.1 |
| A29 | Index backfill/rebuild/cutover survives interruption, cluster rejoin, and concurrent permitted operations. Revision barriers prevent stale successful listings; existing v1/v2 records and rollback artifacts are retained correctly. | Contract, Cluster | P3.1 |
| A30 | Stream 64 MiB and 1 GiB objects at concurrency 1 and 4 through local and gateway paths. Slow upstream/downstream tests stay inside declared memory and queue envelopes, with raw bytes intact. | Resource, Gateway | P3.2 |
| A31 | Disconnect, cancellation, upstream interruption, disk-full, and restart leave no published partial cache/object, runaway task, unbounded mailbox, or permanently leaked staging file. Cold-range and cache eviction during reads remain correct. | Contract, HTTP, Gateway | P3.2 |
| A32 | Crash/fault injection around stage, sync, rename, replication, metadata commit, and response delivery preserves acknowledged data and isolates uncertain operations for recovery. Restart checks content and metadata, not just liveness. | Contract, HTTP | P3.3 |
| A33 | Real leader loss/election/rejoin and data-node failure/recovery preserve read/strict-write behavior. A data outage cannot silently reduce RF/W; invalid bootstrap or stale generation cannot count as a durable replica. | Cluster | P3.3 |
| A34 | GC/packing/repair interleavings preserve blobs referenced by multiple keys, old versions, multipart uploads, readers, and unknown operation intents. Current unsupported-mode guards remain enforced. | Contract, Cluster | P3.3 |
| A35 | Durable job retries, duplicate delivery, lease expiry, stale-worker fencing, replication backlog, read repair, and scrub/corruption cases recover without losing data or accepting stale state transitions. | Contract, Cluster | P3.3, P3.5 |
| A36 | Create a backup, then change/delete originals. Restore into an empty isolated standalone root with originals inaccessible. Verify hashes, versions, tags/attributes, IAM/configuration, and specified multipart/job handling from the backup boundary. | Restore | P3.4 |
| A37 | Restore a supported cluster backup into a clean isolated topology with safe node/lease fencing. Missing/corrupt artifacts or required keys fail verification. Outbound/destructive workers remain disabled until validated activation. | Restore, Cluster | P3.4 |
| A38 | All suites and acceptance profiles pass; production assets/release/container smoke checks pass; diagnostics/metrics/readiness, upgrade/backfill/rollback notes, and release documentation match tested behavior. No package publish or production deployment is required. | All applicable lanes | P3.5, P3.6 |

## Resource profiles and thresholds

These are implementation acceptance workloads, not claims about the current server.

**Listing:** Generate 10k and 100k objects through a realistic fixture path that produces the same index/metadata invariants as normal commits. Include multiple buckets, versions, deletes, deep common prefixes, and unrelated records. Test multiple page sizes. Instrument the actual metadata/index access path: a renamed wrapper around a hidden full scan does not pass. After startup/backfill, changing unrelated bucket size must not cause work proportional to that unrelated size.

**Streaming:** Warm the application, record a baseline, and run 64 MiB versus 1 GiB transfers at fixed concurrency 1 and 4. Generate/read payloads in the client without loading them into one large Python or Elixir binary. Exercise local PUT/GET, gateway write-through, cache miss/hit, and throttled peers; cover remote replica/repair transfers through cluster profiles. Test an actual large transfer and byte hash, not only a mocked Enumerable.

Before the benchmark, check in the expected buffer/concurrency model and numeric memory/queue thresholds. Derive them from configured buffering and normal runtime overhead, not from object size. Record BEAM total/binary memory, RSS, relevant queue depths, and temporary-disk peak; distinguish OS page cache. Threshold changes require a documented architectural explanation. Increasing the permitted memory in proportion to the uploaded object is not an acceptable fix.

**Heavy optional extension:** Provide a reproducible 1M-key scale profile and object-limit/many-part profiles. These extensions do not replace the mandatory 100k-key and 1 GiB runs. Do not report them as validated unless executed.

## Evidence record for each row

Record scenario ID; starting/ending SHA; test path/case; deployment mode; client/toolchain versions; exact command; exit status; relevant assertion output; and sanitized artifact/log path. Resource rows also need measurements and thresholds; restore rows need backup ID, consistency boundary, original-root isolation proof, and verification output.

Allowed outcomes are `pending`, `in_progress`, `passed`, and `blocked`. A command that did not run, an unmet prerequisite, or a required case intentionally skipped is not passed. Aggregate verification returns failure/non-success when any required row remains blocked or pending.
