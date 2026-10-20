# ESS-S3-HARDENING-001 Phase 1 evidence audit

Audit of the actual checkout on `main`, baseline/current committed SHA
`3477bc540c3732dba3d47a7e08779783f9be25fc`. The implementation is uncommitted
coordinated task work. This document evaluates evidence; it does not update the
coordinator's acceptance JSON or claim whole-goal completion.

Evidence filenames below are relative to `docs/reports/s3-hardening/`. Test paths
are relative to the umbrella and refer to their named modules/cases. Successful
scoped runs establish the described assertions, not a fresh whole-tree pass after
every subsequent concurrent edit. Failed historical runs remain retained.

## Current audit checkpoint — 2026-10-18

The machine-readable ledger has **29 in_progress, nine blocked, zero passed**
across all38 rows. A01–A12 and A14–A18 remain in progress; A13 remains blocked.
Recommendations and passing profiles below are historical scoped evidence,
never current complete-row promotion. Required source fingerprints include the
uncommitted implementation; committed HEAD alone does not identify it.

- `phase3-standalone-r2.log` exits1: the32-completion race returns one200,
  twenty-five412, two409 and four503 responses. All five independent P2/client
  programs subsequently pass, but retained-root restart fails primary discovery.
- `phase3-primary-timeout-repro.log` independently records direct50ms read
  returning `quorum_unavailable` in59ms and public50ms read leaking a primary
  timeout exit in5002ms. [Concord#96](https://github.com/gsmlg-dev/concord/issues/96)
  is separate from [#94](https://github.com/gsmlg-dev/concord/issues/94)'s sampled
  transition-cost blocker. Source links recovery replay to the same assertions;
  the failed standalone restart itself has no sampled CPU attribution.
- `phase3-generated-xml-r1.log` passes3/0, including5402 rejected generated
  inputs bounded to4096bytes plus valid round trips and depth limits. The same
  corpus passes within `phase3-operational-metrics-green-r1.log` core13/0,S317/0.
  The earlier missing/queued P1.7 statement is superseded.
- `phase3-retained-standalone-r1/results.json` has complete=true for the unchanged
  SDK exercise, admin/metrics, actual owned-BEAM SIGKILL/same-root restart,
  persisted content SHA256 and cleanup. This separate lane passes; it does not
  clear the failed hardening restart or race.
- Final aggregate `phase3-final-lanes/correctness.json` is blocked/interrupted.
  Format and strict compilation completed exit0. Its retained test log records
  core516/5/1excluded, transport40/0/1excluded, CLI3488/0 and only155S3/2 before
  termination; web and complete S3 results are unexecuted in this invocation.
  The core failures include valid-marker test handling, two authoritative-index
  legacy fixtures, a descriptor fixture conflict and the unchanged60s large
  backfill timeout. S3 has32-writer and subsequent copy-source timeouts. Scoped
  fixture repair is separate work; no full-suite success is inferred.

Fresh gateway/security/cluster profiles and a complete current-source gate
remain mandatory. The retained RustFS run7 and earlier cluster r2 are passing
assertion evidence, not validation of every later source change. See the
coordinator-owned [blocker register](s3-hardening-blockers.md) and
[results](s3-hardening-results.json) for required upstream release/update gates.

## Historical gate and promotion recommendation

Standalone `phase2-standalone-r7.log` and three-voter
`phase2-cluster-r2.log` both finish with exit0. The latter repeats all Phase1
independent signature/checksum/IAM/32-writer/bucket-race groups on both data
endpoints before completing the full Phase2 profile. RustFS run7 completes16/16
with retained mc24/0. These completed runs supersede earlier pending harness
status; historical failures remain retained.

Recommend promotion of **each A01–A18** once the coordinator confirms the complete
`phase2-umbrella-final.log` exits0 and current formatting/strict-compilation gates
pass. That consolidation run subsequently exited2: core422/0,transport39/0,
CLI3488/0 and web21/0 passed,but S3348/5 failed with two null-version response
header regressions and three request/whole-test timeouts. The coordinator fixed
the header cause; fresh focused `phase3-initial-focused.log` S327/0 includes the
version/header cases and synchronized32-writer races. It does not prove the
accumulated-suite timeout is resolved. No row promotion is made before a clean
complete gate. This is a conditional recommendation,not a JSON change
or whole-goal completion. Phase3 A28–A38 remains independent mandatory work.

## Executed evidence by acceptance row

| ID | Actual evidence | Audit outcome |
|---|---|---|
| A01 | `authorization-control-final.log` core7/0 plus S348/0 includes OperationRoutingTest snapshots of bytes/history/bucket configuration on excluded PUT/DELETE; P2.5 routing/tagging tests in `p2-5-integrated-r1.log` core7/0 plus S320/0 establish supported Tagging changes only tags. Independent Botocore `phase2-cluster-r2.log` exercises unsupported selectors on both data endpoints. Gateway run7 compares actual upstream body/version history and **all method/path request counts**, including HEAD/GET, before any direct upstream inspection. | Required contract, HTTP and gateway zero-request assertions covered. Run5 lacked the all-request assertion; run7 supplies executed evidence without erasing that historical omission. |
| A02 | OperationRoutingTest covers conflicts/duplicate selectors, empty and duplicate source-version identities, duplicate copy headers, copy headers combined with multipart initiation/completion, harmless SDK `x-id`, key/metadata limits. `final-control-http.log` S310/0 includes final copy/multipart ambiguity and completion-limit additions. `phase2-cluster-r2.log` Botocore checks duplicate upload IDs, empty version IDs, part0, uploads/uploadId conflicts. | Promote for current scope after normal integration rerun. Exact ambiguous copy controls are Plug contracts; the lane also has real HTTP identity/selector fixtures. |
| A03 | AuthorizationHardeningTest checks independent destination/source grants; signed AuthorizationHTTPTest historical CopyObject requires GetObjectVersion and verifies old bytes. UploadPartCopyHTTPTest rejects destination-only and ordinary GetObject on an explicit source version, then copies selected range after version grant; denied requests create no parts. Recorded auth/phase1 focused runs pass. | Promote. The six Phase2 tag-grant regressions pass; their integration fixture rerun is recorded below. |
| A04 | AuthorizationHardeningTest tests all four endpoints against the original upload identity. Signed AuthorizationHTTPTest first denies unprivileged callers on the original identity, then rejects wrong bucket/key with permitted identity; retained parts remain unchanged. `authorization-control-final.log` passes. | Promote. |
| A05 | PolicyHardeningTest covers exact canonical/legacy ARN scope, HEAD-only legacy grant, legacy multipart scope, standard action mapping and legacy deny precedence. Signed AuthorizationHTTPTest proves canonical HEAD and HEAD-only cannot GET plus explicit legacy deny over canonical allow. `authorization-control-final.log` passes. | Promote. |
| A06 | PolicyHardeningTest fails closed for missing attached deny. `phase2-cluster-r2.log` records six IAM groups on both endpoints: source denial; acknowledged deny attachment; missing and malformed deny records; suspension; key/presign revocation. Reads of policies/users/keys are explicitly linearizable. No consistency sleeps. | Promote for required unreadable/missing-record behavior and declared cluster consistency. No separate backend transport-timeout injection was found; do not claim one was executed. |
| A07 | Contract and signed AuthorizationHTTPTest cover current/version grants, mixed keys and historical versions, denied entries in quiet mode, checksums,1000 limit, duplicate-key rejection,8MiB body and unchanged retained state. Gateway run7 includes real success and path-specific denial in the same batch. | Promote. Cache/index detail of rejected entries is covered under A15/run7. |
| A08 | `integrity-final-http-seed1.log` core20/0 plus S310/0 verifies valid signature/altered declared body leaves bytes and history unchanged. Gateway run7 `checksum-and-signed-payload-rejection-before-upstream` verifies actual upstream bytes/history and zero mutation. | Promote. |
| A09 | `integrity-contract-current.log`65/0 and UploadIntegrityHTTPTest in `integrity-final-http-seed1.log`: Content-MD5,CRC32,CRC32C,CRC64NVME,SHA1,SHA256; correct and corrupt values distinguished from ETag/CAS. Explicit unsupported algorithms reject before publication. | Promote. |
| A10 | Same contract/HTTP runs plus run7 `signed-chunks-and-trailers-valid-and-corrupt-truncated-trailing-rejection`: actual signature chain/terminal chunks, decoded lengths, trailer checksums, corruption/truncation/trailing-byte negatives and retained upstream history. | Promote. |
| A11 | Known AWS SigV4 contract fixtures plus independent Botocore fixture group in `phase2-cluster-r2.log`: space/plus/percent/Unicode keys, repeated/empty query pairs, duplicate headers, invalid service scope, doubled slashes and dot/parent segments. Both listening data endpoints used. | Promote. |
| A12 | Actual signed/presigned PUT in UploadIntegrityHTTPTest constrains header/query/key and checks revocation. AuthorizationHTTPTest independently presigns GET and tests expiry/policy/suspension/key revocation. ReadSemanticsHTTPTest ranged presign tests signed response overrides and tampering. Recorded integrity/read/auth HTTP passes. | Promote. |
| A13 | ConditionalWriteTest contracts plus `phase2-cluster-r2.log`:32 synchronized create-only PUTs exactly one winner,32 conditional multipart completions exactly one winner, stale PUT If-Match and stale/current completion If-Match across both endpoints. | Promote. |
| A14 | BucketLifecycle fault/generation contracts; signed bucket/control HTTP tests show old versions409, marker-only409, then empty204 and injected timeout503 unchanged bucket. `phase2-cluster-r2.log` records8 write/delete/recreate races across endpoints plus multipart/delete race. | Promote. Old framing-failed cluster run is superseded by v3's completed race records, not erased. |
| A15 | Gateway run7 checks denied403, unavailable503, throttled429->SlowDown503, mixed batch, actual read60.004s and DELETE60.019s socket timeouts, retained object metadata/durable intent/upstream bytes. It compares per-key index-entry identity, whole-index identity where applicable, and cache file hash/size/existence **before** any later GET. | Required status/body/per-entry and cache/index preservation assertions covered. Run5 lacked cache/index identity; run7 closes that actual evidence omission. |
| A16 | Gateway run7 distinguishes two real upstream-success/local-commit-failure paths and two unknown-response paths for PUT/DELETE. Full BEAM PID changes, durable intents/staging survive, normal reconciliation resolves, bytes/version/marker inventories and mutation counts prove no extra external mutation. | Promote. Does not claim arbitrary independent requests have exactly-once semantics. |
| A17 | Gateway run7 out-of-band replacement/deletion and cold-range/raw-gzip attributes: returned size/ETag/system/user metadata match body identity. Hosted-only version/multipart/tagging guards reject501 with zero upstream requests of any method. | Required freshness/identity and pre-upstream guard behavior covered. |
| A18 | `authorization-control-final.log` and `final-control-http.log`: actual1MiB config,8MiB batch,16MiB completion limits; malformed/truncated/DOCTYPE/invalidUTF8 controls preserve config, invalid dates ignored safely under S3 semantics, invalid ranges416, sanitized/bodyless HEAD errors. LateStreamHTTPTest actual TCP failure yields a prefix/truncation and closure without appended XML/second500/private details. | Promote. Tests use a listening server for body/error/framing acceptance. |

## Executed run7 evidence closes the gateway audit omissions

The audit identified two real omissions in run5. The integrity worker extended
`e2e/gateway_hardening.py`, `e2e/gateway_fault_proxy.py`, and
`e2e/scripts/gateway_hardening_control.exs` to:

- Snapshot every proxy method/path count before unsupported selectors and hosted-only
  guards; compare before querying upstream directly, so HEAD/GET cannot hide.
- Inspect deterministic cache-index and selected-entry identities plus actual cache
  file SHA256,size,existence. Assert rejection/timeout retention **before** a GET can
  rebuild anything. Mixed batch compares only the denied entry/file, because the
  successful sibling legitimately changes the whole index.

`gateway-rustfs-run7.log` contains all16 PASS groups and
`gateway-rustfs-run7-results.json` records `complete: true` with16/16 passing.
The actual read timeout is60.004s and DELETE timeout60.019s. The retained mc
compatibility lane passes24/0. Boto3/botocore are1.43.114; mc is
RELEASE.2025-08-13T08-35-41Z with Go1.24.6 linux/amd64. The JSON records the
actual RustFS image and repository digests. Separate
`checksum-range-all-paths.log` passes19/0 and
`gateway-range-checksum-repair.log` passes8/0. These are executed scoped results,
not a claim that later whole-goal integration has passed.

## Phase2 authorization defect found during the audit

At the initial RED checkpoint, Authorize checked PutObject without additionally
checking PutObjectTagging for tagged PUT or multipart initiation. CopyObject destination
tag checks must use the actual selected/replacement tags before streaming/commit.
New `TaggingAuthorizationHTTPTest` has six independently signed HTTP regressions.
The coordinator captured6/6 RED failures in `legacy-safety-tag-auth-red.log`,
including unauthorized200 responses and tag-count disclosure. The worker then
added narrow PUT/MPU header authorization plus `authorize_copy_tags/4`, which
checks the actual destination tag map and returns `:ok` or
`{:error, :access_denied}`. Empty copied tag sets need no additional grant.
The coordinator integrated COPY/helper authorization, tag-count filtering and
typed AccessDenied mapping. `tag-auth-legacy-safety-green.log` confirms all six
new tag-authorization tests pass. Its consolidated S3 run is23 tests/1 failure:
an older mixed-batch test still expected a delete marker in a never-enabled
bucket, where Phase2 correctly removes the null object. The worker updated that
HTTP fixture and its matching contract fixture to assert object absence, exact
denied metadata retention, and actual denied-object bytes over signed GET.
The separate version-enabled marker assertions remain. The coordinator then ran `phase2-umbrella-repairs.log`: core8/0,CLI36/0 and
S317/0,including these corrected fixtures. Final whole-umbrella validation remains
a separate gate.

Official AWS action table read on2026-10-18:
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html>.
It conditionally requires destination PutObjectTagging for tagged PutObject,
CreateMultipartUpload and CopyObject. CopyObject source requires GetObject or
GetObjectVersion; it does **not** list an extra GetObjectTagging requirement.
Separately issuing GetObjectTagging, as high-level client copy tooling can do,
requires its own corresponding current/version tagging action. Untagged copies
and empty REPLACE must retain basic copy permissions. Existing unmarked legacy
PutObject grants must not acquire multipart or tagging privileges.

At the initial RED checkpoint, Shared emitted `x-amz-tagging-count` without
tag-read permission. The current implementation gates that header on the
corresponding current/version tag-read action. The same test file now proves
ordinary object GET remains200 while the header is omitted; current and
explicit-version grants are separate, and an explicit tag-read deny removes the
header without denying bytes. This regression passes in
`tag-auth-legacy-safety-green.log`. Official
GetObject response documentation states the header is available only with the
relevant permission to read tags:
<https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>.
The coordinator integrated the Shared repair after the recorded RED execution.

## Remaining integration boundaries

The coordinator owns the running final umbrella gate. Earlier native journal
failures were traced to fixture socket-port collisions; scoped repairs pass in
`phase2-umbrella-repairs.log` and retain all lock ownership tests. Neither that
narrow rerun nor the completed client lanes substitutes for the final complete
core/transport/CLI/S3/web result. Phase3 remains mandatory.
This audit itself starts no Mix process or service, changes no acceptance result,
and performs no production operation, commit, push, publish or deployment.

## Umbrella fixture integration follow-up

`phase2-umbrella-current.log` core421/4 includes MultipartCommitTest failing
`:bucket_not_found`: its historical fixture wrote a v2 upload for literal
`"bucket"` without creating that bucket. New required bucket guards correctly
reject it. The coordinator assigned this test-only integration repair to the
authorization worker. The fixture now creates a unique actual bucket through
Metadata.create_bucket, reads its metadata with a linearizable barrier, and uses
the actual BucketLifecycle.generation in the v2 upload. Cleanup includes that
owned bucket key. This test specifically covers v2 cluster publication; native
standalone Multipart.init_upload would produce a different legacy-key fixture.
Every existing invalid-durability, descriptor/location/outcome atomicity,
idempotent retry and stale/fresh operation-intent assertion remains. No production
guard was weakened. The scoped core repair run passes8/0,and the final umbrella core app now
passes422/0. The LegacyObject/Migration fixtures were separately repaired by the
coordinator; the latest core pass includes them. No production guard was weakened.

## Earlier audit during the resource source freeze

This append-only audit checks the actual test sources, completed logs and
`s3-hardening-results.json` on `main`, HEAD
`3477bc540c3732dba3d47a7e08779783f9be25fc`. HEAD still identifies the baseline,
not the coordinated uncommitted implementation. Earlier recommendations above
are historical; this section does not promote rows or erase failed evidence.
At that checkpoint rows awaited current-source regressions and A13 was blocked.
The generated P1.7 corpus was then queued; its executed result is recorded below.

The completed standalone r7, cluster r2 and RustFS run7 executions establish
the named assertions below. They do not establish a new source fingerprint or
a fresh complete-suite pass after later implementation changes. Evidence names
are relative to `docs/reports/s3-hardening/`; named modules live in their
corresponding application's test directory.

| Row | Recorded passing assertion evidence | Current boundary |
|---|---|---|
| A01 | OperationRoutingTest snapshots bytes, versions and bucket/config records; TaggingHTTPTest and routing tagging case preserve bytes/history. `authorization-control-final.log` passes core7/S348. `phase2-cluster-r2.log` checks unsupported selectors on both endpoints. RustFS run7 checks unchanged body/version inventory and zero upstream requests of every method. | Pending current-source regression. No remaining assertion gap found in these recorded lanes. |
| A02 | OperationRoutingTest rejects duplicate/conflicting selectors, copy-version identities, duplicate copy headers and copy headers combined with multipart operations; harmless SDK parameters preserve CRUD. MultipartEdgeTest checks malformed part numbers. `final-control-http.log` passes10/0 including final ambiguity/control additions; cluster r2 has independently signed invalid identity/selector cases. | Pending current-source regression. Exact copy-header ambiguities are contract cases; independent HTTP covers the operation/identity lane. |
| A03 | AuthorizationHardeningTest and signed AuthorizationHTTPTest separate source, explicit-version and destination grants. UploadPartCopyHTTPTest requires source-version access, verifies selected bytes/range and absence of parts after denial. `authorization-control-final.log`, `integrity-final-http-seed1.log` and completed cluster source-denial group supply passing evidence. | Pending current-source regression; no missing required Contract/HTTP assertion found. |
| A04 | AuthorizationHardeningTest and AuthorizationHTTPTest exercise UploadPart/ListParts/Complete/Abort against the original identity with an unauthorized caller, then wrong bucket/key with a permitted caller; retained parts do not change. `authorization-control-final.log` passes. | Pending current-source regression. |
| A05 | PolicyHardeningTest verifies canonical/legacy ARN scope, legacy HEAD-only and multipart grants, standard mappings and deny precedence. Signed AuthorizationHTTPTest verifies canonical HEAD, HEAD-only GET denial and deny across aliases. `authorization-owned-pass.log` passes42core/60S3; `authorization-control-final.log` passes the extended HTTP cases. | Pending current-source regression. |
| A06 | Missing attached deny contract plus cluster r2 six IAM groups: source denial, acknowledged attached deny, missing/malformed deny, suspended user, revoked key/presign, immediately on both endpoints. Policy/user/key reads use the declared linearizable consistency. | Pending current-source regression. No separate injected backend transport-timeout policy-read test is claimed; missing and malformed/unreadable-record behavior is exercised. |
| A07 | AuthorizationHTTPTest and contracts cover current/version permissions, mixed keys/versions, quiet errors, checksum/count/body limits and unchanged denied entries. `authorization-control-final.log` and repaired fixtures in `phase2-umbrella-repairs.log` pass. RustFS run7 adds a genuinely successful entry and path-specific denied sibling in one batch. | Pending current-source regression; the earlier single-denied-entry gateway omission is closed by run7. |
| A08 | UploadIntegrityHTTPTest correctly signs an intended hash while sending altered bytes and checks unchanged hosted body/version inventory. `core-s3-after-gateway-copy.log` passes372core/281S3. RustFS run7 independently verifies protected upstream bytes/version inventory and no mutation after rejection. | Pending current-source regression. Use the corrected HTTP citation below. |
| A09 | UploadIntegrityTest independent vectors plus UploadIntegrityHTTPTest actual good/bad MD5, CRC32, CRC32C, CRC64NVME, SHA1 and SHA256; unknown algorithms reject without publication. `integrity-contract-current.log` passes65/0; `core-s3-after-gateway-copy.log` passes the hosted HTTP suite. | Pending current-source regression; no required algorithm assertion gap found. |
| A10 | Integrity/AwsChunked contracts and UploadIntegrityHTTPTest validate signed chain/terminal signatures, decoded lengths, framing/checksum trailers and corruption/truncation/trailing negatives. RustFS run7 independently covers signed/unsigned positive bytes plus strengthened terminal, malformed/duplicate-trailer and length negatives with protected history unchanged. | Pending current-source regression; no required streaming assertion gap found. |
| A11 | SigV4 official known fixture plus cluster r2 independent Botocore special-key/query/header group: spaces, plus, percent, Unicode, repeated/empty query values, duplicate headers, invalid credential service, doubled slashes and dot/parent segments. Both listening endpoints are checked. | Pending current-source regression. |
| A12 | UploadIntegrityHTTPTest presigned PUT constrains Content-Type/query and revocation. AuthorizationHTTPTest independently presigns GET with expiry/policy/suspension/revocation. ReadSemanticsHTTPTest checks signed/presigned response overrides and tampering. Hosted suite, authorization-control and range/read scoped logs pass. | Pending current-source regression. |
| A13 | Historical cluster r2 passes32 synchronized create-only PUTs,32 create-only multipart completions and stale/current If-Match across endpoints; scoped repaired race also passes in `phase2-umbrella-repairs.log`. Current `phase3-standalone-r2.log` fails completion with1x200,25x412,2x409,4x503. | Blocked by gsmlg-dev/concord#94. Historical successes do not clear the current failed deadline gate; no dependency patch, deadline inflation or safety weakening is authorized. |
| A14 | BucketLifecycle fault/generation contracts; signed BucketAndControlHTTPTest history409, marker-only409, empty204; injected deletion timeout503 leaves bucket unchanged. Cluster r2 completes8 cross-endpoint write/delete/recreate races plus multipart/bucket deletion race. | Pending current-source regression; earlier framing-failed cluster runs remain historical failures. |
| A15 | RustFS run7 covers403/503/429, mixed batch, actual read60.004s and DELETE60.019s inactivity. Failed entries preserve metadata, durable intent, selected/whole index identity as applicable and actual cache file hash/size/existence before a GET can rebuild anything. All16 groups pass. | Pending current-source regression; earlier cache/index and error-variant omissions are closed by run7. |
| A16 | RustFS run7 covers explicit upstream-success/local-commit failure and unknown response outcomes for both PUT and DELETE. Full BEAM PID changes, retained staging/intents, ordinary reconciliation and exact external version/marker/mutation inventories prove recovery without blind repeat mutation. | Pending current-source regression. No arbitrary-request exactly-once promise is inferred. |
| A17 | RustFS run7 verifies external replacement/deletion, cold ranges, raw gzip and consistent size/ETag/system/user attributes/body. Unsupported gateway version/multipart/tagging combinations reject before any upstream request. Existing cluster gateway-write guard has hosted S3 contract coverage. | Pending current-source regression. |
| A18 | `authorization-control-final.log` and `final-control-http.log` pass actual1MiB config,8MiB delete and16MiB completion limits, malformed XML/date/range checks, retained state, bodyless/sanitized errors. LateStreamHTTPTest uses real TCP and verifies truncation/closure without appended XML/second status/private detail. | Pending current-source regression. `phase3-generated-xml-r1.log` passes3/0 with5402 rejected generated cases, maximum4096bytes; valid round trips and depth bounds also execute. This closes the earlier queued corpus gap without promoting the complete row. |

### Corrections to earlier evidence references

- `integrity-final-http-seed1.log` runs HTTPClientTest,
  UploadPartCopyHTTPTest and AuthorizationHardeningTest, passing core20/S310.
  It does **not** run UploadIntegrityHTTPTest. The A08-A10 hosted HTTP citation
  is `core-s3-after-gateway-copy.log`, whose command is
  `mix test apps/ex_storage_service/test apps/ex_storage_service_s3/test`:
  core372/0 with one separately handled cluster exclusion and S3281/0.
- `phase1-core-s3-v4.log` has core372/0 but S3279/1. It is not a whole-run
  pass. The subsequent Shared/error and hosted-suite passing logs provide the
  applicable repaired evidence.
- `tag-auth-legacy-safety-green.log` has core4/0 and S323/1. Its six named
  tag-authorization regressions pass, but a stale never-enabled bucket marker
  fixture fails. `phase2-umbrella-repairs.log` subsequently passes core8/0,
  CLI36/0 and S317/0 with corrected fixture semantics and preserved assertions.
- `phase3-initial-focused.log` has core67/10 and S327/0. Its S3 version/header
  and conditional cases pass; it is not a passing whole invocation. The core
  failures concern backup checks and remain separately accounted for.
- `phase2-umbrella-final.log` has core422/0, transport39/0, CLI3488/0 and
  web21/0, but S3348/5. Scoped repairs and previous client profiles do not turn
  that full run green. Current-source complete regression remains mandatory.

The results JSON remains coordinator-owned and unchanged by this audit. Its
generic pending descriptions and old citations need the above exact mapping;
they must not become row passes before current-source validation. This worker
ran only read-only inspection and scoped documentation checks, started no Mix
or services, and made no source/e2e change during the resource freeze.
