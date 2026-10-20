# P1.2 authorization evidence

Goal ESS-S3-HARDENING-001. Assignment: authorization/IAM plus multipart identity
binding and batch-delete authorization. Starting checkout: main,
3477bc540c3732dba3d47a7e08779783f9be25fc. User task inputs were the only initial
untracked files. Other workers own resolver/router, stream integrity and core
mutation work; their changes are retained.

## Reproduced baseline

The root baseline run captured the six `PolicyHardeningTest` failures in
`docs/reports/s3-hardening/baseline-tests-retry.log` before policy source edits:
canonical S3 resources denied a matching legacy scope; a missing attached policy
was dropped while the allow policy remained; a statement with unsupported
`condition` was accepted; request-specific action migration API was absent.
The baseline log also contains unrelated pre-existing CLI TLS failures; these are
not authorization test passes.

S3 regression tests added at
`apps/ex_storage_service_s3/test/ex_storage_service_s3/authorization_hardening_test.exs`:
copy source/destination, explicit source versions, presigned resource policy,
wrong-key binding of all four multipart endpoints, and per-entry delete grants.
Their baseline and repaired executions remain pending until root releases shared
Mix/server ownership.

## Policy semantics decision

Newly created policies carry `semantics: :aws_s3_v1`. Unmarked stored policies
retain operation-specific legacy allow evaluation. Request authorization passes
both the real AWS action and the original ESS operation alias: legacy HEAD-only
grant cannot become GET, and legacy ordinary PutObject cannot acquire multipart
capability. Explicit denies check both forms. Canonical `arn:aws:s3:::` and legacy
`arn:ess:::` normalize only their prefixes; bucket/key scope stays unchanged.
Unsupported statement keys or malformed policy records fail closed. No automatic
rewrite of stored policies is performed. Locked Concord get/2 documents a
linearizable VSR barrier for leader/strong/eventual names; cluster propagation
still requires an executed cluster test before A06 can pass.

## Current status

Implementation in progress. No acceptance row is claimed passed. Required HTTP,
cluster policy-revocation and gateway batch-delete verification remain pending.

The same baseline VM subsequently executed the S3 regressions against its original
compiled handlers. The log shows: explicit-version copy was incorrectly allowed
with only GetObject; UploadPart accepted a different key; batch deletion returned
Deleted for both permitted and forbidden entries (lines 950 onward). This is
runtime reproduction, not merely a source audit.

## Focused verification

`PAGER=cat mix test apps/ex_storage_service/test/ex_storage_service/iam
apps/ex_storage_service_s3/test/ex_storage_service_s3/authorization_hardening_test.exs
apps/ex_storage_service_s3/test/ex_storage_service_s3/plugs/authorize_test.exs
apps/ex_storage_service_s3/test/ex_storage_service_s3/multipart_test.exs
apps/ex_storage_service_s3/test/ex_storage_service_s3/multipart_edge_test.exs`
completed exit 0: IAM 42 tests/0 failures; S3 60 tests/0 failures.
Log: `docs/reports/s3-hardening/authorization-owned-pass.log`.

The first implementation validation exposed missing-key shape handling
(`Concord.get` returns `{:error, :not_found}`), then integration validation
exposed action-alias separation for new version permissions and an unset auth
configuration boolean. Both were repaired; cumulative Sol repair rounds: 2,
with the owned suite passing. `unbuffer` is unavailable; Mix commands used
`PAGER=cat` and bounded tool polling.

Additional tests subsequently added (not part of that 60-test pass): independent
real HTTP authorization/presign suite; batch limits/integrity/explicit versions;
initiation attribute retention; synchronized 32-way conditional multipart
completion. The latter revealed a root-owned metadata contention issue:
one success with erroneous 500 responses classified as
`:operation_intent_compare_failed` by bounded runtime tracing. Coordinator
repaired shared per-hash guard contention, retaining the inverse GC lock, and
reported the focused 32-completion regression passing. Its authoritative rerun
is tracked by the coordinator's evidence; this worker does not count its earlier
failed run as a pass.

The independent real HTTP authorization suite completed exit 0: 5 tests,
0 failures (`authorization-http.log`). Its Req signer/presigner covers separate
copy grants, legacy HEAD-only permissions, all four wrong-identity multipart
endpoints, mixed quiet batch authorization, and post-presign policy/key/user
revocation plus expiry. Additional batch checksum/limit/version and attribute
tests passed in `authorization-extended.log`; the same run's concurrency test
failed as described above.

## Scope additions

Single and batch gateway DELETE now propagate upstream failure and preserve local
state. Explicit gateway version deletion is rejected before upstream mutation.
P1.6 DELETE reconciliation is implemented in the new core
`CloudCache.DeleteThrough` module, with handler wiring owned by the coordinator.
It snapshots bucket generation, local metadata revision and cache identity;
persists a prepared Concord intent before a conditional upstream DELETE; and
requires upstream HEAD absence before its local transaction. Unknown outcomes
never repeat DELETE. Recovery removes matching legacy metadata, cache entry and
journal together and refuses newer local identities or recreated buckets.
Hosted v2 history is rejected before gateway mutation. Shared cache blobs are
not physically unlinked without a safe publication/reader fence.

`delete-reconciliation-unit.log`: initial 11 injected-boundary tests passed,
exit 0. `delete-reconciliation-http.log`: 11 core tests plus 2 S3 tests passed,
exit 0. The latter uses real HTTP and Concord: kills the caller after upstream
success/before local commit, observes the persisted prepared journal, and
reconciles from a separate process. Exactly one conditional DELETE occurs. A
second HTTP/Concord test preserves a newer local object during reconciliation.
A service/VM restart durability lane is still required before A16 can be marked
passed.

Multipart initiation attributes are persisted and retained through completion.
Completion carries If-Match/If-None-Match and bucket-generation metadata options.
Control XML SHA/MD5 validation is separate from final object result checksums;
FULL_OBJECT declarations are verified with a bounded 256 KiB CAS stream before
object publication. Composite result checksum support remains mandatory pending
P2 work; it is explicitly rejected, never accepted without verification.

Documentation: `docs/s3-authorization.md`. No commits, publishing, push or
production data operations were performed.

## Configuration and error hardening followup

Bucket configuration publication now uses `Metadata.BucketConfig`: a native
Concord transaction compares/touches the bucket revision and mutates its config
record atomically. The original bucket generation survives transaction retries;
handlers snapshot it before reading the incoming body. Versioning, lifecycle,
and notification requests propagate storage failures; missing or recreated
buckets cannot acquire orphan configuration. Versioning HTTP reads use the
strict `fetch_versioning/1` result while the existing compatibility getter keeps
its public shape.

`config-error-focused-pass.log`: core12/0 and S3 57/0. The earlier
`config-error-focused.log` failed3 existing fixtures that configured nonexistent
buckets. All affected fixtures now create real buckets; no assertion was removed.
One fixture repair round completed with the green rerun.

`control-errors-http.log`: 30 tests,0 failures. Includes three handlers' explicit
1MiB control limits with actual HTTP413, malformed/truncated/DOCTYPE/invalidUTF8
HTTP400 and unchanged stored config; bucket generation races; Shared typed
transport errors; root-owned late-stream TCP closure and operation snapshots.
`config-error-prod-compile.log`: `MIX_ENV=prod mix compile --warnings-as-errors`
exit0. Mix/runtime was then released to the gateway worker; no owned commands
or services remained running.

The shared HTTP1 error path closes declared request-body connections rather than
draining a rejected Expect/continue body. `error_framing_test.exs` proves real TCP
closure and a succeeding fresh request across Shared, authentication, presigned,
rate-limit and router-error paths. HEAD errors remain bodyless; InternalError
messages are sanitized; typed network inactivity/deadline errors return503.

Later source edits included in the coordinating agent's consolidated run:
CloudCache.Config uses the same fence and snapshots before secret-preservation
reads/encryption; transaction/missing/stale-generation/encryption tests extend
`bucket_config_test.exs`. Real signed HTTP coverage adds historical CopyObject
permission/bytes, unauthorized proper-identity multipart endpoints, mixed batch
current/historical/forbidden-version outcomes and batch checksum/count/duplicate
key/body limits. Bucket DELETE accepts an optional injected function while
preserving its existing arity; a real HTTP timeout injection asserts503 and the
unchanged bucket. The consolidated run subsequently passed core383/0 and
S3298/299; its one S3 failure was the intentionally added Shared wrapper RED
regression described below. Separate CLI failures are tracked by the coordinator.

`bucket_and_control_http_test.exs` is an additional real signed HTTP
lane: historical versions and a remaining delete marker independently block
bucket deletion, then removing both permits204; invalid conditional HTTP dates
are safely ignored for GET/HEAD; invalid ranges return416 without object bytes,
and subsequent bytes/version history remain unchanged. This file was added
while the coordinator owned the full-suite runtime and received a separate run.

`authorization-control-final.log`: core7 tests/0 failures and S348 tests/0 failures.
Exact scoped command:

```sh
PAGER=cat mix test \
 apps/ex_storage_service/test/ex_storage_service/metadata/bucket_config_test.exs \
 apps/ex_storage_service/test/ex_storage_service/metadata/bucket_lifecycle_fault_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/handlers/shared_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/authorization_http_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/bucket_and_control_http_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/bucket_lifecycle_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/bucket_configuration_hardening_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/operation_routing_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/error_framing_test.exs \
 apps/ex_storage_service_s3/test/ex_storage_service_s3/late_stream_http_test.exs --trace
```

This passes every new config/batch/version/bucket/date/range test described here.
Shared now unwraps durable gateway uncertainty before mapping upstream HTTP
statuses;503 stays503,429 stays SlowDown503, known403 stays403. A required local
reconciliation returns503 even when its underlying local error is `:eio`;
an unrelated plain `:eio` remains a sanitized500. The coordinator captured the
previous wrapped503->500 failure in `phase1-umbrella-current.log` before repair.
One Shared wrapper repair-and-validation round passed.

`authorization-control-final-prod.log`: strict production compilation exit0.
`git diff --check` exit0. The runtime was explicitly returned to the coordinator;
all this worker's commands completed and no owned service remains running.
The diagnostic CLI TLS fixture instrumentation preserves CA verification and
all deadlines; it was formatted but not executed by this worker. Coordinator
owns that diagnostic run and the remaining cluster/whole-goal acceptance.

## Matrix assertion audit after the final scoped pass

This is a lane audit, not a whole-goal acceptance declaration. The latest
coordinator umbrella run has core383/0, cluster39/0, CLI3485/2, S3299/1 and
web21/0. The S3 failure was the reproduced Shared wrapper RED, repaired and
verified by the separate core7/0 plus S348/0 run above. The two CLI failures
are still genuine failures and do not become passes through this scoped run.

| Row | Executed proof | Remaining assertion or lane |
|---|---|---|
| A01 | `operation_routing_test.exs` rejects unsupported PUT/DELETE and snapshots body, versions, bucket record and actual versioning/lifecycle/notification records plus absent CORS/tagging/cloud config. Gateway run4 proves upstream bytes, versions and mutation counts unchanged. Independent phase1 HTTP script exercises unsupported selectors on both endpoints. | Phase2 supported Tagging must change tags only; current exclusion tests deliberately expect501. This row cannot remain fully passed after Tagging is enabled without updated tests. |
| A02 | Routing contract rejects conflicts, duplicate upload IDs, empty version IDs and duplicate/empty copy-source versions; harmless `x-id` preserves CRUD. Existing real HTTP multipart edge tests cover0,10001,non-numeric,negative part numbers. Independent phase1 script exercises duplicate IDs, empty versions, part0 and uploads/uploadId conflict. | No explicit negative fixture found for a copy-source header combined with multipart initiation/completion or duplicate copy-source headers. These ambiguous copy/multipart paths need named assertions before declaring exhaustive P1.1 coverage. |
| A03 | Authorization contracts test independent copy-source grants and version grants. Real signed CopyObject now checks destination-only denial, explicit-version denial until GetObjectVersion and copied historical bytes. `upload_part_copy_http_test.exs` checks destination-only denial, GetObject insufficient for version, version grant success and no denied parts. | No missing mandatory assertion identified for Contract/HTTP. |
| A04 | Contract binds each endpoint to original key. Real signed HTTP checks all four original identities denied403 with no part mutation for a caller without grants, then wrong bucket/key404 with a globally permitted caller. | No missing mandatory assertion identified for Contract/HTTP. |
| A05 | Core policy hardening tests prove exact ARN aliases, legacy HEAD-only and legacy multipart scope, standard action mapping and legacy deny precedence. Signed HTTP verifies canonical HEAD, legacy HEAD cannot GET and explicit legacy deny over a canonical allow. | No missing mandatory assertion identified for Contract/HTTP. |
| A06 | Core missing-attached-policy contract fails closed. First successful JSON record in `cluster-iam-hardening.log` proves immediate acknowledged changes on both endpoints: deny, missing/malformed attached deny, suspension/reactivation, key and presign revocation, without consistency sleeps. | The full old cluster script later failed its separate bucket-race lane; this does not invalidate its IAM record and does not pass A14. No injected backend transport-timeout policy-read fixture was found; missing and malformed/unreadable record behavior is exercised. |
| A07 | Contract and real signed HTTP distinguish current/delete-version permission, mixed outcomes, quiet errors, integrity,1000-entry bound, duplicate keys and8MiB control body. Run4 proves truthful upstream-denied batch entry and preserved metadata. | Gateway run4 batch had one denied entry only. Mixed upstream-success and upstream-failure in one batch is assigned to integrity run5 and remains pending here. |
| A12 | `upload_integrity_http_test.exs` actual presigned PUT succeeds, changed constrained Content-Type/query/key revocation fails and size remains14. Independent Req presigned GET exercises expiry/policy/suspension/revocation. Read-semantics actual independently presigned ranged GET applies response overrides; changed signed query403. | No missing mandatory assertion identified for HTTP. |
| A14 | Core native transaction failure/no replay and missing bucket contract; HTTP injected timeout503 unchanged bucket; actual signed HTTP history409 then marker-only409 then empty204. Contracts fence stale generation and incomplete uploads. | Required cluster races remain pending the coordinator rerun; old bucket-race run failed connection framing before completion. Do not promote entire row yet. |
| A18 | Actual HTTP1MiB config and8MiB batch limits preserve state; malformed/truncated/DOCTYPE/invalidUTF8 config400. Invalid conditional dates safely ignored200 for GET/HEAD; invalid ranges416 empty and unchanged bytes/versions. Common HEAD errors empty, sanitized internal detail and root realTCP late-source closure pass. | No explicit oversized CompleteMultipartUpload XML HTTP fixture found. Production completion has16MiB bound, but this is source evidence only; add direct oversized completion coverage before claiming every buffered control operation's bound verified. |
