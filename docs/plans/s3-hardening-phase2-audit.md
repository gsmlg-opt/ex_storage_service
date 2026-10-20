# ESS-S3-HARDENING-001 Phase 2 evidence audit

Audit of the coordinated uncommitted checkout on `main`, committed baseline and
current HEAD `3477bc540c3732dba3d47a7e08779783f9be25fc`. This document records
scoped evidence; the coordinator owns acceptance JSON and all runtime.
Evidence filenames below are relative to `docs/reports/s3-hardening/`.

## Current audit checkpoint — 2026-10-18

A19–A21 and A23–A26 remain in progress. **A22 and A27 are blocked** in the
coordinator-owned ledger. No Phase2 row is passed. The earlier promotion
recommendations below are historical and do not override current outcomes.

`phase3-standalone-r2.log` records passing independent versions/lists, tag/CORS,
multipart, eight synchronized multipart races plus response loss, and selected
AWSCLI/mc programs after the Phase1 race failure. Multipart restart preparation
passes, but the actual retained-root server exits at primary discovery before
verification. The complete wrapper exits1. Prior standalone r7/cluster r2
restart passes do not prove this current restart gate.

The separate unchanged SDK/admin/persistence lane now passes:
`phase3-retained-standalone-r1/results.json` complete=true and
`phase3-retained-standalone-r1/restart-lifecycle.log` records owned SIGKILL and a
new BEAM PID against the same roots. The aggregate includes it as a second
standalone command. No signing/checksum setting or original SDK test is removed.

`phase3-final-lanes/correctness.json` records blocked/interrupted: format and
strict test compile exit0; the retained test log has core516/5/1excluded,
transport40/0/1excluded, CLI3488/0 and155S3/2 before termination. Complete S3 and
web are not executed results of that invocation. The two current
VersioningProfile legacy fixture failures arise from direct legacy injection
under a ready authoritative index; scoped repair and complete rerun are still
separate gates. Do not weaken the explicit migration/no-resurrection guard.

[Concord#94](https://github.com/gsmlg-dev/concord/issues/94),
[#95](https://github.com/gsmlg-dev/concord/issues/95) and
[#96](https://github.com/gsmlg-dev/concord/issues/96) remain mandatory upstream
gates. The50ms/5002ms primary discovery reproduction is a successful defect
reproduction, not successful acceptance. No dependency patch, deadline
inflation, checksum bypass or reduced quorum is accepted.

## Historical gate

`phase2-standalone-r7.log` and `phase2-cluster-r2.log` both finish with exit0.
They execute independent versions/lists,tag/CORS/location,multipart/ranged
versioned part copy,response loss plus8synchronized races,selected clients,and
retained-root multipart restart. Cluster r2 repeats Phase1 HTTP/IAM/races and
uses both data endpoints. RustFS run7 has16/16 groups plus retained mc24/0.

Recommend promotion of **each A19–A27** conditional on complete
`phase2-umbrella-final.log` exit0 plus current formatting/strict compilation.
That consolidation run subsequently exited2: core422/0,transport39/0,CLI3488/0
and web21/0 passed,but S3348/5 failed(two null-version response header
regressions,three request/whole-test timeouts). The coordinator repaired the
header cause; fresh `phase3-initial-focused.log` S327/0 includes version/header
cases and synchronized32-writer races. The accumulated-suite timeouts still
need a passing complete gate; no timeout/check was weakened. Scoped fixture integration already passed
core8/0,CLI36/0,S317/0 in `phase2-umbrella-repairs.log`. No additional mandatory
P1/P2 assertion gap beyond that explicit integration failure was found. These results do not satisfy Phase3 resource,
index,recovery,restore or release gates and do not claim whole-goal completion.

## Per-row evidence and promotion recommendation

| ID | Required assertions and actual evidence | Recommendation |
|---|---|---|
| A19 | VersioningProfileTest covers never-enabled/null,Enabled/Suspended,markers,legacy current/history behavior. VersionProfileHTTPTest covers actual served IDs for current/historical GET/HEAD/range/304,marker404/405+LastModified and missing NoSuchVersion/bodyless HEAD. VersionListingHTTPTest covers IsLatest,multiple versions/markers,pagination,encoding,common prefixes,max0 and distinct grants. Independent versions/lists and tag/CORS probes pass standalone r7 and cluster r2,including historical HEAD/COPY,source/destination/null response IDs,explicit DELETE and tag operations. Earlier null/source-version header defects were reproduced and fixed. Focused logs include `version-profile-green.log`31/0,`version-list-pure-final.log`8/0,`phase2-version-attrs-green-r1.log` core29/0,S315/0. | Promote after final umbrella gate. |
| A20 | ListingProfileHTTPTest covers max0,invalid/max limits,context-bound/malformed continuation tokens,v1markers,v2continuation,StartAfter,encoding and common prefixes. VersioningProfileTest and LegacyObjectTest prevent legacy resurrection beneath v2 replacement/deletion. Independent versions/lists probe passes on standalone and both cluster endpoints:both list APIs,zero pages,prefix-only pages,continuation,key order,Unicode/XML-sensitive keys,StartAfter and URL fields. Fine-grained malformed-token/legacy negatives remain local contract/HTTP fixtures; no extra independent cluster injection is claimed. | Promote after final umbrella gate. |
| A21 | MultipartHardeningTest covers equal-key upload markers and part pagination/bounds; MultipartProfileHTTPTest covers HTTP pages,part order/ETag/checksum/XML/count/size constraints. Independently signed UploadPartCopyHTTPTest covers source/version permission separation,range/conditions and no part after denial. GatewayReadHTTPTest exercises a listening remote-only gateway range,negotiated checksum,pinned GET and no local projection. Existing MultipartTest completes a real pack-only part after loose-file removal. Multipart Boto3 probe passes r7/r2 with explicit historical source/range,stale412,range416,peer part identities/checksums and exact completed bytes/attrs/tags. Cluster r2 additionally forces remote object and part source selection with verified peer hashes and data-a local absence while retaining RF2/W2 and location records. | Promote after final umbrella gate. |
| A22 | MultipartHardeningTest uses blocked readers to prove completion/abort fences late writes; validates released overwrite/new completion token and committed finalization-failure recovery. Its strengthened recovery case enables numbered versions,checks exact one-version inventory and reads original bytes before/after replay. The current core422/0 run includes it. HTTP profiles preserve initiation attrs/checksums/tags and reject completed-upload late writes. `s3_hardening_multipart_races.py` passes actual discarded completion XML and4overwrite+4abort/complete races in both lanes,with barrier synchronization and no sleeps. The standalone log records both aborted/completed terminal outcomes; cluster retries/reads remain valid on both endpoints. Retained-root restart verifies active exact parts,previous completion identity,one version,bytes,attrs,tags/checksum and no active completed upload. | Promote after final umbrella gate. |
| A23 | AttributesProfileHTTPTest covers genuine gzip raw bytes,all declared system/user attrs,tags,verified checksum and independent COPY/REPLACE. MultipartProfileHTTPTest covers required algorithms/types and initiation attrs. GatewayAttributesHTTPTest checks actual forwarding and composite-source recomputation. RustFS run7 includes current upstream checksum/attribute/COPY and raw-gzip cold-read groups. `checksum-range-all-paths.log`19/0 and `gateway-range-checksum-repair.log`8/0 prove full-object checksum headers are omitted on partial responses. Standalone r7/cluster r2 default-client profiles verify raw multipart hashes,composite checksums,attrs/tags after ordinary/remote part copy and restart. COPY checksum/ETag fixes retain `phase2-copy-multipart-regressions.log`18/0. | Promote after final umbrella gate. |
| A24 | Tags/TaggingHTTPTest cover bucket/current/version CRUD,object10/bucket50 limits,malformed/duplicate/reserved/oversized tags,marker errors,permissions and no bytes/version replacement. Six independently signed TaggingAuthorizationHTTPTest regressions pass:tagged PUT/MPU/COPY need destination tag-write,explicit source version read stays separate,empty REPLACE/untagged copy stay basic,legacy grants do not broaden,tag-count disclosure is permission-gated. Independent tag/CORS probe passes r7/r2 with cross-endpoint CRUD,historical tag deletion preserving bytes/IDs,COPY/REPLACE,MPU tags and marker preservation. Gateway object tags reject before upstream effects. | Promote after final umbrella gate. |
| A25 | BucketCORSTest validates matching/limits. CORSLocationHTTPTest covers persisted rules,permitted/denied preflight,Vary,actual unauthorized403 with scoped CORS headers,invalid updates preserving state and configured-region behavior. Default-client tag/CORS profile passes r7/r2 with region/header agreement,persistence and actual-auth checks. Actual requests remain authenticated; CORS grants no read/write permission. | Promote after final umbrella gate. |
| A26 | ReadSemanticsHTTPTest covers independently signed/presigned overrides,range/full response headers,injection/duplicates,ETag/date condition precedence and HEAD. Version/listing HTTP and default-client profiles cover Unicode,plus,percent/XML-sensitive fields and bodyless version errors. RustFS run7 proves out-of-band freshness,cold ranges,genuine encoding and a consistent ETag/size/attrs/body identity. Current/version/gateway range-checksum fixtures pass19/0. | Promote after final umbrella gate. |
| A27 | Both fullprofiles record Boto3/botocore1.43.114,AWS CLI2.34.24 and mc RELEASE.2025-08-13T08-35-41Z; normal signing/checksum defaults remain enabled. Selected clients execute real10MiB upload/download/copy/list/stat/delete on standalone and both cluster endpoints. RustFS run7 records exact upstream image/digests and retained mc24/0. The compatibility matrix maps operations,controls/actions/errors/modes to these probes and named HTTP/contracts. Final core/web/CLI/transport/S3 integrity remains conditional on the running umbrella gate; no narrowed fixture run substitutes for it. | Promote only after final umbrella gate and unchanged required workflow/build checks. |

## Execution and lifecycle boundaries

The reproducible profile entry points are:

```sh
bash e2e/run_hardening_phase2_standalone.sh /tmp/ess-s3-hardening-python/bin/python phase2-standalone-r7
ESS_HARDENING_PHASE2=true bash e2e/run_hardening_phase1_cluster.sh /tmp/ess-s3-hardening-python/bin/python phase2-cluster-r2
```

The coordinator executes these after the current strict production build. The
log labels identify the retained completed executions,not instructions to reuse
an active run. Boto3/botocore are pinned in `e2e/hardening-requirements.txt`.
AWS CLI reports2.34.24/Python3.13.15/Linux6.18.33/x86_64/NixOS26. mc reports
commit7394ce0dd2a80935aded936b09fa12cbb3cb8096 and Go1.24.6linux/amd64.

The standalone lifecycle artifact records SIGKILL of BEAM576439,restart576666
with retained roots. Cluster lifecycle records data-b578040→579345 with retained
roots/bootstrap=false and both endpoints ready before verification. The probes
then verify content/metadata/part state,not merely readiness. Lifecycle artifacts
are `phase2-standalone-r7-restart-lifecycle.log` and
`phase2-cluster-r2-multipart-restart-lifecycle.log`. Full restart state artifacts
contain only disposable transfer identities/expected hashes and no credentials.

## Forced remote-source evidence

Controller `remove-multipart-source-local` is restricted to generated
`/tmp/ess-s3-hardening-cluster-<8 alphanumeric>` roots and exact disposable
bucket/key/version or copied-part-0/upload/part identity. It validates actual
node/context roots/generation,RF2/W2/nondegraded settings,all nonsymlink directory
ancestors,regular loose file size,and SHA256 locally and over data-b RPC before
unlinking only data-a's exact file. Strongly read location records must remain
unchanged. It changes no topology,quorum,location or production data.

Cluster r2 records42-byte historical-object and10-byte selected-part boundaries:
local absent,verified peer,unchanged locations,RF2/W2. Copy/completion on data-a
then returns exact historical range/checksum/attrs/tags through both endpoints.
This proves remote selection; healthy local RF2 copies or mocked transport tests
alone were not accepted as substitutes.

## Retained failures and repairs

- Standalone r5/r6 reached mc and failed signatures because the harness
  URI-encoded Base64 credentials. Exact selected mc `cmd/config.go:219` regex
  parsing takes MC_HOST credential captures verbatim before endpoint URL parsing.
  Literal child-environment credentials with delimiter/newline guards fix the
  harness; no authentication,checksum or SDK change was needed. Passing r7
  supersedes this failure after full execution; historical artifacts remain.
- Cluster r1 passed earlier profiles but its controller failed before unlink:
  Elixir `Path.join("", "/")` produced an empty first ancestor lookup. Repair
  starts at the explicit `/` anchor and retains every safety guard. Pure static/
  read-only checks matched ENOENT and both retained blob hashes; live cluster r2
  subsequently passes the complete fixture. Sol repair round1 passed.
- Earlier umbrella fixture failures and RED security/checksum cases remain
  preserved. Their scoped repairs pass,and complete umbrella integration is
  still assessed independently. No failed or unexecuted case is relabeled green.

No implementation/runtime was performed during this final audit. The worker
changed only its three assigned documents,started no service/Mix/BEAM,and has no
running commands. Acceptance JSON,final workflow outcomes and Phase3 remain
coordinator-owned. No commit,push,publish,deployment or production operation.

## Earlier audit during the resource source freeze

This append-only update checks actual test/probe sources, completed logs and
`s3-hardening-results.json` on `main`, HEAD
`3477bc540c3732dba3d47a7e08779783f9be25fc`. The implementation is coordinated
uncommitted work; HEAD alone is not its build identity. Earlier recommendations
above remain history. At that checkpoint A19-A26 awaited final current-source regressions and
A27 was blocked; the current A22 restart blocker is recorded above. No result JSON or source/e2e file is changed by this audit.

Recorded standalone r7 and cluster r2 profiles both exit0. Cluster r2 includes
both data endpoints, actual remote source selection and retained-root restart.
RustFS run7 has16/16 passing groups plus retained mc24/0. These named executions
cover the row assertions below, but are not a fresh complete-suite/source
fingerprint gate after subsequent Phase3 edits.

| Row | Recorded passing assertion evidence | Current boundary |
|---|---|---|
| A19 | VersioningProfileTest covers never-enabled/null, Enabled/Suspended history and markers; VersionProfileHTTPTest covers current/historical GET/HEAD/range/304, explicit marker405 and current404, identity/Last-Modified and missing-version/bodyless errors. VersionListingHTTPTest covers IsLatest, versions/markers, pagination and encoding. Standalone r7/cluster r2 verify explicit GET/HEAD/DELETE/COPY and source/destination/null IDs. `phase2-version-attrs-green-r1.log` passes29core/15S3; later `phase3-initial-focused.log` S3 portion passes27/0 including corrected version headers. | Pending current-source regression. That later log also has10 core failures and is not a whole-run pass. |
| A20 | ListingProfileHTTPTest covers zero/max/invalid limits, malformed/context-bound tokens, v1markers/v2continuation, StartAfter, encoding and prefix-only pages. VersioningProfileTest and LegacyObjectTest prevent legacy resurrection. Standalone r7/cluster r2 independently check ordered pagination and special/XML-sensitive fields across endpoints. | Pending current-source regression; no additional row assertion gap identified. Large-workload index/resource acceptance remains A28/A29. |
| A21 | MultipartHardeningTest, MultipartProfileHTTPTest and UploadPartCopyHTTPTest cover upload/part pagination, part/order/ETag/count/size/XML limits, source/version grants and ranges/conditions. GatewayReadHTTPTest checks pinned remote-only source; MultipartTest covers packed-only parts. Standalone r7/cluster r2 verify historical/ranged copy, errors, peer part identity/checksum and final bytes. Cluster r2 proves data-a source/part file absence, verified data-b hashes, unchanged locations and RF2/W2. | Pending current-source regression. `phase3-mpu-response-green-r1.log` passes43core/11transport/9S3 after later multipart changes; it does not replace the complete independent profile. |
| A22 | Deterministic MultipartHardeningTest barriers fence late writes and abort after freeze; finalization failure verifies bytes, all initiation attributes and one numbered version before/after recovery. ObjectCommitTest covers ambiguous atomic multipart publication and stable retry. Core422/0 in the retained umbrella run includes these additions. Standalone r7/cluster r2 pass actual completion response-body loss,8 synchronized overwrite/abort races and retained-root active/completed upload restart checks. Latest multipart-focused run passes43core/11transport/9S3. | Blocked in the current ledger by Concord#94/#96: r2 independent multipart/race programs pass, but retained-root restart fails before verification. Application-side unread XML after headers is the tested response-loss boundary; historical successful restart is not current-source restart acceptance. |
| A23 | AttributesProfileHTTPTest verifies genuine gzip bytes, required system/user attributes, tags/checksums and independent COPY/REPLACE. Multipart HTTP covers negotiated algorithms/types and initialization attributes; GatewayAttributesHTTPTest and RustFS run7 verify raw gzip/current upstream attrs/checksums/COPY. `phase2-copy-multipart-regressions.log` passes18/0; `checksum-range-all-paths.log` passes19/0. Standalone r7/cluster r2 include multipart/remote/restart byte and attribute checks. | Pending current-source regression. No full-object checksum is expected on partial responses. |
| A24 | TagsTest, TaggingHTTPTest and six TaggingAuthorizationHTTPTest cases cover bucket/current/version CRUD, permissions, limits, marker errors, tagged PUT/MPU/COPY, COPY/REPLACE, legacy scope, tag-count disclosure and unchanged bytes/version history. Standalone r7/cluster r2 verify cross-endpoint CRUD/COPY/MPU and marker preservation; RustFS run7 rejects unsupported gateway tags before requests. Corrected HTTP fixtures pass in `phase2-umbrella-repairs.log` S317/0. | Pending current-source regression. Earlier tag-auth log contains one unrelated stale fixture failure; it is not a whole-run pass. |
| A25 | BucketCORSTest and CORSLocationHTTPTest cover normalization/limits, persisted rules, permitted/denied preflights, Vary/cache scopes, actual unauthorized403, safe invalid updates and configured region. Standalone r7/cluster r2 independently verify region, persistence and real-request authentication. | Pending current-source regression. |
| A26 | ReadSemanticsHTTPTest verifies signed/presigned overrides, injection/duplicate rejection, ETag/date precedence, full/ranged responses and HEAD. Independent Phase1 Botocore profile verifies special-key GET bytes; version/list profiles verify Unicode/plus/percent/XML fields. RustFS run7 verifies cold ranges, gzip and one upstream body/size/ETag/attribute identity. Range/checksum HTTP validation passes19/0. | Pending current-source regression. |
| A27 | Standalone r7/cluster r2 selected-client profiles pass real10MiB AWS CLI/mc upload/download/copy/list/delete on required endpoints with normal signing/checksums. Boto3/botocore1.43.114, AWS CLI2.34.24 and mc RELEASE.2025-08-13T08-35-41Z are recorded. RustFS run7 records exact upstream image/digests and retained mc24/0; the operation matrix maps named probes/contracts. Core422/0, transport39/0, CLI3488/0 and web21/0 are independently recorded. | Blocked: current A13/Concord#94, primary-timeout#96 and incomplete final correctness remain. Retained `s3_compat.py` is now integrated as the second standalone command and its separate r1 SDK/admin/SIGKILL/persistence lane passes. No package/release/production operation is authorized. |

### Remaining mandatory verification and evidence hygiene

- The earlier `phase2-umbrella-final.log` S3348/5 remains failed evidence. The
  later final correctness invocation is interrupted with core516/5 and155S3/2;
  its transport40/0 and CLI3488/0 are scoped passes, not a complete umbrella pass.
- Current `phase3-standalone-r2.log` passes versions/lists,tag/CORS,multipart,
  multipart-races and selected clients after the failed Phase1 race. Retained
  restart then fails with primary discovery timeout. A22 and A27 remain blocked.
- Generated parser acceptance is executed: `phase3-generated-xml-r1.log`3/0,
  5402 rejected cases<=4096bytes plus positive/depth cases. Do not retain the
  earlier queued/missing claim as current.
- Retained `s3_compat.py` integration and execution are complete in the separate
  `phase3-retained-standalone-r1/results.json` lane: unchanged signed SDK exercise,
  admin/metrics, actual SIGKILL/restart and persisted hash/cleanup. The aggregate
  remains fail-fast between commands; when its first hardening wrapper fails,
  the independently executed second lane supplies evidence without making the
  aggregate pass.
- Final source-frozen gateway and both-endpoint cluster P2/client regressions,
  complete correctness and production artifact smoke remain mandatory. Later
  metrics/unknown-outcome work must be validated before the final fingerprint.
- The results JSON still omits many later named passing artifacts from A01–A12,
  A18–A27; generic remaining text is conservative but stale in detail. Update
  those references without promoting incomplete rows. The compatibility page's
  listing section now describes the indexed path; its opening promotion
  recommendation still refers to the older audit gate.

This worker changed only these phase audit documents, ran no Mix/services and
owns no active process. No source/e2e edits, commits, push, publish, deployment
or production-data operations were performed. Current-source regression and
acceptance status changes remain coordinator-owned.

## Cross-phase reconciliation of all38 rows

The current results ledger contains29in_progress and9blocked, with no passed
rows. This audit inspected every evidence reference for existence and compared
the scope/remaining text with retained executions. All evidence paths exist.
The coordinator owns shared ledger/report updates; this appendix records the
remaining boundaries without changing them.

| Rows | Ledger state | Current evidence and unresolved mandatory boundary |
|---|---|---|
| A01–A12,A14–A18 | in_progress (17) | Historical named contracts/HTTP/gateway/cluster evidence above; generated XML corpus now passes. Final current-source correctness and independent gateway/cluster regressions remain. |
| A13 | blocked | Current32-completion statuses fail; #94 transition cost and later#96 availability failures remain, preserving original deadlines. |
| A19–A21,A23–A26 | in_progress (7) | Current standalone five P2/client programs pass; old both-endpoint/remote-source proof retained. Legacy fixture repairs and final complete current-source regression remain. |
| A22,A27 | blocked (2) | Current hardening retained-root restart fails; separate original SDK/admin/persistence lane passes. #94/#96 and complete suites prevent promotion. |
| A28,A29 | blocked (2) | #94 prevents mandatory10k/100k/native backfill scale. The independent `run_hardening_index_cluster.sh` is now prepared and required as the aggregate's second index command: actual backup/quiescence, checkpoint/SIGKILL/rejoin/takeover, concurrent other-bucket HTTP writes, cutover, retained artifacts and a native page-revision barrier. Only shell/Python/aggregate static checks and fail-closed blocker preflight have executed; no actual A29 runtime pass is claimed. |
| A30 | blocked | Local/gateway r6 passes56groups at64MiB/1GiBc1/c4 before later metrics source changes. Cluster r2 fails at1GiBc4 after15completed groups within original bounds; #96 blocks remaining cluster run. Final local/gateway regression remains independent. |
| A31 | in_progress | Actual8fault groups r7 plus named cold-range/eviction HTTP evidence pass; complete current-source/assertion audit remains. |
| A32 | in_progress | Eight whole-BEAM local boundary crashes and three real RF2/W2 durable-ACK crash/rejoin groups pass; complete current-source/assertion audit remains. |
| A33–A35 | in_progress (3) | Resilience r4 all8groups, invalid-bootstrap r3, real packing/open-reader, public full-GET read repair, external outage/SIGKILL/backlog drain and bounded metrics now pass. Earlier maintenance audit's absent/queued claims are superseded by these retained executions. No row promotion substitutes for final source audit. |
| A36,A37 | blocked (2) | #95 same immutable native artifact fails fresh-VM verification; standalone stops before restored-data checks and clean-cluster restore is unexecuted. Original-root isolation proof alone is not restore acceptance. |
| A38 | blocked | Earlier assets/release/container r5 smoke passes against its older source. Current production artifact smoke, complete suites/profiles, finite operation diagnostics and exact release/upgrade/backfill/rollback evidence reconciliation remain mandatory. |

No Mix, service, acceptance runtime, production data operation or shared ledger
mutation was performed for this audit. Only Phase1/Phase2 audit documents changed.
