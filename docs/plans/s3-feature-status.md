# Current daily-use delivery — 2026-10-20

The user explicitly reduced the current verification gate to normal daily use.
The sequential authenticated standalone profile passed **14/14 groups**, exit
**0**, in **3.461 seconds**:

```bash
PAGER=cat bash e2e/run_hardening_daily.sh /tmp/ess-s3-hardening-python/bin/python 20261020-normal-r3
```

See the [daily receipt](../reports/s3-hardening/daily-20261020-normal-r3/daily.json) and adjacent compile/server
logs. Strict production compilation, health/readiness, normal bucket/object
CRUD, listing/COPY, attributes/tags, versions, CORS/location, multipart and
presigned operations passed. Client concurrency is 1, the largest multipart
part is 5 MiB and default request/response SDK checksums remain enabled.
The owned server stopped, listener closed and disposable root was removed.

Verified starting/ending HEAD: `3477bc540c3732dba3d47a7e08779783f9be25fc` on
`main`; source fingerprint:
`f854dedc7146316f830beffea13dcf42c6a9d62ad2525003190de60ed30012ee`.
Versions: Concord/VSR/ExTurso **3.1.3**, HTTP **0.21.13**, Python **3.13.15**,
Boto3/botocore **1.43.114**, Elixir **1.18.5**, OTP **28**, ERTS **16.4.0.5**.

Scale, stress, high-concurrency, extreme/fault/crash and multi-GiB tests were
outside this delivery and were not run. Historical full-suite evidence remains
35/38 accepted on source `60ada0fa…`; A28/A30/A38 remain unpassed. The original
all-38 gate is superseded for this current daily-use delivery, with no historical
failure promoted to a pass. Sections below retain historical dependency/source
and execution scope.

# Historical dependency refresh and verified business profile

Concord, VSR and ExTurso are installed and locked at 3.1.3; HTTP remains 0.21.13.
At current source `60ada0faafb210257f799f275f6709be0830dd4168b27e093ca2572eb6eac781`
(main `3477bc540c3732dba3d47a7e08779783f9be25fc`, uncommitted), all complete
standalone, RustFS gateway and three-voter cluster business profiles passed.
This includes retained SDK/admin/persistence and mc coverage, both data endpoints,
and actual election/rejoin under RF=2/W=2. Independent cleanup passed.
Current correctness also passed: 4496 umbrella tests, zero failures; 24 Python
regressions, formatting and strict compilation. No business verification remains
running. Evidence: `docs/reports/s3-hardening/concord-refresh-20261020/business-summary.json`,
`correctness-summary.json`, and the accepted 35-row `row-promotion-r1/review.json`.

Phases 1 and 2 are accepted. Full hardening remains incomplete: A28 scale,
A30 complete resource coverage, and A38 aggregate are unpassed. The terminal
diagnostic11177 passed32groups and failed group33cadence before target40; it adds
no acceptance pass. Original failedcleanup gates are retained, with actual
observer/tracer and owned fixture absence independently verified. No runtime remains.
Resume and current execution authority: `docs/reports/s3-hardening/concord-refresh-20261020/RESUME.md`.
Historical checkpoints below retain their original source/version scope.

# Current-source verification boundary

Concord/VSR/ExTurso3.1.2 upgrade and all original normal business profiles passed at source830564 (R12). The current6525 source contains only the two-file E2E observer cleanup repair; no production/business-code changes. Its original faults gate passed and full correctness is running. Previous-source passes retain original provenance; all38 acceptance remains incomplete. Current evidence and outstanding work: `docs/reports/s3-hardening/post-concord312-current-r13/RESUME.md`.

# Current-source verified business profile

All three complete business profiles now passed on Concord/VSR/ExTurso 3.1.2 at source 830564e9: original standalone (assets/SDK/admin/metrics and same-root SIGKILL persistence), original tagged/full public cluster (both endpoints and real election/rejoin, RF2/W2), and original RustFS gateway (17/17 groups, true timeouts, four restart reconciliations, retained mc). Full umbrella correctness also passed with zero failures. Process/fixture/container cleanup is verified. These current receipts are under `docs/reports/s3-hardening/post-concord312-current-r12/`. Third-phase resource/scale/recovery/release and complete compound row auditing remain required; no all-38 completion is claimed. Historical results below retain their original identity.

# S3 feature completion

Execution priority updated by the user on 2026-10-19: complete and verify normal
business behavior before addressing dependency exception paths and heavy
hardening. The normal-business gate below is an earlier delivery checkpoint;
At that checkpoint, all 38 original scenarios remained mandatory. The current
2026-10-20 daily-use scope supersedes that completion gate and retains the
historical outcomes.

## Verified business profile — 2026-10-19

The declared normal business profile passed after the coordinated Concord,
Viewstamped Replication and ExTurso upgrade to published `3.1.2`. The r3
standalone, healthy-cluster and gateway runs used implementation/test/harness
fingerprint `b11eedb3d46dd212f471a32a2080bb81031795fa3d157764a0f38d03021b89f3`.
HEAD remains `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`, with
uncommitted changes. Later restore-descriptor and CORS-harness corrections have
fingerprint `f4957d9ededd69b5b2a8fc7291da496cbe3e85e710c6c98b32810922fc48dcba`;
the r3 normal-profile results belong to their recorded earlier fingerprint.

| Business surface | Implementation | Verified normal profile (r3) |
|---|---|---|
| Hosted bucket/object CRUD, COPY, versions and paginated listings | Implemented | Passed standalone and both healthy-cluster data endpoints |
| Multipart upload, part copy, list/complete/abort, initiation attributes | Implemented; abort contention/generation correction retained | Passed standalone and both healthy-cluster data endpoints |
| Attributes, checksums, tags, location and CORS | Implemented | Passed independent profiles; focused HTTP evidence is historical |
| IAM, presigned access, policy changes, suspension/revocation | Implemented; fail-closed behavior retained | Passed healthy-cluster checks; focused HTTP evidence is historical |
| Gateway current-object CRUD, COPY/listing, raw bytes/ranges/freshness | Implemented | Passed RustFS/Boto3 and retained mc profile |
| AWS CLI v2, Boto3 and mc business interoperability | Implemented | Passed selected-client profiles on standalone and healthy cluster |

All three upgraded r3 normal-feature lanes returned exit 0: four standalone
profiles, IAM plus four profiles across both healthy-cluster endpoints, and
eight gateway groups including the existing mc cloud-cache suite. Standalone
uses RF1/W1; cluster retains RF2/W2 with degraded writes disabled. The earlier
`feature-flow-20261019-r1` execution's 10 Python harness contracts, 17 focused
HTTP tests, mc 24/0 result and teardown review are historical pre-upgrade
evidence, not reruns against the upgraded dependencies.

At the later `f4957d9e…` fingerprint, the core suite reports **568 tests,
0 failures, 1 excluded**, and strict production compilation exits 0. The
completed r5 assertion audit supports **A36 and A37 passed**, with no other
acceptance promotions. Standalone recovery took **9.856 seconds** and rejected
all nine negative cases; cluster recovery took **16.333 seconds** and rejected
all six negative cases. All three cluster voters were verified and explicitly
activated at generation 2; standalone also verified its generation-2 target.

The audit verifies the quiesced backup boundary, inaccessible original roots,
read-only backup, isolated empty recovery targets, quarantine restart and stale
job/intent fences. Public checks on every data endpoint verify exact versions
and delete markers, raw hashes/sizes, attributes/checksums/tags, bucket
configuration, IAM grants/denials, retained multipart parts and completion, and
ordinary versioned PUT after activation. Packed standalone data restores without
a loose-source fallback. All eight destructive/outbound worker flags remain
false and the outbound DELETE canary remains zero, including after activation.
These audited restore passes do not establish whole-goal or all-38 acceptance.

Selected clients: Boto3/botocore `1.43.114`, AWS CLI `2.34.24`, mc
`RELEASE.2025-08-13T08-35-41Z`; upstream RustFS `1.0.1` (resolved image digest
recorded in each gateway run's `gateway-results.json`). The upgrade uses
published dependencies plus ESS checksum/restore integration changes; no
dependency source patch is used.

The existing compatibility profile remains authoritative: gateway versions,
multipart destinations and object/version tags are excluded; cluster gateway
writes remain guarded. This work does not expand unsupported mode combinations.

## Follow-up hardening

Keep the original matrix and failures under `s3-hardening-results.json`.
Concord issues #94–#97 are closed and their fixes are released in the selected
`3.1.2` family. Release `3.1.3` has the same runtime source and records the
already-published committed-prefix repair. Issue closure does not establish ESS
acceptance. The historical `b7becc29…` audit accepted 34 of 38 scenarios; those
rows retain their original source identity. No row has been promoted to the
current `329c8e78…` fingerprint, and full acceptance remains false.

The historical upgraded full S3 suite reported **357 tests, 1 failure**: the 32-completion
case returned one HTTP 200, twenty-seven HTTP 412 and four HTTP 503 instead of
the required one success plus thirty-one precondition failures. The local
unchanged-shared-index contention repair subsequently passed full Core572/0
and S3357/0 at `27ad96fb…`; exact evidence is in
`completion-concurrency-20261019-r1/results.json` under verification artifacts.
Those successful repair checks remain historical. Current r10 correctness
reports Core573/0, transport40/0, CLI3490/0, S3357/1 and Web21/0. The sole S3
failure is the original 32-completion case exceeding its 30-second await;
signed standalone completion also returned four HTTP503 responses. Their cause
is unresolved. Standalone versions/listing, tags/CORS, ordinary multipart,
multipart races/restart and selected AWS CLI/mc programs passed independently.

Current r10 crash, resilience, standalone/cluster restore and actual 2GiB
transfer receipts passed. Gateway restart reconciliation, tagged first-write
readiness, fault-fixture setup and 100ms resource coverage remain failed or
unexecuted. Numeric transfer bounds and hashes passed for the groups that ran;
omitted groups remain unpassed. The original complete 10k/100k index fixture is
still running under its original 10800-second command deadline. Release/container
and remaining independent commands follow after runtime release. Exact results
and the resumption boundary are in
`docs/reports/s3-hardening/post-concord312-final-r10/RESUME.md`.

`ESS_CONCORD_COMMAND_VERSION` accepts only `0` or `1` and defaults to `0` for
existing-database compatibility. Empty restore targets must explicitly use `1`
on every voter, retaining it through recovery, restarts and activation. Never
switch an existing version-0 database without a quiesced, backed-up migration
and Concord legacy-state reconciliation. ESS performs no automatic migration;
follow the [recovery operations guide](../operations/cluster-upgrade.md#independent-same-mode-recovery-bundles).

Continue to enforce authentication, checksums, generation fences, strict quorum
and truthful errors in every normal business run. Do not deploy or publish.

## Evidence

The upgraded results and exact source identities are recorded under
`docs/reports/s3-hardening/concord-3.1.2-update-20261019-r1/`:

- `results.json` and `standalone-features-final.log`,
  `healthy-cluster-features-final.log`, `gateway-features-final.log` retain the
  `concord312-20261019-r3-*` normal-profile runs.
- `core-post-location-fix.log` and
  `strict-production-post-location-fix.log` retain the later core/compile checks.
- `restore-standalone-r5.log` and `restore-cluster-r5.log` retain the
  `concord312-20261019-r5-restore` outcomes; `restore-r5-audit.json` records
  the completed assertion audit supporting A36/A37 only.
- `s3.log` retains the historical full-S3 completion failure; later repair
  receipts above record the source-specific successful rerun.

The pre-upgrade six-lane run under `feature-flow-20261019-r1/` and older
full-hardening evidence in `docs/reports/s3-hardening-verification.md` remain
historical. No existing evidence is automatically promoted to a later source
fingerprint. The normal-feature gate passed at the declared r3 scope; the
original 38-row hardening matrix remains incomplete. Current-source failures
and unexecuted checks are preserved separately from the historical 34-row audit.
