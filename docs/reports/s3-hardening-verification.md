## Concord 3.1.8 refresh — 2026-10-25

The current candidate updates Concord, Viewstamped Replication and ExTurso to
3.1.8, which includes the published fix for Concord #105 and the key-only
Turso-engine metadata-page optimization from #106. Turso-store projection backfill is
documented in the README. Fresh source-bound lane receipts and semantic audits
must establish acceptance; earlier failed or successful receipts retain their
original scope. The historical ledger is not automatically promoted by a
dependency update.

The latest full-acceptance attempt tested `45aa26eb16c6b23b7de2314aa34596b722949dc1`, executable
fingerprint `a22306fca957a504ee1ac58e240019ec0b5af814ff00f745e68638202cc17439`.
Fresh full acceptance in
[40129407270](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40129407270)
passed daily checks and nine extended lanes, including index. Cluster and
resources failed. The final aggregate failed its lane-success prerequisite,
before invoking the stock verifier. Publication remains blocked.
The preceding candidate's hosted compilation and formatting passed in
[40127644819](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40127644819).
That dependency set also passed strict local compilation, formatting, all 24 web
tests and the real production asset build. The updated Docker recipe and lock
then passed a complete local container build, liveness/readiness and operator
RPC smoke. This preflight used the preceding source SHA and version 0.6.10;
it is not a release artifact or full acceptance receipt. Five Phase 1 fixture
tests passed after adding filtered error diagnostics. The earlier complete local
application run executed 4,548 tests with zero failures before the native
dependency refresh; the fresh hosted result is recorded below.

Fresh hosted correctness now executed all 4,548 selected Elixir tests with zero
failures and no skips. Its two declared heavy exclusions remain in their separate
lanes. The Python harness discovered 161 tests and executed 160 with one unnamed
skip. All four correctness commands exited zero. Unit backfill fixtures use
injected backends and do not establish live cluster or scale acceptance.

The independent fresh-index audit recomputed all 3,600 samples across the original
10k/100k datasets and before/after growth checks. Original read, byte, page and
native-count bounds passed, with no forbidden calls. Interruption, retained-root
restart, fencing and atomic cutover checks also passed. The actual index job log
corroborates its source and OS/toolchain; CPU hardware, affinity and quotas were
not recorded. The original A28.4 published-cohort/before-and-after probes remain
separate prerequisites; this audit does not promote the historical ledger.

The fresh cluster lane reproduced the 32-way conditional MPU completion failure:
1 HTTP 200, 2 HTTP 412 and 29 HTTP 503 responses, instead of 1 success and 31
precondition failures. All 29 filtered diagnostics report `ServiceUnavailable`
after 4.262–4.383 seconds; the underlying ESS reason is not retained. A later
eight-worker versioned PUT exercise reached its unchanged ten-second read
timeout, before the failure/election/persistence sequence. No upstream cause has
been established.

Fresh resources again failed the original 100 ms observation coverage at local
group 15/56 and cluster group 6/20, both 64 MiB concurrency-four PUTs. Retained
temporary-observation gaps reached 251.790 ms locally and 239.891 ms on cluster
data-a. Observed numeric envelopes, all four export hashes, contiguous generations
and stopped observer identities passed; both workload matrices remain incomplete.
The separate actual 2 GiB transport test passed. Slow temporary probes preceded
serialized sample delays, but their CPU, scheduling or filesystem cause remains
unproven. The bounded
[observer diagnostic](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40130453892)
retains the original limits and cannot certify acceptance.

That probe-only observer diagnostic completed the selected groups 1–6 with zero
dropped events and verified exports and stopped identities. It did not reproduce
the 100 ms coverage violation and omits groups 7–20. Its intentional
`DiagnosticScopeComplete` result and wrapper success do not replace the failed
full resource matrices. Two local original Phase 1 diagnostics, including one
restricted to four CPUs like the hosted runner's recorded affinity, also passed.
Their bounded traces observed one successful completion and 31 precondition
failures, with all stage calls captured and both trace sessions stopped. They
did not reproduce the hosted 503s.

The hosted completion-stage diagnostic
[40133535985](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40133535985)
tested `85efd5f2ab6acb5c315ac11f74b71aff935a9347`, fingerprint
`2936154b99592135101cf43e30b79c4a52e8799b63c21e6363e9edd9ad9ceb84`.
Original Phase 1 and IAM checks passed, including one successful completion and
31 precondition failures. Its 410 sanitized stage rows contained no unexpected
error; both sessions drained and closed with zero unmatched returns, without
reaching either event cap. No mask commands were retained. Reserved EPMD
registrations were absent afterward; independent final OS identities were not
recorded. This diagnostic did not reproduce the full-run 503s, cannot establish
their cause and does not replace the failed cluster or resource acceptance lanes.

Current automatic push correctness
[40129397470](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40129397470)
executed 1,057 server tests and 11 retained CLI contracts with zero failures.
Two declared heavy exclusions remain assigned to separate lanes. The Python
harness executed 160 tests with one unnamed skip (161 discovered); the raw
nonverbose output does not identify that skip. Current strict compile/format
[40129397503](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40129397503)
and CLI compatibility
[40129397511](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40129397511)
passed. The shared push/PR job graph was reviewed; PR execution is unobserved.
Current daily checks passed all 14 groups and the standalone lane passed all
three commands. Both restore submodes passed their original checks, including
command-format fences and negative cases. The clause-level audit remains pending;
these scoped results do not promote the historical acceptance ledger.

The preceding full run
[40127652908](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/40127652908)
passed eight extended lanes and daily checks. Cluster failed: the 32-way MPU
race returned 1 success, 20 precondition failures and 11 HTTP 503s, instead of
1 success and 31 precondition failures. A later eight-worker versioned PUT
exercise hit its existing ten-second read timeout. Both resource topologies
failed original 100 ms observation coverage: local group 15 had a 136.091 ms
temporary-observation gap; cluster group 6 had a 112.237 ms gap on data-b.
Only 15/56 and 6/20 groups executed, respectively. Retained numeric envelopes,
export hashes and stopped observer identities passed; the incomplete matrices
remain failed. These observations do not establish an ESS or upstream cause.
The source refresh cancelled the remaining index lane; the aggregate failed.
Filtered diagnostics now retain operation, status, allowlisted error code and
elapsed time when the Phase 1 SDK helper receives a server error.

Initial runs 40125618553 and 40126527391 failed native dependency prerequisites
before workload assertions. The published fixes are `rustler_precompiled`
0.10.1 and the coordinated DuskMoon 10.0.4 packages. DuskMoon 10.0.5's Vize GNU
artifact requires GLIBC_2.43 and failed the real production asset build; the
compatible pin is tracked in
[Bundler #9](https://github.com/duskmoon-dev/duskmoon-bundler/issues/9).
The dependency refresh also includes `plug_crypto` 2.2.1 for CVE-2026-105963.
The preceding automatic image build failed when `lazy_html` fell back to source
compilation without CMake. The builder now includes CMake, and `elixir_make`
0.10.1 includes the published expanded-header and preserved-download-error fixes.
The masked original download error is unknown.
All 158 original clause decisions and the actual aggregate remain mandatory.

## Historical Gaoxy migration verification

Gaoxy 0.12.1 serves Phoenix, S3 and internal cluster HTTP. With HTTP 0.21.16 and
Concord / VSR / ExTurso 3.1.6, full run
[39795784957](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39795784957)
failed on `6a356c3c90bee76ee6338774efa848b30390b215`, executable fingerprint
`129879f910d5c9c22d6504574b150f85c3e5209f5e619fad7a08ea0712debe09`.
Daily's 14 groups and extended correctness's 4,543 application tests passed.
Eight extended lane command receipts passed; cluster, index and resources failed.
The aggregate guard exited 1 before the stock verifier could run. No aggregate
or publication-verification artifact was produced.

Cluster failed with HTTP 503. A28 timed out while growing the unrelated 100k
fixture; its final 100k measurements remain unexecuted. Independent A29 passed.
Resources stopped after 25 clean standalone groups and six clean cluster groups,
with coverage gaps above the original 100 ms bound. Executed operations and
numeric envelopes passing does not complete these matrices.

Independently reviewed legal VSR runtime probes reproduce full-history rebuilding
on multi-voter events and singleton heartbeat timers in published 3.1.6.
[Concord #105](https://github.com/gsmlg-dev/concord/issues/105) records the blocker
and complete reproductions. Causation of the separate HTTP failures, A28 timeout
and resource delays remains unproven. No local dependency workaround was added.

The [current evidence report](s3-hardening/gaoxy0121-concord316-validation-20261022/README.md)
and [status](../plans/s3-hardening-status.md) retain identities, incomplete modes
and next gates. All results are bound to the tested candidate, rather than later
TODO/documentation changes. The default ledger and C7 sections below remain
historical; no successor acceptance is certified. **v0.7.0 has not been
dispatched or published.**

## Historical ESS-S3-HARDENING-CLOSEOUT-002 C7 verification

Implemented and verified portions are pushed for review; **full acceptance and
release remain incomplete**. Starting checkout was clean
`main@b17282d96a58eff962a35d807c9e7adb39af4774`. Tested candidate C7 is
`fcc02e148ae31f00022d563ebce2b039be5d32dd`, source SHA256
`ee754b0345b3198e84abd78e898b121e966787c1faa563642097ca3151653913`.
The [freeze](s3-hardening/closeout-002/candidate-c7/freeze.json) covers runtime,
tests, harness, dependencies, CI and configuration. Fingerprinting hashes sorted
cached/nonignored input names, NUL, contents, NUL; it includes apps/config/e2e,
scripts/.github, manifests/locks, Docker/formatter inputs and CLI compatibility
JSON. Other documentation/reports are explicitly excluded. Separately hashed
runner inputs are archived. Report-only commits do not require a report to name
its own future SHA. All executable inputs stayed frozen through the run.

Delivery uses [draft PR23](https://github.com/gsmlg-opt/ex_storage_service/pull/23)
and remote ref `codex/ess-s3-hardening-closeout-002`; no protected merge occurred.
**Released SHA and closeout release artifacts: none.** Canonical patch publication,
remote tag/release, both Hex packages, CLI assets and versioned/latest image
verification remain downstream of full acceptance. Automatic CI edge images are
separate. The existing published v0.6.10 daily-use result remains valid for its
original scope.

The two reviewed defects have tests-first evidence:

- `Metadata.ObjectCommit` read head then version independently, turning a deleted
  selected version into false absence. The exact barrier-controlled predecessor
  restoration test failed before the fix with `{:error, :not_found}`. Shared
  bounded head/version/head selection now compares complete revision/identity
  and fences bucket generation across retries. Stable missing references are
  integrity errors; unstable exhaustion and backend failures remain availability
  errors. Explicit versions never fall back. `Storage.Versioning`, `ObjectService`
  and latest tagging/COPY use this boundary; COPY source/destination generations
  remain independent. Domain tests cover PUT/null ABA, markers, delete/recreate,
  corruption, failures/exhaustion and explicit deletion. Signed GET/HEAD/COPY and
  real isolated cluster races return matching body, ETag, length, attributes and
  version identity. This adds bounded metadata reads without body/bucket
  allocation; native history traversal remains the separate A28 limitation.
  [Exact original RED](s3-hardening/closeout-002/coherent-reads-before.log),
  [COPY RED](s3-hardening/closeout-002/coherent-copy-before.log),
  [domain GREEN](s3-hardening/closeout-002/coherent-reads-final.log),
  [HTTP GREEN](s3-hardening/closeout-002/coherent-reads-http.log).
- S3 `Operation` required an empty root query, rejecting `x-id=ListBuckets`.
  Root GET now deliberately permits SDK `x-id` and the six existing presign
  fields, while rejecting unsupported features/selectors and decoded duplicate
  names. Raw query/path/headers remain signed. Independent signer/raw HTTP
  fixtures test valid requests, missing grants, signed-value mutation and
  unsupported-subresource rejection. Generic canonicalization is unchanged.
  [RED: 2 failures/5 tests](s3-hardening/closeout-002/list-buckets-red.log),
  [resolution GREEN: 5/5](s3-hardening/closeout-002/list-buckets-source-green.log),
  [authorization/operation GREEN: 61/61](s3-hardening/closeout-002/list-buckets-scoped-concord314.log).

Additional evidence-backed repairs are scoped to `Metadata.Backend.Concord`,
`ObjectService`, S3 `Handlers.Shared`, the resource observer/sampler and acceptance
harness. View-change failures map to existing retryable availability vocabulary
([43 tests: 3 failures before, 0 after](s3-hardening/closeout-002/view-change/regression.json)).
Owned stages are reclaimed only after a matching positively confirmed abort;
uncertain outcomes retain protection
([23 tests: 3 failures before, 0 after](s3-hardening/closeout-002/protected-stage-cleanup/regression.json)).
The original C5 COPY error remains unclassified; this cleanup repair is not a
claim to have proved that original error's cause. C7 fixture admission reserves
all 32 contenders before barriers, retaining the production 100-token/20-refill
limiter, one-attempt SDK policy, signing and checksums. Stage inventories include
hidden/empty/nested files outside measured windows; observer ESRCH means exited
identity while permission/I/O/parse errors still fail.

Automatic PR/push correctness now runs complete core/S3/transport/web suites and
CLI contracts, with discovered-file inventories and count floors. Current local
extended correctness executed **4,531 application tests**, zero failures/skips:
core606, transport39, CLI3491, S3374, web21; **122 Python tests** also passed.
The excluded core cluster and transport large-stream cases ran separately.
Exact-C7 hosted push/PR Test runs
[39514908019](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39514908019)/
[39514915408](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39514915408)
and strict CI runs
[39514907872](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39514907872)/
[39514915073](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39514915073)
passed. Downloaded inventories agree on 1,040 server tests, 11 CLI contracts and
122 harness tests ([hashed CI receipts](s3-hardening/closeout-002/candidate-c7/hosted-ci/summary.json)).
Heavy jobs remain separate and serial. Local and CI commands align; missing,
skipped, cancelled, timed-out or failed required evidence blocks the aggregate.
Release orchestration re-audits the candidate's downloaded full-acceptance
artifacts before publication; its failure and version-only mapping tests pass.

The authoritative `run --profile all` actually exited **1** in 2,975.240 seconds;
its final freeze check found no changed inputs. Ten lanes passed: correctness,
standalone, gateway (17 groups), strict RF2/W2 cluster/election/rejoin, resources,
faults, crashes, resilience, restore and local release/container. Index is blocked:
compile0, A28 preflight1, independent A29 fixture0. Byte-identical final-lane
selection retains those original outcomes. The separately executed actual
`hardening_acceptance.py verify --output .../candidate-c7/final-lanes` exited
**1** after clause reconciliation, with only A28, A38 and index blocking:

| Acceptance rows | Current outcome |
| --- | --- |
| A01–A27 | Passed: all 90 original compound clauses and required modes |
| A28 | Blocked: all 8 scale clauses remain unaccepted |
| A29 | Passed: 5 migration/interruption/rejoin/cutover clauses |
| A30 | Passed: all 8 resource clauses, full manifests independently audited |
| A31–A37 | Passed: all 40 fault/crash/resilience/restore clauses |
| A38 | 6 clauses passed; A38.1 full-acceptance input clause blocked |

That is **36/38 rows, 149/158 clauses passed**, not full acceptance.
[Actual exit](s3-hardening/closeout-002/candidate-c7/aggregate-final-exit.json),
[aggregate](s3-hardening/closeout-002/candidate-c7/final-lanes/aggregate.json),
[final ledger](s3-hardening/closeout-002/candidate-c7/ledger-final.json) and
[current ledger](../plans/s3-hardening-results.json) retain failed parent commands.
Previously absent A29–A38 mode declarations are explicitly mapped against the
unchanged baseline ledger; existing mode sets and compound requirements remain
intact. Passing compile steps cannot substitute for selected successful runtime
commands or their substantive output assertions.

A28 remains blocked by [Concord#101](https://github.com/gsmlg-dev/concord/issues/101)
and [#102](https://github.com/gsmlg-dev/concord/issues/102). Published 3.1.4 already
contains #99/#100; current upstream main remains
`4b1c4fb7150d9d1e6473530f8a67144fe8c0552a`, with no adoptable #101/#102 repair
([checked inputs](s3-hardening/closeout-002/candidate-c7/upstream-inputs.json)).
The earlier production-path diagnostic used 1,000 normal target ObjectCommits
and 1,000 unrelated commits. ESS page calls/returned bytes remained 100/26,620;
native retained-log validation visits increased **100,400→200,400**. Atomic
transactions still materialize full state. Zero conflicts and no checkpoints did
not establish root-revision contention; this is not a complete explanation of
the historical 10,800-second timeout. **Mandatory 10k/100k datasets, listing
p50/p95, native byte/allocation and unrelated-growth acceptance remain unexecuted.**
No dependency patch or unchanged multi-hour rerun was used.
[Diagnosis](s3-hardening/closeout-002/a28-diagnostic-r1/README.md).
Independent current A29 passed in **13.752 seconds**, preserving v1/v2 records,
rollback epochs, generation fencing and checkpoint identity through interruption,
retained-root rejoin and atomic cutover.

A30 now has complete current-candidate evidence. The observer's synchronous
sample-file export could delay collection. A deterministic 150 ms export barrier
reproduced no fresh rows and a 175.147 ms observation gap before repair; bounded
queued export retained fresh sequences 2–7 and a 25.234 ms gap afterward
([RED](s3-hardening/closeout-002/observer-red/blocked-export-barrier.json),
[GREEN](s3-hardening/closeout-002/observer-green/blocked-export-barrier.json)).
This proves that mechanism without attributing every historical gap to it.
Completed-cache stat elimination preserves exact temporary-byte measurement.
No tracing ran in acceptance, and the 100 ms coverage criterion remains an
observation requirement rather than an S3 latency SLA.

All **56/56 local/gateway** and **20/20 cluster** groups passed at 64 MiB/1 GiB,
concurrency 1/4; the independent actual 2 GiB authenticated transfer also passed.
Independent analysis checked manifest identity, streaming length/hash/metadata,
all six native metric windows, 118,606 exact sample rows (42,766 local; 75,840
cluster), export hashes/extents/sequences and before/after/final staging inventories.
All intervals meet the original bound, no interpolation, and final stages are
zero files/bytes. Independent direct RustFS readbacks cover all 12 upload groups.
Local/gateway took **1,070.615 s**, cluster **635.985 s**, and 2 GiB **5.329 s**.

| Matrix, all executed cases | Peak binary bytes | Peak BEAM total | Peak RSS | Max queue/total | Peak temporary | Largest gap/probe |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Local/gateway,56/56 |22,108,056|99,462,024|175,116,288|3/8|8,589,934,592|50.782/25.520 ms|
| Cluster,20/20 |21,324,776|89,623,688|181,166,080|11/13|4,294,967,296|32.970/7.974 ms|

At 1 GiB/c4, maximum binary/total/RSS deltas were 20,842,104/26,206,076/35,037,184
bytes locally and 20,740,256/21,215,000/48,787,456 in cluster. The predeclared
size-independent memory limits remain binary64MiB+32MiB*c, total128MiB+48MiB*c,
RSS256MiB+64MiB*c, queues128/2048; disk remains 2*size*c+64MiB. Every case passed
its own limits. Per-size/concurrency baselines/peaks/post-idle/throughput are in
[resource analysis](s3-hardening/closeout-002/candidate-c7/resource-observation-analysis.json).
Logical operation throughput ranged 9.91–5,233.65 MiB/s locally and
9.40–122.53 MiB/s in cluster; these mixed-path/page-cache observations are not a
production capacity claim. Observer RSS peaked at 22,061,056 bytes, separately
from BEAM RSS. OS page cache and memory-pressure observations remain in runner
provenance, without treating page cache as BEAM allocation.

Runner: NixOS/Linux6.18.48 x86_64 ms04,16 CPU affinity, no observed cgroup
CPU/memory cap, approximately62.6GiB RAM, BEAM4 schedulers plus2/2 dirty schedulers;
pre-run load0.124/0.160/0.380. Private /tmp uses NVMe/ext4; no overlapping
task-owned heavy workload. Private writable NPM/Buildx client state repairs
runner prerequisites without changing service limits. Python3.13.15,
Elixir1.18.5, OTP28/ERTS16.4.0.5, Concord/VSR/ExTurso3.1.4,
http_fetch0.21.13, Boto3/botocore1.43.114, AWS CLI2.34.24,
mc RELEASE.2025-08-13T08-35-41Z and RustFS1.0.1 are recorded. Checksums and
signing stayed enabled; cluster remained strict RF2/W2/degradedfalse.

The terminal check found all 12 unique recorded PID/start identities absent
(20 receipt observations), no known fixture Erlang name/listener/container match.
[Cleanup evidence](s3-hardening/closeout-002/candidate-c7/terminal-cleanup.json)
states its limits: unrecorded descendants and restore child PID namespaces are
not universally certified. After a verified private metadata archive, all14exact
receipt/inode-bound C7 roots were removed; no other root/process/container was
modified ([disposal](s3-hardening/closeout-002/candidate-c7/terminal-disposal/disposal.json)).
The private archive retains519log/JSON/JSONL files,230,318,864bytes, with each
member size/hash checked before deletion; object payloads/native databases are
excluded. Disposal has acceptance:false and does not change runtime outcomes.
Raw receipts remain unchanged. Five sample files have lossless gzip copies and
five credential-bearing logs have credential-only public derivatives with exact
original/derivative hashes in the
[publication manifest](s3-hardening/closeout-002/candidate-c7/evidence-publication/manifest.json).
Original sensitive logs stay private; no current clause references them as public
execution evidence. The separately reviewed
[terminal supplement](s3-hardening/closeout-002/candidate-c7/evidence-publication/terminal-supplement.json)
adds only disposal receipts/tools. Historical C5 failures and C6 supplements
remain separate and cannot certify C7.

Current standalone/cluster restoration passed in51.505s, including9/6 required
artifact/key negatives, originals-inaccessible/read-only-backup isolation,
generation2 identity and stale-worker fencing, quarantine restarts and postactivation
signed content/metadata/MPU checks. All eight destructive/outbound worker flags
remain false and the outbound canary is0. Existing command-format0 databases are
unchanged; every empty target retained1 through all restarts/activation. Explicit
backfill still requires quiescence and verified backup; old-binary rollback after
v2 writes requires verified metadata/blob restoration, never destructive startup
rebuild. The local release lane passed in218.915s; its 0.6.10 image
`sha256:812591b0b174f02d6173b59a0c7909bb54402d3432a44fa2cc09ccbf4d1f1b62`
reports exact C7 source and build-start2026-10-21T00:12:38Z. Health/readiness/operator
RPC passed; signed CRUD in that image was not certified, and it was not published
as a closeout artifact.

Remaining blockers are the released/validated upstream A28 repairs and original
mandatory scale measurements, then a passing full A38 aggregate, authorized
canonical patch release and independent verification of every intended public
artifact. The task is **not complete**. This closeout narrowly supersedes the
older daily-only delivery instruction; historical receipts below retain their
original dates, sources, outcomes and scope.

---

## Historical daily-use verification — original 2026-10-20 scope

The user explicitly narrowed this delivery to normal daily-use verification.
The sequential authenticated standalone Boto3 profile passed **14/14 groups**,
exit **0**, in **3.461 seconds**:

```bash
PAGER=cat bash e2e/run_hardening_daily.sh /tmp/ess-s3-hardening-python/bin/python 20261020-normal-r3
```

The [actual receipt](s3-hardening/daily-20261020-normal-r3/daily.json) and adjacent
compile/server logs record strict production compilation, health/readiness,
bucket/object CRUD, listing/COPY, attributes/tags, versions, CORS/location,
multipart and presigned operations. Client concurrency was 1; the largest
multipart part was 5 MiB. Authentication and default request/response SDK
checksum policies (`when_supported`) were retained. Cleanup confirmed the
owned server stopped, listener closed and disposable root removed.

Starting/ending Git SHA: `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`;
starting/ending source fingerprint:
`f854dedc7146316f830beffea13dcf42c6a9d62ad2525003190de60ed30012ee`.
Recorded versions: Concord/VSR/ExTurso **3.1.3**, HTTP **0.21.13**, Python
**3.13.15**, Boto3/botocore **1.43.114**, Elixir **1.18.5**, OTP **28**, ERTS
**16.4.0.5**. Execution began at `2026-10-20T03:52:49.956552+00:00` and
finished at `2026-10-20T03:52:53.417401+00:00`.

No scale, stress, high-concurrency, extreme/fault/crash or multi-GiB verification
was run for this delivery. The earlier 35/38 acceptance belongs to source
`60ada0fa…`; A28/A30/A38 remain unpassed in the original full matrix. This daily
result is a separate current receipt and does not certify the full matrix.
Historical checkpoints below preserve original outcomes and requirements;
their full-suite completion wording is superseded by the current user scope.

## Historical terminal Concord3.1.3 update — 2026-10-19T19:23:26.091262+00:00

Concord/VSR/ExTurso3.1.3 installed/locked, HTTP0.21.13 retained; current source
60ada0fa on main3477bc54. All complete normal business profiles and4496umbrella
checks/0failures,24Python,40additionalresource tests, recovery/restore,2GiB/A29
and productionrelease/container passed.35/38rowsaccepted; Phases1/2complete.
A28full10k/100kscale and A30complete100msresourcecoverage remainunpassed;
A38actualaggregate-checkpoint-r3 exits1. No runtime/build or worker remains.
Last diagnostic11177passed32groups then failed33beforetarget40;23unexecuted,
no causaltrace. Originalcleanup gates remainfailed; separateactual observer/tracer,
11identities/6groups/tasks/listeners/exactcontainer absence verified. No new pass.
Read [current integrated report](s3-hardening/concord-refresh-20261020/CONCORD_UPDATE.md)
and final-checkpoint-r3.json/RESUME.md for exact evidence and concrete unblock
conditions. No unchangedruntime retry, dependency patch, relaxedbound or production
operation was used. Historical checkpoints below retain their original scopes.

## Terminal canonical resources checkpoint — 2026-10-19T18:58:52.695706+00:00

Canonical original resources session11044 is terminal/reapedexit1 on source60ada0fa. Productioncompile passed; local/gateway failed group40 of56 (39passed,16unexecuted) at up to150.506ms metric gap; cluster failed group16 of20 (15passed,4unexecuted) at data-a temporary gap108.734ms. Numeric envelopes passed for executed groups, without completing100ms coverage or remaining groups. Independent cleanup passed285identities/137groups with no owned processes/tasks/listeners/container. See concord-refresh-20261020/canonical-resources-summary-r1.json. Terminal semantic review and static group16 trace preparation proceed; no application runtime or acceptance promotion. A28/A30/A38 remain unpassed;35/38 accepted.

Earlier checkpoints below retain their historical scopes.

## Current Concord 3.1.3 checkpoint — 2026-10-19T18:08:54.854315+00:00
## Active canonical resource verification — 2026-10-19 18:40 UTC

Root session11044 runs the unchanged original resources profile on source60ada0fa, including productioncompile plus all56local/gateway and20cluster groups. Diagnostic session88674 completed/reapedexit1: its original56groups and12RustFSreadbackgroups/30objects/90checks passed, while an81B unfinished syscall prefix left strict trace coverage incomplete(analyzerexit2). Independent cleanup checked12owned identities and explicit observer/tracer identities stopped. `concord-refresh-20261020/a30-history-diagnostic-summary.json` preserves both outcomes. No diagnostic lane was synthesized; A28/A30/A38 remain incomplete pending actual original verification.

35/38 scenarios now have current-source completed semantic decisions: A01–A27, A29 and A31–A37 (140 clauses). Source `60ada0fa…`, main/3477bc54. Full correctness, business, resilience, restore, actual2GiB, independentA29 and release/container gates passed. The actual aggregate verifier returned1: A28/A30/A38 remain in progress; complete index/resources profiles are absent. Historical records are preserved in `concord-refresh-20261020/row-promotion-r1/ledger-before.json`; no failed or unexecuted check was promoted.

A28 bounded original-path observation finished170.843s/2958confirmedputs with increasing latency; native work continued but first10k listing measurement was not reached. Source and cleanup verified. This does not prove the original10800s timeout cause. A30 narrow slowPUT did not reproduce a cadence gap; full-history causal observation is being prepared. No owned ESS or main build is running. Resume/evidence: `docs/reports/s3-hardening/concord-refresh-20261020/RESUME.md`.

## Current Concord 3.1.3 verification — 2026-10-19T17:41:33.009766+00:00

Concord/VSR/ExTurso 3.1.3 is installed and locked; only its three lock entries changed and HTTP 0.21.13 is retained. Current uncommitted source `60ada0faafb210257f799f275f6709be0830dd4168b27e093ca2572eb6eac781`, main/3477bc54. Full correctness passed: 4496 reported tests, zero failures (core 587, transport 40, CLI 3491, S3 357, Web 21); two tagged cases use separate mandatory lanes. The 24 Python regressions, format, strict test/production compilation and diff check passed. Correctness root session 30354 was reaped exit 0, with independent cleanup verified.

All original current standalone, gateway and cluster profiles passed; session 35783 was reaped exit 0. This includes retained signed SDK/admin/metrics/SIGKILL persistence, all 17 RustFS gateway groups, tagged voter test, both data endpoints, and actual primary loss/election/rejoin under strict RF=2/W=2. Independent live cleanup passed for 340 observed identities, tasks/groups/listeners and exact fixture containers. Evidence: `docs/reports/s3-hardening/concord-refresh-20261020/business-summary.json` and `business-independent-cleanup.json`. Compound row audit remains separate.

Root session 58050 exclusively runs the original faults, crashes, resilience and standalone/cluster restore profiles, sequentially. Actual 2 GiB, A29, release/container, postrelease compilation and final compound audits remain queued. A28 original 100k timeout and A30 original cadence failures remain unpassed. No historical result was relabelled; full 38-scenario acceptance is false. Resume: `docs/reports/s3-hardening/concord-refresh-20261020/RESUME.md`.

## Current staging-sync verification checkpoint — 2026-10-19T17:04:28.318641+00:00

Concord/VSR/ExTurso are installed and locked3.1.2; latest3.1.3 describes the same runtime fixes. Source944e3831c443c3747c9d01fc6c7a59bc6eda58638c7d602148595f8e2091d6d9 is frozen at uncommitted main/3477bc5. A minimal managed-staging repair syncs/closes completed bytes before creating the protected reservation/replacement rename. Later publication and gateway journal syncs, pre-intent ordering, ownership fences and conservative protected-stage retention remain. This adds file-sync work before submission; it does not claim a general filesystem latency bound.

Actual reproducing RED50tests/7expectedfailures became focusedGREEN80/0, followed by AST-equivalent test formatting and strictcompile/format/diff passes. Original first1GiB/c1 productionPUT now passes with independent completebody+metadataSHA256 equality,147samples, maxmetricprobe1.255ms and maxgap26.336ms under unchanged100ms validation. Root session6717 is reapedexit0;15observedPID/start identities, groups/threads, listeners and exactRustFScontainer were independently checked absent. This is narrow diagnostic evidence, not fullA30.

Root session25757 exclusively runs both original complete resource fixtures (56local/gateway+20cluster), with original sizes, concurrency,25msobserver,100msvalidator, envelopes and10800s commanddeadlines. Final fullcorrectness/business/crash/recovery/release and compoundaudits/aggregate remain pending. No current-source scenario promotion; full38acceptancefalse. A28 original10800.086s timeout stays unpassed, exactcauseunclassified;1000normalput snapshot/restart diagnostic is not100k proof. No dependency source, observer, threshold, auth, checksum, quorum or host configuration change. Latest resume: docs/reports/s3-hardening/post-concord312-current-r14/RESUME.md; focusedrepair and narrowterminal receipts are retained under that directory.

## Current verification checkpoint — 2026-10-19T16:29:57.566411+00:00

Concord/VSR/ExTurso remain locked at 3.1.2; current source `6525a366b65e074c22b1be3196a073d6d745485d592f306884e207ab53087c19`, HEAD3477bc5, uncommitted main. Full current correctness passed: Core579/0, transport40/0, CLI3491/0, S3357/0, Web21/0 (4,488 reported tests; two separately tagged exclusions). Formatting, strict test/production compilation,24 Python regressions and diff-check passed. Session82669 completed/reaped exit0; all five captured process identities/groups and exact fixture roots have no remaining processes. Original current faults passed all eight groups and verified process/container cleanup. The 13-test observer module also passed. No production/business-code changes from the previous R12 business checkpoint.

Root exclusively owns session85850, executing both complete original resource fixtures:56 local/gateway groups and20 cluster groups. Original25ms observation,100ms coverage, workloads, numeric bounds and10800s per-command limits are unchanged. A28 supported-snapshot diagnostic and actual2GiB are being prepared report-only; no concurrent acceptance runtime. Previous original100k fixture timeout remains failed and unattributed. Original current business/recovery/release gates, complete compound audits and aggregate verification remain required. No current-source scenario promotions; full38 acceptance remains false.

Resume: `docs/reports/s3-hardening/post-concord312-current-r13/RESUME.md`.

## Current verification progress — 2026-10-19T16:06:24.738948+00:00

Concord/VSR/ExTurso remain locked at 3.1.2; current source 830564e9 and HEAD 3477bc5 are unchanged. Complete correctness passed all five apps (4488 reported tests, zero failures; two separate tagged cases), formatting/strict compiles/24 Python regressions/diff-check. All three complete current business profiles passed: original standalone with assets/SDK/admin/SIGKILL persistence; original cluster with tagged voter test, both public endpoints and actual primary loss/election/rejoin under RF2/W2; original gateway all 17 groups including real RustFS timeouts, four full-restart reconciliation cases and retained mc compatibility. Process/thread/group/listener and container cleanup has been verified; original tagged fixture was restored exactly. The prior intermittent 503 remains unattributed.

Root now exclusively runs original crashes, resilience, standalone/cluster restore and faults profiles, sequentially in session 72377. Tagged actual 2GiB, full resources, scale, release/container and compound row audit remain mandatory. No workload/deadline/assertion/auth/checksum/quorum changes and no current-source row promotions yet. Full38 acceptance remains false. Resume/evidence: `docs/reports/s3-hardening/post-concord312-current-r12/checkpoint-normal-business-passed.json`.

## Current execution checkpoint — 2026-10-19 15:00 UTC

Concord/VSR/ExTurso3.1.2 are installed and locked. Reviewed aggregate, gateway-journal and multipart retry-budget repairs are applied with actual tests-first failures/passes. Current source is aef52f35; normal strict test/prod compilation passed after refreshing stale release build metadata. Full umbrella and original standalone verification are running; original cluster verification is prepared pending root test release. Old-source public cluster and signed/retained standalone business checks passed, with provenance retained. Original container build is still pending; A28/resource/Docker-prerequisite failures remain unpassed. All38 acceptance is incomplete, with no current-source row promotions. Resume: `docs/reports/s3-hardening/post-concord312-current-r11/RESUME.md`.

## Current execution checkpoint — 2026-10-19 14:45 UTC

Concord/VSR/ExTurso are locked3.1.2. Normal retained signed SDK/admin and SIGKILL/restart persistence passed on frozen329c8e with an independent release-cache snapshot. Reviewed aggregate/gateway/multipart repairs passed scratch RED/GREEN but are not yet applied. New gateway/resource diagnostics could not start RustFS: both hit their unchanged45s Docker setup bounds before business work; no acceptance promotion. Original native container build remains pending. Tagged/public-cluster business checks continue independently. All38 and full_hardening_acceptance remain incomplete. Latest precise handoff: `docs/reports/s3-hardening/post-concord312-final-r10/RESUME.md` and checkpoint-20261019-1445.json.

## Current verification boundary — 2026-10-19 14:30 UTC

Concord/VSR/ExTurso are updated from3.1.1 to3.1.2; no dependency source patch.
Original frozen329c8e release/container build is in progress, not passed. Assets
and host OTP release completed. Both3.1.2/3.1.3 force ExTurso source compilation;
no usable precompiled replacement was found. A provisional compiler candidate
consumed CPU during a bounded observation; useful progress and stall cause remain
unproven (`ex-turso-artifact-audit-r1`, `release-native-worker-audit-r1`).

`a13-release-overlap-r1/execution.json` records actual original selected-case exit0:
32contenders,1x200/31x412, unchanged30s await/seed609273, complete observer cleanup
and owned child/group absence. Only the test failure-manifest changed, within the
explicitly granted cache scope. Fresh roots and concurrent native build mean this
clean replay is inconclusive for the original full-suite timeout; no acceptance
row was promoted. Signed capture and scratch RED/GREEN validation remain separate.
Two prior bwrap setup processes remainD/pendingKILL; their shutdown is unconfirmed.
Current full-goal acceptance remains false. Detailed resume state is in
`post-concord312-final-r10/RESUME.md` and `checkpoint-20261019-1430.json`.

## Original scale result and business coverage — 2026-10-19 13:08 UTC

Concord/VSR/ExTurso remain locked at 3.1.2. Original complete A28 exited 124 after
10,800.086 seconds; A29's independent cluster command passed. The final stack
at index_resource.exs:68 follows SIGTERM during unrelated-bucket fixture writes;
the shutdown cluster_not_ready is not established as the initiating cause.
No complete 100k acceptance result or unchanged full retry is claimed. Original
owned BEAM/group absence was checked. Exact receipt/audit: post-concord312-final-r10/
index.json and index-terminal-audit-r1.json.

Current A01-A27 business audit finds 17 rows with required clauses demonstrated
in completed portions, 8 missing required execution and failures A13/A16; these
are not promotions. Normal versions/listing, tags/CORS, multipart, restart/race
and selected-client portions passed. Independent retained standalone/public-cluster
commands remain queued, plus bounded diagnosis and tests-first local repairs.
The original driver still owns release/container, default-prod rebuild and extra
regressions. Source329c8e remains frozen; full38 completion is false. Business
clause evidence: post-concord312-final-r10/business-profile-row-audit-r1/rows.json.

## Upgrade and diagnosis checkpoint — 2026-10-19T11:08:52.900602+00:00

Concord/VSR/ExTurso3.1.2 remain verified; latest3.1.3 adds no runtime repair. Root3612 retains original completeA28 and10800s command deadline. Gateway, tagged-readiness and pureVSR-read diagnostics now have root static reviews only. Tagged coverage rejects abnormal observers and safely records not_primary classes. No runtime diagnostic, source change, attribution or scenario promotion occurred. Current correctness remains failed at original32-way multipart30s await; priorr9 correctness is historical. Evidence: `post-concord312-final-r10/checkpoint-20261019-1108.json`.

## Resume and aggregate checkpoint — 2026-10-19T10:47:58.797082+00:00

Concord/VSR/ExTurso3.1.2 and source329c8e remain verified. Root3612 still owns original completeA28; no other runtime started. Actual application-free aggregate checkpoint exited1, retaining failed/incomplete profiles and historical-source rows. Tagged readiness audit confirms a quorum-backed read but does not establish write-session readiness or a defect; invalid-bootstrap remains unexecuted. SignedA13 static review passed; gateway/tagged diagnostic preparation remains report-local. No acceptance promotion. Evidence: `docs/reports/s3-hardening/post-concord312-final-r10/checkpoint-20261019-104758.json`.

## Upgrade confirmed; original scale run retained — 2026-10-19T10:30:30.559146+00:00

Concord/VSR/ExTurso3.1.2 remain locked; only their three lock entries and core constraint differ from HEAD in the dependency update. Root3612 still owns original complete A28 with its10800s deadline, after successful A29 command. Root static-reviewed the bounded fault-setup diagnostic; no runtime release yet. A13 source review locates synchronous loser cleanup before412 but establishes no cause. Signed503 XML capture is being prepared report-locally. No source changes, row promotions or new runtime tests in this checkpoint; full acceptance remains false. Evidence: `post-concord312-final-r10/checkpoint-20261019-1030.json`.

## Current receipts reconciled — 2026-10-19T10:05:11.323335+00:00

Concord/VSR/ExTurso3.1.2 remain locked. Actual crashes, resilience and standalone/cluster restore profiles passed all their commands; the outer driver reports earlier cumulative failures. These passed profiles will not be retried. Actual2GiB passed; A29 cluster command exited0 with audit pending. Original completeA28 10k/100k workload is running under sole runtime3612. Failed gateway reconciliation retained timeout reason; faults failed during bucket-creation setup before its cases. No new row promotion. Two independent commands (retained standalone and public clusterBoto3) were genuinely unexecuted and are queued. Details: `post-concord312-final-r10/checkpoint-20261019-1005.json`.

## Final-r10 full source verification — 2026-10-19T09:31:50.609877+00:00

Source frozen at `329c8e783d8b5118008f1cecafa7bf503572e8d071f6693663dfb059106a53d9` after reviewed E2E-only Port isolation. Focused16tests passed. Production/dependencies and all original workload/acceptance bounds unchanged. Full gates not yet executed at this source; root owns serialized runtime. Do not reinterpret dfcb66/b7 receipts as current-source acceptance. Final-r10 evidence will retain independent gate failures and actual aggregate result.

## Current full regression — 2026-10-19T09:07:55.857362+00:00

Concord/VSR/ExTurso3.1.2, frozen dfcb66: final-r9 correctness exit0;
format/strictcompile and4481 umbrella tests passed (573+40+3490+357+21), with
separately tagged integration gates retained. Runtime48360 reaped; private
network reservation preserved. Exact receipts/logs are final-r9/correctness.json,
driver.json and correctness-0/1/2.log. Seven identical-source passing profiles
retain r8 provenance; complete resources,10k/100k,2GiB,release/container and
aggregate remain required. No all38 completion claim.

## Fresh source verification checkpoint — 2026-10-19 08:45 UTC

Concord, VSR and ExTurso are locked and installed at 3.1.2. At complete source
fingerprint dfcb66ed53e4885f0491038ba9808b09e6b0eb58f40f96b8dd344ef7b0227731,
the seven full standalone, gateway, cluster, crashes, resilience, restore and
faults profiles passed. Runtime 75740 completed/reaped exit 0; exact receipts
and original logs are in `s3-hardening/post-concord312-final-r8`. Independent
compound audits and remaining complete-source gates are still required.

The full resource attempt failed after one valid 64 MiB strict RF2/W2 PUT:
both CAS hashes, numeric bounds and zero staging passed, but a 218.227 ms
temporary-file probe exceeded the unchanged 100 ms coverage requirement.
The cause is unclassified. A controlled dirty-I/O matrix and focused real
replay will distinguish queue contention from other latency before any runner
capacity correction. No unchanged full retry is authorized by that failure.

Historical b7 remains 34/38; A28, A30, A31 and A38 remain incomplete. Original
10k/100k scale, all 76 resource groups, fresh correctness/tagged 2 GiB/release,
and actual aggregate verification remain mandatory. Full hardening acceptance
is false. This checkpoint supersedes the older continuation states below.

## Continuation after Concord 3.1.2 — 2026-10-19

Concord, VSR and ExTurso remain pinned to 3.1.2, with no dependency source patch.
Accepted b7 evidence covers 34 of 38 scenarios. A28, A30, A31 and A38 remain in
progress; full hardening acceptance is false.

New E2E source 78c5381f1e93f8caab6f83ed8e5f3a214736dfea556605fe5feba4aea5c4b28d
passed 24 focused regressions and the complete eight-case faults profile,
including per-boot and real ENOSPC cadence and cleanup. The original cluster
resource workload stopped after 11 of 20 groups: the 1 GiB concurrency-1 PUT
and both CAS hashes passed, but a 151.460 ms RSS probe produced a 179.406 ms
conservative coverage gap. Original numeric bounds and zero staging passed;
full A30 did not pass. The earlier concurrency-4 HTTP 500 remains unclassified.
Canonical receipts and raw evidence are retained in final-r6 and its named
resource/fault artifact directories. The local/gateway resource step did not run.

Sampler repair round 2 is under investigation. The original 100k workload and
final source-consistent gates remain mandatory. No publish, push or deploy occurred.

## Verification repair checkpoint — 2026-10-19 08:10 UTC

Final-r5 completed/reaped exit1 at b7. Resources failed after56 local/gateway
and15cluster groups; actual HTTP500 at1GiB c4 remains unclassified. Original
index was intentionally interrupted via its exact owned process group after
79.597s to repair resource evidence before the original full expensive rerun;
exit-15 is retained, never passed. The original10k/100k workload/budget remains
mandatory. Additional Python/harness regression20/0 and actual aggregate exit1
are retained in final-r5/additional-and-aggregate-r1.json.

Root reviewed and promoted b7A32/A36/A37 with complete crash/restore assertions;
ledger34passed/4in_progress is accepted ORIGINAL b7 scope only. All old archives
remain. New cadence-only e2e source is changing; no b7 pass is relabeled as a new
source pass. Production apps/config/mix source remains unchanged. Agent integrity
owns sampler/resources/faults repair; root owns bounded atom-only error tracing
and final native runtime. Source/provenance require a new freeze before finalgates.

Report-owned targeted strictPUT diagnosis native2 completed/reaped exit0/116.288s:
all4 actual1GiB concurrent writes verify at RF2/W2 on both CAS roots. No error
trace event and no original500cause established. It is not fullA30 acceptance.
Earlier diagnostic setup failures (existing evidence directory; transient RPC
parent linked to candidate sampler) are retained. Detached sampler emission and
parent-exit regression passed1/0; actual loaded100ms cadence remains unverified.
Owned error tracing redaction/cap/independent-session regression passed1/0 after
fixing the self-tracing sentinel test fixture; no production behavior changed.

## Fresh cluster resource failure; independent recovery continues — 2026-10-19 07:55 UTC

Final-r5 resources command failed exit1/456.615s after15/20 cluster groups at
1GiB c4 strict-quorum PUT: HTTP500 InternalError. Actual cause is not classified;
private owned root/tmp/ess-s3-hardening-cluster-J9vtRByZ and raw per-node logs
are retained for sanitized diagnosis. Disk has181GiB available; no disk-full
cause is established. Local56-group numerical/byte/cleanup evidence and cadence
concern remain distinct. NoA30 pass or threshold/deadline/quorum change.

Fresh b7 faults exit0/12.241s and local/remote crashes exit0/16.45s+9.936s;
compound audits are pending. Root session43117 continues restore then original
10k/100k index. Reports-only agents audit failure and recovery evidence. All
source remains b7 frozen; ledger31passed/7in_progress until audited promotion.

Current ledger reconciled to31passed/7in_progress at b7. The five98bc passes
are archived rather than counted as current passes. A30 coverage hold is explicit.

## Active verification and sampling concern — 2026-10-19 07:51 UTC

Session43117 remains the sole application runtime at frozen b7 source. Current
correctness/release/standalone/gateway/cluster/resilience and tagged2GiB evidence
is retained. All56 local/gateway resource groups completed exit0/1049.68s;
cluster20-group resources and faults/crashes/restore/real10k+100k index follow.

A30/A31 promotion is held: local raw audit found36 of31,607 active intersample
intervals above100ms (0.114%), worst787ms across20groups; all groups have samples
and first/last edge distances<=29ms. Numeric memory/queue/disk limits, full bytes,
upstream readbacks and staging cleanup independently pass, but nominal25ms sleep
does not prove maximum observed100ms cadence or exclude missed transient peaks.
No independent VM/RSS/queue high-water guard was collected. Full assessment:
post-concord312-final-r5/semantic-resource-cadence-audit-r1.{md,json}.
No requirement is silently relaxed; remaining profiles continue independently.
Reports-only contributor audits cluster cadence/faults. Source edits/competing
Mix/BEAM/E2E remain prohibited until session43117 finishes and is reaped.

## Active final-source verification — 2026-10-19

The current implementation fingerprint is
`b7becc297a1bb708b6327994f30931d66bbea55297e7a200ba389cb1bacb2823`;
HEAD remains3477bc540c3732dba3d47a7e08779783f9be25fc with uncommitted work.
Root reaped session62698: complete correctness, actual tagged2GiB and production
release/container smoke passed. Nine other profiles failed compile validation
before their workloads because cached release build_info differed from the default
build identity. The failed receipts remain under final-r4.
Root now owns session43117 and `post-concord312-final-r5/run_resumed_final.py`:
rebuild and validate production artifacts, retain only passing same-source receipts
with original provenance, then execute every remaining full profile. All11
profiles and all38 row audits remain mandatory. Source remains frozen.

The independent complete A29 workload at this same fingerprint passed exit0 in
16.044s; its14 compound assertions are audited in
`s3-hardening/a29-shared-blob-conflict-20261019-r1/full-workload-r1-audit.json`.
The final index profile and real10k/100k A28 workload remain pending. Earlier
35passed/3in_progress rows and10 passing profiles belong to98bc; they are
historical evidence and do not establish current-source aggregate completion.

Matching Concord/VSR/ExTurso3.1.2 remain installed. The fresh07:11:35UTC upstream
audit confirms3.1.3 changes only package versions/changelog, and#99 remains an
open needed issue. See `s3-hardening/post-concord312-final-r4/upgrade-final-audit.md`.
This section supersedes historical ownership and queued-work statements below.

## A29 explicit retry contract — 2026-10-19

Diagnostic full workload at974ada3e ran/reaped exit1/12.55s. Sanitized remote
RHS is exactly {:error,:build_conflict} during resume, not a timeout or upstream
consensus failure. Shared-CAS metadata contention from permitted other-bucket
writes is being reproduced deterministically in scoped core tests. Documented
backfill contract explicitly returns failed compare without publication/checkpoint
change and permits an explicit freshly planned attempt. Preserve that fail-closed
contract; the controller currently pattern-matches every resume to success.
No core automatic retry or deadline/loop-budget increase is authorized by this
finding. Integrity owns focused test runtime; root edits harness/reports only.
Original98bc and diagnostic974ada3e failed receipts remain immutable.

## Index diagnosis checkpoint — 2026-10-19

Root finished/reaped session19735 exit1 because index failed; all10 other profiles
including release passed at98bc. New A29 independent original workload ran/reaped
exit1/12.55s at98bc after seed/prepare/gated/rejoin passed; resume RPC failed with
its reason discarded. No A29 pass. Private root /tmp/ess-s3-hardening-cluster-Dae5i2qP
and canonical index-cluster-independent-r1.{json,log} retained. No runtime active.

Report-owned actual poll diagnostic r3 passed0/0.816s; it reproduces real Core
stats contamination of unchanged Bounds and verifies required poller resume
after success/injectedfailure plus independent sentinel tracing. Original
10k event remains individually unattributed. r1/r2 toolchain/build-env prerequisite
failures remain retained; no checks disabled. Applied acknowledged poller-only
quiescence around two steady measurement blocks in index_resource.exs; Bounds
unchanged. Current intermediate fingerprint9b7bd52c8d2912849e8385c36b51207345a77e39d3dfb4fbc1b91fecd6db0a37.
35 passed rows retain their actual98bc verification; fresh final-source profile
receipts required after remaining harness fixes. No whole-goal completion claim.

Actual application-free Concord3.1.2 write probe r2 passed0/4.93s, coherent
1000/2000/4000 states and30samples peroperation. p50 adapterlegacy1356/2839/5444us,
materialize769/1609/3139us, observer with no dispatcher511/954/2381us. This
reproduces complete-state write amplification in isolation, not A28 runtime
causality, scale timeout, fsync/network/quorum behavior or100k acceptance.

## Current checkpoint: 35 passed, 3 in progress — 2026-10-19

Source remains98bc241967700e2cf50c0baf6b8d73a9e3cc060f858bf483e89930145848eafd,
HEAD3477bc540c3732dba3d47a7e08779783f9be25fc. Coordinator promoted complete
A30/A31/A32/A36/A37 after actual compound-assertion review. P3.2/P3.3/P3.4 now
pass; A28/A29/A38 remain in_progress. Canonical profile/audit/raw artifacts are
under docs/reports/s3-hardening/post-concord312-final-r3/. Prior failed receipts
and the pre-promotion ledger remain unchanged. Do not rerun either exclusive
archive promotion script. No aggregate completion claim.

Resources:56 local/gateway+20cluster groups,64MiB/1GiB c1/c4 original workloads;
12 direct upstream readback groups/30objects/90assertions. All116 post-idle
staging measurements and actual final local/all3cluster staging are zero. c4
local binary/total/RSS deltas20,738,808/26,243,316/46,772,224bytes; cluster
20,764,592/21,095,376/41,381,888. Fixed original envelopes all passed.
Faults:8groups/316continuous samples, peak queues1/2 against128/2048, all8
zero-staging cleanup snapshots; actual isolated8MiB ENOSPC and recovery.
Crashes:10local halt137/new-process boundaries preserve acknowledged versions/
bytes; protected uncertain stages retained. Separate remote durableACK/origin
data-b crash, origin-only bootstrapfalse restart plus all3 convergence, and
same-ID RF2/W2 exactly-one-version resume passed. This is not an all-voter restart.
Restore:both modes passed; original roots inaccessible, backupread-only, all15
negatives, real packed standalone data, hashes/IAM/config/MPU/jobs/fences and
explicit safe activation all verified. Backups b5800721bf75f5e256ff6bf8 and
0c5de14371e19224cbad7866; recovery9.803s/16.022s measured target prepare through
activated HTTP verification, excluding backup/setup/earlier negatives.

Root still exclusively owns session19735, release build/container smoke. No
concurrent Mix/BEAM/E2E and no source edits until it finishes/reaps. Next run
report-owned real poll-overlap diagnostic, then independent original A29
cluster-index command at98bc. A28 actual command remains exit1/1937.638s after
10k fixture at forbidden native boundary;100k and second cluster command never
ran. Poller contamination is a source-supported hypothesis, not reproduced yet.
Any harness change requires a new source freeze and fresh final-source receipts.

# ESS S3 hardening verification

## Current accepted rows — 2026-10-19

At the frozen98bc source, complete correctness, standalone, gateway, cluster
and resilience pass. Standalone all-profile20.010s plus retained original SDK/
admin/metrics/SIGKILL-persistence10.493s both exit0. Gateway152.554s passes all17
named groups, including full-BEAM reconciliation and retained mc24/0. Selected
Boto3/botocore1.43.114, AWS CLI2.34.24 and mc RELEASE.2025-08-13T08-35-41Z
are recorded with the actual RustFS image digest in canonical artifacts.

Coordinator assertion audit now supports A01–A27 and A33–A35:30passed,8in_progress.
Historical f495 restore passes remain evidence; current A36/A37 await their
queued complete98bc regression. A28/A29/A30/A31/A32/A38 also remain unpassed.
The actual index command failed exit1 after1937.638s at `A28 forbidden native
boundary`. Its exact MFA/origin are unclassified;100k and cluster-index did not
execute. Root session19735 continues independent resource/fault/crash/restore/
release profiles at unchanged98bc. A28/A29 and aggregate remain unpassed.

Actual2GiB tagged transport passes1/0 in5.318s including command startup, with
sender peak8968B/receiver21736B below64MiB process limits. Receipt:
`s3-hardening/post-concord312-large-stream-r1/result.json`. Per-row proof and
pre-audit history are in `s3-hardening/post-concord312-final-r3/row-audit-receipt.json`
and `ledger-before-current-row-audit.json`; exact assertion mappings are in the
final Phase1/Phase2 and Phase3 evidence audits. No push/publish/deploy occurred.

The sections below retain earlier execution checkpoints. Their active-runtime
and pending-verification statements describe that checkpoint; the current accepted
rows and canonical profile envelopes above supersede those historical statuses.

## Complete current-source correctness — 2026-10-19

The unchanged98bc source passes complete correctness: format and strict test
compile exit0; Core572/0 (one separately tagged exclusion), internal transport
40/0 (one separately tagged2GiB exclusion), CLI3490/0, S3357/0, Web21/0.
The full test command completed in602.968s; the private-netns driver exited0,
and verified host namespace/port reservations unchanged. Exact original receipts
are in `s3-hardening/post-concord312-correctness-r2/`; canonical final-r3 imports
the profile with explicit provenance and original log paths. The earlier r1
CLI/Web failures remain preserved. Tagged cluster already passed separately;
actual tagged2GiB transport subsequently passed; its completed receipt is above.

## Current frozen-source verification — 2026-10-19

Current source fingerprint is
`98bc241967700e2cf50c0baf6b8d73a9e3cc060f858bf483e89930145848eafd`,
HEAD remains `3477bc540c3732dba3d47a7e08779783f9be25fc` on main.
Concord, VSR and ExTurso are the published matching3.1.2 family. Earlier
dependency blockers and failed runs below are historical records.

At this source, complete cluster and resilience profiles passed. Cluster stopped
the actual data-a primary at view1/commit1214, elected data-b at view2 without
losing committed metadata, required503 strict writes during the data outage,
and proved all3 voters rejoined at view2/commit=applied1218 before recovering
both-endpoint persistence and strict writes. Resilience includes all8 scenarios
and the independent external backlog/restart/fenced-retry command. Its metadata
primary outage separately retained RF2/W2 success while both data nodes remained.
Driver exit0 and all owned nodes stopped; canonical records:
`s3-hardening/post-concord312-final-r3/{cluster,resilience,driver}.json`.

Root now owns complete correctness session68097. Private namespace prerequisites,
format and strict compilation passed. Core572/0 and transport40/0 completed;
remaining umbrella applications are still running or pending. Tagged cluster
and2GiB tests are separate required executions. No complete correctness or
additional scenario pass is implied yet. Source remains frozen while reviewers
prepare reports only. Remaining profiles and all38 assertion audits must pass
before completion; original failed r1/r2 records remain preserved.

## Latest r2 execution outcome — 2026-10-19

At unchanged b3b5ed… the complete cluster profile passed: actual data-a/view1
primary loss, data-b/view2 election, strict503 data-outage write behavior and
all3 rejoin at view2/commit=applied1183, then both-endpoint persistence/strict
write recovery. Complete resilience failed: first6actions passed but its elected
controllerRPC exited1; verify and external backlog were not executed. r2 driver
exit1 is preserved. Integrity diagnoses this failure under exclusive runtime
ownership; any correction must use a fresh source freeze and output. Current
authoritative evidence: `s3-hardening/post-concord312-final-r2/driver.json`.
Whole-goal status remains incomplete and no additional scenario is promoted.

## Current final-source gate before r2 completion — 2026-10-19

The published Concord/VSR/ExTurso3.1.2 update and ESS integration are installed.
The complete original goal remains active. Final source is frozen at
`b3b5ed28918a863be0786a56cac3687194bdf398eb7ba4df8d40a94746ad7660`;
HEAD remains3477bc540c3732dba3d47a7e08779783f9be25fc on main with the
implementation uncommitted. Exact current profile records are under
`s3-hardening/post-concord312-final-r2/`. Integrity owns cluster+resilience runtime;
root owns later serialized full correctness and remaining original profiles.
Reports/ledger may change, application/test/harness source may not.

The preceding d42b615… full attempt is retained in `post-concord312-final-r1/`:
standalone all+retained and gateway16groups+mc24/0 passed. Core572/0/1excluded,
transport40/0/1excluded and S3357/0 passed within a failed correctness command;
CLI3490/1 hit a journal port collision, and Web21/3 inherited a production
test-runner admin hash inconsistent with its established test defaults. Full
correctness will rerun unchanged in a private reserved-port network namespace
without that runner override. Probe evidence proves host reservations unchanged.
The prepared runner requires namespace UID0 to map only to host user1000 and
rejects capabilities beyond private NET_ADMIN before any namespace sysctl write;
that complete runner has not executed yet.

r1 cluster's business/MPU/restart programs passed, then a hardcoded meta-primary
assertion failed against legitimate data-a/view1/commit=applied1181. The repaired
test records/stops the actual primary, proves majority election and all3 rejoin,
requires503 strict writes for a data-role outage, and verifies post-rejoin bytes
plus strict-write recovery. Separate resource-only assertions now directly hash
RustFS readback of each full-size gateway PUT-only key; original56 measured
groups and all thresholds remain. These fixes have scoped preflight passes;
full actual execution is still required. No new acceptance row is promoted:
2 historical full restores passed,36 rows remain in progress.

Evidence: `cluster-primary-harness-20261019-r1/`,
`resource-upload-readback-20261019-r1/`, `post-concord312-correctness-r2/` and
`post-concord312-large-stream-r1/` below `docs/reports/s3-hardening/`.

## Historical completion and first full-gate checkpoint — 2026-10-19

The local32-way completion defect is repaired without changing ObjectCommit's
comparison budget, the original barrier/30s await, authentication, checksums or
quorum. Actual diagnostic503: compare_retry_exhausted caused by unrelated private
multipart transitions rewriting unchanged shared index records. The corrected
transitions write only their upload record and retain all identity/readiness
compares. Original untraced409s are not individually attributed.

Complete Core572/0 (one default-tagged cluster exclusion) and S3 357/0 pass at
27ad96fb6dd7f7296adb1f34e9e7bf1271191a85b772d8af23671046a3968aa5.
RED, deterministic regression and exact/full-suite evidence:
s3-hardening/completion-concurrency-20261019-r1/results.json.

Final instrumentation source is now frozen at
d42b61515f9a01f70ba06ab5a2d6cb16ac4881bbc5c625ee68725aa81c6462f3.
The A28 request/replica trace and predeclared numeric bounds have matching final
CPU and9-page actual-native preflight passes. Python harness contracts14/0,
script format, shell syntax and diff checks pass. Full correctness, standalone,
gateway and cluster lanes are executing serially under root ownership; Phase3
will follow. Live command statuses/logs: s3-hardening/post-concord312-final-r1/.
No new complete-row promotion is inferred:2 previously audited restores and36
in_progress remain until final-source full assertions are reconciled.

The checkpoints below retain their exact source/version and historical outcome.

## Concord 3.1.2 upgrade checkpoint before local completion repair — 2026-10-19

Concord, viewstamped_replication and ex_turso are upgraded from 3.0.6 to the
published 3.1.2 family. Core requires `~> 3.1.2`; only those three lock entries
changed. Upstream #94–#97 are CLOSED and included in that release. No dependency
source was patched. Existing architecture, authentication, checksums, RF2/W2,
generation/revision fences and finite test budgets remain intact.

The update is applied and verified with one unresolved S3 concurrency test.
Normal standalone, healthy three-voter and RustFS gateway features pass after
the update. Full isolated restore passes for both modes, including the complete
A36/A37 assertion audit. The original goal remains incomplete: **2 passed,
36 in progress, 0 currently blocked**. Released upstream prerequisites are
resolved; unexecuted scale/backfill/resource/rejoin work stays unpassed.

HEAD remains `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`, with
uncommitted changes. Final implementation/test/harness fingerprint:
`f4957d9ededd69b5b2a8fc7291da496cbe3e85e710c6c98b32810922fc48dcba`.
Exact commands, exit statuses, source boundaries and preserved failed attempts
are in [upgrade results](s3-hardening/concord-3.1.2-update-20261019-r1/results.json).

| Upgrade verification | Actual result and boundary |
|---|---|
| Latest Core application suite | 568 tests, 0 failures, 1 default-tagged cluster test excluded; final fingerprint |
| Latest strict production compile | `--warnings-as-errors` exit 0; final fingerprint |
| Full S3 application suite | 357 tests, 1 failure; earlier upgraded fingerprint `0922269f…`, retained as failed |
| Cluster transport application suite | 40 tests, 0 failures, 1 default-tagged large test excluded; `0922269f…` |
| Normal standalone / healthy cluster / gateway | All pass at upgraded r3 fingerprint `b11eedb3…`; both data endpoints retain RF2/W2; gateway eight groups and retained mc 24/0 |
| A36 isolated standalone restore | Passed; recovery 9.856s; nine artifact/key negative cases rejected |
| A37 isolated three-voter cluster restore | Passed; recovery 16.333s; six artifact/key negative cases rejected |
| Scoped format, diff and restore Python syntax | Passed; final fingerprint |
| Compose configuration parsing | Default command version 0 and explicit 1 reach all three voters; no service started |

Full r3 feature fingerprint is
`b11eedb3d46dd212f471a32a2080bb81031795fa3d157764a0f38d03021b89f3`;
full initial-upgrade fingerprint is
`0922269f963e405dd6064516d3767a1f47d25d4f0b0487dc17504750e7399473`.
Later changes concern restore integration and its fixture/diagnostics. The r3
normal feature profiles were not rerun at the final fingerprint. Pre-upgrade
Python 10/0 and focused HTTP 17/0 below remain historical. Web/CLI suites and
production asset/release/container smoke were not rerun for this upgrade.
Selected clients remain Boto3/botocore 1.43.114, AWS CLI 2.34.24 and
mc RELEASE.2025-08-13T08-35-41Z. Retained r3 gateway results and mc assertions
are `concord-3.1.2-update-20261019-r1/r3-{gateway-results.json,mc-cloud-cache.log}`.

The S3 failure is the original strict 32-completion assertion at
`authorization_hardening_test.exs:244`: actual one 200, 27 responses with 412
and four with 503, instead of one 200 and 31 with 412. This run does not reproduce
the former `Task.await_many` timeout, but still fails. The cause of 503 remains
unclassified. No assertion, retry budget, timeout or safety guard was relaxed.
The serialized upgrade driver preserves an overall nonzero exit for this
failure even though its final Core/compile/restore commands all exited zero.

ESS integration now validates `:sha256_deterministic_etf_v1` with deterministic
ETF, preserves legacy checksum semantics, rejects unknown/nil schemes and checks
the same read buffer used for import. New regressions actually failed first
(18 tests, three failures), then backup/native contracts passed 22/0. Restore
configuration/guard regressions also failed first (23 tests, five failures),
then passed 23/0. The cluster restore defect passed raw backend records where
BlobLocations requires typed descriptors; using BlobCatalog preserves revision,
RF, node-generation and location comparisons. Its r4 failure is retained.

Empty recovery targets explicitly set `ESS_CONCORD_COMMAND_VERSION=1` on every
voter and retain it through quarantine, restart and activation. Restore rejects
version 0 before importing or changing its marker. Existing installations retain
the compatibility default 0: no automatic command-format migration occurs.
The upstream documented migration procedure and same-OTP-major restore boundary
are linked in [the recovery runbook](../operations/cluster-upgrade.md).

The [complete restore audit](s3-hardening/concord-3.1.2-update-20261019-r1/restore-r5-audit.json)
records backup IDs/consistency points, changed/deleted originals, inaccessible
source roots, a read-only backup in separate PID/mount namespaces, exact restored
bytes/versions/markers/attributes/tags/checksums/IAM/configuration, retained and
completed multipart uploads, old job/cleanup fence rejection, unknown-intent
preservation and generation-2 cluster targets. Quarantine restart and explicit
activation retain all eight workers disabled; the outbound canary observes zero
requests. Ordinary writes succeed after validated activation. Only A36/A37
are promoted; all earlier failed attempts remain available with `resolved_by`.

Native 50ms read-budget probes returned structured errors at 52.5/50.9ms, and
the 10k retained-log transition probe completed. These are bounded dependency
checks, not ESS listing/resource/rejoin acceptance. Heavy failure/stress work
remains separate under the user's feature-first order. Final read-only cleanup
finds no owned runtimes, recorded endpoint listeners or gateway containers.
The existing aggregate audit still exits 1: A36/A37 have complete records, but
36 rows and older full-lane source identities remain unresolved. Its exact
output is retained in `concord-3.1.2-update-20261019-r1/aggregate-verification.json`;
no previous aggregate was overwritten. No commit, push, publication, deployment
or production-data operation occurred.

## Historical business-feature verification before upgrade — 2026-10-19

The user reprioritized normal business completion ahead of Concord exception
paths and heavy hardening. The declared normal profile now passes on standalone,
healthy three-voter cluster and RustFS gateway. This is a separate delivery gate;
the original 38 hardening scenarios remain 29 in progress, nine blocked and zero
passed. Their recorded failures remain follow-up work.

Starting/ending HEAD is `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`.
Uncommitted implementation/test/harness source fingerprint at both ends is
`5e911161d4e172c1c04cebdcc08903d8f8b6a05c295af3cbc5f1ccc4f02b7c77`.
Exact commands, deadlines, logs, durations and exit statuses are in
`s3-hardening/feature-flow-20261019-r1/results.json`.

| Current-source check | Actual result |
|---|---|
| Python acceptance-harness contracts | 10 tests, zero failures; exit 0 |
| Authorization, attributes, read semantics, checksum/range and gateway attribute HTTP modules | 17 tests, zero failures; exit 0 |
| `MIX_ENV=prod mix compile --warnings-as-errors` | exit 0 |
| Standalone `features` scope | versions/listings, tags/CORS/location, multipart and selected clients all passed; exit 0 |
| Healthy three-voter `features` scope | IAM plus the four business profiles passed across both data endpoints with RF2/W2; exit 0 |
| RustFS gateway `features` scope | eight groups passed, including normal listing/DELETE/batch DELETE and retained mc cloud-cache 24/0; exit 0 |

Independent clients were Boto3/botocore 1.43.114, AWS CLI 2.34.24 and
mc RELEASE.2025-08-13T08-35-41Z. RustFS 1.0.1 image identity and all eight
gateway groups are retained in `feature-flow-20261019-r1/gateway-results.json`;
`mc-cloud-cache.log` retains its 24 passing assertions. This run completed in
58.2 seconds. A startup connection refusal before cluster readiness is retained
in the log; readiness completed before the passing profiles ran.

Normal checks retain authentication, default-client checksums, bucket-generation
fences and strict quorum. The new optional scopes leave default `all` execution
intact. Healthy cluster feature mode avoids replica removal, metadata corruption,
concurrency races and node restarts; gateway feature mode stops before injected
upstream failures/restart reconciliation. Read-only final review confirmed no
owned ESS processes, recorded-port listeners or gateway containers remained.

No production code changes were needed for this work unit. No branch/worktree,
push, publication, deployment or production-data operation occurred. Gateway
versions, multipart destinations and object/version tagging remain deliberate
exclusions; cluster gateway writes remain guarded. Backup/restore, scale and
the unclassified 1 GiB/concurrency-four UploadPartCopy 503 are not validated by
these normal-mode results. See `../plans/s3-feature-status.md` for the current
business feature matrix and the preserved follow-up boundary.

## Historical full-hardening checkpoint — 2026-10-18

**ESS-S3-HARDENING-001 is incomplete.** Implementation spans all three phases;
mandatory completion gates still fail. Sections below preserve the earlier
full-hardening execution chronology. Starting/ending HEAD
is `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`; implementation remains
uncommitted. Most full-lane production/test/harness evidence uses source fingerprint
`42621e3fd6659111a5caede9ea01f35e4c3fcb576f410c4738779365afe884a2`.
No branch/worktree, push, package publication, deployment or production-data
operation occurred. Local release/image construction and disposable tests only.

### Final narrow correction and source boundary

Final source fingerprint is
`3384af47521268590510f3a42efd244a74ecf47d94ad5e900a5f0df01ffa8f81`
(r6); HEAD remains unchanged. Abort previously classified an unrelated shared
bucket-index compare loss as a conflicting completion and could miss a bucket
generation change. Three meaningful RED regressions reproduced those defects;
a fourth RED failure was a fixture error, corrected and explicitly excluded
from production-defect evidence.

The fix moves only the abort metadata transition into the existing
Metadata.MultipartCommit boundary. It re-reads upload status and bucket
generation after contention, uses the existing eight-attempt bound, preserves
real conflicts/backend errors, and permits storage cleanup only after success.
No part retry budget or protocol/transport deadline changed.

At r6:34 focused core tests pass (6.3s),34 affected S3 multipart HTTP tests pass
(4.3s), strict test/production compilation and scoped format/diff checks pass.
Exact commands and evidence:
`abort-compare-retry/{results.json,root-followup.json,task.diff}`. The preceding
UploadPartCopy503 remains unresolved. No full resource replay or full suite
pass is claimed for r6. Earlier full-lane/image evidence belongs to r5.
Aggregate verification after the fix returns1
(`phase3-final-aggregate-verify-r2.json`); all38 mandatory rows remain unresolved
(29in_progress/9blocked). No runtime remains active.

### Implemented scope

- Phase1: one S3 operation registry shared by authorization/dispatch; explicit
  selector validation; canonical/legacy policy compatibility and fail-closed
  reads; source-copy and multipart target authorization; streamed payload/MD5/
  algorithm checksums and applicable chunk signatures/trailers; serialized
  conditions and bucket-generation fencing; truthful durable gateway
  reconciliation; bounded hardened XML/control handling and sanitized errors.
  Principal modules: S3 Operation/Authorize/SigV4/Integrity/Shared, core IAM,
  ObjectService, ObjectCommit/MultipartCommit/BucketLifecycle, WriteThrough and
  DeleteReconciliation. Reviewed defects have retained RED/repair evidence.
- Phase2: hosted versions/null/delete-marker behavior; v1/v2/version/multipart
  listing and pagination; multipart-copy/state/finalization handling; initiation
  attributes/tags/checksum persistence; metadata/tag COPY/REPLACE; encoded bytes,
  conditions/ranges/response overrides; tagging/location/CORS; explicit mode and
  compatibility matrix. See `docs/s3-compatibility.md` and the row audits.
- Phase3: transactional bounded listing tree and resumable backfill/cutover;
  raw bounded local/gateway/replica/repair/replication transfers; safe owned
  staging recovery; crash/fence/GC/packing/repair verification; backup/restore
  manifests/quarantine/activation tooling; bounded-cardinality metrics,
  readiness/capacity/exact-operation diagnostics and separated CI/aggregate lanes.
  Scale and successful restore are **not verified**. New source does not imply
  those acceptance gates pass.

### Executed checks (r5 unless specified)

Exact commands, environment, source identity, duration, logs and exit status are
in `s3-hardening/phase3-independent-final-r2/results.json` and
`s3-hardening/phase3-followup-final-r1/results.json`.

| Check | Actual result |
|---|---|
| `mix format --check-formatted`; strict test compile | exit0; unchanged frozen source |
| Original checksum/condition module scope, seed819058 | 8tests/0failures; original deadlines |
| `mix test apps/ex_storage_service_web/test` | 21/0 |
| Actual tagged2GiB transport | 1/0; raw hash and bounded receiver/sender |
| Tagged three-voter test without `--trace` | 1/0; original120s module deadline |
| Full CLI after narrowly repaired fixture-port collision | 3490/0,321.3s; production Journal unchanged |
| Complete fresh S3 app suite | 357/1,401.0s; original32completion30s timeout |
| Latest complete core portion | 551/1/1taggedexcluded; original60s legacy backfill timeout in Concord |
| Latest complete transport portion | 40/0/1taggedexcluded; large tagged case separately executed |
| Latest full umbrella | outer1800s watchdog, exit124; not a pass |
| r5 gateway/faults/crashes/resilience aggregates | exit0, same r5 fingerprint |
| r5 assets/release/localimage/operator smoke | exit0,226.753s; no publication |
| r5 standalone aggregate | exit1; strict32completion assertion and retained restart fail |
| r5 canonical local/gateway resources | exit1 after49/56completedgroups; cause unclassified |

The umbrella's old CLI fixture collision was corrected with a real occupied-port
reproducer, bounded exact-error-only fixture retries and full fresh CLI pass.
The original failed umbrella remains failed. Its checksum-profile timeout only
has a client-side stack; fresh original modules and complete S3 do not reproduce
that checksum failure. No unsupported causal attribution is made.

Current standalone independent versions/lists, tags/CORS, multipart/races and
selected client programs passed, but one completion race returned1x200/30x412/
1x409. Original helper omitted S3 error detail. Retained restart then hit the
native primary-discovery5000ms exit. The profile remains blocked.

External diagnostics are separate from acceptance. Retained-checksum cloned the
entire original disposable root and verified its inventory unchanged. Startup
failed before tests; allowlisted markers identify GenServer.call/:primary/5000/
timeout, matching #96. No checksum replay pass or causal explanation follows.
Conditional diagnostic r2 passed1x200/31x412; bounded r3 failed0x200/25x412/
6x503/1x409 ConditionalRequestConflict. One upload was durably completed with
correct bytes, but no client acknowledged success in r3. The conflicted upload
was not completed, its later retry returned412, and visible identity remained
unchanged. Reduced preceding workload and strong snapshots can affect timing;
none of these results loosens or clears the original strict acceptance gate.

### Resource measurements and limits

Canonical r8 uses64MiB/1GiB at concurrency1/4, fixed predeclared envelopes and
raw-byte hashes.49groups completed without a recorded envelope violation. The
next1GiBc4gatewayPUT failed503 with two bare pre-upstream WriteThrough timeouts.
A23,571ms sampler gap spans both errors; low pre/post queues do not establish
responsiveness inside that gap. Remaining7groups are incomplete. Measurements
exclude the failing group and OS page cache; full baselines/peaks/post-idle and
sample data remain under `s3-hardening/phase3-resource-r8/`.

| Payload/concurrency | Completed/14 | Max binary/BEAM/RSS delta MiB | Peak queue max/sum | Temp peak MiB |
|---|---:|---|---|---:|
|64MiB/c1|14|4.94/5.05/12.34|6/7|128|
|64MiB/c4|14|18.69/18.80/34.47|3/7|512|
|1GiB/c1|14|5.80/10.40/20.50|2/2|2048|
|1GiB/c4|7|19.72/22.00/34.33|38/39|8192|

Memory delta limits: c1 binary96MiB/BEAM176MiB/RSS320MiB;
c4 binary192MiB/BEAM320MiB/RSS512MiB. Queue limits128/2048.
Temporary-disk limit is2x in-flight bytes+64MiB. No acceptance threshold changed.
Historical r6 56/56 is retained and does not override the current failed run.
Cluster resources separately passed15groups then failed1GiBc4strictPUT via #96;
remaining replica/repair groups have not passed.

**Resource diagnostic v1 r2 failed** after46/56groups (770.241s):1GiB/c4
local UploadPartCopy returned503 ServiceUnavailable; finally-abort then returned
409 ConditionalRequestConflict (multipart upload changed during completion).
The original transfer failure must be retained alongside cleanup failure. The
tracer had stopped earlier at161280events/5165records/991951bytes due to its
512-message backlog stop condition. No native causality coverage exists at the
failure. Worker/group/server groups stopped and container removed, with no
cleanup errors. Evidence: `resource-timeout-diagnostic/run-reviewed-r2/runner.json`.

V2 synthetic tracing probe passed after bounded repairs for sentinel self-tracing
and disabling a dead tracee. It verified all four immediate first reads,100k
nonwriter calls with0nativeevents, success/error/exception/DOWN cleanup, explicit
fifth-writer overload and unrelated-session preservation. Evidence:
`resource-timeout-diagnostic-v2/verification/results.json`.

**V2 original-workload replay also failed46/56**,748.143s: the same1GiB/c4 local
UploadPartCopy503 then finally-abort409 chain. Native tracing remained active
(1178events/1026native/0untracked); its gateway WriteThrough scope does not cover
the local failing call. No native cause is established. Last helper checkpoint
was collecting; VM/group shutdown is verified, rather than claiming an observed
helper teardown summary. The original sampler has a46,861ms maximum gap near the
failure. Do not infer responsiveness or low queues during that interval.

Completedgroup-only maxima for v2 (binary/BEAM/RSS delta MiB; queues max/sum;
temporaryMiB):64MiBc1 4.98/5.00/9.70,2/2,128 (14groups);64MiBc4
19.69/19.74/35.27,38/38,512 (14);1GiBc1 4.98/8.59/19.79,2/3,2048 (14);
1GiBc4 14.94/19.91/30.45,8/9,4096 (4). No recorded envelope violation in those
completedgroups. Failedgroup excluded; no complete resource pass. Exact evidence:
`resource-timeout-diagnostic-v2/run-reviewed-r1/{summary.json,runner.json,resource/results.json}`.
Worker/server groups stopped and generated container removed; cleanup errors[].

No runtime remains active. Upstream#94/#95/#96/#97 remainOPEN (live recheck
`upstream-blockers-final-r3.json`). Aggregate acceptance verification returns1;
29rows remainin_progress and9blocked. Required correctness/cluster/index/resource/
restore profiles remain unresolved. No acceptance row was promoted.

### Crash, recovery, artifact and operator evidence

At r5, actual local crash lane10/10 covers stage/after-stage/sync/rename/directory
sync/blobcommit/metadatacommit/response plus safe anonymous cleanup/protected
retention. Strict replication crash/resume verifies peer durable ACK, retained
three-voter restart, internal-ID resume and two generation-fenced copies. Fault
lane covers disconnect/cancel/truncation/ENOSPC/restart and staging cleanup.
Resilience8actions plus durable external backlog restart/drain verify election,
rejoin, strictRF2/W2 outage rejection, stale-generation/lease fencing, read repair,
scrub and reference protection. Cross-cluster replication never satisfies RF/W.
Protected/foreign/legacy/uncertain staging remains explicit recovery inventory.

Fresh image `sha256:fe8dacb088b4d5630e1e003f862e0eab6073949733261c76ff756529a5666bcd`
passes HTTP health/readiness/build identity and actual capacity, three operation
namespaces, missing job/index, and staging cursor RPCs. Evidence:
`s3-hardening/phase3-final-r4-release/container-smoke.json`. Build version0.6.9
uses actual SHA and build-start2026-10-18T00:07:35Z. After this build a local
no-compile diagnostic hit the expected compile-time metadata mismatch; ordinary
compile reproduced it, forced strict production compile restored normal checkout
metadata with validation enabled. Failed attempts are retained.

Successful independent restore remains **blocked**. Original-root isolation and
nine negative key/artifact cases passed, but native unchanged snapshot checksum
fails in a fresh VM (#95). No restored hashes/versions/tags/IAM/config/jobs, clean
cluster restore or recovery-time success is claimed. Outbound/destructive workers
remain quarantined pending verified restore/activation; no checksum bypass.

### Blockers, resume boundary and operating limits

Live issue recheck `s3-hardening/upstream-blockers-final-r3.json` records OPEN:
Concord#94 retained-log complexity, #95 fresh-VM backup checksum, #96 primary
lookup deadline/exit; #97 bounded native pagination is a needed-feature limit,
explicitly documented for operator backlog pages. Scale100k/backfill/rejoin,
large concurrent cluster, full clean suites and successful isolated restores
require applicable released fixes plus coordinated dependency update and the
original checks. No dependency source patch, timeout inflation, checksum/auth
bypass or quorum reduction is used. The unrelated r8 bare timeout and original
umbrella checksum timeout remain unclassified without matching evidence.

All38 scenario rows have explicit remaining gates in
`../plans/s3-hardening-results.json` and
`s3-hardening/final-acceptance-boundaries-r1.md`. No automatic promotion follows
from a log or helper. The aggregate must remain nonzero while mandatory gates
are failed, blocked or unexecuted. Final aggregate verification returned exit1; exact command and output are retained
in `s3-hardening/phase3-final-aggregate-verify-r2.json`. No runtime remains active.

Selected environment: Linuxx86_64, Elixir1.18.5/OTP28, Python3.13.15,
Boto3/botocore1.43.114, AWSCLI2.34.24, mc RELEASE.2025-08-13T08-35-41Z
(SHA25601f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891).
Versions are selected/pinned, not a claim of newest upstream releases.

Upgrade/backfill instructions are in `docs/operations/listing-index.md` and
backup/restore runbooks. Keep writers quiesced and retain verified Concord/blob
backups before migration; never delete legacy records or clone live node/lease
identities. Current native backup blocker prevents declaring a validated upgrade
or rollback procedure. Old binaries after v2 writes require metadata restore;
backfill readiness/cutover/rollback artifacts remain fenced and non-destructive.
No production upgrade, migration, restore or activation was performed.

Excluded capabilities remain fullIAM/STS, ACL ownership/ObjectLock, SSE/KMS,
website/S3Select/directory buckets/erasure coding, and deliberately guarded mode
combinations documented in the compatibility matrix. Cluster gateway writes,
packing/lifecycle/admin guards remain intact. No full AWS parity is claimed.

## Historical execution chronology

The following sections are preserved historical checkpoints. Their “running” or
“pending” statements describe their recording time; use the authoritative
checkpoint above for current results.

Execution remains in progress. Complete-row promotion awaits the final audit; Phase 3 has substantial executed evidence and still has mandatory blocked and pending gates. Starting and current HEAD: `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`; implementation is uncommitted. No publish, push, deployment or production data operation performed.

Latest umbrella lane ended at the unchanged1800s watchdog: Mix exit124,
1805.928s including owned teardown; aggregate exit1. Core551/1/1excluded,
transport40/0/1excluded, CLI3488/1; S3partial with3failures (checksum-profile
60s and conditional-write/copy timeouts), Webnotreached. Source fingerprint
remained6f9e5207cda3bb6030e8e9688c30eb12616c250221ccf4d2839e83cdcacd1144.
Exact record: `docs/reports/s3-hardening/phase3-final-lanes-r2/correctness.json`.
Earlier running claims below are chronological. Source freeze is now released
for two narrow test/harness corrections; CLI fixture worker owns Mix runtime.
No full-suite pass is claimed.

## Current A31 cleanup checkpoint

Safe staging recovery now has executed RED8/8→focused GREEN. Integrated scope
71/0, storage/repair77/0, affected S3/cluster18+14/0, strict test/prod compile and
format checks pass. The expanded real crash lane passes10/10 with dirty-source
fingerprint91121c7dd7d98b01d175e164cec4dc534f4d8f696eb21eb07e5522bd9ce81023.
It proves dead-owner anonymous cleanup and protected-stage retention across fresh
BEAMs without exposing prepublication objects or losing acknowledged versions.
See `s3-hardening/staging-recovery/cleanup-verification.json` and
`staging-cleanup-crashes-r1/results.json`. Protected/legacy/foreign staging remains
explicit recovery inventory; this is not universal orphan deletion.

The earlier541test core run's100writer failure did not reproduce in isolated
unchanged replays; its exact cause remains unproven. A separate deterministic
index-planning conflict defect was reproduced and repaired within existing
compare budgets (focused87/0; `s3-hardening/index-conflict-retry.md`).

The source-frozen full umbrella rerun in `s3-hardening/phase3-final-lanes-r2/`
is still running. Format and strict test compilation pass. Core551/1/1excluded
fails the original60s LegacyObject backfill deadline; transport40/0/1excluded;
CLI3488/1 fails initial loopback journal-mutex acquisition. S3 has additional
checksum-profile and conditional-write timeouts; its checksum timeout has only
a client-side stack and remains unclassified. No full-suite pass is claimed.
The fingerprint remains6f9e5207cda3bb6030e8e9688c30eb12616c250221ccf4d2839e83cdcacd1144.
Fixture and cluster deadline-harness corrections wait for this lane to finish.
Current resource, client and release/image checks remain pending; earlier
results remain historical. Subsequent sections preserve their original chronology.

## Latest resumption evidence

The interrupted final umbrella run is **not passed**: core516/5/1excluded,
transport40/0/1excluded, CLI3488/0; S3 partial with timeouts; Web not reached.
The owned aggregate was terminated after repeated blocked metadata timeouts,
exit130. Logs remain under `phase3-final-lanes/`.

After correcting reproduced fixtures without changing production guards:
GC/versioning22/0; selected legacy descriptor1/0 (3excluded by line scope);
existing tagged actual2GiB transport1/0; fresh Web21/0. The fresh full S3 suite
then completed357tests/1failure in348.9s; the sole failure is the original
32-completion30s deadline. This is a failed full suite, not an acceptance pass.

Exact-ID uncertain-operation diagnostics RED37/9→GREEN37/0; current scoped
formatting/testcompile passed. Stage collision across real fresh BEAMs
RED3/3→GREEN56/0 after random names/exclusive creation; A31 cleanup remains
in progress under its accepted ownership/protection/bounded-traversal design.
Native-Concord failed-job diagnostic r2 proves actual claim→terminalfailure,
exact allowlisted read and omitted private payload/error. Its r1 failed missing
fixturemasterkey. Aggregate Python contracts r9 pass11/0 with pinned Python;
r8 default-Python missingboto3 failure is retained. Prodstrict r13 and format r5
passed before pending cleanup source integration.

Evidence: `phase3-gc-version-fixtures-green-r1.log`,
`phase3-legacy-fixture-green-r1.log`, `phase3-two-gib-transport-r1.log`,
`phase3-web-final-r1.log`, `phase3-s3-final-r1.log`, `operation-status/`,
`staging-recovery/`, `phase3-failed-job-operator-r2.log`,
`phase3-aggregate-contracts-r9.log`. A29's new source-complete cluster lane
passes shell/Python/outer+7remoteElixir syntax, but actual execution stops at
known#94 preflight exit1. No runtime backfill/rejoin pass is inferred.

All three Concord blockers were rechecked OPEN in this resumption; see
[precise blocker handoff](../plans/s3-hardening-blockers.md). No native checksum,
quorum, authentication or original request deadline was weakened. Mandatory
100k-scale/restore/concurrent-cluster gates cannot be called passed. Remaining
independent checks and source changes are recorded in the status ledger.

## Historical and latest actual results

| Check | Result | Artifact |
|---|---|---|
| Initial format / strict compile | Passed | Baseline logs under `s3-hardening/` |
| Initial full umbrella | Failed; new regressions plus3 CLI failures | `baseline-tests-retry.log` |
| Latest full core/S3 | Core372/0/1 tagged test excluded; S3281/0 | `core-s3-after-gateway-copy.log` |
| Latest strict test compilation | Passed | `integrity-final-strict.log` |
| Latest HTTPClient / part-copy / authorization | Core20/0; S310/0 | `integrity-final-http-seed1.log` |
| Race replay seed2 / seed3 | S39/0 each | `authorization-seed2.log`, `authorization-seed3.log` |
| Real upstream DELETE plus Concord recovery |11unit/0,2HTTP/0; caller process killed and distinct recovery process | `delete-reconciliation-http.log` |
| Independent signed auth HTTP |5/0 | `authorization-http.log` |
| Tagged real cluster |1test/0failures | `cluster-three-voter.log` |
| Independent cluster IAM | Six IAM groups pass on both endpoints; later bucket race fails HTTP framing | `cluster-iam-hardening.log` |
| Independent32-writer cluster | Failed1x200,13x412,18x500; shared task-supervisor saturation | `cluster-phase1-independent.log` |
| Admission regression / cancellation |9tests/0failures after repair; real cluster rerun pending | `cluster-admission-green.log` |
| Signed reads / common errors / transport | Core21/0 and S36/0 | `read-errors-http-v3.log` |
| Fenced bucket config / common error contracts | Core12/0 and S357/0 | `config-error-focused-pass.log` |
| Real RustFS gateway | Partial groups pass; DELETE403 classification repaired; real socket timeout exposed500, rerun pending | `gateway-rustfs-run1.log`, `gateway-rustfs-run3.log` |

## Current checkpoint 2026-10-18

The table above preserves earlier failures and repairs; newer evidence supersedes its unresolved retry notes:

- `phase1-cluster-v3.log` exit0: real three-voter32PUT/32MPU races, independent signatures/IAM and generation races.
- `gateway-rustfs-run7-results.json`:16/16 real upstream groups, retained mc24/0,60s inactivity and full-BEAM reconciliation restarts. Run6 failed range checksum verification; shared206 omission repaired with19/0 HTTP regression and strict test/prod compiles.
- Integrity focused core39/0+S366/0; root version/list/attributes core29/0+S315/0.
- Latest tag/legacy scoped run core4/0+S323/1; stale historical fixture repaired but not yet rerun.
- `phase2-umbrella-current.log` still running: core421/4/1excluded, cluster39/0/1excluded. Saved repairs address guard-incompatible multipart fixture and invalid legacy dummy hashes contaminating migration tests. CLI loopback eaddrinuse failures under investigation. No full umbrella pass.
- New independent P2 standalone/cluster/client probes saved but unexecuted. Phase3 measurement/recovery/release gates remain pending.

## Selected clients and environment

Linux x86_64, Elixir1.18.5/OTP28. AWS CLI2.34.24. Isolated Boto3/botocore1.43.114 (`e2e/hardening-requirements.txt`); mc RELEASE.2025-08-13T08-35-41Z SHA verified against existing workflow. RustFS1.0.1 image available. Client installation/version execution is not interoperability evidence.

## Open verification boundaries

Full umbrella rerun, RustFS gateway fault/restart, all required cluster/security profiles, complete mandatory hosted APIs, actual100k listing resource measurements,64MiB/1GiB transfers at1/4 concurrency, crash/maintenance, independent standalone/cluster restore and production release/container smoke remain mandatory. No unexecuted result is passed. See the machine-readable [acceptance ledger](../plans/s3-hardening-results.json) and [execution checkpoint](../plans/s3-hardening-status.md).

## Upgrade and rollback

No release or migration is authorized/executed. Current work introduces bucket generations and durable gateway intents without deleting legacy storage data. Final upgrade/backfill/rollback instructions require the remaining implementation and independent restore evidence and are not yet approved as an operational procedure.

## Latest independent P2 completion

`phase2-standalone-r7.log` exit0 and `phase2-cluster-r2.log` exit0 now establish complete selected SDK/client profiles in standalone and both cluster endpoints, including actual retained-root restart and stable multipart completion. Cluster includes forced missing-local object and part sources with verified peer bytes, unchanged locations and strictRF2/W2. Gateway run7 remains16/16 plus mc24/0. Deterministic multipart unknown-outcome/finalization assertions and full umbrella consolidation remain. These results do not establish Phase3 resource, maintenance, independent restore or production release/container gates.

## Current continuation checkpoint — 2026-10-18

Goal remains incomplete. Authoritative detailed progress is in
`docs/plans/s3-hardening-status.md`; per-row machine-readable states are in
`docs/plans/s3-hardening-results.json`. No mandatory scenario has been promoted
from partial evidence to complete acceptance.

Independent P2 standalone r7 and cluster r2 finished exit0, including current
Boto3 1.43.114, AWSCLI2.34.24 and mc RELEASE.2025-08-13T08-35-41Z profiles,
retained-root restart, conditional races, and forced remote source proof.
Real RustFS1.0.1 gateway run7 retained16/16 groups plus mc24/0. These precede
current P3 index integration and require regression consolidation.

Final pre-index umbrella: core422/0/1excluded, transport39/0/1excluded,
CLI3488/0, S3348/5, Web21/0, overall exit2. Never-enabled write header repairs
passed focused contracts. Three accumulated request timeouts were not waived.
Current focused transactional index concurrency scope19/0; backup plus bounded
version-deletion scope18/0. These do not replace complete suites.

A genuine dependency blocker is filed at
https://github.com/gsmlg-dev/concord/issues/94. Published VSR3.0.6 repeats an
entire committed-prefix traversal with linear list lookups on protocol
transitions. Actual32completion test still fails its original30s deadline;
replica stack samples consistently identify that loop. Pure unchanged-transition
cost grows from~40us at100entries to~116ms at10000entries. Released3.1.1
retains the same loop. No dependency patches, assertion disabling, snapshot
pressure workaround, quorum changes or deadline inflation were applied.
A13/A27/A28/A29/A38 affected gates remain blocked pending upstream release and
verified dependency update; independent remaining work continues.

First actual resource run used the predeclared fixed envelopes. Completed64MiB
c1 local/gateway and slow client groups passed bounds; c4 local PUT also passed.
The run then failed because fast c4 GET completed between telemetry samples;
no A30pass is claimed. Sampler tightened to25ms and fast workloads repeated for
at least350ms before rerun. Full1GiB/c1/c4, upstream pacing, cluster transfer,
cancellation/disk/crash acceptance remain mandatory.

Restore contracts passed; first actual restore attempt failed unsupported
bucket-policy fixture, replaced with actual ESS IAM policy/attachment checks.
Second attempt reached verified backup boundary but namespace destination
creation failed under read-only root. Mount-order repair awaits actual rerun.
Neither standalone nor clean-cluster restore has passed. All roots are isolated
disposable paths; no production operation or publication occurred.

## Resumed independent verification checkpoint

`phase3-independent-contracts-r3.log` passed85/0 (45.1s), including cache/index/legacy/operations/metrics. `phase3-backfill-migration-r4.log` passed27/0 including latest actual CAS/RF evidence and original migration contracts.

Restore r3 established original-root namespace isolation but failed native metadata snapshot verification. Native two-VM reproduction confirms Concord#95: unchanged backup valid in creator VM and invalid in fresh VM. A36/A37 now explicitly blocked; no checksum bypass/dependency patch.

Resource r2 failed HTTP503 at concurrent gateway PUT after twelve64MiB groups; completed group peaks remained inside the declared envelope. Reproducing BusyCache contract was RED7/1; optional cache-admission repair now passes in r3 scope. The full64MiB/1GiB c1/c4 rerun remains mandatory. Concord#94 still blocks scale/consolidation; no acceptance row promoted.

## Whole-process crash checkpoint

`phase3-crashes-r5.log` exit0: eight actual System.halt137/retained-root restarts at stage, after_stage, sync, rename, directory_sync, after_blob_commit, metadata_commit and response. Every restart verified earlier acknowledged explicit-version bytes. Prepublication objects remained absent with exact absent/pending intent state; response-loss retained exactly one version with correct SHA256 and committed intent. Initial r1–r4 harness failures remain recorded, no production repair or passed claim from them. A32 replication fault portion remains mandatory.

`phase3-resource-r3` exit1 after18 completed64MiB groups; normal concurrent gatewayPUT now passes, paced-upstream c4PUT returned503. Completed resource measurements meet unchanged envelopes. New test-only sanitized function-return trace will capture the next underlying error; no retry/deadline/threshold workaround applied. Full64MiB/1GiB profile remains unpassed.

## Resource and maintenance follow-up

Focused actual upstream resource r4 exit0: all8 paced PUT/coldGET combinations64MiB/1GiB at c1/c4 passed predeclared limits; samples and result retained. No traced WriteThrough failure occurred, so intermittent resource-r3 cause remains unconfirmed. Full resource profile rerun remains required.

Deterministic WriteThrough contention RED8/1 (`phase3-write-contention-red.log`): four valid unrelated bucket updates exhausted4localCASattempts after one upstream mutation. Budget raised to8 to account for publication plus cache bucket revisions; exact snapshots/fences retained and no upstream replay. Broader `phase3-fault-maintenance-contracts-r1.log` core130/4,S37/0: contention repair passes, four GC/Packer reachability failures under investigation (may be uncreated-bucket fixtures; no conclusion yet). Production strict compile r5 passed.

## Fault-driven correctness checkpoint

Core fault/maintenance GREEN `phase3-fault-maintenance-contracts-r2.log`:131/0, including boundedCASrepair, stage/sync/rename, operationintents, durablejobs, repair/scrub, GC/packing and replication contracts. PriorfourGC/Packer failures were fixtures ignoring publication errors against nonexistent buckets; repaired fixtures now assert actualv2publication. Independent newPackerRED21/1 proved activev2MPUpart was packed; narrowlegacy+v2part exclusion repaired and included in131/0.

A31r1 actual signed local/gateway upload disconnect passed, then upstreamtruncation failed defaultSDKerror parsing. Meaningful HTTPRED `phase3-gateway-error-headers-red-r2.log`11/2 confirms staleobject Content-Length/checksum headers on503XML; productionrepair pending. No SDKchecksum bypass or broadexception swallow. Resource source now also exercisesCOPY,MPU,UploadPartCopy and gateway-sourceCOPY at64MiB/1GiBc1/c4 with unchanged envelope; these added groups remain unexecuted.

Gateway errorheader GREEN `phase3-gateway-error-headers-green.log`16/0 covers actual truncated upstream HTTP, GET/HEAD contaminated payload headers, range/checksum/read semantics and late stream closure. ErrorXML now removes staleobject payload/framing/checksum/user-metadata headers; marker/version/Last-Modified/request diagnostics and503mapping retained. Strictprodcompile r6 passed. A31r2 real fault rerun running; source/positivechecks do not replace actualfaultacceptance.

## Actual ENOSPC repair and complete connection-fault lane

`phase3-local-cas-io-red.log` failed2/2: IO.binwrite raised :einval rather than returning a file error. Actual r6 ENOSPC left2zero-byte stages. Narrow filesystem adapter repair uses :file.write; `phase3-local-cas-io-green.log`34/0 and `phase3-prod-strict-r7.log` exit0.

`phase3-a31-r7.log` and `phase3-a31-r7/results.json` exit0:8actualfaultgroups, including true8MiBtmpfs ENOSPC and recovery. Signed local/gateway disconnect, upstream truncation, downstream cancellation and retained-root restart preserve old complete bytes. SDK uses normal checksum/error parsing. All8snapshots return staging0;311continuous samples peakqueue1,total1 against128/2048. Cold-range/open-reader eviction remain independently checked by focused HTTP contracts; full row audit pending. No source checksum/quorum/authentication check disabled.

Resilience r1 successfully exercised maintenance, strict data outage/recovery and leader election, but final all-voter rejoin controller crashed on an unavailable-RPC tuple. Rejoin is unverified; no complete A33/A34/A35 claim. Corrected harness and replication-boundary extension remain in progress.

Current aggregate/proxy/error-redaction contracts: `phase3-aggregate-contracts-r2.log`7/0. Local release r1 failed stale compiled build identity; r2 failed stale consolidated Jason encoder table. Runner now explicitly recompiles the intended identity and reconsolidates protocols without disabling validation. Release rerun pending; no release/container pass claimed.

Production artifact r5 exited0: production assets, strictcompile, localOTP release, cleanDockerbuild and actualcontainer /health+/health/ready+RPCidentity allpass. Image sha256:4ae98ff87aaef1c46caec674db5e2d1808b8488d1d44f022bb0a323583ebc0dd; versions0.6.9, source3477bc540c3732dba3d47a7e08779783f9be25fc, refv0.6.9, buildtime2026-10-17T21:01:32Z. Smokecontainer removed; localimage retained, no publish/deploy. Evidence `phase3-release-r5.log`, `phase3-release-r5/container-smoke.json`. Earlier buildfailures retained. Normaldevelopment compile identity being restored before further acceptance runtimes.

## Real replication-boundary crash checkpoint

`phase3-replication-crash-r1.log` exit0: actual authenticated peer durable-ack then origin System.halt137, surviving public bytes/uncertain absence plus pendingintent/actualpeerhash, retainedrootrejoinbootstrapfalse/all3VSRequality, resume sameinternalid with exactly1version/committedintent and2actualhashverifiedreadygeneration1locations underRF2/W2. Threegroupspass; local8boundarycrashes remain independentlypassed. Root `/tmp/ess-s3-hardening-cluster-bFJJeIvF` retained, ownednodesstopped. No productionrepair needed. This closes replication faultportion evidence, fullA32 audit stillapplies.

Resilience r2 advanced through repaired all3rejoin; then stalenodegeneration writes safelyrejected but returned500 instead of expected503. Source classification regression underinvestigation; do notcallA33passed.

## Complete fresh cluster resilience checkpoint

Stalegeneration HTTPRED1/1 (`phase3-stale-generation-red.log`) computed actualcoordinator invalidreplicaack then realBanditHTTP500. Shared nowmaps that existingfailure to503 only; RF/W/generationvalidation unchanged. CorrectedGREENscope15/0 (`phase3-stale-generation-green-r2.log`); priorgreenlog11/0 includes typo unmatchedfile warning and is notfullscope. Prodstrict r9passed.

`phase3-resilience-r3.log`+`phase3-resilience-r3-result.json` exit0,7actions: durable scrub/corruption, boundedrepair, expiredlease takeover+stalerenew/complete/mutation rejection,3GCsweepspernode retaining versions/shared/parts/readers/unknownintents andpackerguard, strict outage/new+overwrite+part+completeinvisibility, restartactualCAShashesoneachnode, primarylossview0to1/data-aprimary andall3rejoincommit=applied65, stalegenerationregistered2/actual1 yields503. Sourceguardsthreevoters/RF2/W2/nondegradedretained. Root `/tmp/ess-s3-hardening-cluster-T9SnMcNg` retained, ownedservicesstopped. Full A33/A34/A35 auditpending.

Expanded actual resource r5 nowrunning, sourcefingerprint frozen. No completeA30pass untilallgroups+finalfingerprint/cleanupcomplete. Earlierimage r5 predatesoneatomHTTPmapping, so finalimage rebuild/smoke required.

Resource r5 exited1 after17completedgroups, eachinsideunchangedenvelope. Actual64MiBc4CreateMultipartUploadreturned500 beforelocalMPUmeasurement completed. Sourcefingerprintfreeze liftedafterownedruntimecleanup. Authorizationinvestigates metadata/indexinitcontention withREDrequired; no clientretry/limitinflation. Fullresourceprofile remainsFAILED, notpassed. Additionalmandatorylarge actualreplica/repair/externalreplication evidence identifiedbyP3.2audit; source-onlyextension underway.

## Actual concurrent-initiation and response-bound repairs

Resource r5 actual64MiBc4 CreateMultipartUpload500 reproduced in `phase3-mpu-init-contention-red.log`:4synchronizedcalls oneok/threeconflict. Initiation nowbounded32replans onlyondefiniteCASfailure, sameID/initialgeneration/attrs, currentbucketguard/indexrebuilt; unknownerrorsnotreplayed.

Actualexternalmutation responseRED22/1 andinternalreplicaACKRED11/1:1GiBadvertisedresponse caused read_allclosed error instead ofusingstatus/authenticatedheaders; these pathscouldmaterialize arbitrarybodies. Both nowstopresponsebodyandretainsmallstatus/headers, genericHTTPClientbufferedAPIunchanged. GREEN `phase3-mpu-response-green-r1.log`:core43/0,transport11/0,S39/0 includingMPU/newcontention/oldMPUcommit/HTTPprofiles+responsecontracts. Prodstrict r10passed. Taggedactualthreevoter r1passed1/0. Sourcefreeze currentlylifted; newmissinglargeclustertransferandmaintenancecoverage underway. Freshindependentstandaloneprofile r1running.

Fresh integratedstandalone r1 exited1 at32MPU race:one200,twentyfour412,seven503 (`phase3-standalone-r1.log`), so noA13/wholeprofilepass. FreshPUT racepriorassertionpassed. EarlierConcord#94stack/costreproducer remainsblockerdiagnosis;currentlogalone doesnotreprovethestack. Runnernowcontinues independentprofileprogramsafterafailure andreturnsnonzero; noassertiondeleted/disabled.

New20groupactualclusterresourcefirstdiagnosticrunning (`phase3-cluster-resource-r1.log`), allsourcefinalverificationpending; largeRFPUT/GET/bothrepairdirections/externalworker pathswithpernode25mstelemetry+realhashes. Earlierresource r5 remainsfailed; repairedinitfocused63/0doesnotreplaceactualfullresource.

Clusterresource r1 reapedexit1 after14groups (all10at64MiBc1/c4 +1GiBc1strictPUT/peerGET/bothrepairdirections), allcompletegroupsinsidefixedbounds, pernodeCAShashesverified/stagefiles0. Actualpaced1GiBexternalworker failed :request_timeout at30s (`/tmp/ess-s3-hardening-cluster-BzS6Kl3O/cluster-resource-control.log`). Workerupload nowuses existinggatewaypolicy: totalinfinity, connect30s/readwriteinactivity60s, responseack/idleInfinity; no pace/envelopechange. Realrerunrequired. Sourcefingerprint diagnosticrun changedbynewacceptancetests, so nofinalprofilepass.

ExternalStatusRED3/3 missingmodule; newboundedfixedcardinalitysnapshotandmetrics GREEN24/0 withMPU/pack (`phase3-maintenance-metrics-green-r1.log`). Pack/liveFD plusworkerresponse regression31/0 (`phase3-pack-replication-green-r1.log`). Bootstrapr2fixturefailed synchronousstart assumption; actualVSRlog rejects nonemptybootstrapasrequired; testbeingcorrected to assertreadiness/writefence before falsebootstraprestart. Prodstrict r11passed. Newresilience8group r4running; externalbackloglanequeued. Aggregate nowrequires secondclusterresource andbacklogcommandswithinexisting11profiles; CIincludesdockerignorechanges.

Newresilience r4 exited0all8groups (`phase3-resilience-r4.log`/resultJSON), including actualpublicreadrepairwithbothplannersanddispatcherspausedon2guardeddata-nodes:rangeGETdoesnotpublish, fullGETnormalfinalizerrestoreslocalCAShash+2readygen1locationswithunchangedversion/head. Root `/tmp/ess-s3-hardening-cluster-auSmNwzP`, ownednodesstopped. Aggregatecontracts r5failedstaleexpectedsinglecommandaftermandatorysecondsteps; updatedcontractnowchecksnewrequirements, r6GREEN9/0. Externalbacklogr1running(session30086), sourcehashfullfinalresourcesstillpending.

Externalbacklogr1 EXIT0 (`phase3-replication-backlog-r1/results.json`):actualindependentpeeroutage produceddurablepending2/oldestage378ms,4originaljobsretainedthroughownedBEAMSIGKILL/restart,twojobsreclaimedwithnewfence/attemptcount,normaldispatcherdrainedoriginal4jobswithoutnewerstaleremoteoverwrite;currentremotesha256verifiedanddeletedkeyabsent, completebacklogzero. Root `/tmp/ess-hardening-backlog-phase3-replication-backlog-r1`retained, ownedservicesstopped. No processormock/requestoverride/replicaretrybudgetincrease.
