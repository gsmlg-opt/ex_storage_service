# Concord update and current ESS acceptance

Concord, viewstamped_replication and ex_turso are installed and locked at **3.1.3**.
The core constraint is `~> 3.1.3`; the HTTP family remains **0.21.13**. The actual
`mix deps.update concord viewstamped_replication ex_turso` command completed with
exit0; only those three lock entries changed during this update. See
[dependency-update.json](dependency-update.json). Existing database command format
still defaults to0. Empty restore targets require explicit format1 on every voter
through recovery, restarts and activation; no existing database format was switched.

Start/end Git SHA: `3477bc540c3732dba3d47a7e08779783f9be25fc` on `main`, with the
preserved uncommitted implementation identified by source fingerprint
`60ada0faafb210257f799f275f6709be0830dd4168b27e093ca2572eb6eac781`.
No branch, commit, push, publication, deployment or production data operation was made.

## Implemented and accepted behavior

- Phase1, A01–A18: shared operation resolution, source/destination and multipart
  authorization, upload/checksum and aws-chunked validation, atomic conditions and
  bucket lifecycle fencing, truthful gateway reconciliation and bounded control/error
  handling. Main changed boundaries are S3 Operation/Auth/Authorize/Integrity/handlers,
  IAM policies, ObjectCommit/MultipartCommit, ObjectService and gateway journals.
- Phase2, A19–A27: versions and ordered pagination, multipart/copy/parts, attributes
  and actual encoded bytes, tagging, location/CORS and independent default-checksum
  clients. See the [support matrix](../../../s3-compatibility.md).
- Phase3 has implemented bounded index/transfer paths, maintenance/crash/resilience,
  independent backup/restore, metrics/readiness and operator/release tooling.
  A29 and A31–A37 are accepted. Complete A28/A30 scale/resource proof and A38 aggregate
  acceptance remain missing; implementation alone does not complete this phase.

The current-source [row review](row-promotion-r1/review.json) accepts **35/38** rows.
[Business verification](business-summary.json) passed complete standalone,
RustFS gateway and ordered three-voter cluster profiles, including retained
SDK/admin/persistence/mc checks, both data endpoints and real primary
election/rejoin under strict RF2/W2. Auth, checksums and cluster guards are retained.

[Correctness](correctness-summary.json): **4496 umbrella tests, zero failures**,
plus24 Python regressions, formatting, strict test/prod compilation and diff check.
Two tagged defaults are exercised separately by the actual three-voter and2GiB lanes;
40 additional resource instrumentation regressions also passed. Actual2GiB/A29
receipts are in [tagged-index-summary.json](tagged-index-summary.json).
[Fault/crash/resilience/standalone-and-cluster restore](recovery-summary.json) and
[production assets/release/container smoke](release.json) passed. Restores use
isolated empty roots with originals inaccessible and preserve activation fencing.
Exact commands, exit statuses, source identity and logs remain in each lane receipt.

Recorded clients: Boto3/botocore1.43.114, AWS CLI2.34.24 and
mc RELEASE.2025-08-13T08-35-41Z. RustFS uses the retained pinned image identity.
Earlier lane receipts do not separately record Elixir/OTP versions. A later terminal
host snapshot reports Elixir1.18.5/OTP28/ERTS16.4.0.5; it is not retroactive lane
instrumentation. The final diff check passed, and the source fingerprint is unchanged.

## Remaining execution limits

**A28:** the original normal-invariant10k/100k index command timed out after
10800.086s, before complete measurement rows. The current3.1.3 bounded prefix
observation confirmed2958 normal puts in170.843s but reached no10k measurement.
Increasing latency is observed; its cost and the initiating old timeout cause remain
unclassified. The release audit found3.1.3 runtime equivalent to3.1.2; Concord#99/#100
remain open/needed and are relevant capabilities, not proven timeout causes.
No supported small ESS retained-state traversal optimization was evidenced.
The [accepted handoff](a28-current-handoff-r1/README.md) preserves original workloads,
bounds and re-entry requirements: an evidenced released-runtime or targeted ESS
improvement, followed by complete original measurements. No raw-KV fixture, fabricated
backup, dependency patch or increased deadline was used.

**A30:** [canonical original resources](canonical-resources-summary-r1.json) exited1.
Local/gateway passed39/56 groups, then group40 gateway-slow-put1GiB/c1 failed100ms
metric coverage (maximum150.506ms). Cluster passed15/20 groups, then group16
strict-quorum-replica-put1GiB/c4 failed data-a temporary coverage (108.734ms).
All88 executed numeric envelope measurements passed; incomplete timing coverage,
20 missing canonical groups and missing readbacks remain unpassed. Independent
canonical cleanup verified285 identities/137groups and exact fixture removal.

A report-only causal attempt48081 failed before group1 because copying its launcher
lost mode0700; EACCES and failed cleanup verdicts remain intact. A separately reviewed
mode-only correction plus fresh paths ran as session11177. Its unchanged resource
workload passed32 groups, failed group33 local-upload-part-copy1GiB/c1 cadence, and
left23 groups unexecuted before trace target40. No relevant trace was retained;
this attempt does not identify a syscall cause or complete A30. Its [terminal summary](a30-local-r3-terminal-summary.json) records33numeric
measurements passing, zero exact raw-replay mismatches and six upstream readback
groups (15objects/45assertions). Raw windows place91.573ms between native memory
probe end and Python RSS-probe start, with no syscall attribution. The original
cleanup gate and independent verifier remain failed because target40attachment
records were never created. The separate
[physical cleanup recheck](a30-local-current-observation-r3/INSTRUMENTATION_CLEANUP.md)
verified the actual observer/tracer identities, all11tracked identities/6groups,
1528tasks,5listener inodes and exact fixture absent; no leak or read error was found.
The unchanged offline analyzer was not executed because its required cleanup and
target40trace prerequisites are unmet. No diagnostic result promotes acceptance.
No unchanged retry, threshold/deadline relaxation or trace-parser bypass is authorized.

**A38:** the actual final [aggregate checkpoint](aggregate-checkpoint-r3.json) exited1:
A28/A30/A38 unpassed, complete index receipt absent and resources incomplete.
All38 acceptance remains false. Final continuation/cleanup state is maintained in
[RESUME.md](RESUME.md) and the [scenario ledger](../../../plans/s3-hardening-results.json).

## Compatibility and recovery boundaries

Follow the [cluster upgrade runbook](../../../operations/cluster-upgrade.md) and
[listing index runbook](../../../operations/listing-index.md): quiesce writers,
retain verified Concord plus blob-root backups, perform bounded resumable index
backfill/cutover and retain legacy records. Rollback after v2 writes requires the
appropriate metadata restore for old binaries. Checksum scheme/legacy validation,
same-OTP-major recovery and explicit post-verification worker activation remain.

The declared exclusions remain full IAM/STS, ACL ownership, Object Lock, SSE/KMS,
website hosting, S3 Select, directory buckets and erasure coding. Supported-mode
restrictions, including cluster gateway writes, packing and lifecycle, remain guarded.
No full AWS parity or production-validation claim is made.

Current terminal checkpoint: [final-checkpoint-r3.json](final-checkpoint-r3.json).
The next A28/A30 work requires the evidenced changes described there; all current
bounded commands and independent reviews are terminal and no runtime remains.
