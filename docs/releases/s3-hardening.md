# Unreleased — S3 hardening closeout

Candidate `fcc02e148ae31f00022d563ebce2b039be5d32dd` fixes coherent latest-version
selection and root ListBuckets SDK query dispatch, with deterministic domain,
independently signed HTTP and isolated cluster regressions. Automatic PR/push
correctness now covers the complete server suites and retained CLI contracts.
Concord/VSR/ExTurso are coordinated at 3.1.4; HTTP remains 0.21.13.

The candidate passed all 56 local/gateway and 20 cluster resource groups under
the original sizes, concurrency, 100 ms observation bound and numeric envelopes,
plus fault/crash, resilience, standalone/cluster restore and local container
checks. A28's mandatory 10k/100k listing datasets remain blocked by Concord
[#101](https://github.com/gsmlg-dev/concord/issues/101) and
[#102](https://github.com/gsmlg-dev/concord/issues/102); the full entry point exits 1.
No closeout patch version has been selected or published. Local container smoke
does not constitute public artifact verification. The
[closeout report](../reports/s3-hardening-verification.md) records the exact
source fingerprint, evidence and remaining publication prerequisites.

Existing command-format 0 databases remain unchanged. Empty restore targets
must retain format 1 through every restart and activation. Retain verified
metadata/blob backups before explicit backfill; old-binary rollback after v2
writes requires restoration. Destructive/outbound workers remain disabled until
the established validation and activation boundary. The published v0.6.10 notes
below retain their original daily-use scope.

# v0.6.10 — S3 compatibility and daily-use hardening

Published [v0.6.10](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/v0.6.10)
on 2026-10-20 through the canonical GitHub `release.yml` workflow. Both Hex
packages, all 21 GitHub assets and the server image passed independent public
verification. This patch follows v0.6.9 and includes the ESS-S3-HARDENING-001
implementation under the user-authorized daily-use verification scope. See the
[release verification report](../reports/s3-release-0.6.10.md) for source identity,
checksums, actual artifact execution and cleanup.

## Changes

- Resolve S3 operation identity consistently for routing and authorization.
  Preserve canonical actions, resource policies, copy-source checks and
  fail-closed authorization when metadata cannot be read.
- Verify declared payload hashes and supported checksums before making uploaded
  objects visible. Preserve raw gzip bytes, bodyless HEAD errors and consistent
  S3 error responses.
- Add the hosted versions, listings, multipart, tagging, CORS, location,
  metadata, copy and read operations described in the
  [S3 compatibility profile](../s3-compatibility.md). Gateway and cluster
  exclusions remain documented there.
- Fence conditional object writes and multipart publication through Concord.
  Use transactional bucket listing indexes, retain uncertain operation status,
  and preserve strict replica quorum and durable-job ownership checks.
- Keep transfer streams bounded and provide backup, capacity, replication and
  operation diagnostics. Recover eligible abandoned local staging files while
  retaining protected or uncertain work conservatively.
- Update Concord, Viewstamped Replication and ExTurso together to 3.1.3.
  Preserve the coordinated http_fetch 0.21.13 family.
- Handle a browser launcher cleanup race and report transfer journal socket
  admission failures without weakening journal ownership.

## Verification scope

The user explicitly selected normal daily-use verification for this delivery.
The sequential standalone Boto3 profile passed **14 of 14 groups** with small
fixtures, real SigV4 authentication and default SDK request/response checksums.
It covers bucket/object operations, metadata and tags, ranges, listings, copy,
presigned requests, CORS, multipart and versions. The largest multipart part is
5 MiB. The canonical release also passes 40 daily application tests. See the
[release daily verification record](../reports/s3-release-0.6.10-daily.json)
for the exact source fingerprint, command, results and cleanup evidence.

Scale, stress, high concurrency, extreme/fault/crash and multi-GiB profiles are
outside this delivery's verification scope. The retained historical full matrix
has **35 of 38 scenarios passed** at its recorded source fingerprint; A28
(listing scale), A30 (resource observation) and A38 (aggregate acceptance) remain
unpassed. Those results are preserved in the
[historical verification report](../reports/s3-hardening-verification.md) and
are not current-source certification. No production deployment, production-data
validation or benchmark capacity claim is made.

## Upgrade and recovery

New buckets have ready listing indexes. Existing buckets require the quiesced,
backup-verified [listing index conversion](../operations/listing-index.md)
before writers resume. Legacy records and rollback artifacts are retained;
mixed older writers are unsupported.

Existing metadata databases retain `ESS_CONCORD_COMMAND_VERSION=0` by default.
Do not switch an existing database automatically: changing its command format
requires quiesced writers, a verified backup and Concord's legacy-state
reconciliation. Fresh empty restore targets require
`ESS_CONCORD_COMMAND_VERSION=1` on every voter, retained through recovery,
restarts and activation. Verify backup checksums and restore on the same OTP
major before a runtime upgrade.

Metadata schema v1 remains read-only. Rollback to binaries predating v2 writes
requires a compatible metadata backup restore. Keep recovery keys separate from
backup artifacts and follow the [cluster upgrade runbook](../operations/cluster-upgrade.md)
before activating restored writers or outbound workers.
