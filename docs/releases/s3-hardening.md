# v0.6.10 — S3 compatibility and daily-use hardening

Publication is pending completion of the canonical GitHub `release.yml` workflow
and verification of its published artifacts. This patch follows v0.6.9 and
includes the ESS-S3-HARDENING-001 implementation under the user-authorized
daily-use verification scope.

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
5 MiB. See the [daily verification record](../reports/s3-hardening/daily-20261020-normal-r3/SUMMARY.md)
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
