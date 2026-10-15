# Native archive restore

`ilm restore TARGET` maps portable S3 archive restoration and status inspection.
It does not add archive storage, restoration, or remote-tier support to ESS.

## Pinned reference and executable evidence

Reference mc commit `77f82e18b5401a65958f1619df6ebb994634bd88`:
[`cmd/ilm-restore.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/ilm-restore.go)
and [`cmd/client-s3.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/client-s3.go).
Actual pinned executable probes against a loopback S3 peer captured:

| Selection | Reference requests and result |
| --- | --- |
| `--days 3 --vid 'v+1/雪' fixture/bucket/p/a` | Restore POST and HEAD used the same percent-encoded version identifier; XML contained Days 3 and Expedited tier. |
| `--recursive fixture/bucket/p/` | ListObjectsV2, restore POST with Days 1/Expedited, a second list, HEAD; JSON reported one restored object. |
| `--recursive --versions fixture/bucket/p/` | ListObjectVersions, POST for each immutable data version, second version listing, HEAD for each same version; two versions of one key still counted as one restored object. |
| `--versions` without recursive | mc silently ignored versions and restored the current object; ESS rejects this incompatible selection. |
| `--days 0` | Failed before any request. |

The pinned client hardcodes `GlacierJobParameters.Tier=Expedited`; its CLI has no
tier selector. It submits restore requests before waiting for each object's
`x-amz-restore` status. The reference waits without an attempt bound, sleeping
five seconds between pending HEAD responses. The loopback peer returned complete
status immediately; actual archive retrieval on a cloud provider was not tested.
Owned probe HTTP servers were stopped after execution.

## ESS command contract

Accepted flags are `--days N` (default 1), `--recursive`, `--versions`,
`--version-id ID`/`--vid ID`, and the ESS `--dry-run` addition. Days must be a
positive 32-bit integer. The restore document contains only Days and
`GlacierJobParameters.Tier=Expedited`, matching the pinned reference's defaults.
Advanced requests, including Standard/Bulk retrieval, intelligent-tier archive
requests that must omit Days, and other modeled restore fields, use the generic
`s3api restore-object` document interface. No lifecycle tier flag is silently
reinterpreted as a retrieval tier.

A nonrecursive target must name an object. Selected versions retain the same
immutable ID on POST and every HEAD. An explicit version conflicts with recursive
or all-version selection, and `--versions` requires `--recursive`. The source's
boolean lookup typo for its string version flag is not reproduced.

Recursive selection uses the literal target key prefix, without adding a slash;
bucket-only targets select the whole bucket. It completes every listing page
into a private disk-backed manifest before the first restore effect. Version
selection enumerates immutable data versions and skips delete markers. Invalid
keys, missing/invalid version IDs, malformed pagination, or later-page errors
fail inventory before any restore POST. No object body is downloaded or buffered.
Memory use stays bounded while inventory resides on disk.

Restore requests run sequentially, then completion checks reuse the same
manifest. There is no second listing that could change the selection between
submission and polling. Successful output reports `Requested`, `Restored`, and
whether all versions were selected. Counts represent individual restore requests
and completed data versions, unlike mc's key-deduplicated version totals. Missing
or unrecognized `Restore` response headers fail explicitly; they never establish
completion. Provider errors, including `InvalidObjectState`, `InvalidTier`,
`RestoreAlreadyInProgress`, and access failures, remain service errors.

Polling is bounded at **40 HEAD attempts per object/version**, with five seconds
between pending responses (at most 39 delays). Pending status at the last attempt
returns `restore_exhausted`; successful submission alone never reports a restored
object. Cancellation messages interrupt polling delays, and invocation coordinator
cancellation is checked before listing, submission, and polling. Failures stop
further work and report submitted/completed counts in error details; already
accepted requests are not reversible. The manifest is removed on success, error,
or cooperative cancellation. A killed invocation can leave only private temporary
inventory state; it owns no background restore worker.

Dry-run completes and validates selection, reports its count and restore request,
and sends neither restore POST nor status HEAD. Declared ESS providers are rejected
up front because the source-reviewed ESS capability profile lacks a safe restore
route, including in dry-run. Directory buckets are also rejected. Other providers
must implement these S3 operations and permit the requested retrieval tier; the
command does not infer support from a successful generic request. For example,
AWS Deep Archive does not support Expedited retrieval. SSE-C `--enc-c` remains
explicitly unsupported by this convenience path; it is never ignored on polling.

## Validation

Thirteen focused restore tests cover grammar/Management dispatch, actual raw HTTP
POST and HEAD identity/escaping, request XML, complete paginated inventory, data
versions and delete-marker exclusion, later-page failures, pending/completed
status, exactly forty default attempts, SIGTERM cancellation messages,
coordinator cancellation, manifest cleanup, partial submission failures, dry-run,
provider boundaries, and conflicting selections. Together with Management and
lifecycle suites: **58 tests, zero failures**. Strict CLI compilation, scoped
formatting, and diff checks passed. One fixture repair added explicit pagination
completion markers required by the shared paginator; no production workaround
was needed.
