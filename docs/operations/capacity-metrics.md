# Capacity and transfer diagnostics

Run the read-only capacity RPC on a release:

```sh
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.Capacity.snapshot(), pretty: true)'
```

The command probes the configured blob, temporary and metadata filesystems with
`df -Pk` and a five-second `timeout` deadline per root. Linux release images need
their existing `df` and `timeout` utilities (BusyBox works). It creates no files,
walks no object directories and makes no metadata mutations. Missing tools,
missing roots, failures and deadlines return `complete: false`. Treat incomplete
capacity observations as unknown. `storage_capacity_observation_complete{root}`
marks sample validity; prior byte gauges remain visible after a failed sample.
The command never turns a failure into healthy zero capacity. Capacity alone
is not readiness or proof that a write can reach its quorum.

`transfer_active{kind}` and `transfers_total{kind,result}` cover S3 object handler
work, local staging and upstream client calls. Every transfer has one lightweight
owner monitor; chunk progress never accumulates messages in that monitor. Normal
completion, returned errors, exceptions and untrappable owner exits close the
activity observation. S3 request counters classify ordinary returned 4xx/5xx
responses as errors. `s3_response_bytes_total{source}` counts bytes accepted by the
response adapter, not proof that the remote client persisted the body. Interrupted
stream delivery has a separate error counter and marks the handler result failed.

`storage_staging_bytes_written_total` counts successfully written chunks.
`storage_staging_tracked_bytes` and `storage_staging_tracked_files` observe at most
1024 paths created since metrics setup. Scrapes only traverse this capped ETS
observation, never disk. Confirmed disappearance after commit/discard releases a
path. Owner death retains known bytes and marks
`storage_staging_tracking_complete_since_setup` false; it never deletes a staging
file or claims cleanup. Overflow also marks coverage incomplete. These metrics
are not a filesystem inventory: restart leftovers and out-of-band filesystem
changes require the existing operation-intent-aware recovery/cleanup procedures.
Do not delete a stage because its writer exited or because a gauge looks stale.

`gateway_upstream_requests_total{operation,result}` distinguishes success,
not-found, denial, precondition, unavailability, HTTP errors, integrity and
transport errors. `gateway_cache_operations_total{operation,result}` records
validated descriptor hits/misses and cache publication outcomes. A cache hit is
counted only after opening the body descriptor. `s3_errors_total{category}`
separates integrity, precondition and unsupported operations from other errors.
`durable_job_transitions_total{kind,execution,transition}` keeps handler execution
separate from acceptance of a fenced durable completion/retry. Successful handler
execution with a stale lease is not a successful durable transition.

All metric labels come from finite allowlists; arbitrary values collapse to
`other`. Object keys, bucket names, URLs, node/job/request identifiers, exception
text and credentials are never used as metric labels. Liveness and readiness
remain the separate `/health` and `/health/ready` contracts. Listing/index,
repair/scrub/GC and replication backlog/age metrics retain their existing
semantics and bounded diagnostics.

## Unknown durable operations

Unknown upstream or metadata outcomes are separate from unsupported S3 selectors.
Inspect one known operation ID with release RPC (replace the ID):

```sh
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.OperationStatus.get(:hosted, "operation-id"))'
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.OperationStatus.get(:gateway_write, "operation-id"))'
/app/bin/ess rpc 'IO.inspect(ExStorageService.Operations.OperationStatus.get(:gateway_delete, "operation-id"))'
```

Each call performs exactly one linearizable metadata key read. It never lists
metadata or scans from a metrics scrape; summaries expose only operation ID and
state. Retain the known ID from the failed operation for lookup after restart.
This is not global discovery or a backlog inventory. Concord 3.1.2 applies one
native read budget to primary discovery, quorum-confirmed reads and redirects,
returning structured availability errors. Exact operation lookup still uses one
key read; the explicit replication-backlog diagnostic supplies paginated discovery.

Hosted states are `pending`, `unknown`, `committed` and `aborted`. Gateway write
states are `prepared`, `unknown` and `local_committed`; local commit can still
await cache/journal cleanup. An ambiguous gateway DELETE deliberately remains
`prepared`. Neither a prepared state nor an absent journal proves a mutation
failed upstream or authorizes repeating it. The diagnostic does not contact
upstreams, mutate state, weaken GC protection or replace fenced reconciliation.

`operation_status_reads_total{source,result,state}` counts exact-read observations,
including repeats; it is not a current backlog gauge. Read failures and invalid
records are explicit errors. `uncertain_operation_outcomes_total{source,outcome}`
counts returned unknown or reconciliation-required outcomes at the existing hosted
and gateway failure boundaries. It does not claim a corresponding state write
succeeded, count every crash, or measure distinct currently unresolved operations.
Every label is from a fixed allowlist; operation IDs and errors are excluded.
