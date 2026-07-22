# Embedded and Active-Active Storage Architecture

## Status and scope

Phase 0 records the target architecture and adds configuration guards. The
only supported behavior in this branch is the existing standalone local
storage service. Concord clustering, public multi-node writes, an internal
transport, and blob replication remain disabled.

The cluster design is scoped to one datacenter or low-latency availability
zones. Cross-region operation uses separate clusters and asynchronous
replication or disaster recovery.

## API and metadata semantics

The target API tier is active-active: either data node may accept an S3
request. Metadata is not multi-primary. Concord 3 uses Viewstamped Replication
with one primary to serialize metadata changes and reject writes on a metadata
minority.

The architectural shorthand "single-leader Raft metadata" means this
single-writer replicated-log property; the locked implementation is VSR with a
primary, not the Raft protocol.

Metadata and object bytes use separate planes:

- Concord/VSR stores object heads, immutable versions, blob descriptors,
  locations, and durable work records.
- Filesystem-backed content-addressed storage stores object bytes by SHA-256.
- A metadata commit may reference only checksum-verified durable blob
  locations.

The minimum production topology is:

```text
node-a: data + public API + Concord replica + local CAS
node-b: data + public API + Concord replica + local CAS
node-c: metadata-only Concord replica
```

The metadata-only replica stores the full VSR metadata log, stores no object
bytes, and exposes no public S3 endpoint.

## Locked Concord capability record

Phase 0 was verified against Concord `3.0.0` in `mix.lock` and the
checked-out source under `deps/concord`, not against online documentation.
`Concord.Txn.commit/2` accepts a map with `compare`, `success`, and `failure`
lists and returns `{:ok, %Concord.Txn.Result{succeeded: boolean, revision:
revision, responses: responses}}`. A failed comparison runs the failure branch
and returns `succeeded: false`; it is not a transaction error.

The supported compare fields are `exists`, `value`, nested `field`, `version`,
`create_revision`, `mod_revision`, `lease`, and `ttl`. The supported operators
are `==`, `!=`, `>`, `>=`, `<`, and `<=`. Transaction branches support key or
bounded prefix/range reads, puts, deletes by key/prefix/range selector, and TTL
touches. All mutations selected by one transaction share one committed
revision.

`Concord.Txn.commit/2` accepts `idempotency_key:` and `timeout:` options. The
stable release caches and replays the original transaction result for an exact
same-key retry and rejects a conflicting request using the same key. Object
commits retain the `ess:v2:outbox:<operation_id>` record in the same transaction
as the durable application-level result and future dispatch schema; this also
provides resolution beyond Concord's bounded idempotency-result retention.

Read compatibility names `eventual`, `leader`, and `strong` all use the same
linearizable VSR query barrier in this release. `Concord.prefix_scan/2` scans
the authoritative replicated state and no longer uses the unsafe external ETS
lookup reported in `gsmlg-dev/concord#27`, so the prior crash class is fixed.
It is still an O(N) full-store operation and does not provide the pagination
and deterministic ordering required here. The metadata backend retains
`get_all/1` plus local prefix filtering and sorting for this phase.

## Durability policy

The strict cluster target is replication factor 2 and write quorum 2
(RF=2/W=2), with degraded writes disabled. A write becomes visible only after
two selected data nodes durably store and verify the blob and the metadata
transaction commits. Standalone mode retains RF=1/W=1.

Cluster writes remain disabled in this branch because atomic metadata alone
does not provide blob durability. The internal authenticated transport,
replica acknowledgements, placement, quorum coordinator, remote reads, repair,
and orphan cleanup must exist before public multi-node writes are safe.

## Metadata schema and rollback boundary

Schema v2 uses encoded bucket and object-key components, immutable object
versions, and one mutable object head. It does not contain a mutable version
list. Reads try v2 first and fall back to v1, and startup performs no
destructive migration.

`ESS_METADATA_SCHEMA=v2` is the default write decision. Operators should take
a metadata backup before enabling a new binary against important existing
state. After the first v2 write, an old binary cannot see the new record and
must not be restarted against that state. Rolling downgrade is therefore not
supported; rollback requires restoring the pre-v2 metadata backup.

`ESS_METADATA_SCHEMA=v1` is available only as a deliberate read-only
compatibility choice before v2 writes. Object metadata mutations are rejected
in that mode instead of using the unsafe legacy multi-write sequence. A future
explicit migration tool must preserve version IDs and establish the required
blob replica count before active-active traffic is enabled.

## Activation guards

Configuration validates `1 <= write_quorum <= replication_factor`. Cluster
mode cannot expose the public S3 writer while
`ESS_CLUSTER_DATA_PLANE_ENABLED=false`. The flag is a guard, not a data-plane
implementation, and setting it in this branch does not enable Concord
discovery, remote blob transfer, or clustered writes.
