# Embedded and Active-Active Storage Architecture

## Status and scope

Phases 0 through 6 establish atomic metadata, durable streaming local blobs,
embeddable supervision, a fixed three-voter Concord metadata cluster, and a
private authenticated streaming blob transport plus quorum-coordinated object
writes. The default remains the standalone local storage service. Cluster
public writes require an explicit two-flag activation.

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

## Phase 4 membership and discovery

Concord 3 uses fixed, ordered membership. Every voter must receive the same
three `{id, endpoint}` entries and cluster name. `ESS_NODE_ID` is a stable
storage identity independent of a transient process, PID, or connection. Each
voter has its own `ESS_METADATA_ROOT`; Concord's file storage persists and
validates the replica identity, group, and membership configuration there.

Discovery and membership are deliberately separate. Static discovery retries
connections to explicit Erlang node seeds. DNS discovery uses the existing
`dns_cluster` dependency. Neither path adds or removes voters from Concord.
Dynamic membership and reconfiguration are outside Phase 4.

A new, entirely empty three-voter cluster requires
`ESS_CLUSTER_BOOTSTRAP=true` on all voters. Once any voter has durable VSR
state, all restarts use `ESS_CLUSTER_BOOTSTRAP=false`; bootstrapping non-empty
storage is rejected. Readiness is based on `Concord.status/1`, whose
quorum-confirmed read barrier proves a primary and majority are available.

The tagged `:peer` acceptance harness uses three independent VSR directories
and proves cross-node strong reads, minority write rejection, majority writes
with one voter unavailable, catch-up, and restart with the same stable voter
identity. It is excluded from the ordinary unit suite and must be selected
with `--include cluster`.

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
It is still an O(N) full-store operation. Durable background work instead uses
bounded `Concord.KV.list/1` prefix/range pages through the metadata backend.
Concord 3.0.0 currently ignores the documented snapshot revision for these
pages; `gsmlg-dev/concord#55` tracks that upstream bug. Phase 8 therefore treats
pages as a live view and relies on per-job transaction comparisons for
correctness. Compatibility scans retain deterministic key ordering, and
request-time placement reads fixed node keys directly.

## Durability policy

The strict cluster target is replication factor 2 and write quorum 2
(RF=2/W=2), with degraded writes disabled. A write becomes visible only after
two selected data nodes durably store and verify the blob and the metadata
transaction commits. Standalone mode retains RF=1/W=1.

Public object writes remain disabled by default. Phase 6 can open them
explicitly after persistent node registration, deterministic placement,
validated replica acknowledgements, strict quorum enforcement, and atomic
version/head/blob/location/outbox publication. Phase 7 adds remote fallback
reads and request-path read-repair staging. Phase 8 adds durable leased work and
eventual external S3 replication; external copies never satisfy RF/W. Repair
execution and grace-period orphan collection remain Phase 9 work. RF=2/W=2
ensures a successful object or multipart part is already local to both data/API
nodes in the fixed target topology.

## Phase 5 private blob transport

The `ex_storage_service_cluster` umbrella app is an adapter over the core
content-hash interfaces. Its dependency points toward `ex_storage_service`;
the core app refers only to its own transport behaviour and a configured module
atom, so the dependency graph remains acyclic.

Cluster data nodes expose only these private endpoints:

```text
PUT  /internal/v1/blobs/:sha256
HEAD /internal/v1/blobs/:sha256
GET  /internal/v1/blobs/:sha256
```

Requests use a shared-secret HMAC over the method, path, timestamp, request ID,
content hash, and declared size. Verification has bounded timestamp skew,
constant-time signature comparison, and replay rejection within the skew
window. Blob uploads and downloads stream with bounded memory; the receiver
publishes a blob only after validating size and SHA-256, syncing it, and using
the local CAS atomic rename. A repeated upload of identical content is safe.

The excluded large-stream acceptance harness sends a synthetic 2 GiB body
through the real Req/Bandit HTTP path and records the peak memory of both the
upload enumerator and request receiver. Run it explicitly with:

```sh
PAGER=cat mix test apps/ex_storage_service_cluster/test/ex_storage_service_cluster/large_stream_test.exs --include large_stream
```

`ESS_INTERNAL_BIND` and `ESS_INTERNAL_PORT` select the listener, while each
data node publishes its peer-reachable `ESS_INTERNAL_ADVERTISED_URL`. Listener
enablement is derived from `ESS_MODE=cluster` and `ESS_NODE_ROLE=data`; it is
false for standalone and metadata-only nodes. The metadata-only voter therefore
continues to expose no HTTP listener and store no object bytes.

The internal port belongs on a private network and must never be exposed to the
public Internet. TLS or mTLS is the preferred deployment boundary. Direct TLS
termination is configured only when both `ESS_INTERNAL_TLS_CERTFILE` and
`ESS_INTERNAL_TLS_KEYFILE` are present. Every production cluster node must set
the same high-entropy `ESS_INTERNAL_SECRET` containing at least 32 bytes; errors
and configuration inspection do not reveal its value.

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

## Phase 6 placement and quorum commits

Cluster nodes persist stable identity, generation, role, internal endpoint,
enabled/draining state, and optional zone/capacity metadata. These records are
written on startup or explicit control changes, not as a VSR heartbeat.
Rendezvous hashing scores the final SHA-256 against eligible stable data-node
IDs, so process restarts and transient connectivity do not remap placement.

The write coordinator streams a request once into local staging, probes
selected replicas for verified deduplication, transfers missing content with
bounded concurrency, and validates hash, size, node ID, and node generation on
every acknowledgement. Strict mode publishes no metadata below W. Explicit
degraded mode still requires a durable replica and atomically records achieved
durability plus repair intents for every missing desired replica.

Confirmed blob locations, the immutable object version, object head, blob
descriptor, and operation/repair envelope share one Concord transaction.
Pre-commit replicas are retained as grace-protected orphans after an ambiguous
or failed metadata transaction. Multipart parts use the same durability path;
Phase 6 requires acknowledgements from the full part RF even if object writes
use a lower W, so parts uploaded through alternating API nodes can be completed
on either fixed data node without sticky routing.

## Phase 7 remote reads and request-path repair

Object HEAD/version metadata and blob locations are read with strong
consistency. A data node first checks the local CAS and expected size, trusting
current-generation ready-location checksum evidence so file sources retain the
`send_file` fast path. Suspect, stale-generation, or untracked compatibility
content is checksum-verified before use. When a local copy is absent or
corrupt, the read coordinator marks that location unavailable or suspect and
creates a pending repair event in the same Concord transaction. Transient
remote timeouts do not poison a location record.

Remote ready locations are joined to the current node generation and tried in
a deterministic, bounded order. The private transport performs an eager,
checksum-verifying HEAD and fetches one bounded prefix before the S3 response
starts, then streams the remaining exact full or Range GET lazily. This
source-start handshake lets a zero-byte GET failure fall through to another
ready replica and lets all such failures return `ServiceUnavailable` (503)
before headers are committed. Object metadata still exists, so this path never
returns `NoSuchKey`.

Remote stream producers thread the response adapter state explicitly. A client
disconnect halts the upstream HTTP/1 request. Once any response bytes have been
sent, the request is not restarted from another replica because that could
splice or duplicate the object body.

Full-object remote reads may tee one bounded transport chunk at a time into a
staged local file when the local node remains an eligible desired placement
and capacity policy permits. After the client stream finishes, the instance
replica task supervisor verifies the staged size/hash, publishes it through
the normal CAS commit, and conditionally marks the location ready. Range reads,
incomplete streams, checksum mismatches, and disconnected clients never
publish partial repairs.

## Phase 8 durable outbox and eventual disaster recovery

Object and multipart metadata transactions retain their operation record under
`ess:v2:outbox:<operation_id>`. Required cross-cluster PUT/DELETE and degraded
durability repair events are included before that transaction commits, rather
than being appended by a post-commit process. The schema also accepts scrub
follow-up and cleanup events. Materialization atomically creates one immutable
identity under `ess:v2:job:<event_id>` and marks the source event dispatched.
Repeating either step returns the same visible job.

Each job is at-least-once. A worker reads the current record, then atomically
compares its revision and either the pending retry time or an expired running
lease. A successful claim records the stable owner node, owner generation,
lease deadline, and a monotonically increasing fencing token. Renewal,
completion, and failure compare all of those ownership fields, so a stale
worker cannot publish state after takeover. Ambiguous transaction timeouts are
resolved by idempotency key and the persisted job value.

Data-role instances start one dispatcher and a bounded task supervisor.
Metadata-role instances start neither and cannot claim blob work. External S3
PUT requests stream from the object service's selected local, packed, or remote
source in bounded chunks; workers do not assume that the executing node owns
the blob. PUT handlers compare the pinned source version with the current head.
DELETE handlers reconcile the destination to the current head, including an
older version revealed by explicit version deletion. Delayed duplicate work
therefore cannot remove or resurrect newer data.

Only `cross_cluster_put` and `cross_cluster_delete` handlers are enabled in this
phase. Repair, scrub, and cleanup jobs remain durable and pending until Phase 9
adds topology-aware planners and handlers. The former periodic replication
full scan is not supervised because it could independently recreate stale
work; safe planning must use bounded pages and durable event identities.
Cross-cluster replication is eventual external disaster recovery, never part of
the single-datacenter RF/W acknowledgement path.

## Activation guards

Configuration validates `1 <= write_quorum <= replication_factor`, stable
identity, exactly three unique voter IDs/endpoints, discovery inputs, the local
voter/member match, and typed internal bind, port, URL, TLS pair, and auth-skew
settings. Cluster metadata nodes reject all data-plane and public listeners.
Cluster data nodes may enable the private quorum data plane without a public
listener, but public S3 requires `ESS_CLUSTER_DATA_PLANE_ENABLED=true`. Both
flags default to false in cluster mode, so activation is deliberate;
`ObjectService` also checks the data-plane flag independently of listener
configuration. The admin listener remains disabled in cluster mode. Cloud-cache
PUT is rejected in cluster mode until that backend is converted to the
streaming atomic quorum path.
