# PRD: ExStorageService — S3-Compatible Object Storage Server

## Overview

ExStorageService is a standalone S3-compatible object storage server built with Elixir/OTP. The S3 API layer runs on Plug/Bandit (no Phoenix overhead for REST/XML). A separate Phoenix app provides the admin web UI for bucket management, replication status, and monitoring.

**Key insight**: Disk-level redundancy (hardware RAID1) handles drive failure. ExStorageService handles server-level failure via async replication between independent nodes. No distributed cluster, no consensus protocol, no hash ring — each node is a fully independent S3 server that optionally replicates buckets to configured peers.

## Goals

- Drop-in S3-compatible API for self-hosted infrastructure
- Simple single-binary deployment per node
- Optional cross-region bucket replication for disaster recovery
- Production-grade performance with zero-copy reads and streaming writes
- Minimal operational complexity — no cluster coordination, no quorum, no rebalancing

## Non-Goals

- Erasure coding or data sharding across nodes
- Distributed cluster with automatic failover / membership protocol
- S3 Select, Lambda triggers, or advanced S3 features
- Multi-tenancy with isolated namespaces (v1 is single-tenant)
- Serving as a CDN or edge cache

## Architecture

### Single Node

```
┌──────────────────────────────────────────┐
│              ExStorageService Node                │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │    Plug/Bandit (S3 REST API)     │    │
│  │    AWS Signature V4 Auth         │    │
│  │    Port 9000                     │    │
│  └──────────┬───────────────────────┘    │
│             │                            │
│  ┌──────────▼───────────────────────┐    │
│  │       Storage Engine             │    │
│  │  content-addressable local disk  │    │
│  │  zero-copy sendfile for reads    │    │
│  └──────────┬───────────────────────┘    │
│             │                            │
│  ┌──────────▼───────────────────────┐    │
│  │       Metadata Store (Concord)   │    │
│  │  embedded Raft KV via Ra         │    │
│  │  object key → path, etag, size   │    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │     Replication Engine            │    │
│  │  async PUT/DELETE to peers       │    │
│  │  job queue backed by Concord     │    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌──────────────────────────────────┐    │
│  │    Phoenix Admin UI (LiveView)   │    │
│  │    bucket mgmt, replication      │    │
│  │    status, metrics dashboard     │    │
│  │    Port 4000                     │    │
│  └──────────────────────────────────┘    │
└──────────────────────────────────────────┘
```

### Cross-Region Replication

```
   Region A                          Region B
┌──────────────┐    async S3 API   ┌──────────────┐
│  ExStorageService A   │ ───────────────►  │  ExStorageService B   │
│  (primary)   │                   │  (replica)   │
│              │  ◄───────────────  │              │
│  RAID1 disk  │    anti-entropy   │  RAID1 disk  │
└──────────────┘                   └──────────────┘
        │                                  │
    DNS / LB failover (external)
```

Each node is fully independent. Replication is bucket-scoped, async, and uses the S3 API itself — ExStorageService replicates to peers by being an S3 client to sibling nodes.

## S3 API Scope

### V1 — Core Operations

| Operation | Method | Path |
|-----------|--------|------|
| ListBuckets | GET | `/` |
| CreateBucket | PUT | `/{bucket}` |
| DeleteBucket | DELETE | `/{bucket}` |
| HeadBucket | HEAD | `/{bucket}` |
| ListObjectsV2 | GET | `/{bucket}?list-type=2` |
| GetObject | GET | `/{bucket}/{key}` |
| HeadObject | HEAD | `/{bucket}/{key}` |
| PutObject | PUT | `/{bucket}/{key}` |
| DeleteObject | DELETE | `/{bucket}/{key}` |
| DeleteObjects | POST | `/{bucket}?delete` (multi-delete) |
| CopyObject | PUT | `/{bucket}/{key}` (x-amz-copy-source) |

### V1 — Multipart Upload

| Operation | Method | Path |
|-----------|--------|------|
| CreateMultipartUpload | POST | `/{bucket}/{key}?uploads` |
| UploadPart | PUT | `/{bucket}/{key}?partNumber=N&uploadId=X` |
| CompleteMultipartUpload | POST | `/{bucket}/{key}?uploadId=X` |
| AbortMultipartUpload | DELETE | `/{bucket}/{key}?uploadId=X` |
| ListParts | GET | `/{bucket}/{key}?uploadId=X` |

### V2 — Future

- Pre-signed URLs
- Bucket versioning
- Lifecycle policies (expiration, transition)
- Bucket notifications (webhooks)
- CORS configuration

## Authentication & Authorization

### Authentication — AWS Signature V4

- Parse `Authorization` header (AWS4-HMAC-SHA256 scheme)
- Reconstruct canonical request from method, path, query, headers
- Compute HMAC-SHA256 signing key chain: date → region → service → signing
- Compare computed signature against provided signature
- Support both single-chunk and chunked transfer encoding (`aws-chunked`)
- Access key in the signature identifies the user → load their policies for authorization

### Identity Model

**Users** — managed by root admin via Phoenix UI.

```
"user:{user_id}" → %{
  id: "usr_01J...",
  name: "deploy-bot",
  status: :active | :suspended,
  created_at: ~U[...],
  updated_at: ~U[...]
}
```

**Access Keys** — N keys per user. Each key independently activatable/deactivatable for rotation.

```
"access_key:{access_key_id}" → %{
  access_key_id: "AKIA...",
  secret_access_key: "wJalr...",       # stored hashed? see note below
  user_id: "usr_01J...",
  status: :active | :inactive,
  created_at: ~U[...],
  last_used_at: ~U[...],
  expires_at: nil | ~U[...]            # optional TTL keys
}
```

**Secret key storage**: SigV4 requires the raw secret to compute the signing key — it cannot be hashed like a password. The secret must be stored in a recoverable form. Options:
- Plaintext in Concord (simplest, acceptable if Concord's data dir has restricted file permissions)
- Encrypted at rest with a node-level master key (better, adds `:crypto.crypto_one_time` encrypt/decrypt)

Recommendation: encrypted at rest with a master key from env var or file. Same pattern as Phoenix secret_key_base.

**Root Admin** — bootstrapped on first run or via config. Has implicit full access, bypasses policy evaluation. Authenticates to Phoenix admin UI via session (separate from S3 access keys).

```
# config/runtime.exs
config :ex_storage_service, :root_admin,
  username: System.fetch_env!("ESS_ADMIN_USER"),
  password_hash: System.fetch_env!("ESS_ADMIN_PASSWORD_HASH")
```

### Authorization — IAM-Style Policies

Each user has zero or more policies attached. A policy is a list of statements. Evaluation follows AWS semantics: **default deny → explicit allow → explicit deny wins**.

#### Policy Structure

```
"policy:{policy_id}" → %{
  id: "pol_01J...",
  name: "read-only-assets",
  statements: [
    %{
      effect: :allow | :deny,
      actions: ["s3:GetObject", "s3:ListBucket"],
      resources: ["arn:ess:::assets/*", "arn:ess:::assets"]
    }
  ],
  created_at: ~U[...]
}
```

**User ↔ Policy binding:**

```
"user_policies:{user_id}" → ["pol_01J...", "pol_02K..."]
```

#### Supported Actions

Mapped 1:1 to S3 API operations:

| Action | S3 Operation |
|--------|-------------|
| `s3:ListAllMyBuckets` | ListBuckets |
| `s3:CreateBucket` | CreateBucket |
| `s3:DeleteBucket` | DeleteBucket |
| `s3:HeadBucket` | HeadBucket |
| `s3:ListBucket` | ListObjectsV2 |
| `s3:GetObject` | GetObject, HeadObject |
| `s3:PutObject` | PutObject, CopyObject |
| `s3:DeleteObject` | DeleteObject, DeleteObjects |
| `s3:ListMultipartUploadParts` | ListParts |
| `s3:AbortMultipartUpload` | AbortMultipartUpload |
| `s3:*` | Wildcard — all actions |

#### Resource ARN Format

```
arn:ess:::{bucket}              # bucket-level (ListBucket, CreateBucket, etc.)
arn:ess:::{bucket}/{key}        # object-level (GetObject, PutObject, etc.)
arn:ess:::{bucket}/*            # all objects in bucket
arn:ess:::{bucket}/prefix/*     # prefix scoped
arn:ess:::*                     # all buckets and objects
```

Simplified from AWS (no account ID or region — single-tenant, single-region per node).

#### Evaluation Algorithm

```
1. If requester is root admin → ALLOW (bypass)
2. Collect all policies attached to user
3. Collect all statements from all policies
4. Filter statements where action matches requested action
5. Filter statements where resource matches requested resource (glob match)
6. If any matching statement has effect: :deny → DENY
7. If any matching statement has effect: :allow → ALLOW
8. Otherwise → DENY (default deny)
```

Glob matching: `*` matches any sequence within a path segment, `/*` at end matches all descendants. Standard S3 resource matching rules.

#### Predefined Policy Templates

Managed via admin UI, stored in Concord. Common templates created on bootstrap:

| Template | Actions | Resources |
|----------|---------|-----------|
| `ReadOnly` | `s3:GetObject`, `s3:ListBucket`, `s3:HeadBucket`, `s3:ListAllMyBuckets` | `arn:ess:::*` |
| `ReadWrite` | ReadOnly + `s3:PutObject`, `s3:DeleteObject` + multipart actions | `arn:ess:::*` |
| `FullAccess` | `s3:*` | `arn:ess:::*` |
| `BucketScoped({bucket})` | `s3:*` | `arn:ess:::{bucket}`, `arn:ess:::{bucket}/*` |

Admin creates custom policies for fine-grained control. Templates are just convenience — not special.

### Request Flow

```
S3 Request
  │
  ▼
Parse SigV4 Authorization header
  │
  ▼
Lookup access_key in Concord → get user_id
  │ (reject if key inactive/expired/not found)
  ▼
Verify signature (recompute HMAC chain with stored secret)
  │ (reject if mismatch)
  ▼
Map HTTP method + path → S3 action + resource ARN
  │
  ▼
Load user's policies from Concord
  │
  ▼
Evaluate policies against (action, resource)
  │
  ├─ DENY  → 403 AccessDenied XML response
  └─ ALLOW → proceed to storage engine
```

### Admin UI Screens (Phoenix LiveView)

- **Users list** — name, status, key count, attached policies
- **User detail** — manage access keys (create, activate, deactivate, delete), attach/detach policies
- **Access key creation** — generates key pair, shows secret once (never again)
- **Policies list** — name, statement count, attached user count
- **Policy editor** — visual statement builder (effect, actions checkboxes, resource input with glob preview)
- **Audit log** — key creation/deletion, policy changes, user status changes (stored in Concord)

## Storage Engine

### Object Layout on Disk

```
{data_root}/
├── {bucket_name}/
│   ├── objects/
│   │   ├── ab/
│   │   │   └── cdef1234...  # content-addressable by SHA-256
│   │   └── ff/
│   │       └── 98ba7654...
│   └── multipart/
│       └── {upload_id}/
│           ├── part.00001
│           ├── part.00002
│           └── ...
```

### Content Addressing

- Object content stored by SHA-256 hash of content → enables deduplication
- Metadata DB maps `{key, version}` → `{content_hash, size, content_type, etag, custom_metadata, created_at}`
- ETag = MD5 of content (S3 compatibility) for single uploads, MD5-of-MD5s for multipart
- Deletes remove metadata entry; content GC'd when no references remain

### Read Path

1. Lookup key in Concord metadata
2. Resolve content path from hash
3. `Plug.Conn.send_file` / `:file.sendfile` for zero-copy transfer
4. Support `Range` header for partial reads

### Write Path

1. Stream request body to temp file (never buffer full object in memory)
2. Compute SHA-256 and MD5 during streaming (single pass)
3. Atomic rename to content-addressed path
4. Insert/update metadata in Concord
5. Enqueue replication job (if bucket has replicas configured)

### Multipart Upload

- State machine: `initiated → uploading → completing → completed | aborted`
- Parts stored in `multipart/{upload_id}/part.{part_number}`
- CompleteMultipartUpload: concatenate parts → compute final hash → move to objects/ → clean up parts
- AbortMultipartUpload: delete part files + metadata
- Background job to GC stale incomplete uploads (configurable timeout, default 24h)

## Metadata Store

Concord (gsmlg-dev/concord) — the team's own embedded Raft-based KV store built on Ra. Single-node Raft is effectively a durable write-ahead log with snapshot recovery.

### Key Schema

Concord is a KV store, so keys are structured strings and values are Elixir terms (serialized via built-in JSON or `:erlang.term_to_binary`).

**Bucket registry:**

```
"bucket:{name}" → %{
  created_at: ~U[...],
  replicas: [...],
  settings: %{}
}
```

**Object metadata:**

```
"obj:{bucket}:{key}" → %{
  content_hash: "sha256:abcdef...",
  size: 1048576,
  etag: "\"d41d8cd98f00b204e9800998ecf8427e\"",
  content_type: "image/png",
  metadata: %{"x-amz-meta-author" => "..."},
  created_at: ~U[...],
  updated_at: ~U[...]
}
```

**Multipart uploads:**

```
"mpu:{bucket}:{upload_id}" → %{
  key: "path/to/object",
  status: :initiated,
  created_at: ~U[...]
}

"mpu_part:{bucket}:{upload_id}:{part_number}" → %{
  size: 5242880,
  etag: "\"...\""
}
```

**IAM — Users:**

```
"user:{user_id}" → %{
  id: "usr_01J...",
  name: "deploy-bot",
  status: :active | :suspended,
  created_at: ~U[...],
  updated_at: ~U[...]
}
```

**IAM — Access Keys:**

```
"access_key:{access_key_id}" → %{
  access_key_id: "AKIA...",
  secret_access_key_enc: <<encrypted>>,   # encrypted with master key
  user_id: "usr_01J...",
  status: :active | :inactive,
  created_at: ~U[...],
  last_used_at: ~U[...],
  expires_at: nil | ~U[...]
}
```

**IAM — Policies:**

```
"policy:{policy_id}" → %{
  id: "pol_01J...",
  name: "read-only-assets",
  statements: [
    %{effect: :allow, actions: ["s3:GetObject", "s3:ListBucket"],
      resources: ["arn:ess:::assets/*", "arn:ess:::assets"]}
  ],
  created_at: ~U[...]
}
```

**IAM — User ↔ Policy binding:**

```
"user_policies:{user_id}" → ["pol_01J...", "pol_02K..."]
```

**Audit log:**

```
"audit:{timestamp}:{event_id}" → %{
  actor: "usr_01J..." | :root,
  action: :create_user | :create_key | :attach_policy | ...,
  target: "usr_...",
  details: %{},
  timestamp: ~U[...]
}
```

### Listing / Prefix Queries

S3's ListObjectsV2 requires prefix-based listing. Concord has `Concord.Query.keys(prefix:)` and `Concord.Query.where(prefix:)` today — functionally correct but not performant at scale. Current implementation calls `Concord.get_all()` then filters with `String.starts_with?/2` — a full table scan regardless of result size.

**Acceptable for MVP**: works correctly with < ~50K keys per bucket. Beyond that, Concord needs optimization.

**Concord roadmap items for production-grade prefix scan**:

1. Switch ETS table from `:set` to `:ordered_set` — enables range semantics
2. Use `:ets.select/2` with match specs — filter at ETS level, not in Elixir
3. Cursor/streaming support — avoid loading all matches into memory at once
4. `Concord.list_prefix/2` with `limit` and `continuation_token` parameters — maps directly to ListObjectsV2 pagination

### Concord Scale Constraints

Concord's snapshot mechanism (Erlang `term_to_binary` of full ETS table) has known scaling characteristics:

| Metric (1M keys × 1KB avg) | Value |
|---|---|
| ETS memory | ~1.2–1.5 GB |
| Snapshot file on disk | ~200–300 MB (compressed) |
| Snapshot creation time | ~500–1000 ms |
| ETS rebuild on restore | ~250ms–4s |
| Snapshot interval | Every 1,000 commands (hardcoded) |

**Critical gaps**:

- **No incremental snapshots** — Ra sends the full blob; follower blocks until complete
- **Blocking rebuild** — `snapshot_installed/4` does sequential ETS inserts synchronously
- **Production data dir defaults to `/tmp`** — must be overridden or all Raft state is lost on reboot
- **No adaptive snapshot interval** — at high write rates, snapshots fire too frequently

**Mitigation strategy for ExStorageService**:

1. **Capacity plan**: estimate max object count per deployment. At < 100K objects (typical self-hosted), Concord is comfortable (~150 MB ETS, ~30 MB snapshots)
2. **Configure data dir**: override Concord's Ra data dir to persistent storage on startup
3. **Snapshot interval tuning**: expose as ExStorageService config, recommend higher values for write-heavy workloads
4. **Future**: if ExStorageService deployments outgrow Concord's single-snapshot model, migrate metadata to ETS `:ordered_set` with on-disk WAL replay (essentially evolving Concord itself)

**Bottom line**: Concord works for MVP and typical self-hosted deployments (tens of thousands of objects). Million-object scale requires Concord improvements that are tracked separately.

### Why Concord, Not SQLite or Mnesia

- **Dogfooding** — validates Concord in a real production workload, drives its roadmap
- **Zero external deps** — embedded, ships with the app (like SQLite but native Elixir/OTP)
- **Raft durability** — write-ahead log + snapshots, crash-safe
- **Future optionality** — if ExStorageService ever needs multi-node metadata consensus, Concord already speaks Raft
- **Forcing function** — ExStorageService's requirements (prefix scan, scale, persistent data dir) directly improve Concord for all users

### Concord Dependency Policy

ExStorageService treats Concord as a critical upstream dependency. When ExStorageService development encounters a missing Concord feature, a bug, or a performance limitation that blocks progress:

1. **Pause ExStorageService work** on the affected area immediately — do not work around it in ExStorageService
2. **File an issue on gsmlg-dev/concord** with reproduction steps, expected behavior, and ExStorageService context
3. **Switch to Concord** and fix the issue there first
4. **Resume ExStorageService** only after the fix is merged and released in Concord

This policy exists because:
- Workarounds in ExStorageService mask real Concord deficiencies and create tech debt
- Concord improvements benefit all downstream projects, not just ExStorageService
- ExStorageService is Concord's primary proving ground — bugs found here are high-signal

Known Concord items required by ExStorageService (file issues when work reaches these):
- Efficient prefix scan (`:ordered_set` + `:ets.select/2` + cursor pagination)
- Configurable data directory (not `/tmp`)
- Configurable snapshot interval
- Snapshot scalability validation at target object counts

## Replication

### Bucket Configuration

Managed via Phoenix admin UI, stored in Concord. Replication targets are per-bucket, configured with S3 credentials for the peer:

```elixir
# Stored in Concord as "bucket:critical-data"
%{
  created_at: ~U[...],
  replicas: [
    %{
      endpoint: "https://s3.region-b.example.com",
      access_key: "REPLICA_KEY",
      secret_key_enc: <<encrypted>>,
      bucket: "critical-data"     # can map to different bucket name on peer
    }
  ],
  settings: %{}
}
```

### Replication Mechanism

**Event-driven** (primary): After each successful PUT/DELETE, enqueue a replication job (persisted in Concord):

```elixir
# Job queue worker
defmodule ExStorageService.Workers.Replicate do
  def perform(%{op: :put, bucket: b, key: k, replica: r}) do
    # Read object from local storage
    # PUT to replica endpoint via Req + SigV4
  end

  def perform(%{op: :delete, bucket: b, key: k, replica: r}) do
    # DELETE on replica endpoint
  end
end
```

**Anti-entropy** (secondary): Periodic job per replicated bucket (scheduled via `:timer` or custom cron in job queue):

1. List all objects with ETags from local metadata
2. List all objects with ETags from replica (ListObjectsV2)
3. Diff → replicate missing/changed objects, delete orphans on replica
4. Frequency: configurable, default every 6 hours

### Replication Guarantees

- **Async** — writes return success after local persistence, before replication
- **Eventual consistency** — replica may lag behind primary
- **At-least-once delivery** — job queue retries failed replication jobs with exponential backoff
- **No conflict resolution in v1** — primary is authoritative. Replica is read-only backup. Bidirectional sync is a v2 concern.

### Failover

ExStorageService does not manage failover. Failover is external:

- DNS failover (Route53, Cloudflare)
- Load balancer health checks
- Manual DNS switch

When promoting a replica to primary, operator reconfigures replication direction. ExStorageService provides a `/health` endpoint for health check integration.

## Configuration

```elixir
config :ex_storage_service,
  # Storage
  data_root: "/var/lib/exstorageservice/data",

  # S3 API Server
  s3_port: 9000,
  s3_host: "0.0.0.0",

  # Admin UI
  admin_port: 4000,

  # Root Admin (bootstrapped, manages users/keys/policies via UI)
  root_admin_user: System.fetch_env!("ESS_ADMIN_USER"),
  root_admin_password_hash: System.fetch_env!("ESS_ADMIN_PASSWORD_HASH"),

  # Secret key encryption (for access key secrets at rest)
  master_key: System.fetch_env!("ESS_MASTER_KEY"),

  # Multipart
  multipart_gc_interval: :timer.hours(24),
  multipart_max_age: :timer.hours(48),

  # Anti-entropy
  sync_interval: :timer.hours(6),

  # Limits
  max_object_size: 5 * 1024 * 1024 * 1024,  # 5 GiB
  max_part_size: 5 * 1024 * 1024 * 1024,
  min_part_size: 5 * 1024 * 1024              # 5 MiB
```

## Supervision Tree

```
ExStorageService.Application
├── Concord (Raft KV — metadata store)
├── ExStorageService.JobQueue (replication + GC job persistence)
│   ├── queue: :replication (concurrency: 10)
│   ├── queue: :sync (concurrency: 2)
│   └── queue: :gc (concurrency: 1)
├── ExStorageService.StorageEngine (file operations, content addressing)
├── Bandit.child_spec(plug: ExStorageService.S3.Router, port: 9000)
└── Phoenix.Endpoint (ExStorageService.AdminWeb.Endpoint, port: 4000)
    ├── LiveView — bucket management
    ├── LiveView — replication status
    └── LiveView — metrics dashboard
```

Note on job queue: Since we're avoiding PostgreSQL, Oban is not viable. Options:
- **Custom GenServer + Concord** — persist jobs in Concord, GenServer polls/dispatches. Simple, zero-dep.
- **Oban Lite** — if/when SQLite-backed Oban ships
- Recommended: custom job queue backed by Concord. The replication workload is simple (PUT/DELETE fan-out + periodic sync), and doesn't need Oban's full feature set.

## Tech Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| S3 HTTP Server | Bandit + Plug.Router | Pure Elixir, HTTP/2, lightweight for REST/XML |
| Admin UI | Phoenix + LiveView | Real-time dashboard, bucket management |
| Metadata | Concord (gsmlg-dev/concord) | Team's own embedded Raft KV, zero external deps |
| Background Jobs | Custom queue on Concord | No PostgreSQL dependency, sufficient for replication workload |
| S3 Client (replication) | Req + manual signing | Minimal deps, reuse our own SigV4 implementation |
| Hashing | :crypto (stdlib) | SHA-256 + MD5, no external dep |
| XML | xmerl (stdlib) | S3 responses are XML |
| JSON | JSON (Elixir 1.18 stdlib) | Admin API responses, Concord value serialization |

## Performance Targets

| Metric | Target |
|--------|--------|
| GET latency (1 MB object, local) | < 5ms (sendfile) |
| PUT latency (1 MB object, local) | < 20ms |
| ListObjectsV2 (1000 keys) | < 50ms |
| Concurrent connections | 10,000+ (Bandit) |
| Max object size | 5 GiB (via multipart) |
| Replication lag (event-driven) | < 5s typical |

## Development Phases

### Phase 1 — Core S3 Server (MVP)

- Plug.Router with S3 path routing
- AWS Signature V4 authentication (single root key for bootstrap)
- Bucket CRUD (create, delete, head, list)
- Object CRUD (get, put, delete, head, copy)
- ListObjectsV2 with prefix/delimiter/pagination
- Content-addressable storage engine
- Concord metadata store (validate prefix scan capability)
- XML response serialization
- `/health` endpoint
- Integration tests against `aws` CLI and `ExAws`

### Phase 2 — IAM

- User CRUD in Concord (create, suspend, delete)
- Access key management (generate, activate, deactivate, delete, N per user)
- Secret key encryption at rest with master key
- Policy engine — statement model, action/resource matching, glob evaluation
- Policy evaluation middleware in Plug pipeline (default deny → allow → deny wins)
- Predefined policy templates (ReadOnly, ReadWrite, FullAccess, BucketScoped)
- Audit log for all IAM mutations

### Phase 3 — Multipart Upload

- CreateMultipartUpload / UploadPart / Complete / Abort / ListParts
- Part concatenation and final hash computation
- Stale upload GC worker

### Phase 4 — Admin UI

- Phoenix LiveView admin app (port 4000)
- Root admin session auth (separate from S3 access keys)
- User management (create, suspend, view keys, attach policies)
- Access key creation (show secret once)
- Policy editor (visual statement builder)
- Bucket management (create, delete, configure replicas)
- Object browser (list, preview, delete)
- System status dashboard (disk usage, object counts)
- Audit log viewer

### Phase 5 — Replication

- Bucket replication configuration (via admin UI)
- Custom job queue backed by Concord
- Event-driven replication worker
- Anti-entropy sync job
- Replication status dashboard in admin UI

### Phase 6 — Hardening

- `Range` header support (partial reads)
- `If-None-Match` / `If-Modified-Since` conditional requests
- Content deduplication GC (unreferenced content cleanup)
- Rate limiting (per access key)
- Request logging / metrics (Telemetry)
- Prometheus metrics endpoint

### Phase 7 — Extended Features

- Pre-signed URLs (requires policy evaluation at URL generation time)
- Bucket versioning
- Lifecycle policies
- Bucket notifications (webhooks)

## Testing Strategy

- **Unit**: SigV4 signing, policy evaluation (allow/deny/default-deny edge cases), resource glob matching, content addressing, XML serialization
- **Integration**: Full HTTP request/response against running server using `aws` CLI
- **IAM**: Verify access denied for unauthorized actions, key rotation without service interruption, policy combination logic, root admin bypass
- **Compatibility**: Test with real S3 clients — ExAws, boto3, aws-sdk-js, mc (MinIO client)
- **Replication**: Multi-node docker-compose setup, verify sync after writes and after simulated failure
- **Property**: Content integrity (write → read roundtrip), policy evaluation determinism

## Resolved Decisions

1. **Bucket auto-creation**: Explicit `CreateBucket` required. `PutObject` to a non-existent bucket returns `404 NoSuchBucket`. No implicit creation.
2. **Project name**: ExStorageService

## Known Risks

1. **Concord prefix scan performance** — works but O(N) over all keys. Acceptable at < 50K objects, needs Concord optimization for larger deployments. Tracked as Concord roadmap.
2. **Concord snapshot scalability** — full-state snapshots limit practical ceiling to ~100K objects comfortably. Million-object scale requires incremental snapshots (Ra limitation) and adaptive snapshot intervals.
3. **Concord data dir** — defaults to `/tmp`. Must be explicitly configured to persistent storage. Deployment docs must make this prominent.