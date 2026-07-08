# PRD: Git-Style Data Model for ExStorageService

> **Revision 2 — 2026-07-08.** Updated after a feasibility review against the codebase.
> Changes from the original draft:
>
> 1. All new physical storage lives under a reserved `{data_root}/cas/` root ("objects", "manifests", "tmp", "packs", "gc", "multipart" are all legal S3 bucket names and would collide with bucket directories under `data_root`).
> 2. The `idx:*` listing index is dropped from the MVP. Concord's `prefix_scan` is quarantined (gsmlg-dev/concord#27), so all listing is `get_all/0` + filter anyway; listing scans `ref:{bucket}:` keys directly. Reintroduce `idx:*` only if ordered prefix scans become real.
> 3. Cloud-cache buckets are explicitly out of scope for refs/versions/CAS.
> 4. GC starts with mark-and-sweep + quarantine (extending the proven `ContentGC` approach); blob refcounts are a later optimization, not the foundation.
> 5. `Storage.CAS` is a plain module, not a GenServer.
> 6. Phase 1 must remove the admin web UI's synchronous `Engine.delete_content` call (`bucket_live/files.ex`).
> 7. The no-buffering requirement carves out aws-chunked uploads, which currently require buffering to decode.
> 8. Phase 3 must preserve Range support for completed multipart objects (currently works because parts are concatenated into one blob).
> 9. §17 reflects Concord 2.3.0's actual API; the atomic mixed batch is tracked upstream as gsmlg-dev/concord#36.
> 10. Versioning is documented as mostly unwired today: `Versioning.put_version/3` and `delete_version/3` have no callers; Phase 2 is new behavior, and `ver:*` subsumes (migrates, does not parallel) the existing `obj_ver:*` / `obj_ver_list:*` schema.
> 11a. **Phase 3 implementation note (2026-07-09):** multipart parts are CAS blobs (part records carry the blob hash) and CompleteMultipartUpload streams the concatenation with constant memory into a whole-object CAS blob *and* records a content-addressed manifest (`manifest:sha256:{hash}` record + canonical JSON file under `cas/manifests/`, linked from version metadata via `manifest_hash`). **Serving stays whole-blob** — manifest-streaming GET was deliberately not implemented because chunked transfer-encoding drops `Content-Length` and complicates Range, regressing S3 client compatibility (§12.4). Manifest-based serving is deferred to Phase 6 (packs). Part blobs become unreferenced after completion and are reclaimed by Phase 4 GC.
> 11b. **Phase 4 implementation note (2026-07-09):** `Storage.CasGC` implements the candidate → quarantine → delete pipeline for `cas/objects` with per-stage reachability re-checks, restore-on-rereference for quarantined blobs, dry-run mode, `stats/0` for operator visibility, and audit logging. GC roots: `obj:*`/`obj_ver:*` `content_hash` plus active `mpu_part:*` `hash`. Manifest files/records are never swept. Admin *UI* for GC visibility is a follow-up; the legacy `ContentGC` continues to own the legacy tree until it is deleted.
> 11c. **Phase 5 implementation note (2026-07-09):** replication jobs pin the replicated version at enqueue time (`payload.object`: version_id, content_hash, etag, size, content_type); the worker skips transfer when the destination already holds identical content (key-level HEAD etag+size comparison — targets are generic S3 endpoints, so there is no cross-node blob-hash endpoint) and skips superseded-and-collected versions as stale. Delete/delete-marker replication is body-less (plain DELETE, 404-idempotent). "Transfer manifests before refs" is N/A — multipart objects replicate as their materialized whole blob. Follow-ups: streaming PUT bodies (chunked enumerable bodies corrupt pooled HTTP/1.1 connections with Bandit targets; needs explicit content-length streaming) and SigV4 replica auth (still bearer-token v1).
> 11. **Phase 2 implementation note (2026-07-09):** the existing `obj:{bucket}:{key}` record serves as the mutable ref (it is absent when the latest version is a delete marker), and the existing `obj_ver:*` / `obj_ver_list:*` keys serve as the version store — no `ref:*`/`ver:*` key rename or metadata migration was needed since the schema already matched. `bucket_versioning:{bucket}` is retained as a separate key. Version records gained `object_type` and `parent_version_id` fields. Phase 2 follow-ups (not yet implemented): `ListObjectVersions` API, `HeadObject` with `versionId`, `x-amz-copy-source` with `?versionId=`, and `x-amz-version-id` on the CompleteMultipartUpload response.

## 1. Overview

ExStorageService should evolve its storage layer from simple bucket-local content-addressed files into a Git-inspired data model for S3-compatible object storage.

The goal is not to store S3 data inside a literal Git repository. The goal is to borrow Git's strongest storage ideas:

* Immutable content-addressed blobs
* Mutable refs pointing to immutable content
* Version history as commit-like records
* Manifests for composite objects
* Reachability-based garbage collection
* Pack-style cold storage in a later phase

This gives ExStorageService a strong foundation for deduplication, versioning, replication, repair, auditability, and lifecycle management.

## 2. Goals

### 2.1 Product Goals

* Support S3-compatible object storage with strong internal data integrity.
* Enable automatic deduplication across:

  * repeated uploads
  * object copies
  * object versions
  * buckets
* Add a clean foundation for bucket versioning.
* Make object deletion safe by separating logical deletion from physical garbage collection.
* Support future packfile/archive compaction without changing the S3 API layer.
* Keep the system simple enough for self-hosted single-node deployments.

### 2.2 Engineering Goals

* Keep S3 object keys as mutable logical refs.
* Store object content as immutable global CAS blobs.
* Store metadata and refs in Concord.
* Avoid bucket-local duplication of identical object data.
* Make all write flows crash-safe.
* Make GC deterministic and auditable.
* Preserve current Plug/Bandit S3 API routing.
* Minimize changes to the external API surface.
* Remove the `Storage.Engine` GenServer bottleneck: CAS operations become plain functions; no request-path work serializes through a single process.

## 3. Non-Goals

* Do not embed or depend on a real Git repository.
* Do not implement Git protocol support.
* Do not expose Git concepts through the S3 API.
* Do not implement packfile compaction in the first phase.
* Do not require distributed consensus beyond the existing Concord metadata layer.
* Do not implement erasure coding or hash-ring sharding.
* Do not require PostgreSQL, SQLite, or external databases.
* **Do not apply refs/versions/CAS to cloud-cache buckets.** Buckets with an active cloud-cache config dispatch to `CloudBackend` and never touch local CAS; they keep their existing behavior (upstream S3 + `LocalStore` cache). Designing a hybrid model for them is a separate future effort.

## 4. Core Design

The storage model should use this mapping:

| Git Concept  | ExStorageService Concept                       |
| ------------ | ---------------------------------------------- |
| blob         | immutable object content chunk or whole object |
| tree         | optional manifest / object index               |
| commit       | immutable object version record                |
| ref          | bucket/key pointer to latest version           |
| packfile     | future cold-storage archive                    |
| reachability | GC root traversal from live refs               |

The key principle:

```text
S3 bucket/key = mutable ref
S3 object body = immutable content blob
S3 version = commit-like immutable metadata record
Multipart object = manifest pointing to immutable parts
Delete = ref update or delete marker
GC = remove unreachable blobs after grace period
```

## 5. Current Problem

The current engine stores content by SHA-256, but the physical path is bucket-local:

```text
{data_root}/{bucket}/objects/{hash_prefix}/{hash_rest}
```

This works, but it limits deduplication:

* Same file uploaded to two buckets exists twice.
* `CopyObject` across buckets performs a physical `File.cp!` (`handlers/object.ex`).
* Versioning can accumulate duplicate content.
* GC must reason per bucket instead of globally.
* Replication and repair cannot treat content as globally addressable blobs.

Additional problems confirmed by code review:

* The admin web UI deletes content files synchronously on object delete (`bucket_live/files.ex` calls `Engine.delete_content`). Even today this can delete content still referenced by another key in the same bucket; under global CAS it would delete other buckets' data. It must go in Phase 1.
* `Storage.Engine` is a GenServer whose only state is a static `data_root`; every GET path lookup and every buffered PUT serializes through it.
* Multipart completion reads whole parts into memory and concatenates them into one blob.

The new model moves to global CAS under a **reserved root** (see §6):

```text
{data_root}/cas/objects/sha256/{hash_prefix}/{hash_rest}
```

Then all buckets, keys, and versions reference the same immutable content hash.

## 6. Target Physical Layout

All new storage lives under `{data_root}/cas/`. Rationale: buckets currently live directly under `data_root`, and "objects", "manifests", "tmp", "gc", "multipart", and "packs" are all valid S3 bucket names. A single reserved `cas/` directory avoids collisions during and after migration, and `cas` itself must be rejected as a bucket name by `BucketValidator` (buckets may also move under `{data_root}/buckets/{name}` in a later cleanup, but that is not required for this PRD).

```text
{data_root}/
  {bucket}/...                # legacy bucket-local layout (until migration completes)

  cas/
    objects/
      sha256/
        ab/
          cdef1234...

    manifests/
      sha256/
        12/
          34abcd...

    tmp/
      uploads/
        upload-{unique_id}

    multipart/
      {upload_id}/
        part.00001
        part.00002

    gc/
      quarantine/
        sha256-{hash}

    packs/
      # future phase
      pack-{hash}.pack
      pack-{hash}.idx
```

### 6.1 Blob Path

```text
cas/objects/sha256/{first_two_hex_chars}/{remaining_hex_chars}
```

Example:

```text
cas/objects/sha256/ab/cdef1234567890...
```

### 6.2 Manifest Path

For large or multipart objects:

```text
cas/manifests/sha256/{first_two_hex_chars}/{remaining_hex_chars}
```

A manifest is itself immutable and content-addressed.

### 6.3 Temp and Rename Discipline

`cas/tmp/` is on the same filesystem as `cas/objects/`, so commit remains an atomic `File.rename!` — the same discipline `Engine.put_object_stream/5` + `commit_object/4` use today.

## 7. Concord Metadata Schema

### 7.1 Bucket

```elixir
"bucket:{bucket}" => %{
  name: bucket,
  versioning: :disabled | :enabled | :suspended,
  created_at: iso8601,
  updated_at: iso8601,
  settings: %{}
}
```

The existing separate `"bucket_versioning:{bucket}"` key is folded into the bucket record during Phase 2 migration.

### 7.2 Object Ref

The mutable pointer for the latest visible object state.

```elixir
"ref:{bucket}:{key}" => %{
  bucket: bucket,
  key: key,
  latest_version_id: version_id,
  etag: etag,
  size: size,
  is_delete_marker: boolean,
  updated_at: iso8601
}
```

This is equivalent to a Git ref.

**Implemented as:** the pre-existing `"obj:{bucket}:{key}"` record plays this role (with `version_id` instead of `latest_version_id`, and absence instead of `is_delete_marker: true`) — every existing reader across the three apps already consumes it.

For non-versioned buckets, this ref still exists, but old versions become unreachable and GC-eligible.

**Listing is served from `ref:*` keys directly.** There is no separate `idx:*` record (see §7.6).

### 7.3 Object Version

Immutable commit-like metadata record.

```elixir
"ver:{bucket}:{key}:{version_id}" => %{
  version_id: version_id,
  bucket: bucket,
  key: key,

  object_type: :blob | :manifest,
  root_hash: "sha256:...",
  parent_version_id: previous_version_id | nil,

  size: integer,
  etag: string,
  content_type: string,
  metadata: map,

  is_delete_marker: boolean,

  created_at: iso8601,
  created_by: user_id | nil
}
```

**Implemented as:** the existing `obj_ver:{bucket}:{key}:{version_id}` and `obj_ver_list:{bucket}:{key}` keys serve as this store directly — the schema already matched, so no rename or migration was performed. Version records are stamped with `object_type` (`:blob` today, `:manifest` in Phase 3) and `parent_version_id` on write. Version IDs keep the sortable `{microsecond_timestamp}-{random}` format; the ordered `obj_ver_list` key is retained for newest-first listing.

### 7.4 Blob Metadata

```elixir
"blob:sha256:{hash}" => %{
  hash: "sha256:{hash}",
  size: integer,
  physical_path: "cas/objects/sha256/ab/cdef...",
  state: :active | :quarantined | :deleted,
  created_at: iso8601,
  last_seen_at: iso8601
}
```

Note: no `ref_count` field in the MVP. GC is mark-and-sweep (§14); refcounts may be added later as an optimization with their own consistency story.

### 7.5 Manifest Metadata

```elixir
"manifest:sha256:{hash}" => %{
  hash: "sha256:{hash}",
  parts: [
    %{
      number: 1,
      hash: "sha256:...",
      size: integer,
      etag: string
    }
  ],
  total_size: integer,
  etag: string,
  created_at: iso8601
}
```

### 7.6 Listing (no dedicated index in MVP)

S3 requires efficient prefix listing, but Concord's server-side `prefix_scan/2` is currently quarantined — it intermittently crashes the Ra state machine (WORKAROUND in `Metadata`, tracked as gsmlg-dev/concord#27). All listing today is `Concord.get_all/0` + in-Elixir filtering, acceptable for < 50K keys.

A dedicated `idx:*` record therefore adds no listing performance while doubling the records to keep in sync on every PUT/DELETE. Decision:

* `ListObjectsV2` scans `ref:{bucket}:` keys (same `get_all/0` + filter mechanics as today's `obj:` scan).
* Refs with `is_delete_marker: true` are excluded from normal listings.
* If/when concord#27 is fixed and ordered prefix scans are reliable, reintroduce `idx:*` (or switch listing to `prefix_scan`) as a focused follow-up.

### 7.7 Multipart Upload

```elixir
"mpu:{bucket}:{upload_id}" => %{
  upload_id: upload_id,
  bucket: bucket,
  key: key,
  status: :initiated | :uploading | :completing | :completed | :aborted,
  created_at: iso8601,
  metadata: map
}
```

```elixir
"mpu_part:{bucket}:{upload_id}:{part_number}" => %{
  part_number: integer,
  hash: "sha256:...",
  size: integer,
  etag: string,
  created_at: iso8601
}
```

### 7.8 GC Candidate

```elixir
"gc:candidate:{hash}" => %{
  hash: "sha256:...",
  reason: :unreachable | :orphan_detected,
  first_seen_at: iso8601,
  eligible_after: iso8601
}
```

### 7.9 Replication Event

```elixir
"repl:event:{bucket}:{event_id}" => %{
  event_id: event_id,
  bucket: bucket,
  key: key,
  version_id: version_id,
  event_type: :put | :delete | :delete_marker,
  root_hash: "sha256:...",
  created_at: iso8601,
  status: :pending | :sent | :failed
}
```

## 8. Write Path: PutObject

### 8.1 Flow

```text
PUT /{bucket}/{key}
  -> stream body to cas/tmp file (in the request process)
  -> compute SHA-256 and MD5 during streaming
  -> rename tmp file into global CAS if absent
  -> create blob metadata if absent
  -> create immutable version record
  -> update ref:{bucket}:{key}          (last — the visibility commit point)
  -> enqueue replication event
  -> return ETag
```

### 8.2 Requirements

* The request body must not be fully buffered for normal uploads. **Carve-out:** `aws-chunked` (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) uploads may keep the current buffer-then-decode path; a streaming chunked decoder is a separate future task.
* SHA-256 and MD5 must be computed in one streaming pass (already the case in `Engine.put_object_stream/5`).
* Streaming writes stay in the request process; `Plug.Conn.read_body/2` must not run inside any GenServer.
* If the target blob already exists, discard the temporary file.
* `CopyObject` becomes metadata-only when source and destination are local-backend buckets.
* Metadata updates must be ordered so a ref never points to missing content. Ref update is always the final write.
* Failed writes must leave no visible object ref.

### 8.3 Crash Safety

A write is considered visible only after:

1. Blob file exists in CAS.
2. Blob metadata exists.
3. Version metadata exists.
4. `ref:{bucket}:{key}` points to the new version.

If a crash occurs before ref update, the blob may become an orphan and is handled by GC (which already has an mtime grace window for exactly this race).

## 9. Read Path: GetObject / HeadObject

### 9.1 Flow

```text
GET /{bucket}/{key}
  -> read ref:{bucket}:{key}
  -> reject if delete marker (404 NoSuchKey with x-amz-delete-marker)
  -> read ver:{bucket}:{key}:{version_id}
  -> resolve root_hash
  -> if blob: send_file (zero-copy fast path, Range supported)
  -> if manifest: stream parts in order
```

Cloud-cache buckets keep their existing `CloudBackend` read path untouched.

### 9.2 Requirements

* Preserve existing `send_file` fast path for single-blob objects.
* Preserve Range support for blob objects.
* Manifest Range support: see §12.4 — required before Phase 3 ships if existing tests exercise Range on completed multipart objects.
* `HeadObject` must not touch the blob file unless integrity verification is explicitly requested.

## 10. Delete Path

### 10.1 Versioning Disabled

```text
DELETE /{bucket}/{key}
  -> remove ref:{bucket}:{key}
  -> version records for the key become unreachable
  -> GC discovers unreachable blobs on next sweep
```

### 10.2 Versioning Enabled

```text
DELETE /{bucket}/{key}
  -> create delete-marker version
  -> update ref:{bucket}:{key} to delete marker
```

Old versions remain addressable by `versionId`.

### 10.3 Requirements

* Never delete content files synchronously during request handling. (Already true on the S3 path; **Phase 1 must remove the `Engine.delete_content` call from `ExStorageServiceWeb.BucketLive.Files`**, which violates this today.)
* Physical deletion is handled exclusively by GC.
* Delete must be idempotent from the S3 API perspective.

## 11. CopyObject Path

### 11.1 Same-CAS Copy

```text
COPY source -> destination
  -> read source ref/version
  -> create destination version pointing to same root_hash
  -> update destination ref
```

### 11.2 Requirements

* Cross-bucket copy between local-backend buckets must not physically copy blob files (replaces today's `File.cp!` in `copy_local_content/3`).
* Copies involving a cloud-cache bucket keep the current data-transfer behavior.
* Metadata replacement rules should follow S3 behavior.
* Source object must be checked before destination ref update.
* Copy should enqueue replication for the destination object.

## 12. Multipart Upload

### 12.1 UploadPart

Each part is stored as an immutable blob:

```text
UploadPart
  -> stream part to cas/tmp
  -> compute SHA-256 and MD5
  -> commit part blob to global CAS
  -> store mpu_part metadata
```

### 12.2 CompleteMultipartUpload

```text
CompleteMultipartUpload
  -> validate all requested parts (etag match, min part size)
  -> create immutable manifest
  -> create object version pointing to manifest hash
  -> update ref
  -> enqueue replication
```

This removes today's read-every-part-into-memory concatenation in `Storage.Multipart.complete_upload`.

### 12.3 Manifest Format

```elixir
%{
  format: "ess-manifest-v1",
  parts: [
    %{
      number: 1,
      hash: "sha256:...",
      size: 5_242_880,
      etag: "..."
    }
  ],
  total_size: integer,
  etag: multipart_etag
}
```

### 12.4 Compatibility Guard: Range and send_file

Completed multipart objects currently support Range requests and the `send_file` fast path because completion concatenates parts into a single blob. Manifests change that. Before Phase 3 ships:

* Audit `mix test` and `e2e/s3_compat.py` for Range-after-multipart coverage.
* If covered (or if real clients depend on it), implement manifest Range reads (offset math over `parts` sizes, `send_file/5` per overlapping part) as part of Phase 3 — not deferred.
* Full-object manifest GET streams parts in order; per-part `send_file` keeps most of the zero-copy benefit.

## 13. Versioning Semantics

**Current state (code review):** versioning is config-and-read only. `Storage.Versioning.put_version/3` and `delete_version/3` have no callers; the S3 PUT path never creates versions and DELETE never creates delete markers. Only GET-with-`versionId` reads version records. Phase 2 is therefore new write-path behavior, not a refactor of working behavior.

### 13.1 Disabled

* Only the latest object is visible.
* Old versions are not retained.
* Old blobs become GC candidates after overwrite/delete.

### 13.2 Enabled

* Every PUT creates a new immutable version.
* DELETE creates a delete marker.
* Old versions remain reachable by version ID.
* GC treats all version records as roots.

### 13.3 Suspended

* Follow S3-compatible suspended behavior later.
* For initial implementation, suspended behaves like the current `Versioning` module's "null" version semantics, unless existing API tests require stricter behavior.

## 14. Garbage Collection

### 14.1 GC Roots

The following metadata records are GC roots:

* `ref:*`
* `ver:*` (all version records — delete markers keep their parents' history reachable)
* active `mpu_part:*`
* `manifest:*` reachable from any root above (manifest parts are transitively rooted)
* pending replication events (`repl:event:*` with `status: :pending`)

### 14.2 GC Strategy: Mark-and-Sweep First

Phase 1–4 GC extends the existing `ContentGC` mark-and-sweep, which is already proven in this codebase (disk scan vs. referenced-hash set, mtime grace window):

```text
sweep:
  -> build reachable-hash set from GC roots (§14.1)
  -> scan cas/objects and cas/manifests on disk
  -> unreachable + older than grace window -> create gc:candidate
  -> candidate past eligible_after and still unreachable -> move to cas/gc/quarantine
  -> quarantined past second grace period -> delete file + blob metadata
```

Blob refcounts are explicitly **not** part of the MVP: they add CAS churn on every PUT/COPY/DELETE and a class of crash-consistency bugs that sweep-based GC avoids. Refcounting may be added later as an optimization if sweep cost becomes a problem at scale.

### 14.3 Requirements

* GC must never run in the request process.
* GC must be idempotent.
* GC must tolerate missing files and missing metadata.
* GC must produce audit logs.
* GC must support dry-run mode.
* During the migration window, GC must ignore the legacy bucket-local layout (the legacy `ContentGC` keeps handling it until cutover).

## 15. Integrity and Repair

### 15.1 Integrity

Each blob is addressed by SHA-256. The system must be able to verify:

```text
hash(file_contents) == content_hash
```

### 15.2 Repair Worker

Add a future repair worker that can:

* scan blob metadata
* verify physical files exist
* hash-check files
* find orphan files
* find metadata pointing to missing files
* quarantine corrupt blobs
* emit repair reports

## 16. Replication

Replication moves from key-oriented events (today: `Hooks.after_put(bucket, key)` enqueues `%{action: :put, bucket, key}` and the worker re-reads current state) to version/content-oriented replication.

### 16.1 Event Payload

Replication event should include:

* bucket
* key
* version ID
* event type
* root hash
* object type
* metadata
* manifest parts if needed

### 16.2 Requirements

* Destination can skip blob transfer if it already has the content hash.
* Replication should transfer content before updating destination ref.
* Replication should be idempotent.
* Delete marker replication should not require object body transfer.

## 17. Required Concord Capabilities

Concord 2.3.0 (current dependency) already provides:

| Capability | Status |
| --- | --- |
| `get` / `put` / `delete` | ✅ |
| `get_all/0` | ✅ (current listing mechanism) |
| `prefix_scan/2` | ⚠️ exists but quarantined — crashes Ra state machine; tracked as gsmlg-dev/concord#27 |
| Single-key CAS: `put_if/3`, `delete_if/2` (with `expected:` or `condition:`) | ✅ |
| Homogeneous batches: `put_many/2`, `delete_many/2`, `get_many/2` | ✅ |
| **Atomic mixed batch (puts + deletes + CAS in one Raft commit)** | ❌ — requested upstream as **gsmlg-dev/concord#36** (severity: needed) |

### 17.1 MVP Position

MVP proceeds without the mixed batch: writes are strictly ordered (blob → version → ref last) so a ref never points at missing data, and the repair worker reconciles partial failures. `put_if` with `expected:` is available where single-key CAS helps (e.g., ref updates that must not clobber a concurrent write).

### 17.2 Adoption Plan

When gsmlg-dev/concord#36 lands, PutObject/DeleteObject/CompleteMultipartUpload collapse their multi-record updates into one atomic batch, and the repair worker's job shrinks accordingly. Phase 2 should be designed so this substitution is a local change inside the metadata modules.

## 18. Module Design

### 18.1 New / Updated Modules

`Storage.CAS` is a **plain module** — no GenServer. Its only "state" is `data_root`, which is static config; per the no-process-without-a-runtime-reason rule, path math, existence checks, and rename-commits need no serialization point. This also removes the current `Engine` GenServer bottleneck where every GET path lookup is a `GenServer.call`.

```text
ExStorageService.Storage.CAS          (plain module)
  - commit_blob/2
  - blob_path/1
  - has_blob?/1
  - verify_blob/1

ExStorageService.Storage.Ref          (plain module over Metadata)
  - get_latest/2
  - update_latest/4
  - delete_latest/2
  - list/2                            (ListObjectsV2 backing scan)

ExStorageService.Storage.Version      (plain module over Metadata)
  - create_version/1
  - get_version/4
  - list_versions/2

ExStorageService.Storage.Manifest     (plain module)
  - create_manifest/1
  - get_manifest/1
  - stream_manifest/1

ExStorageService.Storage.GC           (GenServer — periodic runtime process, like ContentGC today)
  - mark_candidates/0
  - sweep/0
  - dry_run/0

ExStorageService.Storage.Migration    (mix task / admin-triggered)
  - migrate_bucket_local_cas_to_global_cas/0
```

### 18.2 Existing Module Changes

`ExStorageService.Storage.Engine`

* Delegates physical blob operations to `Storage.CAS`.
* Stops using bucket-local content paths.
* Preserves streaming upload behavior (stream written in caller process, atomic rename commit).
* Sheds GenServer state where possible; remains only as a thin startup/compat shim if anything still needs it.

`ExStorageService.Metadata`

* Add explicit functions for refs, versions, blobs, and manifests.
* No direct Concord key-string usage outside Metadata modules.

`ExStorageService.Storage.Versioning`

* Superseded by `Storage.Version` + bucket record versioning state; `obj_ver:*` / `obj_ver_list:*` / `bucket_versioning:*` keys migrated in Phase 2.

`ExStorageServiceS3.Handlers.Object.LocalBackend`

* PutObject creates version/ref records.
* GetObject resolves ref → version → blob/manifest.
* CopyObject becomes metadata-only for local↔local copies.
* DeleteObject updates refs only; never touches files.

`ExStorageServiceS3.Handlers.Object.CloudBackend`

* Unchanged (out of scope, §3).

`ExStorageServiceWeb.BucketLive.Files`

* Remove the synchronous `Engine.delete_content` call; deletion goes through the same ref-update path as the S3 API.

`ExStorageService.BucketValidator`

* Reject `cas` as a bucket name.

## 19. Migration Plan

### 19.1 Existing Layout

```text
{data_root}/{bucket}/objects/{prefix}/{rest}      # content
{data_root}/{bucket}/multipart|tmp/...            # staging
"obj:{bucket}:{key}"                              # metadata
"obj_ver:{bucket}:{key}:{vid}" / "obj_ver_list:"  # partial versioning metadata
```

### 19.2 New Layout

```text
{data_root}/cas/objects/sha256/{prefix}/{rest}
```

### 19.3 Migration Steps

1. Stop writes or run in maintenance mode.
2. Scan all existing `obj:{bucket}:{key}` metadata.
3. For each object:

   * read `content_hash`
   * locate old bucket-local file
   * move (rename) to global CAS path; if the CAS path already exists (dedup hit), leave the old file for step 7 cleanup
   * create `blob:sha256:{hash}` metadata
   * create `ver:{bucket}:{key}:{version_id}`
   * create `ref:{bucket}:{key}`
4. Migrate existing `obj_ver:*` records to `ver:*` the same way.
5. Verify all refs resolve to existing global CAS files (hash spot-checks on a sample).
6. Enable the new write path.
7. Leave old bucket-local files untouched until migration verification passes; the legacy `ContentGC` continues to own the legacy tree during this window.
8. Delete the old layout only after explicit admin confirmation.

## 20. Implementation Phases

### Phase 1: Global CAS

* Add `Storage.CAS` (plain module) and `cas/` reserved layout; reject `cas` as a bucket name.
* Add blob metadata.
* Update PutObject to store globally; keep writing `obj:{bucket}:{key}` metadata unchanged for compatibility.
* Update GetObject to read globally.
* Update CopyObject to be metadata-only for local↔local (kill `File.cp!`).
* Remove `Engine.delete_content` call from the web UI object browser.
* Migration task for physical files (metadata schema unchanged in this phase).

### Phase 2: Refs and Versions — ✅ done (2026-07-09)

* ~~Add `ref:*` and `ver:*` records~~ → existing `obj:*` / `obj_ver:*` / `obj_ver_list:*` keys serve these roles directly (revision note 11); no migration needed.
* Wire PutObject, GetObject, HeadObject, DeleteObject through refs/versions. ✅ (PUT/copy/multipart-complete create versions with `x-amz-version-id`; DELETE creates markers, supports `?versionId=` permanent deletes incl. marker-delete undelete; GET/HEAD see markers as absent.)
* Implement versioning-enabled behavior (PUT versions, DELETE markers). ✅ (delete-marker/repoint semantics fixed in `Storage.Versioning`.)
* Listing scans the ref records (no `idx:*`). ✅ (delete markers absent from `obj:*` → excluded from listings for free.)
* Write ordering per §17.1: version record → version list → ref last. ✅
* Follow-ups deferred: `ListObjectVersions`, `HeadObject?versionId`, copy-source `?versionId`, version id on CompleteMultipartUpload response.

### Phase 3: Manifests for Multipart — ✅ done (2026-07-09)

* Store each multipart part as CAS blob. ✅ (`mpu_part` records carry the blob hash; no bucket-local part files; part upload dedups via CAS.)
* CompleteMultipartUpload creates manifest. ✅ (`Storage.Manifest`: deterministic canonical-JSON file under `cas/manifests/` + `manifest:sha256:{hash}` record, linked via `manifest_hash` on version metadata.) Completion additionally materializes the whole-object CAS blob by **streaming** part blobs (constant memory — replaces the old read-whole-parts-into-memory concatenation).
* ~~GetObject supports manifest streaming~~ → deliberately deferred to Phase 6: chunked transfer-encoding would drop `Content-Length` and complicate Range, regressing S3 client compatibility (§12.4). GET/HEAD/Range serve the whole blob unchanged.
* Manifest metadata joins the GC root set. → Phase 4 (active `mpu_part` records root part blobs during uploads; after completion part blobs are unreferenced and GC-eligible).

### Phase 4: GC — ✅ done (2026-07-09)

* Extend mark-and-sweep to the global CAS with candidate queue, quarantine, dry-run, audit logs, admin visibility. ✅ (`Storage.CasGC`: `gc:candidate:{hash}` records with stages, `cas/gc/quarantine/` holding area, restore-on-rereference, `dry_run/0` + `stats/0`; per-stage reachability re-checks. Admin UI is a follow-up.)
* Legacy `ContentGC` retires when the legacy layout is deleted. (Unchanged — still owns the legacy tree.)

### Phase 5: Replication Upgrade — ✅ done (2026-07-09)

* Replicate by version/root hash. ✅ (jobs pin version_id + content_hash + etag + size at enqueue time; superseded-and-collected versions skip as stale.)
* Skip already-present blobs. ✅ (key-level HEAD etag+size comparison — targets are generic S3, no cross-node blob endpoint; see revision note 11c.)
* ~~Transfer manifests before refs~~ → N/A: multipart objects replicate as their materialized whole blob.
* Delete-marker replication without body transfer. ✅ (plain DELETE, 404-idempotent — unchanged.)
* Make replication idempotent. ✅ (skip-if-present + stale skip + idempotent delete.)

### Phase 6: Pack Storage

Future phase.

* Add pack writer for cold/unmodified blobs.
* Add pack index.
* Support reading packed blobs.
* Add lifecycle policy transition to packed storage.
* Preserve CAS identity.

## 21. Acceptance Criteria

### 21.1 Deduplication

* Uploading identical content to two keys stores one physical blob.
* Copying an object across local buckets does not copy physical content.
* Overwriting a key with identical content does not duplicate blob data.

### 21.2 Versioning

* With versioning enabled, each PUT creates a new version.
* GET without `versionId` returns latest non-delete-marker object.
* GET with `versionId` returns the requested old version.
* DELETE creates a delete marker.
* Old versions remain readable after delete marker creation.

### 21.3 Crash Safety

* If upload crashes before ref update, object is not visible.
* If blob exists but has no ref/version, GC detects it as orphan (after grace window).
* If ref points to missing content, repair detects it.

### 21.4 S3 Compatibility

* Existing PutObject/GetObject/HeadObject/DeleteObject tests pass.
* Existing ListObjectsV2 tests pass.
* Existing CopyObject tests pass.
* Multipart tests pass after manifest support lands, **including any existing Range-on-completed-multipart coverage** (§12.4).
* Cloud-cache bucket behavior is unchanged.

### 21.5 GC Safety

* GC never deletes content referenced by live refs.
* GC never deletes content referenced by object versions.
* GC never deletes content referenced by active multipart uploads or manifests.
* GC dry-run reports expected deletions without modifying files.
* Web UI object deletion no longer removes content files synchronously.

## 22. Testing Plan

### 22.1 Unit Tests

* CAS path generation (including reserved-root layout)
* CAS commit idempotency (second commit of same hash is a no-op)
* `cas` bucket name rejected
* Blob metadata creation
* Ref update / delete-marker ref state
* Version creation and version-ID ordering
* Manifest creation
* GC candidate creation and quarantine transitions

### 22.2 Integration Tests

* PutObject → HeadObject → GetObject
* PutObject same content twice → one blob
* PutObject same content to two buckets → one blob
* CopyObject same bucket → metadata-only copy
* CopyObject cross bucket → metadata-only copy
* DeleteObject versioning disabled → ref removed, blob unreachable, swept after grace
* DeleteObject versioning enabled → delete marker created, old version readable
* Multipart upload → manifest object readable end-to-end
* Range GET on completed multipart object (per §12.4)
* Cloud-cache bucket PUT/GET/DELETE unchanged

### 22.3 Migration Tests

* Create old bucket-local layout fixture (including duplicate content across buckets).
* Run migration.
* Verify global CAS files exist and dedup collapsed duplicates.
* Verify refs and versions resolve.
* Verify old files remain until cleanup confirmation.
* Verify S3 API behavior after migration.

### 22.4 Property Tests

Useful properties:

* Every visible object ref resolves to a version.
* Every version resolves to a blob or manifest.
* Every manifest part resolves to a blob.
* GC never selects reachable blobs.
* CopyObject does not change source visibility.

## 23. Risks

### 23.1 Multi-record Metadata Updates

A single S3 operation updates several Concord keys. Until gsmlg-dev/concord#36 lands, partial writes are possible.

Mitigation:

* Order writes so refs are updated last.
* Use `put_if`/`delete_if` where single-key CAS suffices.
* Add repair worker.
* Adopt the atomic mixed batch when concord#36 ships (§17.2).

### 23.2 Listing Scale

`ListObjectsV2` requires efficient prefix scans, but the only reliable primitive today is `get_all/0` (concord#27).

Mitigation:

* Accept O(N) scans for the < 50K-key target, as today.
* Keep listing behind `Storage.Ref.list/2` so a `prefix_scan` swap is one function.
* Upstream fix tracked: gsmlg-dev/concord#27.

### 23.3 GC Bugs

Incorrect GC can cause data loss.

Mitigation:

* Conservative grace periods (mtime window already proven in ContentGC).
* Quarantine before deletion.
* Dry-run mode.
* Reachability verification re-check before physical delete.

### 23.4 Manifest Range Reads

Range reads over multipart manifests are more complex, and single-blob multipart objects support Range today.

Mitigation:

* §12.4 compatibility guard: audit coverage first; implement manifest Range in Phase 3 if it is exercised.

### 23.5 Migration Window Hazards

Two layouts coexist during migration; the legacy GC and the new GC must not reach into each other's trees, and a bucket named like a reserved directory must be impossible.

Mitigation:

* Single reserved `cas/` root; `cas` rejected as bucket name.
* Legacy `ContentGC` owns the legacy tree until cutover; new GC owns `cas/` only.

## 24. Final Design Rule

ExStorageService should treat Git's data model as an internal storage discipline:

```text
Immutable content.
Mutable refs.
Versioned history.
Reachability-based GC.
Packable cold storage.
```

But it should remain an S3-compatible object storage server externally.

The user should never see Git concepts. Internally, the system should get Git-like deduplication, versioning, integrity, and repairability.
