# P2.5 implementation ownership handoff

Prepared from current checkout after Phase1 implementation. This is the accepted
source ownership map. Phase2 P2.5 implementation was subsequently authorized by
the coordinator; current verification status is in `s3-hardening-p2-5-evidence.md`.

## Proposed scope for authorization/configuration worker

- New core `storage/tags.ex`: shared validation for bucket/object tags
  (unique UTF8 keys, max10 object tags/max50 bucket tags, key128/value256
  characters, no forbidden `aws:` prefix); deterministic TagSet serialization
  belongs in the S3 handler, not core.
- New core `storage/bucket_cors.ex`: normalized persisted rules and match logic.
  Use existing `Metadata.BucketConfig` for bucket CORS and bucket tags;
  prefixes `bucket_cors:` and `bucket_tagging:` are already removed inside
  bucket deletion's fenced transaction. Capture generation before body reads.
- New S3 `handlers/tagging.ex`, `handlers/cors.ex`, and
  `handlers/location.ex`: bounded safe XML, truthful errors, x-amz-tagging-count
  where required, gateway tagging rejection before any external mutation.
- New S3 `plugs/cors.ex`: handle only a syntactically valid OPTIONS preflight
  with Origin plus Access-Control-Request-Method as a preflight; actual requests
  still run existing authentication/authorization/mode guards. Register actual
  response headers before auth so matching origins can read legitimate error
  responses. Persisted rules control origins, methods, requested/exposed headers,
  max-age and Vary (Origin, Access-Control-Request-Method,
  Access-Control-Request-Headers); never reflect arbitrary origins/headers.
- Own tests in new `tags_test.exs`, `bucket_cors_test.exs`,
  `tagging_http_test.exs` and `cors_location_http_test.exs`; update A01's existing
  tagging-exclusion fixture only when supported Tagging is wired and verified.
- Documentation changes limited to new configuration/support semantics in
  README/AGENTS and compatibility/evidence documents.

## Shared files retained by root

`s3/operation.ex`, `router.ex`, `plugs/authorize.ex`,
`metadata/object_commit.ex`, `metadata/keys.ex`, `object_service.ex`,
`storage/versioning.ex`, and shared listing/versioning code remain root-owned.
The existing permission table already names bucket CORS/location/tagging and
version-aware object tagging actions with fail-closed legacy aliases. Resolver
and router currently reject/omit those operations and need root wiring once
the worker modules are stable.

Router currently authenticates before resolving. Root inserts CORS after request
ID/query setup but before authentication, with only explicit preflight halting;
all non-preflight paths return the connection into the current auth chain.

## Required root-owned metadata interface

Propose version metadata field `:tags` as a validated `%{binary() => binary()}`;
missing means empty tags. PUT/COPY/multipart pass this field with all other
publication metadata. Never store tags as object user metadata, replace CAS
bytes, mint an extra object version, change ETag/content hash, or change the
version's creation time for Tagging CRUD.

Root supplies `ObjectCommit.get_tags(bucket,key,version_id,opts)` and
`ObjectCommit.set_tags(bucket,key,version_id,tags,opts)` (names provisional;
existing arities unchanged). For nil version ID, read/compare the current head
and its version in a native Concord transaction. For explicit version ID,
compare that version's revision. Both paths retain bucket generation, reject
delete markers/missing versions, preserve every unrelated field and fence
delete/recreate. Tags on the version row can be the small mutable metadata
exception to its documented immutable content identity. Root owns any matching
ObjectCommit/index/deletion changes so this worker never races root writes.

Legacy v1/null reads must follow root's chosen version resolution; do not add a
separate raw-write path or bypass `v1` read-only compatibility. Root also decides
whether generic `ObjectService` wrappers are desired; handlers can use the
specific domain interface directly without coupling core to Plug.

## Multipart/COPY boundary with integrity worker

Tag parser returns validated maps only. Integrity worker retains multipart
records/attributes and copies `:tags` through completion; COPY default is source
tags, `x-amz-tagging-directive: REPLACE` uses validated supplied tags. Duplicate
or malformed tag headers reject before staging/publication. An explicit tag
grant is required when the accepted API/action requires it; worker supplies
authorization cases without editing integrity-owned transfer modules.

Gateway tag CRUD and tagged mutation/copy combinations must either use a
verified upstream operation or reject before body/external mutation. Existing
gateway projection/hosted-history guard remains intact.

## Region decision to settle before location wiring

There is currently no global S3 region config; signing accepts a supplied scope,
the presign helper defaults to us-east-1, and cloud configs have per-upstream
regions. Propose an additive core `:s3_region` default `us-east-1`, optionally
configured by `ESS_S3_REGION` in runtime config and documented in README/AGENTS.
GetBucketLocation uses the AWS empty LocationConstraint for us-east-1 and the
configured region elsewhere; x-amz-bucket-region and presign defaults use the
same authoritative value. Upstream region never replaces hosted service region.
Root accepted this additive config interface for later Phase2 implementation.
Document README/AGENTS when adding it, and preserve existing SigV4 region-scope
acceptance rather than rejecting previously accepted signing regions. Root
accepted the ownership split and will provide the transaction-fenced object and
version tag helpers. No Phase2 source implementation is authorized to begin
until Phase1 completes and root dispatches that work.
