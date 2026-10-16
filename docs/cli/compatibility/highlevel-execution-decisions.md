# High-level execution decisions

High-level AWS `s3` and native transfers share `S3.Client`, `Transfer.Engine`, and
`Transfer.Copy`. Targets retain separate immutable endpoint and credential contexts.
`--source-region` applies to source listing, HEAD preflight, GET, and copy requests;
the destination context retains its own region.

## Inventories and synchronization

Complete inventories and plans spill to private disk manifests before mutations.
Runs contain at most 256 records or one MiB, merges open two inputs, and traversal
uses bounded native directory batches. Later listing failures and repeated tokens
fail the plan. Local unreadable entries, followed directory loops, invalid mapped
names, Unicode/case collisions, and file/directory conflicts fail closed. Destination
symlink components are rejected before downloads; publication retains the shared
Sink boundary and its controlled-parent-directory requirement.

AWS sync uses direction-specific size/time decisions. Native mirror requires
explicit overwrite for differing existing entries. Ordered filters protect excluded
destination entries. Required transfers complete successfully before deletion;
candidates are checked again immediately before removal. Ignored archive skips
suppress the entire destination-delete phase even though the invocation succeeds.

Glacier and Deep Archive sources are skipped unless forced. Ordinary skips emit a
warning through the stderr callback and return the skipped category (AWS exit 2).
`--ignore-glacier-warnings` suppresses the warning and skipped exit. Forced transfers
reach the provider and still require a restored/readable object.

## Raw head and bounded inspection

Native `head` writes raw bytes, defaulting to ten newline-terminated lines. Choose
`--lines N` or `--bytes N`; zero emits nothing and opens no request. Once the prefix
is complete, the consumer intentionally stops inside the shared download engine;
its `after` cleanup aborts the remaining owned response. A prefix does not establish
a whole-object checksum result. `stat` returns HEAD metadata and `cat` consumes the
whole validated stream. These operations accept a selected version ID.

`find` and `du` default to at most 1000 matched results, with an explicit `--limit`
between zero and 10000. Their complete filtered inventory count is returned as
`MatchedObjects`; `Partial` is true when the limit excludes matches. Find returns
`Entries` and `Returned`; du returns `Objects` and the corresponding `Bytes`.
Inventory collection remains bounded in memory and may scan all remote pages.

## Versioned deletion

`rm --version-id ID` removes exactly the explicit object version or delete marker.
`rm --all-versions` first inventories every matching version and marker, then deletes
the selected identifiers in batches of at most 100. Without `--recursive`, only the
exact object key matches; recursive mode selects the bucket/prefix tree. `--versions`
is inspection for listing and is rejected as deletion authorization.

Current-object recursive rm and `rb --force` use current-object listings. They do
not substitute historical version IDs returned by HEAD. Conditional batches carry
captured ETags and require an AWS or explicitly verified provider contract. Each
batch revalidates candidates first. Every batch response must confirm every selected
identifier; HTTP 200 with per-object errors remains failure. Object Lock bypass is
never supplied implicitly. No retry of permanent per-key failures is attempted.

The batch authorization is scoped to the completed inventory. Concurrently created
versions are not silently added after enumeration. An explicit `null` version remains
subject to the service's null-version semantics.

## Input and output choices

High-level modeled fields include metadata, content headers, ACL/grants, SSE and
SSE-C, storage class, requester pays, and checksum selection. SSE-C files are read
with a strict 33-byte upper bound and must contain exactly 32 raw key bytes.
Replayable stdin is captured privately on disk with cancellable 64-KiB reads;
AWS `--expected-size` is a nonnegative sizing hint, not an exact-length assertion:
smaller and larger hints upload the actual captured bytes. It selects multipart
threshold and bounded part sizing; actual bytes still determine object limits,
Content-Length and publication. Native pipe keeps its exact-size check. See
[aws-expected-size-decisions.md](aws-expected-size-decisions.md) for independent
pinned executable observations and scoped tests. Progress callbacks are throttled
and write to stderr; raw object stdout remains exclusive.

MIME inference uses the immutable Python built-in MIME table and compressed suffix
rules used by AWS's Python `mimetypes` path. Host-specific `/etc/mime.types` extensions
are not loaded, so platform-added MIME types may differ from an AWS installation.

## Remaining validation boundaries

Wire tests cover the implemented effects and safety gates. These fixtures do not
establish complete differential AWS/mc compatibility. Independent executable cases,
provider/account-specific Glacier restoration, host MIME extensions, share planning,
and any unimplemented command families remain separately tracked by the plan.
