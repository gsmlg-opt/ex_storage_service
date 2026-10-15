# Bounded native tag selection

Scope: `main` at `9b8661e` plus the CLI implementation diff. Existing single
bucket/object tagging and complete JSON or escaped key-value set documents retain
their paths. Native tag get/list/set/remove now expose recursive, all-version
and rewind selection through the existing bounded object-management engine.

Recursive current selection completes the entire paginated inventory before
tag operations. Version selection excludes delete markers. Rewind selects the
latest data version strictly before the cutoff, with a winning delete marker
hiding the object; versions plus rewind selects all eligible older data versions.
Exact-object historical selection does not include neighboring key prefixes.

Every selected identity is confirmed by HEAD before a mutation begins. Bulk and
historical mutations require an immutable version because S3 tagging has no
ETag conditional-write fence for null/unversioned objects. This conservative
restriction is an explicit native difference from mc. The preflight and result
manifests remain bounded disk records and contain no credentials. Failure during
inventory or preflight performs no mutations; later service failure reports the
number already processed and the complete matched count. This is not a
multi-object atomic metadata transaction.

`--limit` bounds returned rows, not mutation selection. Read-only tag output
performs at most that many HEAD/tag reads after completing the inventory and
reports `Partial` plus `MatchedObjects`. Dry-run confirms identities and models
the operations without writing, including when the output limit is zero.
An explicit version conflicts with recursive/versions/rewind selectors. Invalid
limits fail before I/O. Bucket tags without selection retain their existing
bucket operation; recursive bucket targets select object tags.

The original product failed all eight added selector/effect tests in
`/tmp/ess-native-tag-selection-red.log`. The final four-file management and
governance suite passed **63 tests, zero failures** in
`/tmp/ess-native-tag-selection-scoped.log`, including a ninth test through
Runner and actual HTTP ListVersions/HEAD/PutObjectTagging exchanges. That wire
case asserts the immutable version query and tag XML. Strict compilation passed.
The wire fixture declares a generic S3 provider; known ESS safety exclusions are
preserved. These checks do not establish live provider or full mc output parity.
