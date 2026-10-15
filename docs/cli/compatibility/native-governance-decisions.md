# Portable native object governance

The native legal-hold and retention conveniences use the shared S3 catalog,
transport, validation, and capability boundaries. They add no server feature or
provider-specific admin API. The existing full-document `get`, `set`, and
`remove` forms retain their syntax and behavior.

## Commands

```sh
ess legalhold set ALIAS/BUCKET/KEY
ess legalhold clear --vid VERSION ALIAS/BUCKET/KEY
ess legalhold info --recursive ALIAS/BUCKET/PREFIX/
ess retention set governance 30d ALIAS/BUCKET/KEY
ess retention set compliance 1y --version-id VERSION ALIAS/BUCKET/KEY
ess retention clear --bypass ALIAS/BUCKET/KEY
ess retention info --versions --rewind 2026-10-15T00:00:00Z ALIAS/BUCKET/KEY
ess retention set --default governance 30d ALIAS/BUCKET/
ess retention clear --default ALIAS/BUCKET/
ess retention info --default ALIAS/BUCKET/
```

`legalhold set TARGET` writes `LegalHold.Status=ON`; `clear` writes `OFF`.
`info` reads the document. The two-argument `legalhold set TARGET DOCUMENT`
continues to set the complete document.

The three-argument retention form takes governance/compliance mode, a positive
integer duration ending in `d` or `y`, and the target. Mode and unit case are
insensitive. UTC time is captured once per invocation, with second precision.
Days use calendar days; years retain mc's calendar normalization, including
February 29 plus one year becoming March 1. Values outside the supported
four-digit calendar fail before I/O. `retention set TARGET DOCUMENT` remains
the full-document API. `retention clear` writes an empty Retention document.
Full-document set accepts its existing version, dry-run, and retention-bypass
options, but rejects the convenience-only recursive, versions, rewind, default,
and limit options before document reads or provider calls.

`--bypass` and `--bypass-governance-retention` are aliases. Only an explicitly
true option sends `BypassGovernanceRetention=true`, on retention mutations.
Clearing retention does not silently request bypass. The server continues to
enforce governance/compliance policy and caller authorization.

## Selection and bounded work

Object forms accept `--version-id`/`--vid`, `--recursive`, `--versions`, and
`--rewind`. An explicit version conflicts with the other selectors. A
nonrecursive versions selection stays on the exact key; recursive selection
uses the established bucket/directory-prefix inventory boundary. Tombstones
are excluded from object-governance calls.

Rewind uses the shared UTC/duration grammar and strict timestamp cutoff:
modification equal to the cutoff is excluded. Without `--versions`, it selects
one latest prior data version per key and suppresses historical deletions.
With `--versions`, it selects all older data versions. Without an explicit
rewind, all-version selection is cut off at the invocation's current instant.
A historical single-object rewind actually pins the prior version; the pinned
mc executable ignores rewind on that particular nonrecursive path.

All listing pages finish successfully into private sorted disk manifests
before preflight. Metadata HEADs compare listed size, ETag, modification time,
and requested version identity for every listed candidate.
Every selected mutation is validated before the first PUT. Enumerated and
explicit-version mutations require a confirmed immutable version identifier.
`null` or missing version IDs fail on selected mutations because these S3 APIs
have no conditional identity header. Read-only inspection may report a verified
`null` version. A plain current-object command retains normal current-target
semantics, pinning the HEAD version when the server supplies one.

A later-page or preflight failure causes no governance mutation. Once submission
begins, a service error stops the batch and reports the completed count; these
independent S3 operations provide no batch rollback. Cancellation is checked
before provider calls and effects. Every manifest is cleaned on success or
failure.

`--limit` defaults to 1000 and permits 0 through 10000. It caps returned rows,
not the selected mutation batch. Info limits also bound metadata HEADs and
object-governance GETs. Complete matched counts and `Partial` distinguish
omitted output. Dry run performs selection and identity validation but sends no
PUT; its processed count describes validated planned calls.

## Bucket defaults

`retention --default` operates through Get/PutObjectLockConfiguration and
conflicts with object selectors and bypass. It requires a bucket target.
Info returns the complete read document. Set and clear first read the current
configuration, require ObjectLockEnabled to remain Enabled, and replace the
Rule's DefaultRetention or remove Rule respectively. They do not implicitly
enable bucket locking. Deliberate enablement remains available through the
existing full `lock set` document command.

Existing fields are retained through the edit and validated before PUT. An
unknown future field that the locked S3 model cannot serialize causes an
explicit validation error, preserving the original remote configuration rather
than silently dropping it. The declared ESS capability guard and directory
bucket guard reject these unsupported operations before sending requests.

## Pinned executable evidence

Fourteen actual mc invocations against a disposable loopback S3 peer recorded
ON/OFF legal-hold XML, version query strings, retention duration serialization,
explicit set bypass, implicit clear bypass, bucket-default set/clear/info,
single-object rewind behavior, all-version rewind selection, uppercase units,
and zero-duration rejection.

- Source revision: `77f82e18b5401a65958f1619df6ebb994634bd88`.
- Executable: `DEVELOPMENT.GOGET`, runtime `go1.26.6 linux/amd64`.
- Executable SHA256: `a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2`.
- Source: `cmd/legalhold-{set,clear,info}.go`, `cmd/retention-{set,clear,info}.go`,
  `cmd/retention-common.go`, and `cmd/client-s3.go`.
- Retained CLI fixtures: `test/fixtures/differential/capture_native_governance.py`
  and `native-governance-wire.json`.

The capture script checks the executable digest, clears ambient provider/proxy
settings, uses fresh temporary configuration, and shuts down its peer in
`finally`. Retention-until values reflect the capture's clock; they are not
fixed release timestamps. ESS deliberately differs on automatic bypass, ignored
single-object rewind, implicit bucket-lock enablement, bounded output, and
selected mutable versions as described above.

## Validation

Focused validation passed **54 tests**, including 23 new governance tests,
existing Management/VersionSelection/Parser coverage, and an actual Runner
retention-target wire test. Strict compilation, scoped formatting, and diff
whitespace checks passed. The suite covers command/full-document coexistence, exact XML and version wire
requests, duration/calendar arithmetic, explicit bypass, default configuration
preservation, complete inventory/preflight ordering, strict rewind, tombstones,
mutable versions, capped info, dry run, cancellation, partial failure, private
manifest cleanup, and retained oracle auditing. This fixture evidence does not
claim live AWS/MinIO, HTTP/2, or cross-platform acceptance.
