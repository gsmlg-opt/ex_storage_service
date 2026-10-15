# Native share and history decisions

Scope: mc `77f82e18b5401a65958f1619df6ebb994634bd88`; ESS `9b8661e`
plus the current implementation diff. This establishes selected portable native
contracts, not complete mc display, provider or platform parity.

Reviewed the locked share download/upload/list help, actual pinned
`share-download-main.go`, `share-upload-main.go`, `share-list-main.go`,
`share-db-v1.go`, and the S3 upload policy path. No MinIO implementation was
copied. An actual Linux amd64 pinned mc run against a Python stdlib loopback
fixture, with a private temporary config and fixture-only credentials, confirmed
three successful cases: repeated `--expire 1s --expire 2s` signs two seconds;
selected GET version `v +/雪` survives query encoding; recursive upload has a
real `["starts-with", "$key", "folder/a"]` policy condition. Normalized observed
fields and executable hash are retained in
`test/fixtures/s3/sharing-mc-capture.json`; the native test consumes these fields.
The fixture answers bucket-location GET and object HEAD with deterministic
metadata. These captures do not prove a live S3 service accepts the generated
capabilities. Other share behavior below is independently tested implementation,
not an unrecorded executable differential.

`share download` accepts repeated `--expire` durations (last occurrence wins),
compound h/m/s durations, the existing `--expires` integer seconds, and selected
`--version-id`/`--vid`. The two expiry spellings cannot be mixed. Lifetimes are
1..604800 seconds; fractional totals truncate to whole seconds. ESS retains its
existing 3600-second default, single-target grammar, single signed URL result,
and offline signing behavior rather than adding mc's object-existence probe.
Go duration units outside the documented h/m/s form, mc short aliases other than
those already provided globally, and multi-target share commands remain
unavailable. The existing generic repeated-string parser already keeps the last
value; its source was left unchanged.

Selected versions enter the modeled GetObject query before endpoint resolution
and signing. They are unavailable for PUT or recursive downloads. Recursive
GET uses the existing complete paginated, disk-backed inventory, filters directory
markers and signs at most 1000 objects (or a smaller `--limit`). An explicit `--limit` requires recursive
selection, including when the supplied value would otherwise be valid; single
GET sharing rejects it before signing or creating history. Inventory failure,
oversized complete batch, signing failure, or history-write failure returns an
error before any share result is returned. Inventories are cleaned in `after`.
The full remote inventory is enumerated before results; memory is bounded by the
existing disk-backed manifest plus the at-most-1000 signed results, and temporary
disk usage grows with the remote inventory. An empty inventory is a complete
zero-share batch. Listing current objects does not pin their versions.

Legacy upload remains signed PUT unless `--post` is requested. Exact POST
policies keep the existing byte-identical independent HMAC fixture. Recursive
upload is genuinely a POST key-prefix grant, with an internally generated
`${filename}` field and a starts-with condition, rather than a PUT placeholder.
It requires a nonempty literal prefix to avoid accidentally granting a whole
bucket. Content-Type, session-token and content-length constraints remain signed;
provider single-upload limits and credential expiration still apply. Declared
ESS, access-point/directory endpoints without the requisite portable POST
contract, substitution input keys and invalid length ranges fail explicitly.
Content-Type and byte-range fields on nonrecursive PUT still require `--post`.
This preserves ESS's existing PUT interface and differs from mc's always-POST
upload output.

`share list download` and `share list upload` are explicit zero-target offline
commands routed before runtime/context/credential resolution. They return only
unexpired entries, at most 100 per default page, with `--limit` 1..1000 and a
nonnegative numeric `--cursor`; another page supplies `NextCursor`. Default output
contains only id/action/bucket/key/version and creation/expiration metadata.
`--reveal` explicitly includes the signed URL or POST fields. Unlike mc's list
output, signed capabilities are never printed by default. Pagination is an offset
within a stable sorted current snapshot; concurrent changes can move offsets.

`shares.toml` uses invocation `--config-dir` or the existing config-directory
selection. The private store reuses Config.Store's symlink checks, exclusive
fail-fast writer lock, bounded TOML decoder/encoder, fsynced temporary file and
atomic rename. The directory is 0700 and file 0600; reads fail on broad file or
immediate-parent permissions, malformed history, symlinks and oversized files.
History is bounded to 1000 active entries and the existing 1MiB encoded limit.
A batch enters one locked atomic mutation only after every share succeeds;
capacity, byte-size and lock failures preserve the original file. No credentials
are stored separately from the signed capability: secret access keys are absent,
while access key ids and temporary session tokens necessarily remain inside
private signed URLs/forms. Errors omit capability payloads. The generated single
share itself still intentionally prints its capability through existing output.

Expired entries are hidden on list and pruned during the next successful history
mutation. Listing does not rewrite history, create missing state or contact a
provider. This differs from mc's load-time rewrite/prune; expired bytes can remain
in the private file until another successful share is recorded. The lock is not
stolen or automatically removed after another writer's crash. There is no history
revocation feature: deleting a local entry cannot revoke a provider-signed grant.

Validation: the initial seven tests failed before implementation. One successful
Sol repair round fixed nil boolean guards and corrected the new inventory fixture
to include the existing required completion marker. The final scoped six-file
suite passed 44 tests, including captured mc option effects, independent POST HMAC,
selected-version signing, unchanged GET/PUT defaults, offline listing, redaction,
expiry, permissions, symlinks, malformed/locked state, pagination, expired-entry
pruning, count/byte caps and incomplete recursive-inventory atomicity. Strict CLI
compilation and scoped formatting passed. No real provider, Windows/macOS or
minimum-runtime execution is claimed by this slice.

Follow-up review found that single-download `--limit` had been registered but
ignored. The scoped repair now rejects its presence with no recursive selection
or with explicit `--recursive=false`, before signing or any history mutation.
The regression covers negative, valid and oversized values and verifies no state
is created even with unsigned context. Validation of this follow-up is pending
the shared Mix ownership window. Other newly registered share flags were reviewed;
no other silently ignored invalid form was identified.

The retained stdlib-only `test/fixtures/s3/capture_sharing_mc.py` rejects any binary
whose SHA256 differs from the captured pinned executable, creates a private
temporary config, clears inherited MC/AWS configuration variables, starts the
loopback fixture, and cleans up subprocess/server/config resources. It writes only
normalized semantic output to the caller-selected destination. It was rerun with:

```sh
python3 apps/ex_storage_service_cli/test/fixtures/s3/capture_sharing_mc.py --mc /tmp/ess-cli-reference-m0/mc --output /tmp/ess-native-share-mc-recapture.json
```

All three recaptured semantic cases and the binary hash matched the checked-in
fixture; the capture timestamp is intentionally refreshed. The script retains
fixture glue from the original ad hoc capture, not any MinIO implementation.
