# Native pipe option contract

Scope: ESS `9b8661e` plus the implementation diff, with pinned mc
`77f82e18b5401a65958f1619df6ebb994634bd88` reviewed in `cmd/pipe-main.go`.
This is a scoped safety/effect correction, not complete mc pipe parity.

The previous native pipe registration inherited the entire AWS cp option set.
`--dryrun` parsed successfully, yet dispatch unconditionally spooled stdin and
uploaded it. Source-only switches could also parse and disappear without effect.
The agreed red test actually observed an stdin IO request under dry-run; another
source-only option reached a refused loopback provider.

Dry-run now emits the existing native dry-run upload line and returns before
Spool.with_source, stdin reads, private staging creation or upload. Unsupported
options fail in the native parser before runtime/provider resolution; the shared
handler also rejects their presence before decoding source-key files or touching
stdin. Expected-size syntax is validated before spooling; a valid but mismatching
size still fails after capture and before any upload, with cleanup.

Registry now explicitly permits only options with applicable effects: dryrun,
quiet/only-show-errors, expected-size, no-progress/progress-frequency/
progress-multiline, ACL/grants, destination SSE/SSE-C/KMS settings, storage class,
website redirect, content type/encoding/language/disposition, cache control,
requester-pays, checksum algorithm and metadata. The coordinating agent narrowed
the Registry; this slice formatted that approved edit.

Source-side SSE-C, source-region, filters, symlink/MIME source policies, listing
page size, Glacier controls, retrieval checksum mode and copy properties/directives
are rejected for pipe. Direct handler calls also reject recursive/version/rewind
selection. Rejection is based on explicit presence; flags are not accepted as
inert defaults. Native pipe does not acquire AWS s3 command defaults.

Retained upload options flow through existing HighLevelInput and modeled PutObject
fields. The upload remains the existing bounded, cancellable, replayable disk
spool followed by the shared single/multipart engine. Progress now uses the shared
Progress.with callback, so suppression, frequency, multiline and quiet settings
have effects on stderr. Object bytes remain unchanged and do not become progress
output. No new transport, dependency, credential source or public API was added.

Pinned mc instead streams an unknown-size body, exposes concurrent/part-size
controls, metadata/tag/encryption/checksum spellings, and allows zero targets to
copy stdin to stdout. ESS's explicit one-object private spool, retained AWS-style
option spellings and native dry-run extension remain deliberate current contracts.
Missing portable mc option mappings remain open; narrowing the supported grammar
does not claim their implementation or complete reference compatibility.

Validation: initial focused run had seven tests and six failures, including the
actual unsafe dry-run read and missing option/progress effects. Existing upload
assertions exposed test-owned StringIO cleanup races; fixtures now close the
owned process synchronously in try/after around each use. The initial fixture
repair passed the adjacent suite, but a cleanup simplification exposed that unlink
does not retain StringIO after its owner exits; the second fixture repair uses the
synchronous owner boundary. After the implementation and first repair, the six-file scoped suite passed 61 tests with zero failures.
Strict CLI compilation passed. A final focused run and scoped format check passed
after simplifying fixture cleanup. Logs are `/tmp/ess-native-pipe-red.log`,
`/tmp/ess-native-pipe-scoped.log` and `/tmp/ess-native-pipe-final.log`.

The tests count stdin requests, trace the spool's File.mkdir call boundary, assert
no loopback request/state, exercise Runner parser rejection with a configured AWS
credential_process marker, verify raw binary bytes plus retained header/metadata/
ACL/grants/requester/checksum fields, exercise all three destination SSE forms,
and verify progress/quiet and expected-size failure cleanup. These are local
handler/wire fixtures, not real-provider or non-Linux acceptance.
