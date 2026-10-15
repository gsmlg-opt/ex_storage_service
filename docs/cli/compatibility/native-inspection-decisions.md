# Portable native inspection

Native inspection adds selected raw reads, directory projections, prefix size
summaries, and bucket/prefix/version metadata inspection. It sends only S3 GET
and HEAD requests. It does not add provider capabilities or modify objects.

## Pinned executable observations

The actual mc executable at revision
`77f82e18b5401a65958f1619df6ebb994634bd88` was exercised against a loopback S3 peer
in sixteen JSON and sixteen text invocations. The peer exposed four objects:
`top.txt` (5 bytes), `a/one.txt` (7), `a/two.bin` (11), and `a/b/deep.txt` (13),
plus two immutable data versions per key. Raw reads used a separate sixteen-byte
fixture. Relevant source contracts are `cmd/cat-main.go`, `tree-main.go`,
`du-main.go`, `stat-main.go`, and `head-main.go` in the pinned source tree.

| Selection | Actual mc requests and result |
| --- | --- |
| `cat --offset 4` | HEAD, then GET `Range: bytes=4-`; exact suffix `456789abcdef`. |
| `cat --tail 4` | HEAD, then GET `Range: bytes=12-`; exact suffix `cdef`. |
| `cat --part-number 2` | HEAD, then GET `?partNumber=2`; exact part `89abcdef`. |
| `tree` text | Delimiter listings for every directory; directories `a` and `b`, with no files by default. |
| `tree --files --depth 1` text | Root files, child-directory files, and boundary directory `b`; no deeper file. |
| `tree --depth 0` | Exit one before I/O; default -1 means unlimited depth. |
| `tree` JSON variants | All raw file rows; files/depth flags did not project this output format. |
| `du` | Complete flat list; 4 objects and 36 bytes. |
| `du --recursive` | Delimiter traversal; prefix totals 13, 31, and 36 bytes in postorder. |
| `du --recursive --depth 1` | Root total only; 36 bytes. |
| `du --versions` | ListObjectVersions; 8 versions and 72 bytes. |
| `stat BUCKET/` or `PREFIX/` | Immediate delimiter listing, HEAD each immediate object, and folder rows. |
| `stat --recursive` | Flat listing, then HEAD each object. |
| `stat --versions OBJECT` | Version listing, then HEAD with each immutable version ID. |
| `stat --recursive --versions PREFIX/` | Flat version listing, then HEAD each selected immutable data version. |
| `head --n 2` | GET only. mc appended a newline to a fixture without one. |

The peer and its thread were shut down after both captures. These are reference
client observations against fixtures, not live AWS or MinIO infrastructure proof.

## Selected raw reads

`cat` accepts nonnegative `--offset` and `--tail` byte counts. Zero leaves that
selection inactive. Positive offset and tail conflict; `--part-number` accepts
1 through 10000 and conflicts with either positive byte selection. Offset must
be smaller than a nonempty object's size. Oversized tails select the whole
object. A selected read obtains size and ETag through HEAD, pins the returned
version when available, sends If-Match, and rejects a response that ignores the
requested range or returns a changed identity before delivering entity bytes.

Part reads HEAD the same selected part first and require its Content-Range,
Content-Length, and part count. The GET must return 206 with the same range,
consistent part count and entity length, and matching pinned object identity.
This adds verification beyond mc's whole-object HEAD. A provider that cannot
confirm part metadata fails explicitly instead of streaming an unverified part.
The transport retains exact raw and Content-Encoding bytes. Streams and sinks
remain bounded and keep their existing cancellation and ownership contracts.
Whole-object and composite checksum declarations do not verify a selected
range/part; the engine reports `partial_not_verified` for that checksum surface
while validating selection and entity length. It never reinterprets a whole
object checksum as a checksum of a partial byte sequence.

`head --n N` and `head -n N` map to the existing `--lines` behavior. Cat, head,
and stat accept `--vid` as an alias for `--version-id`. Conflicting alias values
fail rather than choose silently. Head preserves exact returned bytes and does
not append mc's synthetic newline. Existing single-object reads remain on the
shared transfer engine.

## Bounded tree, summaries and metadata

`tree --depth N` and/or `--files` select the portable projection. Files are
optional; depth -1 is unlimited and positive N permits file parents through N
levels plus boundary directories. Depth zero fails. The same projection applies
to every ESS output format, unlike mc's JSON bypass. Existing tree invocations
without either new option retain the legacy output contract.

`du --recursive` reports totals for the root and descendant prefixes.
`--depth N` is root-inclusive: depth one reports only the root; an explicit
positive depth also enables descendant summaries without `--recursive`. Zero
leaves recursive depth unlimited; this is the ESS bounded projection convention.
`--versions` enumerates version records. Delete markers contribute neither bytes
nor object counts and are never downloaded. Prefix
summary output is capped independently from complete inventory totals. Results
retain exact matched counts and `Partial` when rows are omitted. The existing
simple limited `du` form retains its earlier bounded-summary contract.

Tree and recursive summaries derive sorted records into private disk manifests
and aggregate adjacent groups. They hold one group plus the configured output
limit, rather than a map proportional to all remote prefixes. Tree deduplicates
projected nodes. Ordering is deterministic prefix order rather than mc's
postorder summary output. Relative projections never change the original keys
used in S3 requests.

`stat OBJECT` keeps the existing HEAD response shape. Bucket and slash-suffixed
prefix targets enumerate immediate objects and folders; recursive mode lists all
descendants. A missing exact object may fall back to prefix inspection. Version
mode selects immutable data versions; delete markers remain explicit metadata
rows and never cause a failing object HEAD. An explicit version selects one
object and conflicts with recursive/all-version inspection. HEAD responses must
confirm requested immutable version IDs.

All listing pages must finish successfully before metadata HEADs begin. A
later-page failure is an error and cleans private manifests. `--limit` defaults
to 1000 and permits 0 through 10000; omitted stat rows do not cause HEAD requests.
Page size remains 1 through 1000. Every capped result explicitly reports its
returned and matched counts. Folder metadata is derived from listing prefixes;
ESS does not fabricate mc's current-time folder modification timestamp.

## Historical inspection

`cat`, `head`, `ls`, `stat`, `du`, and `tree` accept `--rewind`. Find does not
expose this flag in the pinned mc command. The shared selector accepts bounded
UTC dates, RFC3339 timestamps, and nonnegative duration strings. It selects
modification times strictly before the cutoff, preserving nanosecond cutoff
precision against S3 microsecond timestamps. A version equal to the cutoff is
excluded. Date-only values use UTC in ESS; mc interprets date-only values using
its host timezone. Use an explicit RFC3339 offset for an unambiguous cross-client
instant. Rewind conflicts with an explicit version identifier.

All pages of ListObjectVersions finish into a private sorted manifest before
selection or metadata HEADs. Cat and head choose the newest prior data version;
a selected delete marker means the object did not exist. The selected version is
HEADed and its ETag, size, modification time, and VersionId must equal the listed
record. This also fences mutable `null` versions. The GET sends VersionId plus
If-Match and must confirm both before the first raw byte reaches stdout. Full
reads retain expected-length and checksum validation; selected ranges retain
their partial-read contract. Missing or changed historical identity fails
explicitly. ESS makes this verifying HEAD in addition to mc's version-list/GET.

Historical ls/stat inventory uses one latest prior record per key, retaining
selected tombstones as explicit metadata rows; historical tree projects only
selected data objects. These new bounded snapshot forms use flat literal-prefix
inventory before applying tree projection or output caps. They do not claim
mc's text/JSON envelopes or delimiter traversal. `--versions --rewind` lists all
version and marker records strictly before the cutoff. Historical stat HEADs
only returned data rows and verifies them against their listed identity.

Pinned mc's `du --rewind` totals **all older data versions**, even without
`--versions`; it does not total only the latest prior snapshot. ESS follows that
observed behavior, excludes every marker, and preserves complete totals when
prefix output is capped.

The actual pinned executable was run in nine historical cases using a disposable
loopback peer with future/equal/prior versions, a selected tombstone, an older
version beneath that tombstone, and a nested key. The reproducible source and
capture are `test/fixtures/differential/capture_native_rewind.py` and
`native-rewind-wire.json` in the CLI app. Executable identity:

- Source revision: `77f82e18b5401a65958f1619df6ebb994634bd88`.
- Executable: `mc version DEVELOPMENT.GOGET`, Go `1.26.6 linux/amd64`.
- Executable SHA256: `a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2`.
- Capture timezone: `Asia/Shanghai`; absolute test cutoffs include `Z`.

Reproduce from the CLI app with:

```sh
python3 test/fixtures/differential/capture_native_rewind.py \
  --mc /path/to/pinned/mc --output /tmp/native-rewind-wire.json
```

The script checks the executable digest, strips ambient provider/proxy settings,
uses a fresh temporary mc config, and shuts down its peer in `finally`. The
captured cases assert strict timestamp equality, exact historical GET identity,
tombstone listing, and the 18-byte/three-data-version DU result. Loopback URLs
contain the captured ephemeral port; they are fixture evidence, not production
endpoint assertions.

## Validation

The final focused validation passed **79 tests** across NativeInspection,
HighLevel, Engine, VersionSelection, Parser, and Runner. Wire cases cover
range/part reads, immutable and `null` IDs, ignored selections, changed
identities before stdout, raw head output, bucket stat, historical cat/head,
and actual Runner tree/rewind routes. Module cases cover projected depth,
deduplication, complete capped totals, explicit-depth DU, marker-free DU counts,
selected versions/tombstones, historical equality and drift, malformed metadata,
later-page errors, cancellation, and manifest cleanup. The retained pinned
rewind capture is audited by the scoped tests. Strict CLI compilation,
formatting, and diff whitespace checks passed.

Live provider, HTTP/2, and cross-platform inspection acceptance remain separate
evidence. No fixture capture claims these surfaces are verified.
