# Native find predicates and version inspection

Native `find` uses the shared complete, paginated, disk-backed inventory. It
applies ordered include/exclude rules to original relative object names, then
projects depth and applies the selected name/path/ignore, regex, size, and time
predicates. It retains at most `--limit` result rows in memory while counting all
matching records. The result explicitly includes `Returned`, `MatchedObjects`,
and `Partial`; a capped result does not claim exhaustive output. Depth limits
control displayed paths, not the number of remote listing requests.

The implementation was checked against mc commit
`77f82e18b5401a65958f1619df6ebb994634bd88`, especially `find-main.go`, `find.go`,
`duration.go`, and the time comparison helpers. A local HTTP peer and the pinned
mc executable independently confirmed the following object-selection behavior.
No MinIO implementation was copied into ESS.

## Portable predicates

- `--name` matches a basename glob or an exact literal path component, matching
  mc's component fallback. Existing ordered `--include`/`--exclude` filters still
  apply to original full relative names. `--path` matches the relative path and
  `--ignore` excludes matching relative paths. These use ESS's existing bounded,
  non-backtracking glob engine (`*`, `?`, and its documented character classes).
  Full Go `filepath.Match` escaping/class semantics are not claimed.
- `--larger` and `--smaller` compare strictly greater/less than the parsed byte
  value. Decimal quantities and case-insensitive metric/IEC suffixes are
  supported, including `1KB`, `1kiB`, and `1.5m`. Fractional bytes truncate. A zero
  threshold disables that size predicate, matching the pinned implementation.
  Negative values, unknown units, and unsigned 64-bit overflow fail before I/O.
- `--older-than` includes modification times at or before its cutoff;
  `--newer-than` includes times strictly after its cutoff. The invocation freezes
  one current time for all records. Compound signed decimal durations support
  `ns`, `us`, `µs`, `μs`, `ms`, `s`, `m`, `h`, `d`, `w`, and `y`, where days are
  24 hours and years are 365 days. Durations are bounded by signed 64-bit
  nanoseconds. Absolute RFC3339 timestamps, dotted dates/times such as
  `2026.10.14T12:00:00`, and unambiguous UTC forms are supported. Bare dates/times
  use UTC. Arbitrary timezone abbreviations and local-zone guessing are rejected.
  Inventory timestamps have microsecond precision. A missing/unknown timestamp
  fails explicitly when a time predicate needs it.
- `--maxdepth N` projects a relative path to at most N components, retaining a
  trailing separator for projected prefix nodes. Zero means no projection. Each
  original matching object retains a row, including repeated projected prefix
  nodes. Predicates run on projected names while size/time remain those of the
  original object. This is a path projection, not a skip of deeper objects or a
  deduplicated directory traversal. Depth and output limits are bounded at 10,000.

For example, the pinned executable received four objects below `p/`:
`top.txt`, `a/one.txt`, `a/two.bin`, and `a/b/deep.txt`. With `--maxdepth 1`, it
returned `top.txt` and three `a/` rows. With `--maxdepth 2`, it returned `top.txt`,
`a/one.txt`, `a/two.bin`, and `a/b/`. With `--maxdepth 1 --name '*.txt'`, only
`top.txt` matched. ESS preserves those projected names and selection effects,
with the existing deterministic inventory sort order.

The pinned mc path matcher has a trailing-slash anomaly: it appends another
separator to a relative target that already ends in `/`, so a relative
`--path 'a/*'` pattern can instead be tested against the full alias path. ESS
consistently matches relative paths regardless of target trailing slash; this is
an explicit compatibility difference. ESS also retains the existing file/object
inventory contract: it does not synthesize every local directory as a find row,
or silently omit all GLACIER-class objects.

## Bounded regex subset

`--regex` supports common Unicode RE2/PCRE syntax through Erlang's regex runtime,
including literals, ordinary groups/alternation, character classes, repetition,
anchors, and the common inline `i`, `m`, `s`, and `U` flags. Pattern input is
limited to 4,096 bytes. Backreferences, lookaround, recursive/conditional groups,
PCRE control verbs, possessive quantifiers, and other excluded control constructs
fail explicitly. This is a bounded common subset, not a full RE2 implementation
or a claim of complete semantic equivalence.

Each match has a 100,000-step match limit and a 1,000-level recursion limit.
Exhaustion returns `find_regex_budget`, stops the command, and cleans its private
manifest; it never silently treats exhaustion as no match. Tests exercise a
catastrophic nested-quantifier pattern and assert the typed failure. Invalid,
oversized, and excluded patterns fail before any inventory request. No new regex
dependency or unbounded external process is introduced.

## Versions and result contract

`--versions` uses `ListObjectVersions` with complete pagination and retains each
immutable version ID and delete-marker identity. For this mode, `Entries` are
structured records rather than plain strings: the original `key`, `version_id`,
`delete_marker`, `size`, `etag`, relative `object_name`, and `modified` microseconds
remain available; `name` contains the projected display path. Two versions of one
key never collapse into one record. Time filters can select individual versions.
Later-page failure is an error, not successful partial completion. Local
`--versions` fails explicitly because local files have no portable S3 version
history. Existing native `stat --version-id` and `ls --versions` continue to use
their existing contracts.

Current-object `Entries` remain relative strings. ESS returns its established
structured summary rather than mc's per-record JSON envelope. Shell execution
(`--exec`), custom formatting/presigning (`--print`), MinIO server metadata/tag
regex search, alias-wide multi-bucket traversal, no-target current-directory
defaulting, and event watching are outside this bounded slice and remain
excluded/rejected. Notification configuration is not an event consumer.

## Evidence and limits

`test/ex_storage_service_cli/cli/find_test.exs` covers combined predicates,
projection duplicates and ordering, strict size/time boundaries, duration and
absolute-time formats, Unicode regex and budget failures, capped output,
version/delete-marker pagination and timestamps, later-page errors, local files,
and private-manifest cleanup. It also exercises the HighLevel dispatch and proves
the prior `du` summary behavior remains intact. Existing HighLevel wire tests
retain the basename selection contract.

The focused Find, HighLevel, and inventory-policy suites pass 39 tests in the
standalone CLI application. Strict compilation and scoped formatting pass. The
pinned executable probes used local files and a loopback S3-shaped HTTP peer;
there is no claim of live AWS/MinIO search, live version history, cross-platform
acceptance, complete mc output parity, or completed executable Registry/Runner
integration from these module tests alone.
