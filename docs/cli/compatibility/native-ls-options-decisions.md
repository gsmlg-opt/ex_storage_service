# Native listing selectors and summaries

Native `ls` accepts `--incomplete`, `--summarize` and `--storage-class VALUE`.
Unflagged legacy invocations retain their output path. These options require a
remote bucket or prefix; they do not modify bucket inventory or object data.

`--storage-class` is an exact, case-sensitive filter. Empty or `*` accepts all
classes, and rows without a class remain eligible, including common prefixes
and delete markers. This follows pinned mc `ls.go:244–246`. Filtering precedes
the result limit. Summary counts include every retained row, following
`ls.go:257–271`, with zero bytes contributed by prefix/delete-marker rows.

ESS returns `Summary` with `Entries`, `Bytes` and `Complete`. It describes the
retained result window: an additional matching row sets `Partial=true` and
`Complete=false`. The default limit remains 1000, maximum 10000; page size is
1–1000. This bounded structured output differs from mc's text summary. Version
listing also supports the class filter and summary. Historical rewind rejects
these new options explicitly; their combined historical behavior remains open.

`--incomplete` selects ListMultipartUploads and preserves Key, UploadId,
Initiated and StorageClass plus common prefixes. Pagination uses both key and
upload markers; prefix/delimiter and recursive behavior remain literal. The API
does not report upload sizes, so summary `Bytes` is `null`. No fabricated zero,
ListParts, object HEAD, abort or write is used to fill that gap. Combining
incomplete uploads with versions or rewind fails before provider calls.

Ten new reproducing tests initially failed; the final listing/inspection/route
suites pass **57 tests, zero failures**. Real HTTP fixtures verify multipart
queries and paired markers; other tests cover filtering before limits, missing
sizes, version rows, NDJSON summaries, invalid uploads, later provider failures
and repeated tokens. Strict compilation and formatting pass. Source observations
are independent of ESS but do not claim actual mc executable output parity or
live provider acceptance. See `cli/native_ls_options_test.exs` under CLI tests.
