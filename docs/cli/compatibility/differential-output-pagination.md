# AWS output and pagination executable differential

The actual pinned AWS CLI 2.31.18 executable produced fourteen independent
`ListObjectsV2` captures against a two-page loopback peer. Both `text` and
`yaml-stream` cover full output without a query, `Contents[].Key`, the scalar
query `length(Contents)`, `--max-items 3` with no query, projected keys or
`NextToken`, and `--starting-token` resume.
The resume token comes from a separately retained AWS JSON truncation capture.
No ESS formatter, paginator or serializer supplies expected results.

The executable SHA256 is
`87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`;
fixture SHA256 is
`5cd3d386576416d8f88cffde82a0ab1c3347a0384761204bafe1d2f332a1dc8f`.
The fixture also pins the service model digest. Files relative to the CLI app:

- Capture and isolation instructions: `test/fixtures/aws_output_pagination/`.
- Frozen observations: `test/fixtures/aws_output_pagination/reference.json`.
- Replay: `test/aws_output_pagination_differential_test.exs`.

Replay compares stdout/stderr and exit status, literal HTTP method/path,
query name/value pairs including duplicate counts, stable headers and body
bytes, and the complete request sequence. Query ordering is insignificant;
opaque service-token values and requested page sizes are exact. Generated CLI
resume tokens compare as decoded JSON envelopes; only their JSON whitespace
and member order may vary, with all surrounding stdout bytes exact. Each owned
wire peer must terminate normally. Full-page cases retain service continuation and
non-result fields so projected queries cannot conceal pagination errors.

## Reproduction and validation

On source `d9dacc621b50082f892b5513c4412725d41b4772`, an isolated standalone
CLI copy with the published coordinated HTTP 0.21.12 graph strict-compiled.
The first replay ran **11 tests with three failures**, seed 628644, in 6.6
seconds. The other eight tests passed. Expanding to fourteen captured cases
produced **15 tests with six failures** in 8.6 seconds, at the same seed.
All wire-request assertions and owned peer shutdown checks passed before the
stdout comparisons. The observed mismatches were:

- YAML-stream full-page output drops `IsTruncated`, `KeyCount`, `Name` and
  `NextContinuationToken`, while adding absent `Prefix`/`RequestCharged` nulls.
- YAML-stream truncation with a query appends an extra `- null` item after
  the truncated service pages.
- Text full-page output omits AWS scalar placeholders: the first page has two
  rather than three `None` columns, and the second page omits its `None` line.
  The same placeholders are missing with a no-query maximum-item limit.
- YAML-stream no-query truncation retains the same field-loss problem and adds
  a resume-token record. Its `NextToken` query adds a third item. AWS emits only
  the two service pages, or two null query results, without a resume-token item.

These are local AWS presentation/pagination mismatches, separate from upstream
HTTP #131/#132. The captures require only small GET responses and no live AWS
account. This acceptance slice owns only its tests, captures and documentation;
the production correction was made separately.

The final production correction retains raw YAML-stream service pages, restores
missing text result-key placeholders, and suppresses YAML-stream resume-token
records while preserving text query behavior. Testing that `cli/api.ex`
(SHA256 `95b32f609ca11b816d45020efea04a57bc15cc9198b7aaa27a001d43e08c159f`)
in the same isolated graph passed **1,085 tests with zero failures**, seed
628644, in 28.2 seconds. The scope includes all fourteen new cases, the existing
executable paginator matrix, and paginator, output, presenter, output-session
and query tests. Strict compilation and scoped format checks also passed.
An independent repeat of all fourteen AWS captures was byte-identical.

Evidence root: `/tmp/ess-output-pagination-20261016/`, including
`harness.json`, `mix.lock`, `initial-test.log`, `expanded-baseline-red.log`,
`corrected-compile.log`, `corrected-scoped-test.log` and
`reference-expanded-repeat.json`.
This bounded fixture does not establish every output/paginator/query
combination, live-provider semantics, platform parity or release acceptance.
