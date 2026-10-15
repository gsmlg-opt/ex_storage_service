# Bounded pinned executable differentials

On Linux x86_64, the actual AWS CLI 2.31.18 executable and mc commit
`77f82e18b5401a65958f1619df6ebb994634bd88` were run against private local socket
fixtures, with isolated config/home roots and fixture-only credentials. Their
binary SHA256 values were independently rechecked against `reference-lock.json`:

- AWS: `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`
- mc: `a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2`

`test/fixtures/differential/reference-wire.json` retains command arguments,
exit codes, separate stdout/stderr, literal request bodies/headers, and the
provider fixture responses. Temporary home roots become `@HOME@`; their forced
relative paths become `@RELATIVE_HOME@`. Authentication, date, host, user-agent
and SDK invocation headers are omitted. The retained ESS observations describe
the original captured executable, before the resulting fixes; regression tests
compare the current executable with the independent AWS/mc observations.

`executable_differential_test.exs` builds and copies an actual standalone escript,
then replays the fixture responses without requiring AWS, mc or network model
downloads in the test environment. It covers:

- Complex notification response structures, XML-to-model names, nested filters,
  Unicode/entity decoding and empty EventBridge configuration.
- Literal AWS multi-delete XML, escaped object/version identifiers, explicit
  CRC32C, governance bypass and requester-pays headers. The independently
  observed checksum is `A1U2lA==`. Mixed Deleted/Errors output remains a modeled
  successful `s3api` response, as in the pinned executable.
- Raw upload bytes under Content-Encoding gzip, literal encoded Unicode/S3 key,
  SHA256 checksum and payload hash, metadata and AES256 SSE headers.
- Ordered sync filters, protected exclusions, dry-run upload/delete selection,
  and exact relative local-path rendering.
- HEAD 403 during a high-level download: empty stdout, transfer status 1 and
  exact plain AWS fatal stderr with one configured attempt.
- Native listing selection and du totals against mc's two-object/eight-byte
  inventory, plus mc's `find --name '*.txt'` selection.

JSON objects are compared after decoding because member order has no JSON
meaning. Multi-delete XML and applicable checksum/metadata headers are compared
literally. Empty query values differ in spelling (`?delete` versus `?delete=`),
and notification PUT XML differs in member order and empty-element spelling;
those raw differences remain visible in the capture and are not represented as
literal wire equality. Native result envelopes and mc NDJSON are compared only
for the explicitly tested names, counts and byte totals. Transport-generated
headers, mc timestamps/local timezone, and untested fields are not parity claims.

The initial seven executable regressions produced three failures: AWS high-level
status/error presentation, relative source-path display, and native find's absent
name flag. After the coordinating source owner fixed those cases and compiled
them, all seven executable regressions passed in 5.3 seconds. The test run used
`mix test --no-compile test/ex_storage_service_cli/executable_differential_test.exs`
and built a fresh escript from those compiled modules. The original observations
are retained separately from this passing regression result.

This is a representative differential suite. It does not establish complete
AWS/mc compatibility, live provider support, every modeled operation/member,
hardware/account-dependent AWS cases, or platform portability. Live provider
validation and the complete inventory ledger remain separate acceptance work.
