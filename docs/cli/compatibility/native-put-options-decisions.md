# Native put transfer controls

Native `put` supports `--part-size SIZE`, `--disable-multipart` and
`--checksum ALGORITHM` for local/stdin uploads to a remote target. Invalid
directions, conflicting checksum flags and invalid values fail before source or
provider access. Unflagged legacy defaults remain: this change does not apply
mc's default 16 MiB part size or four parallel workers to existing invocations.

Part-size accepts bounded SI/IEC byte text, commas and decimal fractions using
integer arithmetic with byte flooring. An explicit empty value selects mc's
32 MiB fallback; zero selects minio-go's 16 MiB fallback. Multipart starts above
the selected size, and parts use that exact size. Requests that would exceed the
provider part count fail rather than silently increasing the requested size.
ESS validates provider part bounds eagerly, including small single uploads;
the pinned SDK defers some checks. Exact floating-point edge behavior is not
claimed. Stdin retains private bounded-disk spooling and known-length upload.

Disabling multipart selects one PUT, independent of the threshold. An object
above the provider single-PUT limit fails before requests. Combining this flag
with an internal forced-multipart setting is invalid.

Checksum names are case-insensitive: CRC32, CRC32C, CRC64N/CRC64NVME, SHA1 and
SHA256 use existing bounded checksum and response-verification machinery.
Multipart parts and completion retain the selected algorithm; conditional
creation remains on completion. MD5 and CRC32-FO/CRC32C-FO remain explicitly
rejected. ESS uses precomputed modeled headers rather than promising mc's
trailer framing. Prefix/alias `--enc-*` selection remains open; direct AWS SSE
flags do not establish those native contracts.

Explicit `--parallel N` accepts 1 through 64 workers. Concurrent parts retain
the shared endpoint request and byte budgets, so admission may reduce effective
concurrency. Only the journal claimant records completed parts and checkpoints;
the completion manifest stays ordered even when responses arrive out of order.
The owned group drains workers and registered HTTP cancellation owners before
multipart abort. A stopped group still routes through owned-upload cleanup.
Borrowed invocation signals are observed without cancelling their owner. Disabling
multipart conflicts with requesting more than one part worker. The unflagged
legacy path stays serial; mc defaults to four and has no explicit upper bound.
The `-P` shorthand is not exposed. These are source-authoritative reference
observations, with actual ESS wire tests rather than an mc concurrency capture.

Pinned sources are mc `cmd/put-main.go:33–58,114–130,179–210`,
`cmd/flags.go:129–175`, `cmd/client-s3.go:1089–1105`, minio-go v7.0.90's
`api-put-object.go:337–363` and `api-put-object-common.go:85–116`, and
go-humanize v1.0.1's `bytes.go:110–148`. These source observations are separate
from actual mc executable acceptance.

The final exclusive scoped run passes **67 tests, zero failures**, seed 288806,
including sixteen new tests and existing conditional/planner/multipart/high-level
cases. Actual wire fixtures verify part boundaries for unit forms, checksum
headers/digests, provider checksum rejection, no-I/O validation, single-PUT
selection and the combination with conditional creation. Strict compilation and
formatting pass. Earlier shared-host journal collisions and a corrected query
fixture are retained in the private validation logs. Full provider/output/mc
parity is not inferred from these tests.

The initial parallel scoped suite passes 76 tests. Parent integration adds a
stopped-group regression: it first reproduced an uncaught exit from the group
reason lookup, then verifies socket drain, owned abort and released admission
after the exit is converted to a multipart failure. The complete current-source
acceptance is recorded in integration-validation.md.
