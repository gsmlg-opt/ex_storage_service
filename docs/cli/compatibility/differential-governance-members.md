# Object retention and legal-hold optional members

This extends the optional-member executable matrix with ten actual AWS CLI
observations for `PutObjectRetention` and `PutObjectLegalHold`. The current
fixture contains 52 cases across 27 operations. The original 42 observations
were recaptured without changes. The historical optional-member report retains
its original 42-case scope.

## Reference and capture

- AWS CLI: `2.31.18`, independently executed on Linux x86_64.
- Executable SHA256:
  `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
- Locked model SHA256:
  `6be3b05aac4ff8719e9a30a376de413523e9eab28a248195c8ab1cf9baacac80`.
- Expanded fixture SHA256:
  `4157f20345468174ad1fdf726ab5b1ea2c682d58f05f2c8a38198ecec63f927c`.
- Capture: `apps/ex_storage_service_cli/test/fixtures/differential/capture_optional_members.py`.
- Fixture: `apps/ex_storage_service_cli/test/fixtures/differential/optional-members-wire.json`.
- Test: `apps/ex_storage_service_cli/test/ex_storage_service_cli/executable_optional_member_matrix_test.exs`.

Capture checks the executable digest before running. Every request uses a
literal loopback endpoint, static fixture credentials, cleared provider/proxy
environment variables, disabled metadata credentials, one attempt, and temporary
files. Every peer is closed in `finally`. No live AWS account or storage service
is involved. Requests and outputs come from the pinned executable, not ESS.

## Added cases

| Operation | Cases and observed effects |
| --- | --- |
| PutObjectRetention | GOVERNANCE plus retain-until timestamp, version ID, expected owner, requester payer, SHA256 and bypass `true`; COMPLIANCE plus timestamp and bypass `false`; omitted payload; empty Retention structure; AccessDenied |
| PutObjectLegalHold | Nested status ON and OFF with version ID, expected owner, requester payer and SHA256; omitted payload; empty LegalHold structure; AccessDenied |

Populated requests use the key `governance/雪 % +?#/key` and opaque version ID
`version+/=雪`. The captures retain their exact encoded path/query values and
stable headers. The retention timestamp with `+00:00` normalizes to the AWS wire
form ending in `Z`. Explicit bypass `false` remains a header with the value
`false`; omitted bypass, payer and owner headers remain absent.

For both operations an omitted payload is zero bytes with Content-Length 0,
CRC64NVME `AAAAAAAAAAA=`, and the SHA256 of an empty entity. An explicit empty
structure instead produces a 61-byte namespaced XML root in AWS. ESS may spell
empty tags differently; XML structure and each executable's checksums are
compared independently. Neither representation is substituted for the other.

Charged responses decode to `{"RequestCharged":"requester"}`. Responses without
that header produce empty CLI output and an empty typed result. The raw-client
checks retain HTTP status, request ID and response headers. AccessDenied cases
compare AWS exit 254 and stderr, then separately assert ESS's typed service
category, code, HTTP 403 status, operation, request ID, bucket region and service
message. These are synthetic protocol responses, not evidence that AWS accepts
every empty configuration as a valid retention policy.

## Validation and discovered implementation mismatch

The first expanded scoped run on an independent published HTTP 0.21.12 graph
reported **57 tests, three failures**, seed `628644`, in 25.3 seconds. Two
executable cases fail for omitted retention/legal-hold payloads; the additional
typed-success loop reaches the same failure. Other captured executable cases,
including empty structures and both new AccessDenied cases, pass.

AWS sends the empty body and succeeds against the supplied response. ESS instead
returns `:checksum_source_required` with `Request checksum body is not replayable`
before making a request. `S3.Encoder.body/3` represents an omitted modeled payload
as `nil`; both operations require a request checksum, but the original
`S3.RequestChecksum.digest/3` only accepted a binary or captured source. Empty XML
structures remain binary bodies and do not hit this path. The production fix
checksums `nil` as an empty entity after the captured-source guards, preserving
the distinct omitted and empty-structure representations.

The independent graph then passed the complete optional-member matrix plus
`s3/request_checksum_test.exs` and `s3/upload_checksum_test.exs`: **94 tests,
zero failures**, seed `628644`, in 25.4 seconds. Formatting checks passed for the
matrix, production checksum module and both checksum test files. Existing tests
still preserve caller-supplied checksum headers, reject nonreplayable sources
without consuming them, validate provider digests, and reject source mutation or
cancellation. No optional-input rule, integrity check or expected AWS result was
waived.

The recorded red log is
`/tmp/ess-cli-governance-members-20261016/optional-member-red.log`; the green log
is `/tmp/ess-cli-governance-members-20261016/optional-member-green.log` and
reference capture output is `/tmp/ess-cli-governance-capture.log`. This bounded
evidence does not establish live Object Lock policy enforcement or exhaustive
optional member coverage.
