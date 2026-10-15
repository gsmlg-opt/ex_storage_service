# AWS move underlying-path validation

`aws s3 mv --validate-same-s3-paths` and the exact case-insensitive environment
value `AWS_CLI_S3_MV_VALIDATE_SAME_S3_PATHS=true` activate a bounded read-only
preflight for matching source and effective destination keys. Access-point ARNs
and aliases resolve through S3 Control `GetAccessPoint`; plain aliases first use
STS `GetCallerIdentity`. MRAP ARNs resolve all region buckets through paginated
`ListMultiRegionAccessPoints`. Outposts aliases require an ARN. Equal underlying
paths stop before transfer dispatch with status 252. Ordinary literal path overlap
continues through the shared overlap guard. Native moves retain their own contract.

The preflight reads at most 1 MiB per response and 1024 MRAP pages, rejects repeated
pagination tokens, and fails closed on incomplete or malformed lookup results. It
uses the invocation's credential owner, TLS settings, retry policy, cancellation,
and coordinator reservation. It adds no general S3 Control command namespace.

## Independent reference evidence

The pinned AWS CLI 2.31.18 executable has SHA256
`87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
The source revision is `f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc`.
Reference behavior is defined in `awscli/customizations/s3/subcommands.py`
(`_validate_path_args` and underlying-path methods) and
`awscli/customizations/s3/utils.py` (`S3PathResolver`, lines 705–850).

`test/fixtures/differential/capture_same_paths.py` runs that executable through an
answering loopback proxy. `same-paths-wire.json` records 21 actual exchanges,
stdout, stderr, statuses, signing scopes, and signed-header sets. Its SHA256 is
`1ac223ee4085bb0c54a8d23585e5892967acb4cab0ee8ee27af7add067e91883`.
`ExecutableSamePathsTest` replays every exchange through a private subprocess DNS
map and independently recomputes each captured request's SigV4 HMAC.

The captures cover access-point destination and source-region lookups, STS alias
resolution, Outposts ARN and region errors, Outposts alias rejection, two-page
MRAP lookup, missing MRAP aliases, environment enablement and whitespace rejection,
different keys and buckets, denied access-point requests, nested STS denied/invalid-token
errors, required MRAP checksums, and environment/profile FIPS,
dualstack, and service endpoint settings. Separate executable safety tests cover
pagination cycles, missing bucket metadata, response bounds, and invalid accounts.

## Endpoint and wire decisions

S3 Control and STS use their locked upstream endpoint-rule assets. The data
`--endpoint-url` is independent. Configured service endpoints follow environment
service URL, environment global URL, profile service URL, then profile global URL;
`AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` suppresses these configured values. Custom
endpoints suppress FIPS and dualstack, matching `botocore/args.py`.

S3 Control uses `AWS_ENDPOINT_URL_S3_CONTROL`. Regular access-point ARNs supply
name and account; their embedded region does not replace the source/global region.
Outposts rules select `s3-outposts` signing and enforce ARN region policy; the
captured custom endpoint preserves the percent-encoded complete ARN in the request
path. STS signs `sts` and omits `x-amz-content-sha256`. MRAP requests retain opaque
page tokens and send the empty CRC64NVME checksum even when checksum calculation
is configured `when_required`: the pinned S3 Control model marks
`ListMultiRegionAccessPoints` with `httpChecksumRequired: true`. STS
`ErrorResponse` envelopes preserve the nested service code/message, HTTP status,
and request ID through the existing redaction boundary.

Without validation, potentially matching special paths emit the pinned warning.
AWS remote single-object dry runs still issue a source `HeadObject` request before
printing the operation; the shared AWS frontend now preserves that request.
