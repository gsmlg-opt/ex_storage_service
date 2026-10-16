# Independent AWS s3api operation wire matrix

This matrix audits all 108 operations in the pinned AWS CLI 2.31.18 model. The actual pinned executable produced 223 cases covering 107 operations: success and AccessDenied for each, six automatic-versus-explicit listing-encoding cases, and three RenameObject token/default/retry cases. The latter contribute five exchanges. WriteGetObjectResponse is explicitly excluded from this loopback-IP matrix because of its modeled host-prefix injection. Its separate [controlled-hostname executable fixture](differential-write-response.md) captures binary success, forwarded error and service AccessDenied exchanges; it is not counted among this matrix's 107 operations.

## Evidence boundaries

The capture generator uses the pinned model to construct required inputs and bounded synthetic typed responses. Expected requests and parsed outputs come from executing the independently pinned AWS binary, not from the ESS serializer or decoder. The replay test requires no AWS installation or network access beyond its local wire peer. It exercises real escript dispatch, signing/header bindings, XML requests, output formatting, error/status behavior, and output files.

These fixtures do not establish exhaustive optional-member coverage, service-side validity of every synthetic configuration, authorization correctness, or live cloud behavior. CreateSession/RenameObject/ListDirectoryBuckets use a custom standard loopback endpoint and static fixture credentials; their S3 Express session/endpoints remain separate acceptance work. SelectObjectContent receives independently encoded Records and End EventStream frames with CRCs, and its output file is compared byte-for-byte. GetObject/GetObjectTorrent similarly verify binary bytes 0–255.

The test preserves literal AWS request method/target/headers/base64 body and exact stdout/stderr/status in the fixture. Assertions require literal method/path, equivalent ordered query pairs, matching modeled header values, and identical parsed output. XML comparison uses Python ElementTree's captured semantic tree: distinct sibling-field ordering and empty-element spelling may differ, while repeated-list order, text, attributes, and structure must match. Request Content-Length and SHA256 are validated against actual bytes. Checksums retain their literal reference values; algorithm/header presence is compared when semantically equivalent XML has different bytes, with independent checksum-vector suites supplying algorithm evidence. stderr permits only surrounding whitespace differences.

## Reproduction

AWS executable SHA256: `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
Fixture SHA256: `c437b8532d19a00190e430f5140924351af180d3bf790c10fd20098f8f3087a5`.

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_operations.py \
  --aws /path/to/pinned/aws --extra-list-encoding \
  --output /tmp/operation-wire.json
```

The generator verifies the executable digest before launching it, clears inherited AWS/ESS/MC settings, supplies only fixture credentials, disables metadata discovery, bounds subprocess/connect/read timeouts, and starts only loopback peers. It does not repurpose HOME. Optional `--ess /path/to/private/escript` captures initial differential diagnostics; those observations do not become the oracle.

Replay: `mix test test/ex_storage_service_cli/executable_operation_matrix_test.exs` from the standalone CLI app. The fixture checksum and union of captured/excluded operations are asserted. Test names are `pinned AWS <Operation> <scenario>`.

## Independently observed listing rules

Automatic URL encoding requests decode literal `%252F` exactly once to `%2F`, preserve plus signs encoded as `%2B`, and decode UTF-8 keys, delimiters, CommonPrefixes and operation-specific markers. Explicit `--encoding-type url` preserves the encoded output. Version IDs and continuation tokens stay opaque. AWS's V1 ListObjects automatic customization leaves Prefix encoded while V2 and ListObjectVersions decode it; the literal oracle captures this distinction.

## Operation audit

Pinned botocore generates a UUIDv4 for omitted `RenameObject.ClientToken`.
Actual AWS captures confirm omission, an omitted-token 503-to-200 retry and an
explicit-token retry. ESS now generates once before the retry loop and preserves
the token across retries; independent invocations receive distinct tokens.
The replay checks UUID shape and per-invocation stability instead of literal
random values. Two module regressions and three executable cases reproduced
the missing behavior. The 23-test customization/retry/client suite and five
selected RenameObject executable cases pass. Other operation captures are
unchanged. Directory-session authentication and live rename remain separate.

Every row below identifies executable fixture coverage, not an implementation or live-service completion claim.

| Operation | Request/parsed success | AccessDenied | Additional scope |
| --- | --- | --- | --- |
| AbortMultipartUpload | Captured | Captured | Bounded synthetic response |
| CompleteMultipartUpload | Captured | Captured | Bounded synthetic response |
| CopyObject | Captured | Captured | Bounded synthetic response |
| CreateBucket | Captured | Captured | Bounded synthetic response |
| CreateBucketMetadataConfiguration | Captured | Captured | Bounded synthetic response |
| CreateBucketMetadataTableConfiguration | Captured | Captured | Bounded synthetic response |
| CreateMultipartUpload | Captured | Captured | Bounded synthetic response |
| CreateSession | Captured | Captured | Loopback standard endpoint/auth only |
| DeleteBucket | Captured | Captured | Bounded synthetic response |
| DeleteBucketAnalyticsConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketCors | Captured | Captured | Bounded synthetic response |
| DeleteBucketEncryption | Captured | Captured | Bounded synthetic response |
| DeleteBucketIntelligentTieringConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketInventoryConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketLifecycle | Captured | Captured | Bounded synthetic response |
| DeleteBucketMetadataConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketMetadataTableConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketMetricsConfiguration | Captured | Captured | Bounded synthetic response |
| DeleteBucketOwnershipControls | Captured | Captured | Bounded synthetic response |
| DeleteBucketPolicy | Captured | Captured | Bounded synthetic response |
| DeleteBucketReplication | Captured | Captured | Bounded synthetic response |
| DeleteBucketTagging | Captured | Captured | Bounded synthetic response |
| DeleteBucketWebsite | Captured | Captured | Bounded synthetic response |
| DeleteObject | Captured | Captured | Bounded synthetic response |
| DeleteObjectTagging | Captured | Captured | Bounded synthetic response |
| DeleteObjects | Captured | Captured | Bounded synthetic response |
| DeletePublicAccessBlock | Captured | Captured | Bounded synthetic response |
| GetBucketAccelerateConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketAcl | Captured | Captured | Bounded synthetic response |
| GetBucketAnalyticsConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketCors | Captured | Captured | Bounded synthetic response |
| GetBucketEncryption | Captured | Captured | Bounded synthetic response |
| GetBucketIntelligentTieringConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketInventoryConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketLifecycle | Captured | Captured | Bounded synthetic response |
| GetBucketLifecycleConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketLocation | Captured | Captured | Bounded synthetic response |
| GetBucketLogging | Captured | Captured | Bounded synthetic response |
| GetBucketMetadataConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketMetadataTableConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketMetricsConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketNotification | Captured | Captured | Bounded synthetic response |
| GetBucketNotificationConfiguration | Captured | Captured | Bounded synthetic response |
| GetBucketOwnershipControls | Captured | Captured | Bounded synthetic response |
| GetBucketPolicy | Captured | Captured | Bounded synthetic response |
| GetBucketPolicyStatus | Captured | Captured | Bounded synthetic response |
| GetBucketReplication | Captured | Captured | Bounded synthetic response |
| GetBucketRequestPayment | Captured | Captured | Bounded synthetic response |
| GetBucketTagging | Captured | Captured | Bounded synthetic response |
| GetBucketVersioning | Captured | Captured | Bounded synthetic response |
| GetBucketWebsite | Captured | Captured | Bounded synthetic response |
| GetObject | Captured | Captured | Binary output file |
| GetObjectAcl | Captured | Captured | Bounded synthetic response |
| GetObjectAttributes | Captured | Captured | Bounded synthetic response |
| GetObjectLegalHold | Captured | Captured | Bounded synthetic response |
| GetObjectLockConfiguration | Captured | Captured | Bounded synthetic response |
| GetObjectRetention | Captured | Captured | Bounded synthetic response |
| GetObjectTagging | Captured | Captured | Bounded synthetic response |
| GetObjectTorrent | Captured | Captured | Binary output file |
| GetPublicAccessBlock | Captured | Captured | Bounded synthetic response |
| HeadBucket | Captured | Captured | Bounded synthetic response |
| HeadObject | Captured | Captured | Bounded synthetic response |
| ListBucketAnalyticsConfigurations | Captured | Captured | Bounded synthetic response |
| ListBucketIntelligentTieringConfigurations | Captured | Captured | Bounded synthetic response |
| ListBucketInventoryConfigurations | Captured | Captured | Bounded synthetic response |
| ListBucketMetricsConfigurations | Captured | Captured | Bounded synthetic response |
| ListBuckets | Captured | Captured | Bounded synthetic response |
| ListDirectoryBuckets | Captured | Captured | Loopback standard endpoint/auth only |
| ListMultipartUploads | Captured | Captured | Bounded synthetic response |
| ListObjectVersions | Captured | Captured | Automatic and explicit encoding boundary |
| ListObjects | Captured | Captured | Automatic and explicit encoding boundary |
| ListObjectsV2 | Captured | Captured | Automatic and explicit encoding boundary |
| ListParts | Captured | Captured | Bounded synthetic response |
| PutBucketAccelerateConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketAcl | Captured | Captured | Bounded synthetic response |
| PutBucketAnalyticsConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketCors | Captured | Captured | Bounded synthetic response |
| PutBucketEncryption | Captured | Captured | Bounded synthetic response |
| PutBucketIntelligentTieringConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketInventoryConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketLifecycle | Captured | Captured | Bounded synthetic response |
| PutBucketLifecycleConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketLogging | Captured | Captured | Bounded synthetic response |
| PutBucketMetricsConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketNotification | Captured | Captured | Bounded synthetic response |
| PutBucketNotificationConfiguration | Captured | Captured | Bounded synthetic response |
| PutBucketOwnershipControls | Captured | Captured | Bounded synthetic response |
| PutBucketPolicy | Captured | Captured | Bounded synthetic response |
| PutBucketReplication | Captured | Captured | Bounded synthetic response |
| PutBucketRequestPayment | Captured | Captured | Bounded synthetic response |
| PutBucketTagging | Captured | Captured | Bounded synthetic response |
| PutBucketVersioning | Captured | Captured | Bounded synthetic response |
| PutBucketWebsite | Captured | Captured | Bounded synthetic response |
| PutObject | Captured | Captured | Bounded synthetic response |
| PutObjectAcl | Captured | Captured | Bounded synthetic response |
| PutObjectLegalHold | Captured | Captured | Bounded synthetic response |
| PutObjectLockConfiguration | Captured | Captured | Bounded synthetic response |
| PutObjectRetention | Captured | Captured | Bounded synthetic response |
| PutObjectTagging | Captured | Captured | Bounded synthetic response |
| PutPublicAccessBlock | Captured | Captured | Bounded synthetic response |
| RenameObject | Captured | Captured | Loopback standard endpoint/auth only |
| RestoreObject | Captured | Captured | Bounded synthetic response |
| SelectObjectContent | Captured | Captured | Records + End EventStream and raw output |
| UpdateBucketMetadataInventoryTableConfiguration | Captured | Captured | Bounded synthetic response |
| UpdateBucketMetadataJournalTableConfiguration | Captured | Captured | Bounded synthetic response |
| UploadPart | Captured | Captured | Bounded synthetic response |
| UploadPartCopy | Captured | Captured | Bounded synthetic response |
| WriteGetObjectResponse | Excluded | Excluded | Controlled DNS/TLS host-prefix fixture required |
