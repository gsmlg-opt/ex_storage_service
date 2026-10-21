# ESS S3 compatibility

This documents the ESS general-purpose, path-style support profile under
ESS-S3-HARDENING-001. Hosted standalone remains RF=1/W=1; cluster remains RF=2/W=2.
The operation matrix below describes implemented behavior and its named tests.
It does not claim complete AWS parity or production validation.

Concord, VSR and ExTurso are installed and locked at published 3.1.4, including
the upstream #94–#97 fixes. Backup validation supports the declared deterministic
checksum scheme and legacy checks. Fresh restore targets require command version 1
on every voter through quarantine, restarts and activation; existing databases
retain compatibility default 0 and must never switch automatically.

Current closeout candidate C7 is `fcc02e148ae31f00022d563ebce2b039be5d32dd`, source
fingerprint `ee754b0345b3198e84abd78e898b121e966787c1faa563642097ca3151653913`.
The full entry point exited 1: ten lanes passed, including complete resources,
restore and local release/container checks; index remains blocked by A28.
Correctness executed 4,531 application tests and 122 Python harness tests with
zero failures or skips. Exact-candidate PR and push CI also passed. The backend
maps interrupted view-change reads/transactions to retryable 503 semantics and
retains uncertain write intents. Deterministic observer export-blocking and
confirmed-abort staging regressions pass; historical failures remain preserved.

Standalone, all 17 RustFS gateway groups and both public cluster endpoints passed,
including real leader election/rejoin and strict RF=2/W=2 during a data outage.
Selected clients:
Boto3/botocore 1.43.114, AWS CLI 2.34.24, mc RELEASE.2025-08-13T08-35-41Z,
RustFS 1.0.1. Current receipts retain exact provenance under
`docs/reports/s3-hardening/closeout-002/candidate-c7/`; earlier candidates,
original daily and Concord 3.1.3 receipts retain their separate historical scope.

The dependency update changes only the Concord family lock entries; HTTP remains
0.21.13. A smaller production-path diagnostic reproduced residual whole-history
read validation and full-state transaction work in 3.1.4, tracked by
[Concord #101](https://github.com/gsmlg-dev/concord/issues/101) and
[#102](https://github.com/gsmlg-dev/concord/issues/102). Mandatory 10k/100k scale
acceptance remains blocked. C7 completed all 56 local/gateway and 20 cluster
resource groups at 64 MiB/1 GiB and concurrency 1/4, plus the separate actual
2 GiB transport check. Independent streaming sample analysis verified all groups,
hashes, staging inventories and unchanged resource envelopes; maximum observation
gaps were 50.782 ms and 32.970 ms, respectively, under the original 100 ms bound.
This is an observation-coverage criterion, not a request-latency SLA.
Independent cluster index migration/rejoin/cutover passed in 13.752 seconds. All 38
acceptance scenarios are not yet complete. See the
[candidate freeze](reports/s3-hardening/closeout-002/candidate-c7/freeze.json),
[business feature record](plans/s3-feature-status.md) and
[verification report](reports/s3-hardening-verification.md). Historical passes
are not automatically relabeled as current-source executions.

## Authority and mode boundaries

Hosted buckets use Concord/VSR metadata and immutable content-addressed blobs.
Standalone remains RF1/W1. Hosted cluster writes retain RF2/W2 by default and
all listener,node-generation,metadata-role and data-plane guards. Cross-cluster
replication is eventual disaster recovery and never satisfies local RF/W.

Gateway object authority is the configured upstream. Every GET/HEAD revalidates
its identity; only a matching validator permits cached bytes. Availability or
authorization failure never permits a stale read or fabricates NoSuchKey. Cold
GET uses If-Match and durable bounded disk staging before delivery. Size,ETag,
attributes and body must identify the same upstream object. Opening the cache
file descriptor before delivery preserves a reader across eviction.

Gateway PUT verifies the full declared payload/checksums in durable staging
before upstream publication,and uses raw bytes,explicit lengths,no redirects
and `decode_body: false`. Durable operation intents precede mutation. Unknown
external results are reconciled without blind destructive replay. DELETE pins
upstream identity and requires confirmed absence before removing the original
local projection; changed local/bucket identities prevent stale reconciliation.
Shared cache files remain available for safe collection. Operator entry points
are bounded `CloudCache.WriteThrough.reconcile_page/3` and
`CloudCache.DeleteThrough.reconcile_page/3`. Historical RustFS run7 verifies full BEAM
restart recovery for unknown outcomes and upstream-success/local-commit failure.

Gateway explicit versions,bucket versioning,multipart and object/version tags
return NotImplemented before any upstream request. A gateway source can feed a
hosted COPY/UploadPartCopy through a pinned bounded source; this does not enable
multipart destinations on gateway buckets. Existing hosted delete markers still
shadow upstream data,and gateway DELETE does not silently remove hosted history.
Cluster gateway writes remain ServiceUnavailable. Bucket configuration is
ESS-local and does not configure upstream CORS,tags,lifecycle or notifications.

## Operation and authorization contract

Every operation is resolved once by `ExStorageServiceS3.Operation` before
authorization/dispatch. Raw signing path/query pairs remain separate. Duplicate
selectors,conflicting identities and malformed part/copy IDs reject before
mutation; benign SDK `x-id` does not choose an operation. Canonical resources are
`arn:aws:s3:::bucket` and `arn:aws:s3:::bucket/key`. Existing unmarked ESS policies
retain their operation scope; legacy HEAD-only grants do not become GET grants.
Denies apply across aliases,and missing/malformed policy reads fail closed.

Root GET supports the unpaginated ListBuckets operation with no query, optional
`x-id` SDK metadata, and the existing SigV4 presigned authentication parameters
`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
`X-Amz-SignedHeaders` and `X-Amz-Signature`. These can be combined; the `x-id`
value does not select an operation. Every root query name may occur at most once,
including names that become equal after percent decoding. Unknown parameters,
root subresource selectors, and unimplemented ListBuckets filtering/pagination
(`prefix`, `bucket-region`, `max-buckets`, `continuation-token`) reject rather
than silently returning an unfiltered result. `s3:ListAllMyBuckets` on `*` remains
required. Dispatch preserves the raw path, query and headers: `x-id` and every
other query pair participate in signature verification, including presigned
requests. `ListBucketsOperationTest` and the independent signer/raw HTTP fixtures
in `AuthorizationHTTPTest` cover this boundary.

Latest hosted GET/HEAD, COPY source and tagging selection share bounded coherent
head/version/head reads with full revision identity and bucket-generation fencing.
An overlapping replacement or deletion retries; exhaustion and backend failures
return retryable availability errors. A stable missing referenced version is an
integrity error. Explicit version requests retain their own absence/marker errors
and never fall back to the newest version. Body, attributes, ETag and version ID
refer to the same selected immutable version. This adds bounded metadata reads;
the separate native whole-history limitation remains the A28 blocker.

In this table H means standalone hosted,C means hosted cluster,G means gateway
object path. Metadata/configuration rows concern ESS-local bucket state. Actions
have the `s3:` prefix. Common errors include AccessDenied,NoSuchBucket,
InvalidRequest/InvalidArgument and ServiceUnavailable; specific errors are listed
where they define behavior. Named test modules run in the corresponding app's
`test/ex_storage_service*` directory; probe source lives under `e2e/`.

| Operation / method | Controls and response contract | AWS IAM action | Modes | Specific errors / evidence |
|---|---|---|---|---|
| ListBuckets GET `/`; bucket PUT/HEAD/DELETE | Bucket deletion includes all history/markers/uploads and serializes with writers under generation fencing | ListAllMyBuckets; CreateBucket; ListBucket; DeleteBucket | H,C; ESS-local bucket state for G | BucketNotEmpty,NoSuchBucket; BucketAndControlHTTPTest,BucketLifecycleTest; Phase1 cluster races |
| ListObjects GET bucket; v2 `list-type=2` | prefix,delimiter,max-keys0..1000,marker,start-after,opaque continuation-token,encoding-type=url; common prefixes count toward page capacity | ListBucket | H,C,G; G delegates tokens upstream | InvalidArgument for malformed/context-mismatched tokens; ListingProfileHTTPTest; versions/lists probe |
| Versioning GET/PUT `?versioning` | Enabled/Suspended; never-enabled overwrite has one null slot; Suspended replaces only null; retains numbered history | GetBucketVersioning / PutBucketVersioning | H,C; G501 | InvalidRequest/MalformedXML; VersioningProfileTest,ExtendedFeaturesTest; versions/lists probe |
| ListObjectVersions GET `?versions` | key/version markers,IsLatest,delete markers,prefix/delimiter,max0..1000,URL fields | ListBucketVersions | H,C; G501 | InvalidArgument; VersionListingTest,VersionListingHTTPTest; versions/lists and tag/CORS probes |
| Object GET/HEAD | Current or `versionId`; single Range; ETag/date conditions; signed response overrides; served version ID | GetObject or GetObjectVersion; scoped legacy HEAD alias retained | H,C; G current only | NoSuchKey/NoSuchVersion; current marker404,explicit marker405+LastModified; InvalidRange416,304/412; VersionProfileHTTPTest,ReadSemanticsHTTPTest,ChecksumRangeHTTPTest |
| PUT object | If-None-Match/If-Match decided atomically; system/user attrs,tags and checksums | PutObject; additional PutObjectTagging when tagging header supplied | H,C,G; cluster G writes503 | BadDigest/InvalidDigest,PreconditionFailed412,ConditionalRequestConflict409; UploadIntegrityHTTPTest,ConditionalWriteTest,TaggingAuthorizationHTTPTest; independent32-writer profile |
| DELETE object; POST `?delete` | Explicit-version selection; quiet/mixed batch; per-entry permission/integrity; max1000 entries | DeleteObject / DeleteObjectVersion per entry | H,C,G current only; cluster G writes503 | Per-entry AccessDenied/upstream errors retained; AuthorizationHTTPTest; gateway mixed batch/run7 |
| COPY PUT object with `x-amz-copy-source` | Immutable source version,source ETag/date conditions; independent metadata/tag COPY/REPLACE; source and destination version IDs; resulting representation/checksum recalculated | Source GetObject/GetObjectVersion plus destination PutObject; PutObjectTagging if actual resulting tags nonempty | H,C,G current; cluster G writes503 | PreconditionFailed; AttributesProfileHTTPTest,AuthorizationHTTPTest,GatewayAttributesHTTPTest; versions/lists and selected-client probes |
| Initiate MPU POST `?uploads`; upload PUT `?uploadId&partNumber`; complete POST `?uploadId` | Initiation attrs/tags/checksum negotiation persist; parts1..10000; strict order/ETag/size/count; atomic freeze/conditions; completed retries retain identity | PutObject; initiation tag header also PutObjectTagging | H,C; G501 | NoSuchUpload,InvalidPart,InvalidPartOrder,EntityTooSmall/TooLarge,ConditionalRequestConflict; MultipartProfileHTTPTest,MultipartHardeningTest; multipart/race/restart probes |
| UploadPartCopy PUT with uploadId/partNumber and copy source | Explicit source version/range/conditions,negotiated checksum; local,packed and remote readers where supported | Destination PutObject plus source GetObject/GetObjectVersion | H,C destinations; gateway source pinned | NoSuchUpload,AccessDenied,PreconditionFailed412,InvalidRange416; UploadPartCopyHTTPTest,GatewayReadHTTPTest; multipart cluster forced-remote probe |
| ListUploads GET `?uploads`; ListParts GET `?uploadId`; Abort DELETE `?uploadId` | key/upload markers,prefix/delimiter,max-uploads; part-number-marker,max-parts0..1000; full bucket/key/upload binding; frozen/terminal state fenced | ListBucketMultipartUploads; ListMultipartUploadParts; AbortMultipartUpload | H,C; G501 | NoSuchUpload,InvalidArgument,ConditionalRequestConflict; MultipartHardeningTest,MultipartProfileHTTPTest,AuthorizationHTTPTest; multipart probe |
| Bucket tags GET/PUT/DELETE `?tagging` | Persisted tag set,limit50,duplicates/reserved names/length checks | GetBucketTagging; PutBucketTagging for PUT/DELETE | H,C ESS-local | NoSuchTagSet,MalformedXML,InvalidTag; TagsTest,TaggingHTTPTest; tag/CORS probe |
| Object/version tags GET/PUT/DELETE `?tagging[&versionId]` | Limit10; never changes bytes/mints versions; marker404/405; tag-count header only with tag-read grant | Get/Put/DeleteObjectTagging or Get/Put/DeleteObjectVersionTagging | H,C; G501 | NoSuchKey/NoSuchVersion,InvalidTag; TaggingHTTPTest,TaggingAuthorizationHTTPTest; tag/CORS probe |
| Location GET `?location`; CORS GET/PUT/DELETE `?cors`; OPTIONS | ESS_S3_REGION; persisted origin/method/header rules,exposed headers,max-age,Vary; actual request remains authenticated | GetBucketLocation; GetBucketCORS; PutBucketCORS for PUT/DELETE; preflight grants no object access | H,C ESS-local | NoSuchCORSConfiguration,MalformedXML,AccessDenied; BucketCorsTest,CorsLocationHTTPTest; tag/CORS probe |
| Lifecycle GET/PUT/DELETE `?lifecycle` | Existing prefix/status/expiration-days and internal PACKED transition subset; no full AWS lifecycle parity | GetLifecycleConfiguration / PutLifecycleConfiguration | ESS-local configuration; cluster lifecycle/packing workers remain guarded | NoSuchLifecycleConfiguration,MalformedXML; ExtendedFeaturesTest,BucketConfigurationHardeningTest,Storage.LifecycleTest |
| Notifications GET/PUT/DELETE `?notification` | Existing TopicConfiguration XML maps event patterns to ESS HTTP webhooks; not AWS SNS/SQS/Lambda infrastructure | GetBucketNotification / PutBucketNotification | ESS-local configuration; existing worker/role guards retained | MalformedXML; ExtendedFeaturesTest,BucketConfigurationHardeningTest,HTTPClientTest |

Object attributes are Content-Type,Content-Encoding,Cache-Control,
Content-Disposition,Content-Language,Expires,user metadata,tags and applicable
checksums. PUT/MPU/COPY/REPLACE/GET/HEAD roundtrips preserve actual encoded bytes;
Content-Encoding never authorizes transparent object decompression. Partial
responses omit whole-object checksum headers. COPY computes the checksum/ETag of
its resulting representation; a multipart source's composite value is not blindly
reused as a whole-object checksum.

## Signing, integrity and limits

SigV4 signing preserves meaningful slashes/dot segments,percent/plus/Unicode,
repeated and empty query pairs and duplicate headers. Independent Botocore/Req
fixtures test credential scope and canonicalization. Presigned GET/PUT still
require active key,user and current resource permissions; parameter/header
changes,expiration,suspension and revocation reject.

Declared payload SHA256,Content-MD5 and CRC32,CRC32C,CRC64NVME,SHA1,SHA256 wire
checksums are verified independently of CAS hash/ETag before publication.
Supported negotiated multipart full/composite types are validated by
MultipartChecksumsTest/MultipartProfileHTTPTest. Accepted signed PAYLOAD chunk
chains verify signatures,decoded lengths,terminal framing and trailing bytes;
accepted unsigned checksum trailers verify framing and declared checksums.
Signed-trailer/unknown streaming modes reject deliberately. No default-client
checksum setting is disabled in the tested profiles.

Key limit is1024 UTF8 bytes,user metadata2048 bytes. Buffered bucket config XML is
limited to1MiB,batch delete8MiB,and completion XML16MiB. Hardened parsing rejects
DOCTYPE,invalid UTF8,malformed identifiers and oversized bodies. HEAD errors are
bodyless. A late stream failure closes/truncates the transfer and never appends
XML or a second error response to object bytes. See ControlXMLTest,
BucketAndControlHTTPTest,ErrorFramingTest and LateStreamHTTPTest.

Hosted object, version, upload and part listings now use a revision-fenced,
transactional per-bucket index. New buckets publish ready descriptors; legacy
buckets require explicit verified backfill/cutover and fail closed until ready.
Offline capture still materializes one Concord export and is separately budgeted;
it is never a request-path fallback. See [index operations](operations/listing-index.md).
The locked Concord 3.1.4 includes the released #94–#97 fixes, but its native read
and transaction paths retain the scale blockers #101/#102. Mandatory 10k/100k
scale acceptance remains unexecuted; the separate current cluster backfill,
interruption, rejoin and cutover fixture passed. Implementation and scoped tests
do not establish full acceptance. Hosted tokens
are bound to bucket/prefix/delimiter and do not promise an immutable snapshot
across concurrent writes. Complete C7 transfer-resource evidence and independent
analysis are in `reports/s3-hardening/closeout-002/candidate-c7/` and adjacent
`closeout002-c7-resources*` directories. Earlier partial/failed resource runs
retain their original outcomes and source identities.

On first hosted mutation,legacy normalization includes at most256 historical rows
plus current null slot under revision/bucket fencing. Original v1 records remain.
Larger histories fail explicitly before publication and require quiesced backed-up
migration; v1 metadata mode is read-only. This is not startup migration. Old-binary
rollback after v2 writes requires metadata restoration. Whole-cluster migration
still requires verified metadata/blob backup and quiesced writers.

`ESS_CONCORD_COMMAND_VERSION` defaults to `0` for existing-database
compatibility and accepts only `0` or `1`. Empty restore targets require
explicit `1` on every voter, retained through recovery, every restart and
activation so revision-aware metadata remains readable. Switching an existing
version-0 database requires quiesced writers, a verified backup and explicit
Concord legacy-state reconciliation; ESS never migrates it automatically.
Follow the [recovery operations guide](operations/cluster-upgrade.md#independent-same-mode-recovery-bundles).

## Independent profiles and deliberate exclusions

Current C7 complete business profile receipts are under
`docs/reports/s3-hardening/closeout-002/candidate-c7/` at the candidate and
the explicitly stated C7 fingerprint above. Adjacent `closeout002-c7-*` receipts
retain profile details; earlier candidate evidence retains its original source.
Standalone covers signed/default-checksum clients, versions/listing, multipart
races/restart, attributes, tags, CORS and retained SDK/admin/metrics/persistence.
Cluster repeats the supported profile on both data endpoints, forces remote
object/part reads, and proves actual primary election/rejoin with strict RF2/W2.
Gateway passes all17 groups, including actual60s socket timeouts, four full-BEAM
reconciliation restarts and retained mc24/0 compatibility. AWS CLI and mc transfer
10 MiB using normal copy/list/read/delete commands. Tool versions and exact
commands are retained in the profile logs and gateway JSON; these results do not
claim every command/flag in those clients works.

Current C7 standalone/cluster restore evidence is
`docs/reports/s3-hardening/closeout002-c7-restore-standalone/` and
`closeout002-c7-restore-cluster/`. The fixtures verify
quiesced backup boundaries, inaccessible original roots, read-only backups,
empty targets, quarantine restarts, job/intent retention and stale-fence rejection.
Every public data endpoint checks versions/delete markers, exact hashes/size,
attributes/checksums/tags, bucket configuration, IAM grants/denials, multipart
parts/completion and versioned PUT after activation. Packed standalone data
restores without a loose-source fallback. All eight destructive/outbound worker
flags remain false, and the outbound DELETE canary stays zero after activation.
Historical restore evidence retains its own source in the verification history;
these C7 executions certify only the stated C7 SHA/fingerprint.

Full AWS IAM/STS,ACL ownership,Object Lock,SSE/KMS,website hosting,S3 Select,
directory buckets,erasure coding and multi-range reads are outside this delivery.
Excluded selectors/headers fail deliberately instead of falling through to CRUD.
No new public STS/IAM service,virtual-hosted addressing or AWS infrastructure
notification/lifecycle parity is advertised. Mandatory scale results
and final aggregate acceptance remain required before ESS-S3-HARDENING-001 can
be called complete. C7 local release/container health, readiness and operator
smoke checks passed; signed object CRUD in that image was not certified. Its local
image is `sha256:812591b0b174f02d6173b59a0c7909bb54402d3432a44fa2cc09ccbf4d1f1b62`,
with version 0.6.10 and C7 build-source identity; this is a local acceptance build.
No closeout patch release, canonical version tag, Hex version or versioned/latest
image has been published. Automatic CI edge images are separate from release
acceptance and published-artifact verification.
