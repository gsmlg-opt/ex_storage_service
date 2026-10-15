# Upload checksums and streaming signature contracts

`S3.UploadChecksum` provides JSON-safe algorithm/type selection and independently
computed local checksums for the shared upload engine. An initiation request does
not have an `httpChecksum` model trait, so default selection must happen before
`CreateMultipartUpload`; request-body checksum injection alone cannot provide it.

For local uploads, the pinned default is CRC64NVME with FULL_OBJECT. CRC32, CRC32C, SHA1 and SHA256
select COMPOSITE by default for multipart. Explicit FULL_OBJECT supports the CRC
algorithms; SHA multipart FULL_OBJECT and CRC64NVME COMPOSITE fail before upload.
Single PUT uses FULL_OBJECT for every supported algorithm. The request checksum
policy `when_required` suppresses an unrequested optional default.

Each acknowledged local-upload part retains a checksum computed from its captured
bounded `Transfer.Source`, even if the provider omits checksum response headers. All known
algorithms returned by the provider are computed in one pass and checked, rather
than trusting ETag as a checksum. Source identity/content validation and invocation
cancellation remain enforced by Source. Selected algorithm and type fit the existing
journal `options.checksum_algorithm` and `options.checksum_type` fields.

Local-upload completion requires consecutive part numbers and locally retained digests. It
returns modeled Parts, ChecksumType, and MpuObjectSize. FULL_OBJECT combines CRC
part digests and byte lengths and adds the object checksum header. COMPOSITE sends
part digests and computes the expected response as base64(digest(raw ordered part
digests))-part-count; it does not invent a top-level composite request checksum.
Explicit caller checksums must match the locally computed object checksum.

Response verification distinguishes `verified`, `digest_verified` (matching digest
but no returned type), `partially_verified`, and `not_provided`. A conflicting type
or mismatching digest fails. An absent provider checksum never becomes a claim of
provider checksum verification. Initiation algorithm/type acknowledgements are
checked separately and cannot erase the locally selected contract.

## Server-side multipart copy

Server copy follows the independently captured pinned AWS behavior in
[differential-multipart-copy.md](differential-multipart-copy.md). An unrequested
checksum algorithm is omitted from initiation and completion, even when the
provider returns checksum fields. Completion parts retain ETag and PartNumber.
No source entity GET is issued to compute part checksums.

An explicit algorithm selects only its returned `Checksum<Algorithm>` from each
UploadPartCopy result. Base64 encoding and digest length are validated; a missing
selected digest fails and triggers the owned abort path. These returned digests
are server assertions, not independently computed source-byte verification.
Existing aggregate checksum/type validation uses the selected returned parts;
source identity checks remain separate. Other returned checksum algorithms are
ignored. Copy and local-upload journal selection preserve their distinct default
contracts across resume.

## aws-chunked interface

`S3.StreamingSignature` implements SigV4 and SigV4a payload signature chaining,
signed checksum trailers, and unsigned checksum trailers. Header construction precedes request
signing; the initial Authorization signature is then passed as `seed_signature`.
The payload marker, decoded Content-Length, content encoding and trailer declaration
must all be present when signing that initial request. The encoded stream uses
fixed-size rechunking, so encoded Content-Length is exact even if source reads arrive
in different bounded fragments. Upstream chunks may not exceed the configured
chunk size (one MiB maximum). Short/long sources and early failures never emit a
successful terminal frame.

Signed chunks include the previous signature and SHA256 hashes in the AWS string
to sign. The terminal zero chunk is signed. Canonical trailing headers use LF for
signing while their wire representation uses CRLF. Source bytes are never decoded,
compressed or rewritten. The signer hides its key and prior signature from Inspect.
A failed or cancelled enumeration propagates through the owner; the caller owns
transport cleanup. Retries need a fresh signer and replayed captured source.

The fixture `special_s3/streaming-signatures.json` extracts public AWS S3 signatures
from a pinned aws-c-auth source revision and records the source SHA256. Existing
checksum vectors come from the independent AWS CRT/Python capture. Tests cover
these golden values, all checksum trailer sizes, exact lengths, lazy bounded
rechunking, and malformed/truncated input.

SigV4a uses `AWS4-ECDSA-P256-SHA256-PAYLOAD` and `-TRAILER` strings to sign,
with region omitted from the credential scope. DER signatures vary per signing
operation. Each chunk and trailer signature is hex encoded and padded to 144
characters with `*`; padding is removed before including the previous signature
in the next string to sign. This makes encoded Content-Length exact without
buffering or pre-signing the full object. Key derivation uses the existing SigV4a
signer, and keys/previous signatures remain excluded from inspection.

`special_s3/sigv4a-streaming.json` captures the independent aws-c-auth public-key
coordinates and expected chunk/trailer strings to sign from the same pinned
revision. Tests verify generated ECDSA signatures against that published key and
those strings, including the zero chunk, trailer hashing, padding, and lengths.
They do not compare random ECDSA signatures with implementation-generated golden
values. The pinned botocore `crt/auth.py` selects
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` for trailer checksums even when request
authentication uses SigV4a; this unsigned framing also works without chunk keys.
Live provider acceptance and integration through the shared Client/Engine remain
separate acceptance checks from these pure helper tests.
