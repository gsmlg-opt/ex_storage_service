# Independent AWS multipart-copy checksum behavior

Pinned AWS CLI 2.31.18 performs same-service high-level multipart copy without retrieving source object data. A 9 MiB source produces two server-side UploadPartCopy calls, covering `bytes=0-8388607` and `bytes=8388608-9437183`. The only GET in default-property mode is GetObjectTagging; `--copy-props none` removes that metadata request too.

## Literal executable evidence

The actual AWS executable SHA256 is `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`. The captured [wire fixture](../../../apps/ex_storage_service_cli/test/fixtures/differential/multipart-copy-wire.json), SHA256 `817a6c2317ffa55f7baf99ea9ad26a48c06fd2fa29b95bb5dfcb9e71d215d9ef`, contains three successful local-peer executions:

| Invocation | CreateMultipartUpload | Completion part records | Object entity GET |
| --- | --- | --- | --- |
| Default copy properties | No ChecksumAlgorithm header | ETag and PartNumber only | None |
| `--copy-props none` | No ChecksumAlgorithm header | ETag and PartNumber only | None |
| `--checksum-algorithm SHA256` | `x-amz-checksum-algorithm: SHA256` | ETag, PartNumber and returned ChecksumSHA256 | None |

All UploadPartCopy request bodies are empty, with the source/range in headers. Their response offers synthetic valid Base64 digests for CRC32, CRC32C, CRC64NVME, SHA1, and SHA256; default completion ignores them, while explicit SHA256 includes only its selected digest. The synthetic digests test propagation, not actual content integrity. Completion orders part numbers even when concurrent copy responses arrive out of order.

Reproduce using the pinned executable and local controllable peers:

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_multipart_copy.py \
  --aws /path/to/pinned/aws --output /tmp/multipart-copy-wire.json
```

The helper verifies the AWS executable hash, clears inherited credential/profile settings, disables metadata discovery, uses fixture credentials, and rejects unexpected object data requests. It does not require source object bytes or a provider container. Its fixture header states the source is 9,437,184 bytes; no corresponding object is allocated.

## Pinned source corroboration

AWS CLI source revision `f4f66e65e70f23f1bf232ecb4a6d8696f4dda3bc` (`2.31.18`), `awscli/s3transfer/copies.py`, SHA256 `ad967c8b2eda257e9d2e0a3c4ee5b484116ca95089700116b8961a314857dcf4`, independently explains the trace:

- Lines 230–245 pass `call_args.extra_args.get("ChecksumAlgorithm")` to each CopyPartTask rather than enabling a default algorithm or downloading ranges.
- Lines 369–388 invoke `client.upload_part_copy`, return ETag/PartNumber, and copy the selected `Checksum<Algorithm>` from CopyPartResult when present.

Consequently, a local range GET performed solely to compute multipart-copy checksums changes the server-side-copy contract and transfer cost. The client should propagate validated returned part metadata for an explicitly selected algorithm and retain source-identity safeguards independently. The shared Multipart engine now preserves this contract. Focused effect tests assert zero source entity GETs for default and selected-algorithm copy, omission of default checksum fields, selected SHA256 propagation, and owned abort when the requested part checksum is absent. These tests do not turn server-returned digests into independent byte-integrity evidence.
