# Pinned SDK customization observations

Reference: checksum-verified AWS CLI 2.31.18, executable SHA256
`87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
These captures use owned HTTP fixtures, not live AWS resources.

## Raw SSE-C and ASCII metadata

The 22 cases in `test/fixtures/differential/ssec-customizations-wire.json`
capture the eight registered ordinary SSE-C hooks, the two copy-source paths,
explicit-MD5 controls, text/binary key files, ASCII metadata and six pre-I/O
metadata rejections. Sixteen successful cases produce one exchange each; the
six negative cases return status 252 without a request. Fixture SHA256:
`964ea227989f3057ddf85c49ffa6f1eae18b1b6bbcb3f768d3e5bd3a90e4b641`.

Raw s3api customer keys are base64 encoded and their base64 MD5 generated only
when the corresponding MD5 member is absent. Explicit MD5 presence preserves
both caller-provided wire values, including the high-level transfer engine's
already prepared pair. Preparation happens before retries. Binary `fileb://`
unwrapping is restricted to those two customer-key members on the registered
operations; other modeled string/file semantics are unchanged.

PutObject, CopyObject and CreateMultipartUpload reject non-ASCII metadata names
or values before provider access, with a bounded error that does not echo the
metadata. Other parameter types remain subject to ordinary model validation.
The pinned implementation is `awscli/botocore/handlers.py`, functions
`sse_md5`, `copy_source_sse_md5` and `validate_ascii_metadata`.

Actual escript replay compares status, output, path/query and relevant headers
with every captured case. Separate regressions cover explicit MD5 absence versus
presence, unchanged high-level pairs, stable retries and unrelated hook scope.

## Empty bucket location

The four `bucket-location-wire.json` cases capture an empty self-closing element,
empty paired tags, a named region and legacy `EU`. Pinned AWS renders empty root
text as JSON null, retaining nonempty text literally. Fixture SHA256:
`5800707a7672e707efa89c38c31d3dd218fddb1c107e73a65cabdc44bbadfcc7`.
Decoder regressions compare all four outputs with those actual observations.

These scoped fixtures do not establish every SSE member, checksum/metadata
variant or live provider encryption contract. Native prefix-selected encryption
grammar remains a separate documented difference; high-level uniform keys and
raw API hooks do not implement mc's per-alias/prefix key maps.
