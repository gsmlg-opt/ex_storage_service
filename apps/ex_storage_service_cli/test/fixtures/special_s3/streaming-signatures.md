# Streaming signature oracle

`streaming-signatures.json` contains factual golden signatures from the official
aws-c-auth test `tests/test_chunked_signing.c`, which reproduces AWS S3's published
65-KiB chunked upload example and its trailing-header signature. The JSON records
the immutable revision, raw source URL, and SHA256. Only example credentials are
included. They are public AWS documentation values, not account credentials.

The first payload contains 65536 ASCII `a` bytes; the second contains 1024 `a`
bytes. The terminal frame is empty. Trailing headers are first=1st, second=2nd,
third=3rd. Tests consume the expected signatures directly without importing ESS
into any reference program. Source algorithms are Apache-2.0; no source code is
vendored by this factual vector extraction.
