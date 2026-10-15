# SigV4a implementation and independent evidence

The CLI signer uses OTP `:crypto` for HMAC-SHA256, P-256 key generation and
ECDSA-SHA256. It adds no Hex dependency, subprocess, AWS SDK, or runtime reference
binary. AWS's SP800-108 credential key derivation uses `AWS4A` plus the secret,
the `AWS4-ECDSA-P256-SHA256` label, access-key context, and a bounded octet
counter. Candidate scalars above N-2 are rejected; accepted scalars are increased
by one. The derived private key is scoped to signing and is never returned or
persisted. BEAM memory erasure and constant-time BEAM arithmetic are not claimed.

`ExStorageServiceCli.S3.SigV4a.sign_headers/5` and `presign_url/3` follow the
existing SigV4 API. `:signing_region_set` accepts a string or ordered list;
`:region` is the fallback, followed by `*`. The credential scope contains
date/service/aws4_request, without a region. The region set is signed in
`x-amz-region-set` or `X-Amz-Region-Set`. Session credentials use the same complete
key pair and token. OTP produces randomized ASN.1 DER signatures, hex-encoded
on the wire, so tests verify signatures rather than compare signature bytes.

S3 defaults preserve slash/dot segments and existing percent escapes. Optional
`:normalize_uri_path`, `:double_uri_encode`, `:include_content_sha256`, and
`:omit_session_token` retain the reference library's signing configuration for
golden tests and operation-specific rules. Presigning accepts required caller
headers via `:headers`. Streamed bodies require a precomputed SHA256 or explicit
`UNSIGNED-PAYLOAD`; signing never reads the stream.

## Reference provenance

AWS CLI **2.31.18** pins **awscrt 0.27.6** in `pyproject.toml`. Its
[`aws-c-auth` submodule](https://github.com/awslabs/aws-crt-python/tree/v0.27.6/crt)
is pinned to **cd9d6afcd42035d49bb2d0d3bef24b9faed57773**.

- The test fixture imports 38 complete cases from that commit's
  [`tests/aws-signing-test-suite/v4a`](https://github.com/awslabs/aws-c-auth/tree/cd9d6afcd42035d49bb2d0d3bef24b9faed57773/tests/aws-signing-test-suite/v4a),
  retaining canonical requests, strings to sign, published signatures and public
  coordinates. Two duplicate-query directories in that version lack completed
  signature fixtures; duplicate-query coverage instead uses the pinned CRT.
- The fixed key-derivation public vector comes from
  [`tests/key_derivation_tests.c`](https://github.com/awslabs/aws-c-auth/blob/cd9d6afcd42035d49bb2d0d3bef24b9faed57773/tests/key_derivation_tests.c),
  which attributes its coordinated values to C, Go and IAM implementations.
- Eight extra header/query cases are produced by **awscrt 0.27.6**, and their
  canonical messages and signatures are independently checked using
  **cryptography 50.0.2**. They cover wildcard/multiple region patterns, temporary
  tokens, unusual encoded object keys, duplicate queries, unsigned streaming
  bodies and a raw-byte payload digest. The development-only generator lives in
  `test/fixtures/sigv4a/generate_crt_vectors.py`; these Python packages are not
  CLI dependencies.
- Amazon's Apache-2.0 license and copyright notice are retained alongside the
  imported test fixture in `test/fixtures/sigv4a/LICENSE` and `NOTICE`.

## Validation and limits

On 2026-10-15, Elixir **1.18.5**, OTP **28**, and OpenSSL **3.6.3**:

- The scoped signer suite passed **86 tests, 0 failures**: 76 official header/
  query vector cases, eight CRT S3 cases, the fixed public-key derivation case,
  and invalid region/expiry/payload-input checks.
- All **84 newly produced OTP signatures** were exported and independently
  verified with cryptography against the reference public keys and canonical
  strings to sign. The verification includes token and region-set binding,
  rather than verifying only an arbitrary message with the same signer.

Fixture SHA256 values:

| File | SHA256 |
| --- | --- |
| `vectors.json` | `2474639a3349cb05af7d2f6da71b6d0c8ac75b3985572bef22e4a4b517650100` |
| `crt-vectors.json` | `3e6c67022032515ffa8300aec5fcf852c5261a469491eaa42043badf622357e4` |

The signer test command from the repository root is:

```sh
PAGER=cat MIX_ENV=test mix cmd --app ex_storage_service_cli mix test \
  test/ex_storage_service_cli/s3/sigv4a_test.exs --no-start
```

This evidence establishes header and query signing with the tested primitives
and configurations. AWS MRAP infrastructure, live temporary credentials, other
operating systems, ECDSA signed chunks/trailers and EventStream signing are
separate acceptance surfaces and are not established by this test suite.
