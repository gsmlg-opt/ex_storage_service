# Independent AWS paginator wire matrix

The pinned AWS CLI 2.31.18 executable produced 42 successful loopback captures: six scenarios for every paginator in its seven-entry S3 paginator model. The fixtures preserve AWS stdout, stderr, exit status, literal request targets and headers, and the exact synthetic response sequence. No ESS observations supply expected output.

| Operation | Aggregate | Max-items within page and resume | Max-items at page boundary and resume | No paginate |
| --- | --- | --- | --- | --- |
| ListMultipartUploads | Captured | Captured | Captured | Captured |
| ListObjectVersions | Captured | Captured | Captured | Captured |
| ListObjects | Captured | Captured | Captured | Captured |
| ListObjectsV2 | Captured | Captured | Captured | Captured |
| ListParts | Captured | Captured | Captured | Captured |
| ListDirectoryBuckets | Captured | Captured | Captured | Captured |
| ListBuckets | Captured | Captured | Captured | Captured |

The two service pages contain two primary results followed by one. Fixtures include secondary CommonPrefixes, ObjectVersions DeleteMarkers, multiple service continuation tokens, integer part markers, first-page-only Owner retention, and ListObjects' last-key marker fallback when NextMarker is absent. URL listing fixtures contain literal percent sequences, plus signs, spaces, Unicode keys, and opaque version/continuation identifiers. These exercise automatic response customization alongside aggregation and token handling.

Resume cases pass the actual AWS-produced starting token to ESS. Output tokens are compared as Base64-decoded JSON maps because JSON map order and whitespace are not token fields. All other parsed output must match. Requests compare literal method/path, decoded query pairs including duplicate counts and marker values, entity bytes, captured headers, request count, and sequence. Bare subresources and explicit empty values are equivalent; query order may differ. This proves interoperability for the captured AWS tokens and does not claim every malformed, legacy, or future token is accepted.

The capture uses a standard custom loopback endpoint and fixture credentials. It does not establish live S3/S3 Express behavior, automatic endpoint selection, arbitrary page sizes, text-output pagination, or exhaustive optional response members. The separate operation matrix covers automatic versus explicit encoding with single service responses.

## Reproduction

AWS executable SHA256: `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
Fixture SHA256: `43cc6abc45c0669b852aed35eb37d720b17f9569b6bd85c31f36b574eb7d6dc8`.

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_paginators.py \
  --aws /path/to/pinned/aws --output /tmp/paginator-wire.json
```

The generator verifies the independent executable hash, clears inherited AWS/ESS/MC environment settings, disables metadata discovery, and bounds subprocess and socket waits. HOME is preserved. Optional `--ess` adds historical diagnostics; remove those fields before promoting oracle fixtures.

Replay from the standalone CLI app:

```sh
PAGER=cat mix test test/ex_storage_service_cli/executable_paginator_matrix_test.exs
```

The audit checks fixture, executable, service-model and paginator-model hashes; it requires every modeled paginator and all six scenarios, and verifies resume inputs came from the corresponding AWS truncation output.

Validation: 43 replay/audit tests passed with zero failures. A fresh independent AWS run reproduced all 42 captured cases exactly as JSON data.
