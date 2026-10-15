# Independent WriteGetObjectResponse wire evidence

The pinned AWS CLI 2.31.18 executable captured three Object Lambda exchanges: binary success, successful forwarding of an application error, and a service AccessDenied response. ESS replays them through its real escript, including file-body streaming, route-prefix endpoint resolution, signing, transport, and exit/error formatting.

## Controlled hostname fixture

AWS uses an HTTP proxy listening only on loopback. The endpoint is `http://loopback.invalid:<port>`, and AWS's modeled host-prefix injection produces `fixture.loopback.invalid:<port>`. The proxy responds directly without resolving or forwarding that hostname. The fixture preserves the actual absolute proxy target, Host, Authorization and signing timestamp. Python independently recalculates each captured AWS HMAC using only fixture credentials and requires a body SHA256 plus the `s3-object-lambda` credential scope.

ESS uses the same endpoint shape with a private `ERL_INETRC` passed only to its escript subprocess. The file maps exactly `fixture.loopback.invalid` to loopback and disables other lookup methods in that subprocess. No system hosts file, global resolver, shared process environment, or shared DNS configuration changes. It connects directly to the wire peer; proxy variables are cleared. HOME is preserved.

Replay requires literal operation path, prefixed Host, route/token/forwarded headers, raw entity bytes, explicit Content-Length, body SHA256, status/stdout/stderr, and exactly one request. It independently recomputes ESS's HMAC from wire headers without calling the production signer. Dynamic port/date/signature values are verified semantically rather than compared literally across runs. HTTP proxy absolute-form and direct origin-form targets share the same canonical operation path.

## Captured boundaries

| Scenario | Body and modeled input | Service result |
| --- | --- | --- |
| Binary success | File bytes 0–255, length 256, forwarded status 206, content type/encoding/range, metadata | HTTP 200, no output, exit 0 |
| Forwarded error success | Empty body, length 0, forwarded status 403, AccessDenied code and message | HTTP 200, no output, exit 0 |
| Service AccessDenied | Same binary file and forwarded headers as success | HTTP 403, modeled error text, exit 254 |

The forwarded encoding value is synthetic metadata; it does not claim these binary bytes form a gzip stream. No real AWS credentials or live Object Lambda service is used. These HTTP fixtures establish the mandatory signed payload hash on cleartext requests. They do not prove HTTPS certificate/SNI behavior, live service semantics, every optional modeled header, or resource bounds for large uploads; dedicated transport tests supply separate streaming evidence.

The first replay exposed two local caller defects: HTTP requests forced `UNSIGNED-PAYLOAD`, and absent Body was left as nil despite signing Content-Length 0. The latter caused transport normalization to remove the signed zero length. An explicit empty binary is required when forwarding an error with an empty entity.

After the caller fixes, the four independent fixture/audit checks plus four focused WriteResponse unit checks passed (8 tests, 0 failures). Combined with the broad matrix's 107 operations and 220 exchanges, these three additional exchanges give independent executable coverage of all 108 modeled operations within the specific documented fixture boundaries.

## Reproduction

AWS executable SHA256: `87630ec5232080b737502f4098f4c9adbf820c01d23c63b62affcad50059ad0a`.
Fixture SHA256: `a765219d04897f05917d19558062a213f5d208b83800872ad27bea3b5f0d964f`.

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_write_response.py \
  --aws /path/to/pinned/aws --output /tmp/write-response-wire.json
```

The generator verifies the pinned executable and bounds subprocess/socket timeouts. Optional `--ess /path/to/private/escript` captures diagnostics; remove those fields before promoting independent oracle fixtures. Captured signatures and ports naturally vary between runs.

Replay from the standalone CLI app:

```sh
PAGER=cat mix test test/ex_storage_service_cli/executable_write_response_matrix_test.exs
```

The audit pins fixture, executable and service model hashes and recalculates the captured AWS signatures. This closes the operation-specific executable gap deliberately excluded by the broad 107-operation loopback-IP matrix.
