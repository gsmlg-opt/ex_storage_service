# Special S3 protocol and checksum evidence

The special handlers share the modeled client, signing, endpoint rules, raw HTTP
transport and invocation resource coordinator. They add no runtime dependency.

## EventStream and Select

`S3.EventStream.new/1`, `feed/4`, and `finish/1` decode EventStream incrementally.
The parser validates the prelude CRC before accepting a declared size, enforces
frame/header byte limits, validates the message CRC before delivering payload,
parses all ten header types without dynamic atoms, and rejects duplicate names,
unknown types, invalid UTF-8 strings and truncated frames. Arbitrarily small
transport fragments are coalesced into 16 KiB blocks, so retained metadata does
not grow by one list cell per byte.

`S3.Select.call(context, input, initial, reducer, opts)` uses
`Client.call(..., mode: :stream)` and the transport's acknowledged reads. Its
reducer receives `%{type: binary, payload: binary, details: map_or_nil}` and
returns `{:ok, accumulator}` or a structured error. Records remain raw bytes:
record boundaries may cross event frames. Stats and Progress are parsed with
the bounded XML decoder. The result is `{:ok, accumulator, summary}` only after
an explicit empty End event and complete transport termination. HTTP-200 error
and exception events produce safe service errors. Missing End, corrupt CRC,
truncation, invalid details, or data after End cannot report success.

The call reserves twice the maximum frame size plus 128 KiB in the invocation
byte budget, accounting for temporary contiguous-frame assembly. Direct callers
of `Select.consume/4` must reserve equivalent resources before opening their
response. Callback errors and exceptions retain the transport's unconditional
resource cleanup. The accumulator belongs to the caller; callers writing a file
or stdout should write records incrementally instead of collecting them.

Nine raw frame vectors are imported from AWS CLI **2.31.18**'s
`tests/unit/botocore/test_eventstream.py`. They independently cover signed
integers, byte/string values, booleans, timestamp, UUID, payload and error headers.
The Apache-2.0 license and Amazon copyright notice accompany the fixtures.

## Object Lambda WriteGetObjectResponse

`S3.WriteResponse.call(context, input, opts)` validates the RequestRoute host
label, models the forwarded headers, and uses the shared client with
`UNSIGNED-PAYLOAD` and one attempt. Binary Body values remain raw. A bounded
`body_source` requires explicit ContentLength, and conflicting binary/source
bodies or lengths fail before transport. The model's operation-specific endpoint
and RequestRoute host prefix must be applied by shared endpoint resolution.

Local wire fixtures prove forwarding of request route/token, metadata, gzip
Content-Encoding, temporary credentials, raw binary bytes and Content-Length.
Default AWS Object Lambda routing and live invocations require separate evidence;
a custom-endpoint fixture does not establish that infrastructure behavior.

## Checksums

`S3.Checksum` has incremental `new/1`, `update/2`, `finish/1` and `encoded/1`
functions for every pinned algorithm: **SHA256, SHA1, CRC32, CRC32C and CRC64NVME**.
SHA uses OTP crypto; CRC32 uses OTP's CRC primitive; Castagnoli and NVME use fixed
reflected lookup tables. Wire values are base64 of big-endian raw digests.

- `composite/2` hashes ordered raw part digests and appends the part count.
  CRC64NVME is full-object only and rejects composite mode.
- `combine/2` combines finalized CRC part digests plus byte lengths in GF(2) to
  produce a full-object CRC without downloading/concatenating the object. SHA
  digests cannot be combined this way.
- `verify/3` distinguishes full-object and composite checksums. A composite
  value requires ordered part checksums; a whole-object digest is never accepted
  as evidence for a composite checksum.
- `verify_response/4` reads header/trailer values, rejects conflicts, and returns
  `{:ok, :not_provided}` when no selected checksum is advertised. Absence is never
  reported as successful integrity verification.
- `encode_trailers/3` lazily emits the pinned botocore unsigned aws-chunked
  checksum envelope and rejects source chunks above its byte bound. It does not
  start transport or add HTTP headers. Callers must arrange the matching TLS,
  aws-chunked headers and actual encoded Content-Length/HTTP framing; the decoded
  object length is not the length of this envelope.

Twenty-five independent digest/composite vectors use **awscrt 0.27.6** for CRCs
and Python hashlib for SHA. Raw concatenation digests also verify CRC part
combination. Cases include empty data, `123456789`, all byte values, zeros and
multiple chunks. The classic check values are CRC32 `CBF43926`, CRC32C `E3069283`
and CRC64NVME `AE8B14860A799888`.

## Validation boundary

The isolated CLI command from the repository root is:

```sh
PAGER=cat MIX_ENV=test mix cmd --app ex_storage_service_cli mix test \
  test/ex_storage_service_cli/s3/checksum_test.exs \
  test/ex_storage_service_cli/s3/event_stream_test.exs \
  test/ex_storage_service_cli/s3/select_test.exs \
  test/ex_storage_service_cli/s3/write_response_test.exs --no-start
```

Validation on 2026-10-15 uses Elixir 1.18.5 / OTP 28 and local raw TCP fixtures.
Live AWS S3 Select availability, Object Lambda invocation, full transfer-engine
checksum/publication integration, signed SigV4/SigV4a chunks/trailers, and native
platform matrices remain separate acceptance surfaces. Unsigned checksum framing
is not evidence for signed streaming support.
