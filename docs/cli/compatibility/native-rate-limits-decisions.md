# Native aggregate transfer limits

`--limit-upload SIZE` and `--limit-download SIZE` pace raw HTTP entity bytes.
Zero, including the default, leaves that direction unlimited. Positive decimal
byte quantities accept SI and IEC suffixes, such as `1.5MB` and `512KiB`, up to a
signed 64-bit byte count. Invalid, negative, overflowing, or nonzero sub-byte
quantities fail before I/O. The flags take bytes per second; `/s` is not appended
to their values. The mc `MC_LIMIT_UPLOAD` and `MC_LIMIT_DOWNLOAD` environment
variables are not implemented.

## Reference contract

Pinned mc commit `77f82e18b5401a65958f1619df6ebb994634bd88` declares these string
flags in [`cmd/flags.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/cmd/flags.go),
parses human byte sizes in `cmd/globals.go`, and installs an upload/download
limiter around its cached client transport in `cmd/client.go`. The reviewed
[`pkg/limiter/limiter.go`](https://github.com/minio/mc/blob/77f82e18b5401a65958f1619df6ebb994634bd88/pkg/limiter/limiter.go)
wraps every request and response body reader. Its independent buckets have one
second of capacity and initial burst. HTTP and TLS headers do not consume this
budget. The limits apply to the shared client transport, including small and
control requests, rather than being renewed for each object or multipart part.

An actual pinned executable probe used `mc --limit-download 1KiB cat` against a
loopback S3 peer. It sent HEAD followed by GET, returned exactly 2,048 fixture
bytes with exit zero, and took 1.019 seconds, consistent with the initial
one-second burst plus one second of refill. The first probe fixture omitted
Last-Modified and was rejected by mc before GET; the completed probe supplied
the required header. The probe server and thread were stopped afterward.

## ESS implementation boundary

One limiter belongs to the invocation supervisor and terminates with its owner.
Each sanitized configured endpoint and direction has a shared FIFO token bucket;
concurrent parts and requests use the same initial burst and refill budget.
Source and destination endpoints have independent budgets. Bucket identities
omit credentials, query parameters, and fragments. Integer nanosecond accounting
avoids float rounding at large configured rates. Waiting does not serialize
unlimited requests or hold the limiter server while sleeping. Pending waiters
are bounded by invocation request concurrency, and endpoint/direction buckets
have a finite bound.

The S3 client passes its configured service endpoint before bucket addressing
and object-path resolution, so aliases using the same endpoint share their
budget across objects and buckets. Host case and trailing endpoint slashes are
normalized consistently with endpoint resolution. Direct adapter callers that
omit the configured endpoint share an origin bucket (scheme, host, and port)
across request paths; separate services at different base paths on that origin
require an explicit `rate_endpoint` to obtain separate buckets.

The adapter charges slices of at most 64 KiB and at most one second of the
configured budget before sending upload bytes or delivering download bytes to
the sink. An upload proxy retains at most one bounded source chunk and forwards
the source acknowledgement only after all of that chunk has been sent. It
preserves trailers and exact entity bytes. Finite binary uploads receive the
same derived Content-Length that http_fetch would ordinarily generate; an
existing explicit length remains intact. No payload checksum, signed header,
AWS streaming encoding, compressed body, or Content-Encoding is rewritten.

Download pacing covers both control-body materialization and raw object
stream consumption. An already materialized control response is marked as
charged so it cannot consume the budget twice. Compressed bytes are charged and
delivered unchanged with `decode_body: false`.

Cancellation, caller/guard death, invocation death, and overall request deadlines
remove waiting reservations without borrowing future credit. The upload proxy
owns its worker and wrapper stream and stops them when its request owner exits.
Existing transport cancellation owns the original upload/response stream.
Application entity-byte pacing does not implement independent socket idle
deadlines. `--conn-read-deadline` and `--conn-write-deadline` await a compatible
0.18 release of the main-branch fix for
[`gsmlg-dev/http_fetch#126`](https://github.com/gsmlg-dev/http_fetch/issues/126);
infinite request timeout support separately depends on
[`gsmlg-dev/http_fetch#123`](https://github.com/gsmlg-dev/http_fetch/issues/123).

## Validation

Ten limiter tests verify the human-size grammar, sanitized identity, deterministic
one-second burst and refill, FIFO sharing, endpoint/direction independence,
conflicting rates, queue bounds, deadline/abort/owner cleanup without future
credit debt, and bounded exact-byte reduction. Seven real-wire and ownership
tests verify signed finite upload length/hash/body preservation, acknowledged
stream upload framing, raw gzip download bytes and bounded sink slices,
concurrent aggregate upload pacing, shared control download pacing, proxy
cleanup on owner death, and refusal before I/O without invocation ownership.

Together with the HTTP adapter, Runtime, native globals, and S3 Client suites:
**41 tests, zero failures**. Strict CLI compilation and scoped formatting passed.
Two fixture corrections enabled the existing streaming handler and supplied the
correct `endpoint_url` context option. No production workaround was needed.
The contract is source-reviewed against pinned mc, with the representative
download timing probe above; it does not claim exact human-size grammar parity.
No remote cloud-provider throughput or HTTP/2 rate test was run. All owned test
commands and loopback servers completed or were stopped by fixture cleanup.
