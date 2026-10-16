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
Application entity-byte pacing remains distinct from socket inactivity. The
approved working-copy integration now uses published HTTP 0.21.12, independent
native read/write inactivity defaults of ten minutes, request infinity and an
infinite response-stream idle policy. The older #123/#126 limitations of the
0.18 line and #129 network-await limit of 0.21.11 no longer block that selected
graph. These changes do not extend consumer acknowledgement deadlines.

## Current slow-delivery release blocker

[http_fetch#131](https://github.com/gsmlg-dev/http_fetch/issues/131) is an open
Feature/internal-request blocker for the paced-transfer contract and M9 release.
Published 0.21.12 preserves a global 60,000 ms limit both for an outstanding
consumer ACK in `HTTP.Stream` and for the socket client's wait for chunk
acknowledgement. Delivering one bounded chunk can exceed that limit at a valid
low configured rate; an infinite response-stream idle policy does not cover it.

A real unscaled loopback response immediately returned 512 bytes. With native
request infinity, read/write socket inactivity 600,000 ms and download pacing
at 8 bytes/s, the adapter delivered 64 bytes at 7,052 ms, 448 at 55,100 ms and
512 exact bytes at 63,108 ms, then returned transport `:timeout` at 63,109 ms.
The stream, controller and cancellation processes were stopped at the result;
the diagnostic then stopped its limiter. Global streaming timeout remained
60,000 ms. The full result is in
`/tmp/ess-cli-deadline-02112-20261016/validation/candidate-slow-ack.log`.
All bytes reaching the sink does not establish successful transport completion
or atomic publication.

The response-consumption callsite records the upstream TODO. Work on the
blocked delivery contract stops until a published per-request ACK/delivery
policy or supported bounded progress lease fixes both timers. Do not acknowledge
before delivery, switch to unbounded mailbox consumption, alter VM-wide timers
or patch dependency source. After the upstream release, independently repeat
real pacing past 60 seconds, concurrent policy isolation, finite total limits
and cancellation/owner cleanup. Passing shorter pacing tests or 65-second gaps
between immediately acknowledged network chunks does not close this gate.

## Validation

Ten limiter tests verify the human-size grammar, sanitized identity, deterministic
one-second burst and refill, FIFO sharing, endpoint/direction independence,
conflicting rates, queue bounds, deadline/abort/owner cleanup without future
credit debt, and bounded exact-byte reduction. Seven real-wire and ownership
tests verify signed finite upload length/hash/body preservation, acknowledged
stream upload framing, raw gzip download bytes and bounded sink slices,
concurrent aggregate upload pacing, shared control download pacing, proxy
cleanup on owner death, and refusal before I/O without invocation ownership.

On the prior 0.18.1 snapshot, the HTTP adapter, Runtime, native globals and
S3 Client suites together passed **41 tests, zero failures**. Strict CLI
compilation and scoped formatting passed.
Two fixture corrections enabled the existing streaming handler and supplied the
correct `endpoint_url` context option. No production workaround was introduced
by that slice; the later long-delivery probe above exposed the separate upstream
ACK limit.

The current 0.21.12 combined CLI suite passed **3,244 tests, zero failures and
one resource exclusion** (seed 628644, 272.2 seconds); all four affected-app
suites also passed with zero failures. Logs are
`/tmp/ess-cli-http02112-coordinated-20261016/{cli-full,affected-apps}.log`.
These runs do not close the independently reproduced unscaled #131 failure
or establish fresh package or remote acceptance.

The contract is source-reviewed against pinned mc, with the representative
download timing probe above; it does not claim exact human-size grammar parity.
No remote cloud-provider throughput or HTTP/2 rate test was run. All owned test
commands and loopback servers completed or were stopped by fixture cleanup.
