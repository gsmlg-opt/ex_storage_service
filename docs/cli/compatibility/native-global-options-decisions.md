# Native global options

This slice implements invocation-scoped `--config-dir` / `-C`,
`--disable-pager` / `--dp`, `--insecure`, and repeated `--custom-header` /
`-H`. Native rate flags are integrated by the separate rate-limiter slice.
The six focused native-global tests and existing parser/resolver checks pass
(19 tests total), and `mix compile --warnings-as-errors` passes. This document
does not claim complete native-global parity.

Native `--resolve` now has a separate implementation and independent socket/TLS
evidence in [native-resolve-decisions.md](native-resolve-decisions.md). It uses
http_fetch's authority-preserving address pin API. The integrated inactivity
deadlines and separate slow-delivery acknowledgement blocker are described below.

`--config-dir` selects `<directory>/config.toml` for native profiles, aliases,
interactive configuration, and transfer recovery. It overrides the normal ESS
configuration location for this invocation without changing environment
variables. AWS-profile references still use their separate AWS configuration.
`--disable-pager` selects the existing no-pager option; native help/output does
not start an implicit pager. Explicitly disabling TLS verification emits one
stderr warning per native invocation, leaving structured stdout intact.

Custom headers are split at the first colon, trim surrounding spaces/tabs,
validate HTTP field names and values, and retain repeated values in input
order. They replace matching modeled headers case-insensitively before
checksums and signing. Authentication, Host, hop-by-hop/framing, Content-Length,
Content-MD5, Content-Encoding, security-token, checksum, and encoded-trailer
headers are reserved. Errors identify the invalid option without echoing its
potentially sensitive value. Content-Encoding is reserved because callers must
not accidentally turn ordinary bytes into an `aws-chunked` framing claim.

## Pinned mc evidence and deliberate differences

The reference is mc revision
`77f82e18b5401a65958f1619df6ebb994634bd88`, executable SHA256
`a62c4c85f9efbe5d07e03e2ff9923625fbf659a9f272a077a0cd846279f1ccc2`.
Its `cmd/flags.go` declares these spellings, `cmd/globals.go:211–224` parses
repeated fields, and `cmd/client.go:415–419` copies custom headers in the
transport after signing. ESS deliberately signs the final custom headers and
rejects reserved overrides. ESS permits mixing long and short custom-header
spellings; the pinned mc executable rejects that combination with
`Cannot use two forms of the same flag: H custom-header`.

The loopback generator is `test/fixtures/differential/capture_native_globals.py`.
Its committed `native-globals-wire.json` records one successful repeated-header
exchange and three actual invalid-header outcomes. Every valid request contains
`first:part`, then `second`; mc's SignedHeaders omit `x-trace`. All three malformed
inputs exit zero and issue requests. `cmd/main.go:367` calls
`setGlobalsFromContext(ctx)` without checking its returned validation error.
ESS deliberately rejects these malformed inputs before dispatch. The fixture
SHA256 is `8402add47920ae4df847b7a9fc4a58d1681cd5597c14a90c6d6a02c4d867eb11`.

Capture command (repository root):

```sh
PAGER=cat python3 apps/ex_storage_service_cli/test/fixtures/differential/capture_native_globals.py --mc /tmp/ess-cli-reference-m0/mc --output apps/ex_storage_service_cli/test/fixtures/differential/native-globals-wire.json
```

The first invocation used mixed `-H` / `--custom-header` and failed with
`Cannot use two forms of the same flag: H custom-header`. The same command after
switching to repeated long flags reached the peer but failed with
`501 Unsupported method ('HEAD')` (first fixture repair). The next invocation,
after adding HEAD support, passed the valid-header assertions but failed at
`assert result.returncode != 0 and not requests` (second fixture repair).
The coordinating agent approved bounded capture recovery: record actual invalid
outcomes without asserting expected rejection. The final command passed; the
ESS rejection tests remain strict. These were reference-capture expectation
failures, separate from the single successful production integration repair.

The pinned mc pager applies to terminal help (`cmd/main.go:526`), rather than
all command output. Environment aliases such as `MC_CONFIG_DIR`,
`MC_DISABLE_PAGER`, and `MC_INSECURE` are outside this scoped implementation.

## Independent inactivity deadlines and acknowledgement boundary

`--conn-read-deadline` and `--conn-write-deadline` are now accepted and routed
through both modeled and legacy clients to published HTTP 0.21.12. Native
contexts default to 600,000 ms for each socket inactivity limit and no total
request deadline; connection establishment retains its separate 60,000 ms
limit. Zero disables the selected read/write inactivity limit. These are not
aliases for total request or connection durations.

The pinned mc implementation resets read/write deadlines in
`pkg/deadlineconn/deadlineconn.go`. ESS retains actual Go-duration oracle cases,
including compound/fractional syntax and signed zero, while enforcing the
transport precision boundary: positive values must be exactly representable in
whole milliseconds from 1 through 4,294,967,295. Negative, overflowing,
malformed, invalid UTF-8 and oversized inputs fail before dispatch; positive
sub-millisecond values are rejected rather than rounded. This precision/range
boundary is an explicit native difference.

The user approved and the working copy integrates the coordinated 0.21.12
dependency update. #126's socket-inactivity fix and #129's request-scoped
response-stream idle fix are available on this selected line. Typed read/write
stalls, reset trickles and ownership cleanup have scoped real-wire tests;
native-default and AWS-zero reads also passed an unchanged-default 65-second
network gap. The combined CLI suite passed 3,244 tests with zero failures and
one resource exclusion (seed 628644); core, cluster, S3 and web suites also
passed with zero failures. Fresh package/install/runtime acceptance passed on Linux x86_64 Elixir
1.18.5/OTP 28; see integration-validation.md for the artifact and source scope.
New-source remote checks remain pending. Current logs are
`/tmp/ess-cli-http02112-coordinated-20261016/{cli-full,affected-apps}.log`.

A separate current release blocker is
[http_fetch#131](https://github.com/gsmlg-dev/http_fetch/issues/131): slow
application delivery can exceed the fixed 60-second consumer acknowledgement
budget even while socket inactivity is disabled or longer. A 512-byte response
paced at 8 bytes/s delivered exact bytes at 63,108 ms but returned `:timeout`
at 63,109 ms. No global timer change or local ACK workaround is applied. See
[http-021-evaluation.md](http-021-evaluation.md) for the independent deadline
evidence, historical 0.18/0.21.11 limits and the required upstream acceptance.
