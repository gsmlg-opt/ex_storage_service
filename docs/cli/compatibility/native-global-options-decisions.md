# Native global options

This slice implements invocation-scoped `--config-dir` / `-C`,
`--disable-pager` / `--dp`, `--insecure`, and repeated `--custom-header` /
`-H`. Native rate flags are integrated by the separate rate-limiter slice.
The six focused native-global tests and existing parser/resolver checks pass
(19 tests total), and `mix compile --warnings-as-errors` passes. This document
does not claim complete native-global parity.

Native `--resolve` now has a separate implementation and independent socket/TLS
evidence in [native-resolve-decisions.md](native-resolve-decisions.md). It uses
http_fetch's existing authority-preserving address pin API; the inactivity
deadline dependency block below remains separate.

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

## Independent inactivity deadlines remain blocked

mc's `--conn-read-deadline` and `--conn-write-deadline` reset separate socket
deadlines before each read/write (`pkg/deadlineconn/deadlineconn.go`), with a
ten-minute default. These are independent inactivity deadlines, not total
request durations. The locked `http_fetch` 0.18.1 API cannot express this
contract. [http_fetch#126](https://github.com/gsmlg-dev/http_fetch/issues/126)
was fixed on upstream main by PR #127 on 2026-10-15, but no compatible 0.18
release contains the fix. Dependency availability still blocks these two flags.
They remain rejected; no total-timeout substitution or local transport workaround
is implemented.
