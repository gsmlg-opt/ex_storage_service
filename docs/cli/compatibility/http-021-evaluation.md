# HTTP 0.21.12 integration and remaining acknowledgement blocker

The user approved the coordinated dependency update on 2026-10-16. The working
copy based on main `2fe20dbe8ba643344f179da945e82e1d5b0d4a6a` now adds
`~> 0.21.12` to core's existing 0.17/0.18 alternatives, requires `~> 0.21.12`
in the CLI, and selects the published coordinated 0.21.12 HTTP family in the
umbrella lock. DuskMoon's already-allowed 9.16.16 npm/QuickBEAM components accept
this family. No dependency override or patched source is used. These are
integrated working-copy changes, not a completed commit, remote check or release.

The adapter now passes independent HTTP/1 socket read/write inactivity options
and `response_stream_idle_timeout: :infinity`. AWS `--cli-read-timeout` is a
socket read limit: positive seconds become read inactivity, zero disables it,
and neither imposes a whole-request deadline. `--cli-connect-timeout` remains
independent, with zero selecting infinity. Native contexts have no total request
deadline and default to separate 600,000 ms read/write inactivity; the two
`--conn-*-deadline` options can change or disable those limits. Explicit finite
adapter/overall retry budgets remain effective. The native parser uses retained
Go-duration observations and rejects positive durations not exactly representable
in whole milliseconds or beyond the transport's 4,294,967,295 ms bound.

## Published source and scoped wire evidence

Upstream `http_fetch` v0.21.12 is commit
`689260cf0af92689793ad8dd888496a62bb140b1`; fix
`8accaad52e760a5b325346dabafcacd3f5dc8fa3` (PR #130) closes
[#129](https://github.com/gsmlg-dev/http_fetch/issues/129). Exact Hex-resolved
API/options/stream/socket-client/response files match that released source.
The new request-scoped response-stream idle option fixes the separate
network-await limit. It deliberately preserves finite consumer acknowledgement
budgets; that distinction matters for the remaining blocker below.

Independent published-dependency tests passed six real TCP cases: direct ACK
consumption and `HTTP.Response.read_all()` each completed exact `xy` after a
65,000 ms peer gap (65,002 ms observed), typed socket read expiry occurred at
122 ms, and socket/stream finite-idle reset and expiry cases passed. Concurrent
requests with 100 ms versus infinite stream policies expired versus completed
independently and closed their owned peers. Global streaming timeout stayed
60,000 ms; no application environment or dependency source was mutated.

The CLI integration's native-default and AWS-zero paths independently completed
raw-byte transfers over a 65,000 ms gap at 65,054 ms, with response controllers
and cancellation processes stopped afterward. The focused deadline acceptance
run passed 73 tests, including real socket read/write stalls, idle resets, finite
AWS one-second read semantics with a progressing response lasting longer than
one second, zero-disable behavior, explicit total/retry bounds and cleanup.
The combined CLI suite then passed **3,244 tests, zero failures and one
resource exclusion** with seed 628644 in 272.2 seconds. A fresh affected-app
snapshot passed core 344 tests (one cluster exclusion), cluster 39 tests (one
large-stream exclusion), S3 224 tests and web 21 tests, all with zero failures
and the same seed. These runs validate the current local integration; fresh
package acceptance and remote checks remain separate evidence, and the unscaled
slow-delivery failure below still blocks release.

Evidence roots:

- `/tmp/ess-cli-http02112-wire-20261016/`: `mix.lock`,
  `source-verification.json`, `wire-result.log`, `concurrent-result.log`.
- `/tmp/ess-cli-deadline-02112-20261016/validation/`:
  `candidate-long-idle.log`, `aws-positive-read-green.log`,
  `deadline-acceptance-final.log` and `candidate-slow-ack.log`.
- `/tmp/ess-cli-http02112-coordinated-20261016/`: `cli-full.log` and
  `affected-apps.log`.
- Repository regressions: `test/deadline_integration_test.exs`,
  `test/aws_read_deadline_test.exs`, `test/native_deadline_duration_test.exs`
  under `apps/ex_storage_service_cli/`.

## Current release blocker: paced consumer acknowledgement

[#131](https://github.com/gsmlg-dev/http_fetch/issues/131) is the current
upstream **Feature / internal request / blocker** for CLI M9 and release of
bounded slow-rate transfers. With published 0.21.12, a native request with total
timeout infinity, 600,000 ms socket inactivity and response-stream idle infinity
still inherits a fixed 60,000 ms consumer ACK deadline. Both
`HTTP.Stream`'s pending-ACK wait and `SocketClient.stream_chunk_timeout/1` impose
this limit; changing only the response stream's idle policy does not remove it.

An unscaled real loopback response immediately supplied 512 bytes. The
invocation-owned download limiter delivered eight-byte slices at 8 bytes/s:
64 bytes at 7,052 ms, 448 at 55,100 ms and all 512 exact bytes at 63,108 ms.
Consumption then returned transport `:timeout` at 63,109 ms. The stream,
controller and cancellation processes were dead; the diagnostic subsequently
stopped its limiter. Global `HTTP.Config.streaming_timeout()` stayed 60,000 ms.
This failed transfer is not a successful completion merely because all payload
bytes reached the sink. It is consumer delivery time, not network inactivity
or an overall request deadline.

The callsite in `Transport.HTTPFetch.consume_owned/4` records #131. No early ACK,
unbounded mailbox reading, global timer mutation, dependency patch or finite
substitution is used as a workaround. The blocked slice stops until a published
request-scoped ACK/delivery policy or bounded progress lease is available. Both
upstream ACK limits, concurrent policy isolation, finite overall deadlines and
cancellation/owner cleanup require validation, followed by independent real
paced-transfer acceptance in ESS. The passing 65-second network-gap probes do
not close this gate.

## Historical 0.21.11 evaluation

On 2026-10-15 the repository still selected 0.18.1 and the dependency scope
change had not been authorized. Published 0.21.11 contained HTTP/1 socket
inactivity from PR #127, fixing #126; #123 remained the supported-line infinity
backport request. A temporary copy at
`/tmp/ess-cli-http021-evaluation-20261015` changed only its own requirement and
build/dependency/lock roots. An unfinished sharing guard was corrected only in
that copy to permit investigation. The repository dependency graph was unchanged
at that historical checkpoint.

The 47-test transport investigation exposed a cancellation-adoption race, fixed
in the ESS adapter by checking its owned abort controller before reading an
already-terminated stream. That correction also has a deterministic regression
on 0.18; it was not an upstream defect. A prior nonfixed-seed source-close
notification assertion failed at 100 ms with unresolved cause; its failed log
was retained, no assertion weakened. A fixed-seed run passed 47 tests. Three
additional raw-socket probes passed infinity, idle reset and typed read expiry;
these short probes did not establish the default long-gap contract. Historical
logs remain `/tmp/ess-cli-http021-{deps,transport,cancellation-diagnostic,transport-fixed,transport-fixed-rerun,deadline-probe}.log`.

A later isolated 0.21.11 candidate at
`/tmp/ess-cli-deadline-integration-20261015-b` passed 67 scoped tests, but its
response stream still used the separate global 60-second idle timer. With no
configuration or source mutation, an acknowledged first byte arrived at 22 ms,
the next byte was due at 65 seconds, and generic `:timeout` occurred at
60,023 ms despite request infinity and 120,000 ms socket read inactivity. The
peer subsequently finished normally. The diagnostic observed a failure; its
zero exit status did not mean a successful transfer. This was #129's
version-specific blocker, now fixed in published 0.21.12.

The original unapplied 0.21.11 proposal remains historical at
`/tmp/ess-cli-deadline-integration-20261015-b/deadline-proposal.patch`, SHA256
`18363c6e8cba31da6560ff4aa2d82f593b5abd2eee12d8b52f4e16f9b076f6b5`.
Its then-unverified duration prototype is superseded by the integrated parser
and retained Go oracle. #123's older-line backport status and #126/#129's old
limitations are not blockers of the selected 0.21.12 graph; #131 is a distinct
remaining capability gap.
