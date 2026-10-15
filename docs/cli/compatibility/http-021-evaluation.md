# HTTP 0.21.11 isolated evaluation

This is a dependency-selection investigation, not a repository dependency update
or full acceptance of a different HTTP family. The repository still selects
0.18.1. The CLI scope in implement-plan.md section 1.1 remains in force while the
scope question is pending.

On 2026-10-15, Hex and the upstream release metadata confirmed http_fetch
0.21.11 contains the independent HTTP/1 socket inactivity fix from PR #127.
Published 0.18 versions remain 0.18.0 and 0.18.1; issue #123 is still open for a
compatible infinity-deadline backport. Core requires the 0.17/0.18 families and
locked DuskMoon npm/QuickBEAM components require 0.18. Selecting the newer HTTP
family in the umbrella requires a separately authorized dependency scope change,
without overrides.

A copied CLI snapshot under `/tmp/ess-cli-http021-evaluation-20261015` used
independent dependency, lock and build directories. Only that temporary project's
HTTP requirement changed to `~> 0.21.11`. Published dependencies resolved and
compiled under Linux x86_64, Elixir 1.18.5 and OTP 28. The copy initially captured
an unfinished sharing edit with an invalid guard; a temporary-only guard fix
allowed the transport investigation. No repository production dependency was
changed by these commands.

The existing 47 transport/spike tests initially exposed a cancellation handoff
race: after coordinator cancellation and before response adoption, the new
transport could already have terminated the stream. Monitoring then observed
`:noproc`, losing the known interrupted outcome. The adapter now checks its owned
abort controller during adoption and returns the typed aborted result before
reading. This client ownership correction is present in the repository and has
a deterministic regression on the selected 0.18 line. It is not an upstream
defect or a substitution of a transport failure as success.

With that temporary correction, the fixed-seed transport run passed **47 tests**.
A prior nonfixed-seed run failed its 100 ms source-close notification assertion;
the cause was not established and the failed log is retained. It is not counted
as passing or silently removed. No source-close assertion was weakened.

Three additional actual raw socket probes passed: infinite connect/request
settings with acknowledged raw-byte consumption; read inactivity resetting
across fragments over a total duration longer than the idle bound; and stalled
response termination with `read_idle_timeout`. The temporary adapter removed its
0.18 infinity guard and forwarded the actual new options. These probes do not
establish CLI flag integration, independent write inactivity, retry/rate-limit
arithmetic with infinity, full transfers, providers or the platform matrix.

Evidence:

- `/tmp/ess-cli-http021-deps.log`
- `/tmp/ess-cli-http021-transport.log` (initial cancelled-response failure)
- `/tmp/ess-cli-http021-cancellation-diagnostic.log` (closed-stream reason)
- `/tmp/ess-cli-http021-transport-fixed.log` (unresolved source-close timing failure)
- `/tmp/ess-cli-http021-transport-fixed-rerun.log` (47 passing fixed-seed tests)
- `/tmp/ess-cli-http021-deadline-probe.log` (three passing deadline probes)

The shared repository dependency graph, complete CLI tests and source package
must be validated again if the new HTTP line is selected. This investigation
alone does not close either release gate.

## Narrower dependency selection found during follow-up

Current Hex metadata shows that DuskMoon's published 9.16.16 npm/QuickBEAM
components accept `~> 0.20.0 or ~> 0.21.0`. The existing web requirement
`duskmoon_bundler ~> 9.8` already permits that version; selecting HTTP 0.21.11
does not inherently require a UI major-version change. The remaining source
constraint outside CLI scope is core's `~> 0.17.2 or ~> 0.18.0` requirement.
The pending scope decision still governs changing that constraint and validating
core plus the coordinated graph. No repository requirement or lock was changed.

The concrete source change would add `~> 0.21.11` to core's supported alternatives
and require `~> 0.21.11` in the CLI, then resolve the coordinated HTTP/DuskMoon
packages without overrides. Selection alone would still require complete
transport/transfer and affected-application validation. Published metadata:

- [DuskMoon npm 9.16.16](https://hex.pm/api/packages/duskmoon_npm/releases/9.16.16)
- [DuskMoon QuickBEAM 9.16.16](https://hex.pm/api/packages/duskmoon_quickbeam/releases/9.16.16)

## CLI integration candidate and remaining response-stream blocker

A second standalone copy at
`/tmp/ess-cli-deadline-integration-20261015-b` prepared the actual CLI changes
against published HTTP 0.21.11, again with independent dependency/build/lock
roots and no repository constraint or lock changes. Its focused suite passed
67 tests, including eleven new deadline cases: AWS zero parsing and real wire
execution; native ten-minute read/write defaults, durations and zero-disable;
legacy and modeled paths; real header/body read stalls and a socket write
stall; idle-reset trickles; lease/controller/peer cleanup; upload/download
pacing; finite overall retry caps and idle-error classification. These are
scoped candidate results, not acceptance of the repository or the full default
deadline behavior.

Further checking found a separate documented global response-stream idle timer.
`HTTP.Stream` expires using `HTTP.Config.streaming_timeout()` (default 60,000
ms) without inheriting the individual request's read-idle policy. A controlled
real socket reproducer consumed and acknowledged a first body chunk, then
waited for the next network bytes. Scaling that separate global fixture timer
caused the stream to expire before the larger per-request read-idle deadline,
despite `timeout: :infinity`. Thus selecting 0.21.11 alone cannot supply the
required ten-minute native body inactivity policy or AWS's disabled read timeout.

The required upstream Feature request is
[gsmlg-dev/http_fetch#129](https://github.com/gsmlg-dev/http_fetch/issues/129),
severity **blocker**, with the network-await reproduction and expected
request-scoped stream policy. The repository response-stream callsite records
the TODO. No global configuration mutation, patched dependency or finite-timeout
substitution is used in the product. This blocked slice stops until an upstream
release supplies the contract and the coordinated graph is authorized and
validated. The separate native write-idle candidate remains scoped evidence;
it does not close the combined deadline gate.

The unscaled default-config probe independently reproduced the same blocker
without any application environment or dependency source mutation. With request
timeout infinity, socket read idle 120,000 ms and the peer's second byte due at
65,000 ms, the first byte was consumed/acknowledged at 22 ms and the stream
reported generic `:timeout` at **60,023 ms**. The peer then completed normally
and was monitored down. The diagnostic exited zero because it observed the
expected failure; this is not a successful two-byte transfer. Exact local
evidence: `default_stream_idle_reproducer.exs` and
`default-stream-idle-result.log` in the second isolated copy above.

The temporary 11-file proposal is retained at
`/tmp/ess-cli-deadline-integration-20261015-b/deadline-proposal.patch`, SHA256
`18363c6e8cba31da6560ff4aa2d82f593b5abd2eee12d8b52f4e16f9b076f6b5`.
Its native exact Go duration grammar is still unverified and requires tightening
before any future integration. The patch remains unapplied; short scoped passes
cannot override the default-config failure or close the release gate.
