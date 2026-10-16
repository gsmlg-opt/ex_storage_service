# Coordinating CLI integration validation

Original source baseline: `main` at `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the implementation diff. These are local pre-release checks on Linux x86_64, Elixir 1.18.5 and OTP 28. They do not establish a published next-version artifact. The latest coordinated dependency integration is recorded at the end; earlier results retain their historical source scope.

The focused final-repairs command passed 132 tests with seed `628644`: the optional-member executable matrix, upload/request/checksum families, native sharing and cancellation ownership. Log: `/tmp/ess-cli-scoped-final-repairs.log`.

The subsequent full isolated CLI command was `PAGER=cat mix test --exclude resource --seed 628644`, run from `apps/ex_storage_service_cli`. It finished in 212.3 seconds with 3,155 tests, one failure and one resource exclusion. Log: `/tmp/ess-cli-final-integration-20261015-rerun.log`. The failing JournalTest received `journal_busy` while creating an index checkpoint; the original error does not identify which peer occupied the mutex port. This failed run is retained, not counted as a passing integration run.

The unchanged journal suite then passed all eleven tests in isolation with the same seed and `--no-compile`. Log: `/tmp/ess-cli-journal-diagnostic.log`. Journal ownership deliberately fails closed on deterministic TCP-port collisions or unrelated listeners. Its tests now run synchronously to avoid sharing the host port namespace with concurrent wire fixtures. Cross-process, equivalent-path, unrelated-listener and abrupt-VM-death tests retain their assertions. This isolation change does not alter production ownership or prove the exact source of the earlier collision.

The accompanying native audit found accepted options without effects in `pipe` and versioned `ls`. Their fixes passed 61 and 62 scoped tests respectively. After integration, the same complete command finished in 212.8 seconds with **3,175 tests, zero failures and one resource exclusion**. Log: `/tmp/ess-cli-final-integration-20261015-rerun2.log`. The full CLI format check and strict compile also passed.

The following safety audit identified missing selected-version GET confirmation, premature recursive-move source deletion and absent source SSE-C copy headers. Those subsequent changes require their own focused regressions and another complete integration run. Native mirror/diff policy flags also require explicit rejection rather than accepting unsupported AWS policy switches. The passing 3,175-test snapshot does not validate later edits or establish the final release gate.

The move/selection/source-encryption correction and native policy guards then
passed 89 scoped tests, with strict compile and format checks. See
move-source-encryption-decisions.md and native-policy-flags-decisions.md. The
next complete run finished in 213.9 seconds with **3,192 tests, two failures and
one resource exclusion**, retained at
`/tmp/ess-cli-final-integration-20261015-current.log`. It exposed two further
integration failures: TransferCommandsTest received `journal_busy` during
concurrent wire tests, and ExecutableRawTest's AWS stdout fixture omitted the
ETag selected by its preceding HEAD. Journal-using test modules now execute
synchronously; production ownership and collision assertions remain intact.
The raw fixture now returns the selected ETag while retaining its exact 256-byte
alphabet assertion. These corrections require a fresh passing integration run;
the failed snapshot is not counted as acceptance.

Full-object bridge GET identity now has its own reproducer: the original code
staged mismatched full GET bytes and uploaded them to the destination despite
the selected source identity. The corrected bridge plus executable raw/copy
suite passed 13 tests; see `/tmp/ess-bridge-selection-scoped.log`. Selected
GETs confirm ETag/version before staging or upload. A native styling correction
then passed 31 focused tests, and bounded tag selection passed 63 management
tests including an actual Runner wire case. Their decision reports describe
the exact limits. Native age/storage-class transfer selection subsequently
passed 78 scoped tests, including twelve new selection cases and an actual
locked mc capture.

The integrated implementation then passed the complete CLI command with the
same seed: **3,222 tests, zero failures and one resource exclusion**, in 218.3
seconds (23.5 seconds async, 194.7 seconds sync). Log:
`/tmp/ess-cli-final-integration-20261015-stable.log`. This run includes the
move/source-encryption, bridge identity, journal-test isolation, native policy,
color, tag and age/class corrections above. A final indentation-only format
correction was followed by a passing whole-CLI `mix format --check-formatted`
and `mix compile --warnings-as-errors`. This result precedes the native
portability repairs recorded below.

This local result does not close the HTTP dependency/deadline gate, remote
platform/provider jobs or publication verification. The independent 2 GiB
resource evidence remains in resource-filesystem-acceptance.md; the exclusion
above does not count it as part of this command.

Staged whitespace checking preserves the byte-exact upstream reference snapshots
and licensed parser ports. The full check reports their original whitespace;
the scoped check excluding those imported files passes. The native checklist's
own trailing whitespace was removed. Reference hashes and deterministic output
checks pass, so no pinned capture was rewritten to satisfy whitespace style.
The historical final source-only edit added the required TODO for http_fetch#129; it changed
no execution behavior and was followed by another passing format/strict compile.

## Remote compiler and formatter repair

The first pushed implementation exposed CLI portability issues in real CI:
fortified Ubuntu rejects an unchecked signal-handler write; Darwin's selected
feature macros hide `O_NOFOLLOW`; Elixir 1.19 formats two expressions differently.
The C handler now checks the write, retries EINTR and coalesces a saturated
nonblocking pipe while preserving pending bits, errno and handler cleanup. Darwin
uses its real declarations, with no zero-valued no-follow fallback. Small bindings
retain equivalent multipart and credential assertions under both formatters.

Fortified Linux and a Linux-header non-Linux guarded C build pass; the latter is
not Darwin runtime evidence. Both Elixir 1.18.5 and 1.19.6 whole-CLI format checks
pass. Strict compilation and the affected signal/executable/cancellation,
filesystem, multipart and credential-provider suites pass **46 tests, zero
failures**, seed 628644, in 9.4 seconds. Log:
`/tmp/ess-cli-ci-portability-scoped.log`. Assertions were retained. The remote
platform, complete CLI and resource/provider jobs must be inspected after pushing
these repairs; the earlier failed jobs are not counted as accepted contracts.

The next [remote run](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38738333317)
cleared the formatter and signal-bridge errors, then exposed equivalent fortified
write-result and Darwin-declaration errors in the pager helper. Both native
artifacts now pass explicit fortified builds. Pager ready/result writes retry
EINTR and handle short writes; a broken gate kills/reaps the child and restores
the terminal before reporting failure. The runner handles SIGPIPE while the exec
child retains its normal default. The real no-follow flag remains required.

Strict compilation and pager PTY, executable/native signals and filesystem
regressions then passed **25 tests, zero failures**, seed 628644, in 8.6 seconds.
Log: `/tmp/ess-cli-pager-portability-scoped.log`. Direct injected write/subprocess
probes also cover zero/EPIPE/EAGAIN, failed-gate no-exec and exact result bytes.
Actual remote platform/resource/provider acceptance remains separate from these
local results.

## Committed checkpoint acceptance

Source `2380deac68b3181c6192c7047364390d6cd607c9` was pushed to
`origin/main`. In [CLI run 38739026057](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38739026057),
all four standalone package jobs, the throttled 2 GiB resource job and the
independent ESS/RustFS limitation-policy job passed. Downloaded package manifests
record clean source; their tar/escript hashes were recomputed independently.
See packaging-platform-decisions.md, resource-filesystem-acceptance.md and
provider-acceptance.md for the exact contracts and retained provider failures.

The complete remote CLI test job failed: **3,221 tests, four failures and one
resource exclusion**, seed 172135, in 334.9 seconds. All four failures were
CompletionTest subprocesses receiving `:enoent` for the runner's absent `zsh`.
The [umbrella Test run](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38739026195)
also failed, with the same completion failures and one CLI RuntimeTest isolation
failure: **3,222 CLI tests, five failures**, seed 530948. Its core, cluster, S3
and web suites passed. Logs are retained at
`/tmp/ess-cli-2380dea-failed.log` and `/tmp/ess-test-2380dea-failed.log`.
Neither failed job is counted as complete-suite acceptance.

The runner workflows now install the required Zsh shell without skipping its
completion contracts. RuntimeTest's standalone no-server assertion belongs in
a fresh VM because umbrella Mix has already started server applications; its
invocation-owned task/provider cleanup assertions remain in process. These
test-environment corrections require a fresh remote result. No release or
deadline gate is closed by the successful package/resource/provider jobs.

The corrected runtime and unchanged Bash/Zsh completion suites passed **13
tests, zero failures**, seed 628644, in 10.5 seconds. The runtime test formats
under Elixir 1.18.5 and 1.19.6, and both edited workflows pass actionlint.
Local umbrella execution did not reach tests because its existing web
`build_info` compile environment differs from the checkout; it was not altered
or bypassed. The clean remote umbrella job remains the integration check.

## Accepted CI environment correction

Clean source `6816ee14c87ccc8de76a37a41811ec46a0c7aa9e` was pushed to
`origin/main`. [CLI run 38740996062](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38740996062)
passed all seven jobs. Its full CLI suite passed **3,222 tests, zero failures
and one resource exclusion**, seed 146899, in 339.1 seconds. Inventory/tooling,
formatting and strict compilation also passed. All four standalone package jobs,
the separate 2 GiB test and independent provider limitation-policy job passed;
their downloaded evidence is recorded in the companion reports. Log:
`/tmp/ess-cli-6816ee1-run.log`.

[Umbrella Test run 38740995957](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38740995957)
also passed: **3,223 CLI tests, zero failures**, seed 456119, in 357.6 seconds,
including the default-size resource test. Core, cluster, S3 and web suites had
zero failures. The new fresh-VM startup test and actual Zsh completion contracts
passed in both contexts; the earlier failures are resolved. Log:
`/tmp/ess-test-6816ee1-run.log`. CI, Cluster Integration and E2E workflows also
passed at the same source. No server source, dependency constraint or lock was
changed for these CI corrections.

This closes the current complete-suite repair gate. It does not close the
documented HTTP deadline blocker, remaining independent compatibility evidence,
live AWS account-specific acceptance or publication gates. No version bump,
release tag, CLI publication or next-version release has occurred.

## Approved HTTP 0.21.12 integration

The user approved the coordinated dependency proposal on 2026-10-16. Core adds
`~> 0.21.12` alongside its existing ranges; the CLI requires it. Sixteen lock
entries change: the eight HTTP/QUIC/SSL packages select 0.21.12 and the eight
Bundler/native packages select 9.16.16. Existing Bandit 1.12.5, thousand_island
1.5.0, phoenix_duskmoon 9.16.11 and web constraints are preserved. No overrides,
path dependencies, core behavior or UI changes are used.

A fresh exported snapshot of baseline `2fe20dbe8ba643344f179da945e82e1d5b0d4a6a`
contains the approved graph, native/AWS deadline integration and AWS stdin
expected-size correction. It passed locked dependency resolution and complete
umbrella strict compilation. CLI formatting and all 28 Python tooling tests also
passed. Temporary validation root: `/tmp/ess-cli-http02112-coordinated-20261016`.

From its CLI app, `PAGER=cat ERL_FLAGS='+S 4:4' mix test --exclude resource
--seed 628644` passed **3,244 tests, zero failures and one resource exclusion**
in 272.2 seconds (71.1 seconds async, 201.1 seconds sync). Log: `cli-full.log`.
This includes the real 65-second response-stream regression; it does not count
the excluded resource test or historical platform/provider jobs as new passes.

From the same fresh umbrella, `mix test apps/ex_storage_service/test
apps/ex_storage_service_cluster/test apps/ex_storage_service_s3/test
apps/ex_storage_service_web/test --seed 628644` used `PAGER=cat`,
`ERL_FLAGS='+S 4:4'` and a private `ESS_DATA_ROOT` under that validation root.
The results were core **344 tests, zero failures, one cluster exclusion**;
cluster **39 tests, zero failures, one large-stream exclusion**;
S3 **224 tests, zero failures**; web **21 tests, zero failures**.
Log: `affected-apps.log`. No shared checkout data or server behavior was changed.

The deadline acceptance separately passed 73 focused cases. Actual native and
AWS-zero requests crossed a 65-second peer gap, preserved exact bytes and
released owned response/controller/cancellation processes. Global HTTP stream
settings remained unchanged. Independent pinned AWS 2.31.18 captures and 41
focused tests establish stdin expected-size as a sizing hint while preserving
native exact-size checks. See http-021-evaluation.md and
aws-expected-size-decisions.md for their exact evidence.

A separate fresh standalone probe built the current CLI source package, resolved
the published HTTP 0.21.12 dependency family, strict-compiled it and installed
its escript. After source, build and dependency directories were renamed away,
the installed artifact passed version/help/skeleton, Unicode HTTP, exact
65,792-byte binary stdout and real pager PTY/terminal-restoration checks from an
unrelated directory. Model/license/native assets and absence of server runtime
apps were inspected. A fresh-VM runtime suite passed six tests, zero failures,
seed 700778. This evidence is Linux x86_64, Elixir 1.18.5/OTP 28 only.

Evidence root: `/tmp/ess-cli-package02112-20261016/`, including
`consumer/manifest.json`, `consumer/commands.json`, `asset-runtime-evidence.json`,
`snapshot-manifest.json`, `runtime-test.log` and `verification-summary.json`.
The package SHA256 is
`3b6227a65cfdb8fac8eea492f4fd0d9ae35fc253f10e407a3488a89584cef770`;
escript SHA256 is
`205a70a46d1c91fdf9120cbc991e358f2ae69dc1e84f5c7581c86ce07b9e04cc`.
The manifest explicitly records uncommitted source based on `2fe20dbe`; it does
not claim a clean immutable release or public package. Snapshot hashes match
the checked CLI source. New-source remote/platform/resource acceptance remains
separate from this local package probe.

Network-await issue #129 is resolved by the selected release. Slow response
consumer ACK waits remain capped at 60 seconds: an actual 512-byte native
download at 8 B/s delivered exact bytes at 63,108 ms, then failed with transport
timeout at 63,109 ms. This is upstream Feature blocker
[http_fetch#131](https://github.com/gsmlg-dev/http_fetch/issues/131).
The permanent diagnostic is
`scripts/cli_compatibility/http_consumer_ack_reproducer.exs`; failure exits
nonzero. No VM-wide timeout mutation, unbounded draining or dependency patch is
used. Passing tests do not close M9, provider limitations or publication gates.

## Pushed 600c875 checks and upload finalization blocker

Clean main source `600c8755de5ff58ee2ee3fa33203d6ea40e0497c` was committed,
pushed and independently matched against `origin/main`. The current-graph
[CLI run 38865376090](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38865376090)
passed its four Linux/macOS package jobs, throttled 2 GiB resource job and
ESS/RustFS provider-policy job. Every downloaded package manifest records this
exact clean source. Recomputed tar/escript SHA256 and inner Hex checksums match;
all six standalone HTTP-family apps are 0.21.12, native helpers/manifests are
present, and no server application is packaged. The source tar SHA256 shared by
all four platform jobs is
`c5d2b725866fc5133e3ec2f5dca7bc14ffc95755a514e636b85b1b24b214c754`.
Downloaded evidence: `/tmp/ess-cli-ci-600c8755/verified-packages.json`.

The separate 2 GiB job passed one test, zero failures, seed 952849, in 152.4
seconds with a 1 MiB budget, mailbox peak two and zero active requests/reserved
bytes after completion. Provider policy preserved the four known ESS/RustFS
failures and reported zero client regressions. Its executable was not uploaded,
so the reported provider-binary digest could not be independently recomputed.
See `verified-resources.json` and `providers/independent-policy-review.json`
under that evidence root. Live AWS account-specific evidence remains unrun.

The complete CLI unit/wire/executable job **failed**: **3,243 tests, two failures
(one excluded)** as printed by Elixir 1.19, seed 288806, in 373.2 seconds.
Log: `cli-unit-failed.log`. The stalled-write test expected `write_idle_timeout`
but received `econnrefused`. The file-range transport test received a normal
body-stream DOWN but no `:source_closed` notification. The earlier local pass
does not supersede these failed remote results.

[Umbrella run 38865375826](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38865375826)
also failed the source-cleanup test: **3,244 CLI tests, one failure**, seed
734335, in 363.4 seconds. Core printed 343 tests, zero failures, one exclusion;
cluster printed 38 tests, zero failures, one exclusion; S3 224 and web 21 tests
had zero failures. Log: `umbrella-failed.log`. The compile/format, E2E, cluster
and image workflows passed at the same source.

The stalled-write fixture failure was independently reproduced with eight busy
workers on one scheduler. Default CRC64NVME calculation over its 16 MiB body
can exhaust the fixture's five-second accept window before connection setup.
Selecting SHA256 in that test avoids the unrelated preparation cost; the same
contention then passes. The real 16 MiB socket stall, 80 ms write-idle setting,
typed failure and ownership cleanup assertions remain intact. The entire
deadline file passed **12 tests, zero failures**, seed 288806, in 68.2 seconds,
including the real 65-second regression. Evidence:
`/tmp/ess-cli-write-idle-ci-20261016/{contention-before,contention-after,scoped-after,checksum-baseline}.log`.

The source-cleanup failure was reproduced locally on repetition four with
`mix test test/ex_storage_service_cli/transport_spike_test.exs:10
--repeat-until-failure 30 --seed 288806`; see `source-close-repeat.log`.
Published HTTP 0.21.12 acknowledges the final DATA before producer completion.
An immediate peer response can stop the retained upload and its stream, whose
termination kills the enumerable producer before its resource callback finishes.
The missing notification does not prove a leaked descriptor or corrupt bytes;
it does establish that successful response/normal stream DOWN does not confirm
enumerable finalization. Production file-source wrapping uses the same API.

This is upstream **Bug / internal request / blocker**
[http_fetch#132](https://github.com/gsmlg-dev/http_fetch/issues/132), distinct
from #131. Both production and regression callsites retain TODOs; the cleanup
assertions are unchanged. No timeout relaxation, ignored failure, dependency
patch or client workaround is applied. Stop the blocked finalization slice
until a supported published fix, then repeat the cleanup/race and complete
integration checks. M9 and publication remain open on both #131 and #132.
