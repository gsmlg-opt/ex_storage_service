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

## Output, omitted payload and journal fixture acceptance

[Remote CLI run 38868473781](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38868473781)
on clean source `d9dacc621b50082f892b5513c4412725d41b4772` failed with
**3,243 tests, one failure and one resource exclusion**, seed 846572, in 376.7
seconds. TransferCommandsTest's retained-upload setup received `journal_busy`.
The failed log is `/tmp/ess-cli-d9dacc6-ci-failed.log`. All five package jobs,
including both macOS 26 architectures, and the separate 2 GiB/provider jobs
passed. A separate source-finalization race remains independently reproduced;
its absence from this one failed run does not resolve HTTP #132.

Journal mutex ports overlap Linux ephemeral ports. A private diagnostic actively
closed a loopback connection bound to the deterministic journal port, then closed
both peers/listener. The remaining `TIME_WAIT` state made the journal listener
return `eaddrinuse` and Multipart.upload fail with the same initial `journal_busy`
shape before HTTP. CI did not log its port/bind error, so the exact CI cause
remains unknown. The transfer-command fixture now tries at most four fresh roots
only for an initial, unannotated busy error, checks that no request preceded a
retry, and retains strict assertions for every other result. The deterministic
occupied-index-port regression also confirms that the unrelated listener remains
owned. Production mutex/collision policy and HTTP #132 cleanup assertions stay
intact. Journal/multipart/transfer-command suites pass **20 tests, zero failures**;
the four-test transfer-command file passes **51 consecutive runs**. Logs and the
TIME_WAIT diagnostic are under `/tmp/ess-cli-governance-members-20261016/`.

Independent AWS captures also found local text/YAML-stream page presentation
and omitted retention/legal-hold payload checksum errors. Their production
corrections pass **1,085** output/query/paginator tests and **94** optional-member/
request/upload checksum tests respectively, with zero failures, seed 628644.
See differential-output-pagination.md and differential-governance-members.md for
the exact oracle hashes, comparisons, red results and limits.

The integrated CLI working copy based on `d9dacc6` was copied into
`/tmp/ess-cli-output-governance-acceptance-20261016/apps/ex_storage_service_cli`.
All tracked and new CLI file hashes match the checked source; the root lock and
published HTTP 0.21.12 dependency graph use private build/dependency paths without
patches or overrides. On Linux x86_64, Elixir 1.18.5/OTP 28, these commands pass:

```sh
PAGER=cat MIX_ENV=test mix deps.get
PAGER=cat MIX_ENV=test mix compile --warnings-as-errors
PAGER=cat MIX_ENV=test mix format --check-formatted
PAGER=cat MIX_ENV=test mix test --exclude resource --seed 628644
```

The complete run reports **3,271 tests, zero failures and one resource exclusion**,
in 283.9 seconds (73.2 async, 210.6 sync); log `full-cli.log`. The excluded large
resource test is not counted as new acceptance. Inventory regeneration/check and
all **28 Python tooling tests** pass. The 41-row evidence update changes no
requirement IDs or implementation statuses and does not imply live-provider or
exhaustive member/output parity. This pass does not resolve either HTTP #131 or
#132, establish remote acceptance of these repairs, or publish a new release.

The integration was committed and pushed as
`5ee5306796faff270caa622c9a2452dd7676594b`; `origin/main` independently matched.
Its remote CLI job failed before compilation/tests in the existing evidence
guard. Thirty member rows had gained independent wire fixtures but retained
the old description asserting they had no independent evidence. The local
tooling suite had been run before those overlay edits, so its earlier pass did
not validate that final metadata. The scoped descriptions now identify the
captured fixtures and remaining limits. Regeneration/check and all 28 unchanged
tooling tests then pass (`/tmp/ess-cli-ci-5ee5306/tooling-corrected.log`). No
production/test assertion or implementation status changed for this repair;
the full CLI source still matches the 3,271-test local pass above. A new remote
run must validate the corrected metadata rather than counting the skipped job.


## Pushed evidence correction: standalone pass and retained upstream failure

Documentation correction `582639746e65376ea7d1f5aaeb203bca34b5d79d` was
committed, pushed and matched to `origin/main`. The CLI source and compatibility
tooling are unchanged from 5ee5306; only the member descriptions and reports
changed. [CLI run 38871534043](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38871534043)
passes all eight jobs. Its complete suite prints **3,270 tests, zero failures and
one resource exclusion**, seed 425051, in 340.4 seconds (88.9 async, 251.4 sync).
All 28 tooling tests, inventories, format and strict compilation pass. Five
package/runtime jobs, separate measured 2 GiB resources and the provider
limitation-policy job pass. Downloaded package assets/digests and the exact clean
source are independently verified in
`/tmp/ess-cli-ci-5826397/verified-packages.json`; the unit log is
`/tmp/ess-cli-ci-5ee5306/corrected-cli-unit.log`.

[Umbrella run 38871533923](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38871533923)
**fails** with **3,271 CLI tests and one failure**, seed 613727, in 382.9 seconds
(70.2 async, 312.7 sync). TransportSpikeTest's unchanged file-range upload again
receives a normal body-stream DOWN but no `:source_closed`, reproducing HTTP
#132. Core prints 343 tests/zero failures/one exclusion, cluster 38/zero/one,
S3 224/zero and web 21/zero. The failed log is
`/tmp/ess-cli-ci-5826397/umbrella-failed.log`. Separate CI/E2E/cluster workflows
pass at the same source. A passing standalone run does not establish reliable
producer finalization or close the separate #131 slow-consumer ACK contract.

Both upstream issues remain open and Hex still publishes HTTP 0.21.12 as latest
stable. No dependency patch, larger assertion wait, source-ownership workaround,
version bump, release tag or publication is used. Native differences and live AWS
acceptance remain explicitly bounded in the existing guide/reports; this result
does not declare M0–M9 complete.

## Published HTTP 0.21.13 local integration

The uncommitted integration on main based on 167cabd keeps the original upload
source-finalization assertion, selects the published patch family and adds the
request-scoped ACK infinity policy. Independent acceptance and version boundaries
are in http-021-evaluation.md. The first complete run had 3280 tests, one
MFA-negative-fixture failure and one resource exclusion. Owning stdin in that
test prevents an inherited terminal from invoking valid production prompting;
98 related tests pass.

A subsequent frozen-source run has **3344 tests, four failures and one resource
exclusion**, seed 628644, 299.9 seconds. Failures were RetryIntegrationTest's
initial request wait, JournalTest's initial ownership claim, BrowserTest's timed
launcher outcome and NativeConditionalPutTest's initial journal claim. Private
agent tests were running in the same host network namespace; their own journal
claims also encountered busy ports. Journal deliberately fails closed on an
unrelated loopback listener or port collision. This observation alone does not
establish every failure's cause. No production mutex or assertion was weakened.

With those private tests stopped, all four affected suites pass **33 tests, zero
failures** in 2.8 seconds. A complete integrated rerun remains required. Logs and
frozen source hashes are retained under
`/tmp/ess-http-02113-acceptance-20261016/` as `full-cli-current.log`,
`full-cli-current-source-sha256.json` and `isolated-failed-suites.log`.

## Final standalone integration before 0.6.8

The frozen working copy based on `167cabd` integrates published HTTP 0.21.13,
the native workflow fixes, command-specific parsing, raw SDK customizations,
STS endpoint rules and Darwin credential-process support. All other task test
VMs were stopped before the parent run. Linux x86_64 / Elixir 1.18.5 / OTP 28:

```sh
PAGER=cat ERL_FLAGS='+S 4:4' mix format
PAGER=cat ERL_FLAGS='+S 4:4' mix compile --warnings-as-errors
PAGER=cat ERL_FLAGS='+S 4:4' mix test --exclude resource --seed 628644
```

The complete CLI run passes **3,483 tests, zero failures and one resource
exclusion** in 329.5 seconds (85.0 async, 244.4 sync). Log:
`/tmp/ess-cli-final-suite-20261016.log`; 435 source/fixture/lock digests are in
`/tmp/ess-cli-final-suite-source-20261016.json` and were unchanged at completion.
The version-only bump to 0.6.8 follows this pass; committed-source CI must
validate that version. Inventory checks and all **32 Python tooling tests**
pass; strict compilation and formatting pass. This supersedes the earlier
integrated-run gate, retaining its failures above. It does not yet claim remote,
2 GiB, current-source provider, Darwin or published-artifact acceptance.

## 0.6.8 remote package, resource and umbrella acceptance

Clean source `eb3f3327519d98fbc21f0d3d06d48c4d52edc3d2` is pushed and
independently matches origin/main. [Umbrella run 38896782363](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38896782363)
passes: CLI **3,483 tests/zero failures**, core 343/zero/one exclusion, cluster
38/zero/one exclusion, S3 224/zero and web 21/zero. The CLI phase takes
308.8 seconds. Format and strict compilation CI passes. Standalone [CLI run 38896782503](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38896782503)
passes all eight jobs. Its unit/wire/executable suite passes **3,482 tests, zero
failures and one resource exclusion**, seed 399322, in 439.1 seconds (95.1 async,
344.0 sync), plus all 32 Python tooling tests. The resource test runs separately
below. The downloaded complete log is `/tmp/ess-cli-ci-eb3f332/cli-workflow.log`.

All five package jobs pass with version 0.6.8 and clean source. Independent
inspection compares all 789 source entries per package to Git, recomputes
tar/escript/native hashes and checks packaged models/notices. Both macOS 26
architectures run all ten installed credential-process tests with zero
failures/skips/exclusions. See packaging-platform-decisions.md.

The separate 2 GiB resource job passes in 151.2 seconds: 2,147,483,648 bytes,
1 MiB admission budget, peak total memory 64,322,728 versus baseline 63,515,872
(806,856-byte delta), peak binary 2,026,496 versus baseline 1,247,632, peak
mailbox two, and maximum one admitted request. Sampling is bounded-test
evidence, not a throughput or universal-memory promise.

Independent provider acceptance retains ESS ten passes/three known server
failures and RustFS thirteen passes/one known server failure. Exact pinned
images, AWS reference hash, expected error signatures and both removed buckets
per provider pass the unchanged limitation policy. This is zero detected
client regressions, not four additional provider successes. The report's CLI
artifact SHA256 is `d22c9be9cafdfd10fa6db4e69f7587cfa13b31182f1d2a9943843902fdbcc334`. Downloaded logs, provider
report and resource measurements are under `/tmp/ess-cli-ci-eb3f332/`. Live
AWS account/hardware infrastructure remains unrun and explicitly labeled.

## Subsequent 0.6.8 CI regression gate

Documentation-only source `8f2f8630fa463d92dce49da49f8746c8e8b9c9cf` has no
packaged CLI/tooling/lock changes from eb3f332. Nevertheless
[CLI run 38897870239](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38897870239)
fails with **3,482 tests, two failures and one resource exclusion**, seed 713992, in 318.1
seconds. NativePutParallelTest's source-mutation case receives an unexpected
transport error; MultipartTest's initial retained-upload setup receives an
unannotated journal_busy before the expected retention outcome. The failed
log is `/tmp/ess-cli-ci-8f2f863-failed.log`. Diagnosis and deterministic repairs
are required; the earlier green source run does not erase this failure.
The same source's umbrella run 38897869985 passes.

Publishing was stopped before either external write step. Dispatch
38897878369 used a short ref and was cancelled at checkout; corrected full-SHA
dispatch 38897960818 was cancelled during validation after the failed matrix
was found. Both Hex publish and GitHub release steps were skipped. Version
0.6.8 remains prepared but unpublished until the corrected matrix passes.

The source-mutation fixture's server observation established byte arrival, not
normal body-producer termination. A deterministic early-mutation diagnostic
reproduces transport_failure with aborted cleanup. The corrected test monitors
the actual body producers and waits for normal completion before mutating, so
it specifically exercises post-response source validation while the sibling
response remains held. Its source_changed, socket cancellation, abort and
no-completion assertions and deadlines are unchanged; 24 scoped tests pass.

The retained-upload fixture now tries at most four fresh roots only for an
initial unannotated journal_busy, with a zero-wire guard. Its occupied-listener
regression first reproduced the CI error and now verifies the original listener
remains owned, the blocked root untouched, exactly POST+PUT, retained cleanup and
no DELETE. Exact CI port occupancy remains unknown. Source/options mismatch
assertions and production mutex semantics are unchanged; 21 scoped tests pass.

The parent runs both repaired areas together with cancellation, journal,
multipart and transfer-command suites: **45 tests, zero failures**, seed 713992,
4.0 seconds (`/tmp/ess-cli-multipart-fixtures-integration-20261016.log`). These
changes affect two test files only. Complete corrected CI acceptance remains
required before resuming publication. All five source-8f2f863 packages also
pass independent source/native/model/hash inspection at
`/tmp/ess-cli-ci-8f2f863/verified-packages.json`; their success does not erase
the failed test job.

## Corrected 0.6.8 acceptance and verified publication

Source `515207e8c4063f689e499d999cf408e7f4c40ce1` passes all eight jobs in
[CLI run 38899567013](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38899567013):
**3,483 tests, zero failures, one resource exclusion**, 452.6 seconds, all 32
tooling tests, five package/runtime combinations, separate 2 GiB resources and
the unchanged provider-limitation policy. [Umbrella run 38899566818](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38899566818)
passes **3,484 CLI tests/zero failures** plus all other application suites.
This completes the corrected full-run gate without erasing the failures above.

[CLI-only publishing run 38900749007](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/38900749007)
passes another **3,484 tests/zero failures**, 437.3 seconds, and publishes
[CLI v0.6.8](https://github.com/gsmlg-opt/ex_storage_service/releases/tag/cli-v0.6.8)
from that complete source SHA. Anonymous Hex/GitHub/docs verification and a
separate fresh public consumer pass. The [release verification report](release-verification.md)
records exact hashes, the public tar's 789 Git-matching files and two independently
reproduced generated outputs, actual macOS 26 runner images, measured resources,
source/tag mapping and retained provider/live-AWS boundaries. The prepared
acceptance report shipped in release assets remains its historical snapshot.

## Unified 0.6.9 prepublication diagnostics

[Release run 39036650397](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39036650397)
on `0048eaff69aa0b55ac399af9dc7697b01ac4c8c2` passed umbrella validation,
then remained in the fresh core consumer step for 49 minutes 26 seconds in
attempt 1 and approximately 21 minutes in attempt 2. Both attempts were
cancelled before publication. The helper buffered subprocess output, so neither
run establishes the executing subcommand, dependency graph or cause. A bounded
local core consumer resolved Concord/ex_turso 3.1.1 and HTTP 0.21.13 and passed
dependency resolution, strict compilation and standalone assertions. Further
local compilation passed with Elixir 1.19.5; its Rust target was warm. These local
passes do not explain the remote delay. Commit `dbd1bc8` streams and retains the
core consumer's subprocess output without changing its commands or gates.

[Release run 39044011182](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39044011182)
on `dbd1bc895b7c1d0a81365e04aa19ba3a8b5871e1` failed before that consumer:
CLI **3,484 tests, one failure**, in the credential owner-death cleanup test.
All publication jobs were skipped. The exact test subsequently passed 21 local
isolated executions, with all 42 original fixture process identities absent or
terminated. This does not establish why CI's cleanup assertion for PID 22144
failed.

A separate deterministic startup probe paused the caller after `open_port/2`
returned but before its cleanup watcher existed. Killing the caller left the
original helper and descendant alive in their owned process group. Recorded
start identities excluded PID reuse; explicit diagnostic cleanup removed both.
The fix replaces that watcher with a port-owning worker that monitors its caller
before launch. The regression fails on the old source and passes with the fix;
all eleven scoped process tests pass, including the existing 256-invocation
timeout case, and the startup regression passes ten additional warm-VM runs.
This proves the startup defect independently of the unresolved CI failure.
Installed-package gates require eleven tests and the new eighth named check,
`prelaunch-owner-monitor`; old ten-test evidence cannot satisfy the new gate.

In [CLI run 39043996433](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39043996433),
both unit compilation and resource acceptance waited on Mix build locks after
dependency compilation. Resource acceptance reached its 20-minute job limit;
the unit job was cancelled after 25 minutes to collect its log. Neither reached
test execution. The full logs identify lock-holder OS PIDs 2537 and 3475, both
BEAM processes terminated during cleanup. The resource artifact omitted the
lock message because it captured stdout only. Its command now retains stderr,
and the affected compile/consumer steps enable Mix task diagnostics. Locking,
test assertions and time limits are unchanged.

Three isolated compiler checks pass without changing the custom compiler or
shared build paths. The final check includes the parent umbrella descriptors
and configuration, Elixir 1.19.6, four schedulers and a fresh build directory;
strict CLI compilation completes in 9.70 seconds. Local ERTS is 16.4.0.5 versus
CI 16.4.0.7, and generated grammar/rebar state was retained. The underlying
remote lock cause remains unproven. Both macOS 26 architectures and all three
Linux package jobs passed on the old source, but do not validate the new
startup regression. Fresh complete acceptance remains required before release.

The local complete CLI suite with the startup fix passes the new process checks
but records **3,485 tests, three failures**, seed 850199, in 316.1 seconds with
32 concurrent cases. The failures are an initial 250 ms loopback connection
deadline, a two-second redirect task wait, and the STS TLS fixture's credential
transport. At the CI setting of eight concurrent cases, those three unchanged
files pass **38 tests, zero failures** with the same seed. This scoped recheck
does not replace complete-suite acceptance or establish the original latency
cause; test assertions and deadlines remain unchanged.

The complete corrected CLI graph then passes **3,485 tests, zero failures** in
323.3 seconds with the same seed and eight concurrent cases. Strict compilation,
formatting, inventory consistency, all **47 compatibility-tooling tests** and
all **25 release-tooling tests** pass. The failed 32-case run remains above;
the successful complete run does not establish a fix for those latency failures.

## Current-source unified 0.6.9 prepublication acceptance

Clean source `dfb3a314ae822b51944136ba6b923c3eb7ba443e` passes all jobs in
[CLI run 39047670050](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39047670050)
and [umbrella Test run 39047670108](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39047670108),
along with the four other automated workflows. The standalone CLI suite records
**3,484 tests, zero failures, one resource exclusion**, in 317.4 seconds.
Umbrella suites pass: core **343/zero failures/one exclusion**, cluster
**38/zero/one**, CLI **3,485/zero/zero** in 440.1 seconds, S3 **224/zero** and web
**21/zero**. All **47 compatibility-tooling tests** and **25 release-tooling
tests** pass. These complete runs retain the failed runs and unresolved causes
documented above.

All five downloaded package artifacts independently pass source, Hex payload,
escript, native and bundled-model verification. Each package matches **789 Git
source files**, with matching source/probe identities and clean-state manifests.
All **68 recorded consumer commands exit zero**. Every installed credential
probe records **eleven tests, zero failures/skips/exclusions and eight named
checks**, including `prelaunch-owner-monitor`. Linux x86_64 covers Elixir
1.18.0/OTP 27, 1.18.5/OTP 28 and 1.19.6/OTP 28. Actual retained macOS runner logs
prove **26.6.2 arm64** and **26.6.1 x86_64**, both Elixir 1.19.6/OTP 28. The
digest/count/runner-label table is retained in
`/tmp/ess-unified-069-ci/ci-dfb3a31/audit-summary.md`, with full hashes and
probe records in `verified-packages.json` beside it. These pre-bump installed
executables report **0.6.8**, not 0.6.9.

The separate resource test passes **one test, zero failures** for a 2 GiB
transfer with a 1 MiB admission budget: peak total memory **64,440,344 bytes**,
peak binary memory **1,960,600 bytes**, peak mailbox **two**, and admitted-byte
high-water mark **131,072 bytes**. These are bounded-test measurements.
The unchanged provider policy accepts **ESS ten passes/three known failures**
and **RustFS thirteen passes/one known failure**; not all provider contracts
passed. Live AWS and platform-specific pager limitations remain unchanged.

At this evidence checkpoint, root [release run 39048384577](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39048384577)
is still running from `dfb3a31`. This prepublication acceptance does not establish
release success, public artifact availability or 0.6.9 runtime proof.

## Unified 0.6.9 publication and public verification

Root [release run 39048384577](https://github.com/gsmlg-opt/ex_storage_service/actions/runs/39048384577)
completes all four jobs. Its pre-bump source is `dfb3a314ae822b51944136ba6b923c3eb7ba443e`; canonical version
commit/tag is `5efddeac829cec1358a8c206a44b0ed401a27ba8`, changing only six version literals to 0.6.9.
Root validation passes **47 compatibility-tooling**, **25 release-tooling** and
all umbrella suites: core **343/zero failures/one exclusion**, cluster **38/zero/one**,
CLI **3,485/zero/zero** in 462.6 seconds, S3 **224/zero**, web **21/zero**. The CLI
publisher independently passes **3,485/zero** in 351.8 seconds; core passes
**343/zero/one**. Fresh candidate and public Hex consumers pass. Both CLI package
manifests record eleven credential tests, zero failures/skips/exclusions and
eight named checks. Earlier cancelled/failed runs and unknown causes remain above.

Independent anonymous checks match all 21 public assets, all 20 checksummed
payloads and both public Hex tars to fresh Actions candidate hashes and canonical
source. Actual downloaded `ess 0.6.9` runs on Linux Elixir 1.18.5/OTP 28 without
build/install commands and passes eleven credential tests, modeled help/skeleton,
Unicode HTTP, 65,792-byte raw stdout, pager PTY and extraction cleanup. Runtime
probe scripts bind to canonical `5efddeac829cec1358a8c206a44b0ed401a27ba8`. Independent Linux amd64 image
verification checks all compressed layers/DiffIDs, isolated health/readiness/login,
four app versions/build metadata, cleanup, and identical version/latest digests.
Both stable Hex indexes, exact versioned docs and GitHub latest release agree.
The initial docs probe rejected HexDocs' canonical per-package redirect; the
corrected exact-host/path/title check passes, with both attempts retained.

[Release verification](release-verification.md) records public digests and source
mapping. Pre-bump Mac evidence remains version 0.6.8; live AWS, platform pager,
known provider failures and other stated compatibility limits remain unchanged.
