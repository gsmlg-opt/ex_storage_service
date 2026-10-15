# Coordinating CLI integration validation

Source baseline: `main` at `9b8661e58107eddb24cb8f15495cee4ac979219a` plus the implementation diff. These are local pre-release checks on Linux x86_64, Elixir 1.18.5 and OTP 28. They do not establish a published next-version artifact.

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
and `mix compile --warnings-as-errors`. No production behavior changed after
this passing test snapshot.

This local result does not close the HTTP dependency/deadline gate, remote
platform/provider jobs or publication verification. The independent 2 GiB
resource evidence remains in resource-filesystem-acceptance.md; the exclusion
above does not count it as part of this command.

Staged whitespace checking preserves the byte-exact upstream reference snapshots
and licensed parser ports. The full check reports their original whitespace;
the scoped check excluding those imported files passes. The native checklist's
own trailing whitespace was removed. Reference hashes and deterministic output
checks pass, so no pinned capture was rewritten to satisfy whitespace style.
The final source-only edit adds the required TODO for http_fetch#129; it changes
no execution behavior and was followed by another passing format/strict compile.
